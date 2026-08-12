/**
 * Retained managed-filesystem authority for the chat-daemon command channel.
 *
 * The channel exchanges request/response/settlement documents inside the
 * daemon's own directory, and those files are explicitly untrusted: any process
 * that can write the parent may rename the directory aside and leave a symlink,
 * a replacement directory, or a special file at the pathname.
 *
 * A `lstat` precheck followed by an ordinary pathname `open`/`link`/`rename`/
 * `unlink` cannot defend against that. The kernel re-resolves the pathname on
 * the second call, so the check and the operation can observe different objects
 * — and a claim can be redirected outside the managed root entirely. That is a
 * time-of-check/time-of-use hole, not retained authority, no matter how many
 * times the pathname is re-verified.
 *
 * This module therefore holds a *descriptor*. `openRetainedCommandDir` resolves
 * the agent directory once as the trust root, walks the managed suffix
 * (`sdk/daemons/<kind>/commands`) component by component without ever following
 * a link, repairs and re-proves owner-only permissions on the opened descriptor,
 * and retains it. Every later operation — list, stat, exclusive create, read,
 * rename, link, unlink, directory flush — is a `*at` syscall relative to that
 * descriptor:
 *
 * - the pathname is never resolved again, so replacing it changes nothing;
 * - `O_NOFOLLOW` refuses a planted final-component link;
 * - `O_CREAT|O_EXCL` remains the single-winner arbitration primitive;
 * - reads require a regular, single-linked, owner-only file and re-prove the
 *   descriptor's identity after streaming it;
 * - removals may be bound to an exact `dev`/`ino` so a retention sweep can never
 *   retire a successor that took the same name.
 *
 * Hosts without descriptor-relative filesystem authority fail closed: the scope
 * is unavailable and the command channel reports itself unusable rather than
 * silently downgrading to pathname operations.
 *
 * Hosts that cannot provide an atomic no-replace rename fail closed the same
 * way: the retirement protocol needs it to put a captured object back without
 * ever overwriting a third one, and there is no safe downgrade.
 *
 * Every guarantee below is claimed for local filesystems on a single host only.
 * A network mount (NFS, SMB) can accept the same `flock`, directory `fsync`, and
 * rename calls while providing weaker semantics, and that is not detectable from
 * here — so nothing here verifies, or claims, correctness on such a mount.
 */

import * as crypto from "node:crypto";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { openRetainedCommandDir, type RetainedCommandDir } from "@gajae-code/natives";
import { type ChatDaemonKind, chatDaemonPaths } from "./chat-daemon-control";

/** Command documents are identifiers only; anything larger is not ours. */
const MAX_COMMAND_ENTRY_BYTES = 8 * 1024;
const MAX_ENTRY_NAME_LENGTH = 128;
const OWNER_ONLY_DIRECTORY_MODE = 0o700;
const OWNER_ONLY_FILE_MODE = 0o600;

/** Hosts on which descriptor-relative directory authority is available. */
const RETAINED_AUTHORITY_PLATFORMS: ReadonlySet<NodeJS.Platform> = new Set([
	"darwin",
	"linux",
	"freebsd",
	"openbsd",
	"netbsd",
	"sunos",
	"aix",
]);

/** Exact identity of one entry, as proven without dereferencing it. */
export interface ScopedEntryIdentity {
	dev: string;
	ino: string;
	mtimeMs: number;
}

/**
 * A retained command directory.
 *
 * The descriptor is the authority; `directory` is diagnostic only and is never
 * re-resolved to perform work.
 */
export interface ChatDaemonCommandScope {
	readonly directory: string;
	readonly authority: RetainedCommandDir;
}

export interface OpenChatDaemonCommandScopeInput {
	agentDir: string;
	kind: ChatDaemonKind;
	/** Creates the daemon and command directories owner-only when absent. */
	create?: boolean;
	/** Overridable only so the fail-closed host contract is testable. */
	platform?: NodeJS.Platform;
}

/** Entry names are built from validated request ids; reject anything that could traverse. */
function isSafeEntryName(name: string): boolean {
	return (
		name.length > 0 &&
		name.length <= MAX_ENTRY_NAME_LENGTH &&
		!name.includes("/") &&
		!name.includes("\\") &&
		!name.includes("\0") &&
		name !== "." &&
		name !== ".."
	);
}

function assertSafeEntryName(name: string): string {
	if (!isSafeEntryName(name)) throw new Error("chat daemon command entry name is not addressable");
	return name;
}

/**
 * Capture the command directory as a retained descriptor, or fail closed.
 *
 * The agent directory is the caller's own trust root and is the only pathname
 * resolved here. Everything below it is attacker-reachable and is walked without
 * following a link, so a swapped ancestor is refused rather than retained.
 */
export async function openChatDaemonCommandScope(
	input: OpenChatDaemonCommandScopeInput,
): Promise<ChatDaemonCommandScope | undefined> {
	const platform = input.platform ?? process.platform;
	if (!RETAINED_AUTHORITY_PLATFORMS.has(platform)) return undefined;
	const daemonDirectory = chatDaemonPaths(input.agentDir, input.kind).dir;
	const directory = path.join(daemonDirectory, "commands");
	const relative = path.relative(input.agentDir, directory).split(path.sep).join("/");
	if (relative.length === 0 || relative.startsWith("..") || path.isAbsolute(relative)) return undefined;
	try {
		// The agent directory is this process's own trust root, so it is the one
		// pathname that may be materialised conventionally. Everything below it is
		// created through the retained descriptor walk instead.
		if (input.create === true) await fs.mkdir(input.agentDir, { recursive: true, mode: 0o700 });
		const authority = openRetainedCommandDir(
			input.agentDir,
			relative,
			input.create === true,
			OWNER_ONLY_DIRECTORY_MODE,
		);
		const identity = authority.identity();
		if (!identity.ok || identity.identity?.kind !== "directory") {
			authority.close();
			return undefined;
		}
		return { directory, authority };
	} catch {
		return undefined;
	}
}

/** Release the retained descriptor. Nothing may use the scope afterwards. */
export function closeChatDaemonCommandScope(scope: ChatDaemonCommandScope | undefined): void {
	try {
		scope?.authority.close();
	} catch {
		/* a descriptor that cannot be released is already unusable */
	}
}

export async function listScopedEntries(scope: ChatDaemonCommandScope): Promise<string[]> {
	try {
		const listed = scope.authority.list();
		return listed.ok ? (listed.names ?? []) : [];
	} catch {
		return [];
	}
}

/**
 * Read one owner-only regular file together with the exact identity it was read
 * from, without following a link.
 *
 * A symlink, directory, FIFO, device, hard-linked, group-readable, or oversized
 * entry is not a command document and never becomes one: it is reported as
 * absent rather than opened. Binding the returned identity to a later removal is
 * what lets a retention sweep retire exactly the object it decided about.
 */
export async function readScopedDocument(
	scope: ChatDaemonCommandScope,
	name: string,
): Promise<{ value: unknown; identity: ScopedEntryIdentity } | undefined> {
	try {
		const read = scope.authority.readEntry(assertSafeEntryName(name));
		if (!read.ok || !read.data || !read.identity) return undefined;
		if (read.data.byteLength > MAX_COMMAND_ENTRY_BYTES) return undefined;
		return {
			value: JSON.parse(new TextDecoder().decode(read.data)),
			identity: { dev: read.identity.dev, ino: read.identity.ino, mtimeMs: read.identity.mtimeMs },
		};
	} catch {
		return undefined;
	}
}

export async function readScopedJson(scope: ChatDaemonCommandScope, name: string): Promise<unknown> {
	return (await readScopedDocument(scope, name))?.value;
}

/** Exact identity of one entry, used to bind a later removal to this object. */
export async function scopedEntryIdentity(
	scope: ChatDaemonCommandScope,
	name: string,
): Promise<ScopedEntryIdentity | undefined> {
	try {
		const stat = scope.authority.statEntry(assertSafeEntryName(name));
		if (!stat.ok || !stat.identity) return undefined;
		return { dev: stat.identity.dev, ino: stat.identity.ino, mtimeMs: stat.identity.mtimeMs };
	} catch {
		return undefined;
	}
}

/** Age of an entry by its own (non-dereferenced) mtime, used only for retention sweeps. */
export async function scopedEntryAgeMs(
	scope: ChatDaemonCommandScope,
	name: string,
	now: number,
): Promise<number | undefined> {
	const identity = await scopedEntryIdentity(scope, name);
	return identity === undefined ? undefined : now - identity.mtimeMs;
}

export async function scopedEntryExists(scope: ChatDaemonCommandScope, name: string): Promise<boolean> {
	try {
		const stat = scope.authority.statEntry(assertSafeEntryName(name));
		if (stat.ok) return true;
		return stat.code !== "not_found";
	} catch {
		// An entry that cannot be classified is treated as present so nothing
		// downstream proceeds on an unproven path.
		return true;
	}
}

/**
 * Whether a namespace change that already applied can also be proven to survive
 * a crash.
 *
 * `durability_unknown` is not a failure of the change — it is already visible —
 * and it is not a success either. Callers whose protocol depends on a
 * publication surviving must treat it as indeterminate rather than definitive.
 */
export type ScopedDurability = "durable" | "durability_unknown";

function directoryBarrier(scope: ChatDaemonCommandScope): ScopedDurability {
	try {
		return scope.authority.syncDir().ok ? "durable" : "durability_unknown";
	} catch {
		return "durability_unknown";
	}
}

/** Outcome of publishing an entry that may already exist. */
export type ScopedPublication = "published" | "exists" | "durability_unknown";

/** Outcome of replacing an entry that the caller already owns. */
export type ScopedReplacement = "written" | "durability_unknown";

/**
 * Outcome of a removal.
 *
 * `identity_mismatch` is a safe deferral, never a failure: the name no longer
 * describes the object the caller decided about, so the decision does not apply
 * and the successor is left exactly where it is.
 */
export type ScopedRemoval = "removed" | "absent" | "identity_mismatch" | "unavailable";

/**
 * Create an entry exclusively.
 *
 * `O_CREAT|O_EXCL` is the arbitration primitive of this channel: exactly one of
 * the submitter and the serving daemon can create a given response object, and
 * the loser learns it definitively through `exists`. `O_NOFOLLOW` also refuses a
 * planted symlink, and because the creation is relative to the retained
 * descriptor it can never land outside the managed directory.
 */
export async function claimScopedEntry(scope: ChatDaemonCommandScope, name: string): Promise<boolean> {
	const created = scope.authority.createExclusive(assertSafeEntryName(name), undefined, OWNER_ONLY_FILE_MODE);
	if (created.ok) return true;
	if (created.code === "exists" || created.code === "symlink_refused") return false;
	throw new Error(`chat daemon command entry could not be claimed (${created.code ?? "unknown"})`);
}

function temporaryName(name: string): string {
	const suffix = `.${process.pid}.${crypto.randomUUID()}.tmp`;
	const room = MAX_ENTRY_NAME_LENGTH - suffix.length;
	return `${name.slice(0, Math.max(room, 1))}${suffix}`;
}

function writeTemporary(scope: ChatDaemonCommandScope, name: string, value: unknown): string {
	const temporary = assertSafeEntryName(temporaryName(name));
	const bytes = new TextEncoder().encode(`${JSON.stringify(value)}\n`);
	if (bytes.byteLength > MAX_COMMAND_ENTRY_BYTES) throw new Error("chat daemon command document is not bounded");
	const created = scope.authority.createExclusive(temporary, bytes, OWNER_ONLY_FILE_MODE);
	if (!created.ok) throw new Error(`chat daemon command document could not be staged (${created.code ?? "unknown"})`);
	return temporary;
}

/**
 * Replace an entry atomically. The document is complete before it is ever
 * visible.
 *
 * A directory barrier that cannot be proven is reported, not discarded: the
 * replacement is applied, but nothing here proves it survives a crash.
 */
export async function writeScopedJson(
	scope: ChatDaemonCommandScope,
	name: string,
	value: unknown,
): Promise<ScopedReplacement> {
	const target = assertSafeEntryName(name);
	const temporary = writeTemporary(scope, target, value);
	const renamed = scope.authority.renameEntry(temporary, target);
	if (!renamed.ok) {
		scope.authority.unlinkEntry(temporary, undefined, undefined);
		throw new Error(`chat daemon command document could not be published (${renamed.code ?? "unknown"})`);
	}
	return directoryBarrier(scope) === "durable" ? "written" : "durability_unknown";
}

/**
 * Publish an entry that must not already exist.
 *
 * `link()` from a fully written temporary makes the document atomically visible
 * under its final name and fails `exists` when the name is taken, so a replayed
 * or planted identifier can never be silently overwritten.
 *
 * A taken name is a definitive loss independent of durability. A published name
 * whose directory barrier fails is applied but unproven, and is reported as
 * such.
 */
export async function publishScopedJsonExclusive(
	scope: ChatDaemonCommandScope,
	name: string,
	value: unknown,
): Promise<ScopedPublication> {
	const target = assertSafeEntryName(name);
	const temporary = writeTemporary(scope, target, value);
	try {
		const linked = scope.authority.linkEntry(temporary, target);
		if (linked.ok) return directoryBarrier(scope) === "durable" ? "published" : "durability_unknown";
		if (linked.code === "exists") return "exists";
		throw new Error(`chat daemon command document could not be published (${linked.code ?? "unknown"})`);
	} finally {
		scope.authority.unlinkEntry(temporary, undefined, undefined);
	}
}

/**
 * Remove one entry.
 *
 * With an `identity`, the removal is bound to that exact object. The native
 * protocol serializes every create, link, rename, and removal in the directory
 * against each other and then retires the object through a private, single-use
 * name, so no concurrent process can install or retire the name while this
 * decision is in flight and no successor can be deleted on its behalf.
 *
 * A removal whose directory barrier cannot be proven is still a removal; an
 * entry resurrected by a crash is simply swept again.
 */
export async function unlinkScopedEntry(
	scope: ChatDaemonCommandScope,
	name: string,
	identity?: ScopedEntryIdentity,
): Promise<ScopedRemoval> {
	try {
		const removed = scope.authority.unlinkEntry(assertSafeEntryName(name), identity?.dev, identity?.ino);
		if (removed.ok) {
			directoryBarrier(scope);
			return "removed";
		}
		if (removed.code === "not_found") return "absent";
		if (removed.code === "identity_mismatch" || removed.code === "identity_unrestored") return "identity_mismatch";
		return "unavailable";
	} catch {
		/* an entry that cannot be classified is never removed on a guess */
		return "unavailable";
	}
}
