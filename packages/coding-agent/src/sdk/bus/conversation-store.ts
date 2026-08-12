import { randomUUID } from "node:crypto";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { isProcessIncarnation, processIncarnation } from "../broker/process-incarnation";

export const CONVERSATION_STORE_VERSION = 1;
export const MAX_DEDUPE_IDS = 128;

export interface ConversationRecord {
	generation: number;
}

export interface ConversationStoreDocument<T extends ConversationRecord> {
	version: typeof CONVERSATION_STORE_VERSION;
	conversations: Record<string, T>;
	/**
	 * The in-flight state machine of exactly one mapping replacement.
	 *
	 * Its presence means the document in the namespace is mid-transaction. Which
	 * mapping it describes depends on the marker's `phase`, and every reader
	 * resolves it the same way, so the namespace never carries an ambiguous
	 * document. See {@link ConversationStorePendingPhase}.
	 */
	pending?: ConversationStorePending<T>;
}

/**
 * How far one mapping replacement got through its authority fence.
 *
 * These are the durable states of one transaction. The only thing separating a
 * replacement that is merely *published* from one that is *committed* is
 * whether its closing authority proof had already completed when the document
 * was written:
 *
 * - `staged`: the replacement is published but no authority proof has decided
 *   it. The mapping does not exist: every reader resolves the key back to
 *   `previous`, and recovery republishes that.
 * - `activating`: authority was proven live *before* this publication, and the
 *   closing proof has not run yet. Nothing durable attests that the activation
 *   was ever allowed to complete, so this is an *unconfirmed* activation and it
 *   resolves exactly like `staged`. A writer that dies here leaves behind no
 *   evidence a later process could use to tell an allowed activation from a
 *   forbidden one, so recovery restores what the replacement displaced rather
 *   than inferring the proof that never ran.
 * - `confirmed`: the closing proof returned live and this publication carries
 *   the resulting {@link ConversationActivationProof}. This is the
 *   linearization point: it is the first document in which the replacement is
 *   the mapping, and it is the only one that survives a crash as such. The
 *   writer drops the marker afterwards purely to retire the state machine; that
 *   publication carries exactly the same mapping and changes nothing a reader
 *   can observe.
 */
export type ConversationStorePendingPhase = "staged" | "activating" | "confirmed";

const PENDING_PHASES: readonly ConversationStorePendingPhase[] = ["staged", "activating", "confirmed"];

/**
 * Durable evidence that one exact replacement completed its closing authority
 * proof.
 *
 * It is written *after* that proof returned live and never before, so its
 * presence is the fact recovery needs: a marker without it proved nothing and
 * is rolled back. `generation` binds the evidence to one exact replacement, so
 * a confirmation can never be carried over to a different mapping, and `owner`
 * binds it to the writer that took the proof.
 */
export interface ConversationActivationProof {
	/** Generation of the replacement whose closing proof completed. */
	generation: number;
	/** Clock reading taken the instant the closing proof returned live. */
	at: number;
	/** The writer that completed the closing proof. */
	owner: { pid: number; incarnation: string };
}

export interface ConversationStorePending<T extends ConversationRecord> {
	key: string;
	phase: ConversationStorePendingPhase;
	owner: { pid: number; incarnation: string };
	at: number;
	/** What the key held before the replacement; absent means it held nothing. */
	previous?: T;
	/** Present exactly on `confirmed`, absent on every other phase. */
	proof?: ConversationActivationProof;
}

export interface ConversationStoreFileHandle {
	sync(): Promise<void>;
	close(): Promise<void>;
	writeFile(data: string, encoding: "utf8"): Promise<void>;
}

/** Minimal persistence seam; callers can inject it to make durability failures deterministic. */
export interface ConversationStoreFs {
	mkdir(directory: string, options: { recursive: true; mode: number }): Promise<unknown>;
	chmod(target: string, mode: number): Promise<void>;
	readFile(file: string, encoding: "utf8"): Promise<string>;
	writeFile(file: string, data: string, options: { mode: number }): Promise<void>;
	rename(from: string, to: string): Promise<void>;
	unlink(file: string): Promise<void>;
	open(file: string, flags: string): Promise<ConversationStoreFileHandle>;
	stat?(file: string): Promise<{ mtimeMs: number }>;
}

export class ConversationLockTimeoutError extends Error {
	constructor(
		readonly lockFile: string,
		readonly timeoutMs: number,
	) {
		super(`Timed out waiting ${timeoutMs}ms for conversation store lock: ${lockFile}`);
		this.name = "ConversationLockTimeoutError";
	}
}

/**
 * Typed commit certainty for a failed persistence attempt.
 *
 * `refused` means the replacement never became visible, so no mapping changed
 * and the caller may report a definitive failure. `uncertain` means the
 * replacement rename already applied and only the durability barrier after it
 * failed: the mapping is visible now and may or may not survive a crash, so the
 * caller must report an indeterminate outcome and must never compensate by
 * deleting or overwriting it.
 */
export type ConversationCommitCertainty = "refused" | "uncertain";

export abstract class ConversationCommitError extends Error {
	abstract readonly certainty: ConversationCommitCertainty;

	constructor(
		readonly file: string,
		message: string,
		readonly reason: unknown,
	) {
		super(message);
	}
}

/** The document was never replaced; nothing changed. */
export class ConversationCommitRefusedError extends ConversationCommitError {
	readonly certainty = "refused" as const;

	constructor(file: string, reason: unknown) {
		super(file, `Conversation store commit was refused before any mapping changed: ${file}`, reason);
		this.name = "ConversationCommitRefusedError";
	}
}

/** The document was replaced, but its durability could not be proven. */
export class ConversationCommitUncertainError extends ConversationCommitError {
	readonly certainty = "uncertain" as const;

	constructor(file: string, reason: unknown) {
		super(file, `Conversation store commit applied but could not be proven durable: ${file}`, reason);
		this.name = "ConversationCommitUncertainError";
	}
}

/**
 * The operation completed, but the lock that guarded it could not be released.
 *
 * Lock-cleanup certainty is tracked separately from commit certainty on purpose:
 * whatever the operation decided is exactly what `result` carries, and a failure
 * to release a lock afterwards may never be reported as a decision that did not
 * happen.
 */
export class ConversationLockCleanupError extends Error {
	constructor(
		readonly lockFile: string,
		readonly result: unknown,
		readonly reason: unknown,
	) {
		super(`Conversation store lock could not be released after the operation completed: ${lockFile}`);
		this.name = "ConversationLockCleanupError";
	}
}

interface ConversationStoreLock {
	pid: number;
	incarnation: string;
	timestamp: number;
}

const nodeFs: ConversationStoreFs = fs;
const UNPUBLISHED_LOCK_STALE_MS = 30_000;

export function conversationStorePath(agentDir: string, kind: string, fileName = "conversations.json"): string {
	return path.join(agentDir, "sdk", "daemons", kind, fileName);
}

export function boundedDedupe(ids: readonly string[], limit = MAX_DEDUPE_IDS): string[] {
	const unique: string[] = [];
	const seen = new Set<string>();
	for (const id of ids) {
		if (!id || seen.has(id)) continue;
		seen.add(id);
		unique.push(id);
	}
	return unique.length <= limit ? unique : unique.slice(unique.length - limit);
}

function emptyDocument<T extends ConversationRecord>(): ConversationStoreDocument<T> {
	return { version: CONVERSATION_STORE_VERSION, conversations: {} };
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Durable, per-transport mapping store. Every mutation takes an exclusive
 * lock file shared across processes and reloads the on-disk document before
 * applying its monotonic-generation compare-and-swap, preventing either stale
 * writers or unrelated-key updates from replacing newer mappings.
 *
 * A read takes the same lock only when the document carries an in-flight marker,
 * so the common path stays lock-free while no observer can ever look inside a
 * writer's authority fence.
 *
 * Durability is claimed for local filesystems on a single host only. The
 * exclusive `O_CREAT|O_EXCL` lock file and the parent-directory `fsync` barrier
 * can both be accepted by a network mount (NFS, SMB) while providing weaker
 * semantics, and that is not detectable from here.
 */
export class ConversationStore<T extends ConversationRecord> {
	readonly #directory: string;
	readonly #file: string;
	readonly #fs: ConversationStoreFs;
	readonly #clock: () => number;
	readonly #locks = new Map<string, Promise<void>>();
	readonly #pid: number;
	readonly #pidIncarnation: (pid: number) => string | undefined;
	readonly #pidAlive: (pid: number) => boolean;
	readonly #sleep: (ms: number) => Promise<void>;
	readonly #lockTimeoutMs: number;
	readonly #platform: NodeJS.Platform;
	/** Exact identity written into the lock file this process currently holds. */
	#lockTenure: ConversationStoreLock | undefined;

	constructor(input: {
		agentDir: string;
		kind: string;
		fileName?: string;
		fs?: ConversationStoreFs;
		now?: () => number;
		pid?: number;
		pidIncarnation?: (pid: number) => string | undefined;
		pidAlive?: (pid: number) => boolean;
		sleep?: (ms: number) => Promise<void>;
		lockTimeoutMs?: number;
		platform?: NodeJS.Platform;
	}) {
		this.#file = conversationStorePath(input.agentDir, input.kind, input.fileName);
		this.#directory = path.dirname(this.#file);
		this.#fs = input.fs ?? nodeFs;
		this.#clock = input.now ?? Date.now;
		this.#pid = input.pid ?? process.pid;
		this.#pidIncarnation = input.pidIncarnation ?? processIncarnation;
		this.#pidAlive = input.pidAlive ?? defaultPidAlive;
		this.#sleep = input.sleep ?? (async ms => await Bun.sleep(ms));
		this.#lockTimeoutMs = input.lockTimeoutMs ?? 1_000;
		this.#platform = input.platform ?? process.platform;
	}

	get filePath(): string {
		return this.#file;
	}

	/**
	 * Read the last *decided* document.
	 *
	 * A document with no marker is already decided, so it is returned straight
	 * away: the common path takes no lock at all.
	 *
	 * A marker means some writer is inside its authority fence right now, or died
	 * inside it. Neither state may be interpreted from outside the fence, so this
	 * read takes the same mutation lock the writer holds. It therefore cannot
	 * observe the interval between any fenced publication and the proof that
	 * decides it: by the time the lock is granted, the transaction has decided,
	 * or its writer is gone and this call resolves the marker itself.
	 *
	 * The resolution is durable but its republication is best-effort, and safely
	 * so: the rule depends only on the document already in the namespace, so a
	 * reader that cannot rewrite it returns exactly what the next writer will. An
	 * unconfirmed activation resolves to what it displaced whether or not that
	 * rollback is ever republished.
	 */
	async load(): Promise<ConversationStoreDocument<T>> {
		const document = await this.#readDocument();
		if (!document.pending) return document;
		try {
			return await this.#withLock(async () => {
				const observed = await this.#readDocument();
				if (!observed.pending) return observed;
				const resolved = resolvePending(observed);
				await this.#replaceDocument(resolved).catch(() => undefined);
				return resolved;
			});
		} catch (error) {
			// A read is not a decision, so a lock whose release failed does not
			// change what was read.
			if (error instanceof ConversationLockCleanupError) return error.result as ConversationStoreDocument<T>;
			throw error;
		}
	}

	async read(key: string): Promise<T | undefined> {
		return (await this.load()).conversations[key];
	}

	/**
	 * Atomically write one mapping when its observed generation still matches.
	 * `undefined` only creates an absent mapping; successful writes must advance
	 * the generation by exactly one.
	 */
	async write(key: string, expectedGeneration: number | undefined, record: T): Promise<boolean> {
		return this.#withLock(async () => {
			const document = await this.#recoverPending(await this.#readDocument());
			const current = document.conversations[key];
			if (current?.generation !== expectedGeneration) return false;
			if (!current && expectedGeneration !== undefined) return false;
			const nextGeneration = (expectedGeneration ?? 0) + 1;
			if (record.generation !== nextGeneration) {
				throw new Error(`Conversation generation must advance to ${nextGeneration}`);
			}
			document.conversations[key] = record;
			await this.#persist(document);
			return true;
		});
	}
	async delete(key: string, expectedGeneration: number): Promise<boolean> {
		return this.#withLock(async () => {
			const document = await this.#recoverPending(await this.#readDocument());
			const current = document.conversations[key];
			if (!current || current.generation !== expectedGeneration) return false;
			delete document.conversations[key];
			await this.#persist(document);
			return true;
		});
	}

	/** Apply a synchronous update under the mapping lock, retrying no stale state. */
	async transact(key: string, update: (current: T | undefined) => T | undefined): Promise<T | undefined> {
		return await this.transactWithSnapshot(key, current => update(current));
	}

	/**
	 * Apply one key update while atomically observing every mapping in the same
	 * store snapshot. The observation and write share the cross-process file lock,
	 * allowing callers to enforce uniqueness constraints across different keys.
	 *
	 * The update may be asynchronous so a caller can re-prove local authority
	 * (process, endpoint, or session state) inside the lock, immediately before
	 * its commit. It must never perform a remote/network call while holding the
	 * lock: the lock is a short fence, not a place to wait on a provider.
	 *
	 * `finalize` is the commit decision, and the instant it observes authority is
	 * this store's logical linearization point.
	 *
	 * It runs *after* the replacement document is already published, not before:
	 * a proof taken before an asynchronous `rename` cannot cover the rename
	 * itself, so authority could always roll in between and still be reported as
	 * a success. Publishing first and proving afterwards inverts that. The
	 * published document carries a `pending` marker, so it is invisible to every
	 * reader until the decision resolves:
	 *
	 * - authority that still holds at the proof commits, by republishing the same
	 *   document without the marker;
	 * - authority that has already rolled — including one that rolled during the
	 *   publication itself — is rolled back to the previous mapping, and the
	 *   caller sees a definitive, mutation-free refusal;
	 * - a rollback that cannot be proven complete raises
	 *   `ConversationCommitUncertainError`, never a refusal.
	 *
	 * Authority that rolls *after* the proof is a subsequent lifecycle event and
	 * is handled by ordinary generation fencing, not by this transaction.
	 */
	async transactWithSnapshot(
		key: string,
		update: (
			current: T | undefined,
			conversations: Readonly<Record<string, T>>,
		) => T | undefined | Promise<T | undefined>,
		options: { finalize?: () => Promise<boolean> } = {},
	): Promise<T | undefined> {
		return this.#withLock(async () => {
			const document = await this.#recoverPending(await this.#readDocument());
			const current = document.conversations[key];
			const snapshot: Readonly<Record<string, T>> = Object.freeze({ ...document.conversations });
			const next = await update(current, snapshot);
			if (!next || next === current) return current;
			const expectedGeneration = current?.generation;
			const expectedNext = (expectedGeneration ?? 0) + 1;
			if (next.generation !== expectedNext) {
				throw new Error(`Conversation generation must advance to ${expectedNext}`);
			}
			document.conversations[key] = next;
			const provisional = options.finalize ? { key, previous: current, finalize: options.finalize } : undefined;
			return (await this.#persist(document, provisional)) === "committed" ? next : current;
		});
	}

	/**
	 * Serialize one operation behind the in-process queue and the cross-process
	 * lock file, then release both.
	 *
	 * A release failure never rewrites the operation's own outcome. A refused
	 * commit stays refused, and a completed one is reported through
	 * `ConversationLockCleanupError` carrying exactly what it returned, so the
	 * caller can separate "the decision did not happen" from "the decision
	 * happened and its lock could not be cleaned up".
	 */
	async #withLock<R>(operation: () => Promise<R>): Promise<R> {
		const previous = this.#locks.get(this.#file) ?? Promise.resolve();
		const gate = Promise.withResolvers<void>();
		const tail = previous.then(() => gate.promise);
		this.#locks.set(this.#file, tail);
		await previous;
		const leave = (): void => {
			gate.resolve();
			if (this.#locks.get(this.#file) === tail) this.#locks.delete(this.#file);
		};
		let fileLock: ConversationStoreFileHandle;
		try {
			fileLock = await this.#acquireFileLock();
		} catch (error) {
			leave();
			throw error;
		}
		let outcome: { ok: true; value: R } | { ok: false; reason: unknown };
		try {
			outcome = { ok: true, value: await operation() };
		} catch (error) {
			outcome = { ok: false, reason: error };
		}
		const cleanupFailure = await this.#releaseFileLock(fileLock);
		leave();
		if (!outcome.ok) throw outcome.reason;
		if (cleanupFailure !== undefined)
			throw new ConversationLockCleanupError(`${this.#file}.lock`, outcome.value, cleanupFailure);
		return outcome.value;
	}

	/**
	 * Release the lock, reporting rather than throwing a failure.
	 *
	 * A descriptor that could not be closed leaves the lock in an unknown state,
	 * so its file is deliberately *not* removed: deleting a lock this process
	 * cannot prove it released would let another writer in behind it. For the
	 * same reason the file is only unlinked while it still carries this
	 * process's exact tenure — a lock that was reclaimed mid-operation belongs
	 * to somebody else and removing it would admit a third writer.
	 */
	async #releaseFileLock(fileLock: ConversationStoreFileHandle): Promise<unknown> {
		const tenure = this.#lockTenure;
		this.#lockTenure = undefined;
		try {
			await fileLock.close();
		} catch (error) {
			return error;
		}
		const held = await this.#readLockTenure();
		if (held === undefined) return undefined;
		if (!tenure || held === null || !isSameLockTenure(held, tenure))
			return new Error(`Conversation store lock is no longer held by this process: ${this.#file}.lock`);
		try {
			await this.#fs.unlink(`${this.#file}.lock`);
		} catch (error) {
			if (!isMissing(error)) return error;
		}
		return undefined;
	}

	/**
	 * Read the tenure currently recorded in the lock file, or `undefined` when
	 * the file is gone. An unreadable or malformed lock file is reported as a
	 * tenure nobody holds so callers fail closed rather than assume ownership.
	 */
	async #readLockTenure(): Promise<ConversationStoreLock | null | undefined> {
		let parsed: unknown;
		try {
			parsed = JSON.parse(await this.#fs.readFile(`${this.#file}.lock`, "utf8"));
		} catch (error) {
			if (isMissing(error)) return undefined;
			return null;
		}
		return isConversationStoreLock(parsed) ? parsed : null;
	}

	/**
	 * Prove this process still holds the exclusive lock that fences the whole
	 * transaction.
	 *
	 * The lock is the fence's exclusion mechanism, and its lifetime is proven
	 * rather than assumed: a stale-lock reclaim can hand the namespace to another
	 * writer while this one is mid-transaction, and every publication after that
	 * point would be racing an unrelated writer. That is not a refusal — the
	 * other writer may already have applied something — so it is reported as
	 * indeterminate and never rolled back over.
	 */
	async #assertLockTenure(): Promise<void> {
		const tenure = this.#lockTenure;
		const held = await this.#readLockTenure();
		if (tenure && held && isSameLockTenure(held, tenure)) return;
		throw new ConversationCommitUncertainError(
			this.#file,
			new Error(`Conversation store lock tenure was lost mid-transaction: ${this.#file}.lock`),
		);
	}

	async #acquireFileLock(): Promise<ConversationStoreFileHandle> {
		const lockFile = `${this.#file}.lock`;
		const deadline = Date.now() + this.#lockTimeoutMs;
		await this.#fs.mkdir(this.#directory, { recursive: true, mode: 0o700 });
		for (;;) {
			try {
				const handle = await this.#fs.open(lockFile, "wx");
				try {
					const lock: ConversationStoreLock = {
						pid: this.#pid,
						incarnation: this.#pidIncarnation(this.#pid) ?? "unavailable",
						timestamp: this.#clock(),
					};
					await handle.writeFile(`${JSON.stringify(lock)}\n`, "utf8");
					await handle.sync();
					this.#lockTenure = lock;
					return handle;
				} catch (error) {
					await handle.close().catch(() => undefined);
					await this.#fs.unlink(lockFile).catch(() => undefined);
					throw error;
				}
			} catch (error) {
				if (!isAlreadyExists(error)) throw error;
				if (await this.#reclaimStaleLock(lockFile)) continue;
				if (Date.now() >= deadline) throw new ConversationLockTimeoutError(lockFile, this.#lockTimeoutMs);
				await this.#sleep(Math.min(10, Math.max(1, deadline - Date.now())));
			}
		}
	}

	async #reclaimStaleLock(lockFile: string): Promise<boolean> {
		if (!(await this.#isStaleLock(lockFile))) return false;
		const reclaimFile = `${lockFile}.reclaim`;
		const reclaimLock = await this.#acquireReclaimLock(reclaimFile);
		if (!reclaimLock) return false;
		try {
			if (!(await this.#isStaleLock(lockFile))) return false;
			await this.#fs.unlink(lockFile).catch(() => undefined);
			return true;
		} finally {
			await reclaimLock.close().catch(() => undefined);
			await this.#fs.unlink(reclaimFile).catch(() => undefined);
		}
	}
	async #acquireReclaimLock(reclaimFile: string): Promise<ConversationStoreFileHandle | undefined> {
		try {
			return await this.#createLockFile(reclaimFile);
		} catch (error) {
			if (!isAlreadyExists(error) || !(await this.#isStaleLock(reclaimFile))) return undefined;
			await this.#fs.unlink(reclaimFile).catch(() => undefined);
			try {
				return await this.#createLockFile(reclaimFile);
			} catch (retryError) {
				if (isAlreadyExists(retryError)) return undefined;
				throw retryError;
			}
		}
	}
	async #createLockFile(lockFile: string): Promise<ConversationStoreFileHandle> {
		const handle = await this.#fs.open(lockFile, "wx");
		try {
			const lock: ConversationStoreLock = {
				pid: this.#pid,
				incarnation: this.#pidIncarnation(this.#pid) ?? "unavailable",
				timestamp: this.#clock(),
			};
			await handle.writeFile(`${JSON.stringify(lock)}\n`, "utf8");
			await handle.sync();
			return handle;
		} catch (error) {
			await handle.close().catch(() => undefined);
			await this.#fs.unlink(lockFile).catch(() => undefined);
			throw error;
		}
	}
	async #isStaleLock(lockFile: string): Promise<boolean> {
		let parsed: unknown;
		try {
			parsed = JSON.parse(await this.#fs.readFile(lockFile, "utf8"));
		} catch (error) {
			if (isMissing(error)) return true;
			return await this.#isExpiredUnpublishedLock(lockFile);
		}
		if (!isConversationStoreLock(parsed)) return await this.#isExpiredUnpublishedLock(lockFile);
		const currentIncarnation = this.#pidIncarnation(parsed.pid);
		return (
			!this.#pidAlive(parsed.pid) ||
			(parsed.incarnation !== "unavailable" &&
				(!isProcessIncarnation(parsed.incarnation) ||
					(currentIncarnation !== undefined && currentIncarnation !== parsed.incarnation)))
		);
	}
	async #isExpiredUnpublishedLock(lockFile: string): Promise<boolean> {
		const stat = this.#fs.stat ? await this.#fs.stat(lockFile).catch(() => undefined) : undefined;
		return Boolean(stat && this.#clock() - stat.mtimeMs >= UNPUBLISHED_LOCK_STALE_MS);
	}

	async #readDocument(): Promise<ConversationStoreDocument<T>> {
		try {
			const parsed: unknown = JSON.parse(await this.#fs.readFile(this.#file, "utf8"));
			if (!isRecord(parsed) || parsed.version !== CONVERSATION_STORE_VERSION || !isRecord(parsed.conversations)) {
				throw new Error("Invalid conversation store document");
			}
			// A marker that cannot be read is a marker that cannot be resolved, so
			// the document fails closed instead of surfacing an arbitrary record.
			// Confirmation evidence is held to exactly the same standard: partial
			// evidence, evidence on a phase whose closing proof never ran, and
			// evidence that does not attest to the replacement actually present all
			// fail the document closed rather than promoting an unproven mapping.
			if (parsed.pending !== undefined && !isStorePending(parsed.pending, parsed.conversations)) {
				throw new Error("Invalid conversation store document");
			}
			return {
				version: CONVERSATION_STORE_VERSION,
				conversations: parsed.conversations as Record<string, T>,
				...(parsed.pending === undefined ? {} : { pending: parsed.pending as ConversationStorePending<T> }),
			};
		} catch (error) {
			if (isMissing(error)) return emptyDocument<T>();
			throw error;
		}
	}

	/**
	 * Resolve a marker left behind by a writer that never returned.
	 *
	 * Reaching here means this call holds the lock, so the writer that published
	 * the marker is no longer inside its transaction. The resolution is exactly
	 * the one every reader applies, so recovery can neither invent a mapping nor
	 * hide one:
	 *
	 * - `staged` and `activating` carry no evidence that their replacement was
	 *   ever allowed to activate, so both restore what it displaced. An
	 *   activation whose proof completion is not durably knowable is rolled back,
	 *   never inferred;
	 * - `confirmed` carries the closing proof itself, so the replacement it
	 *   attests to is kept and only the marker is dropped.
	 *
	 * Unlike the lock-free read, a mutation may not proceed on an unpublished
	 * resolution: a restore whose durability cannot be proven surfaces as a typed
	 * commit failure, so the caller reports an indeterminate outcome instead of
	 * layering a new mapping on a rollback that may never have landed.
	 */
	async #recoverPending(document: ConversationStoreDocument<T>): Promise<ConversationStoreDocument<T>> {
		if (!document.pending) return document;
		const resolved = resolvePending(document);
		await this.#replaceDocument(resolved);
		return resolved;
	}

	/**
	 * Publish the replacement inside a two-sided authority fence.
	 *
	 * Without a `provisional` decision this is the plain, single-replacement path
	 * every other caller uses and is unchanged: `rename` is the commit point, and
	 * only the durability barrier after it can fail about an already-applied
	 * mapping.
	 *
	 * With one, no single act can be both "prove authority" and "make the
	 * replacement visible", so the visible act is bracketed by proofs instead:
	 *
	 * 1. publish the replacement `staged` — every reader still resolves the key
	 *    to `previous`, so the mapping does not exist yet;
	 * 2. prove authority (the *opening* proof);
	 * 3. publish the replacement `activating` — still invisible. This publication
	 *    records that an activation was attempted, never that it was allowed,
	 *    because the proof that decides it has not run yet;
	 * 4. prove authority again (the *closing* proof). Authority here is monotone
	 *    — a rolled endpoint generation, a replaced owner tuple, a lost
	 *    single-winner claim and a retired request never come back — so authority
	 *    that is live at (2) and live at (4) was live across the whole of (3);
	 * 5. publish the replacement `confirmed`, carrying the proof taken at (4).
	 *    This is the linearization point: the first document in which the
	 *    replacement is the mapping. It is written only *after* the closing proof
	 *    completed, so the evidence a later process recovers is evidence of a
	 *    proof that actually ran, not of one that was merely intended;
	 * 6. prove authority once more, so a roll that lands inside (5) is observed
	 *    and rolled back rather than reported as a success;
	 * 7. drop the marker. `confirmed` and this document resolve identically, so
	 *    this is pure retirement and its own failure changes nothing.
	 *
	 * A failure at (2), (4) or (6) rolls back. A rollback that is proven durable
	 * is a definitive, mutation-free refusal; one that is not is indeterminate
	 * and never a refusal.
	 *
	 * A writer that dies anywhere before (5) lands leaves an unconfirmed marker,
	 * which recovery restores to `previous` — it never infers the missing proof.
	 * A writer that dies after (5) lands leaves the closing proof itself in the
	 * namespace, and that mapping is kept. Authority that rolls during (5) and is
	 * then lost to a crash is not a stale success, because no success was ever
	 * reported: the mapping carries the endpoint generation it was proven under
	 * and any newer one fences it by ordinary generation compare-and-swap.
	 *
	 * The store lock is the fence's exclusion mechanism, so its tenure is proven
	 * — not assumed — on both sides of (5). A lock this process can no longer
	 * prove it holds means another writer may own the namespace, which is
	 * indeterminate and is never rolled back over.
	 *
	 * Every publication before (7) carries its own directory barrier. Nothing
	 * assumes that a rename which was not flushed leaves the previous document
	 * behind: POSIX does not promise that, so an unproven publication is rolled
	 * back rather than trusted.
	 */
	async #persist(
		document: ConversationStoreDocument<T>,
		provisional?: { key: string; previous: T | undefined; finalize: () => Promise<boolean> },
	): Promise<"committed" | "aborted"> {
		await this.#fs.mkdir(this.#directory, { recursive: true, mode: 0o700 });
		await this.#fs.chmod(this.#directory, 0o700);
		if (!provisional) {
			await this.#replaceDocument(document);
			return "committed";
		}
		const replacement = document.conversations[provisional.key];
		if (!replacement) throw new Error("Conversation replacement is missing from its own transaction");
		const owner = { pid: this.#pid, incarnation: this.#pidIncarnation(this.#pid) ?? "unavailable" };
		const marker = (
			phase: ConversationStorePendingPhase,
			proof?: ConversationActivationProof,
		): ConversationStoreDocument<T> => ({
			version: CONVERSATION_STORE_VERSION,
			conversations: document.conversations,
			pending: {
				key: provisional.key,
				phase,
				owner,
				at: this.#clock(),
				...(provisional.previous === undefined ? {} : { previous: provisional.previous }),
				...(proof === undefined ? {} : { proof }),
			},
		});
		await this.#publishFenced(marker("staged"), document, provisional);
		if (!(await this.#prove(document, provisional))) return "aborted";
		await this.#publishFenced(marker("activating"), document, provisional);
		if (!(await this.#prove(document, provisional))) return "aborted";
		// The closing proof has returned live. Only now does a durable record of
		// its completion exist to be written, and only that record makes the
		// replacement the mapping.
		const proof: ConversationActivationProof = {
			generation: replacement.generation,
			at: this.#clock(),
			owner,
		};
		await this.#assertLockTenure();
		await this.#publishFenced(marker("confirmed", proof), document, provisional);
		if (!(await this.#prove(document, provisional))) return "aborted";
		await this.#assertLockTenure();
		// Retiring the state machine. The confirmed document and this one resolve
		// identically, so a failure here changes nothing a reader resolves.
		await this.#replaceDocument(document).catch(() => undefined);
		return "committed";
	}

	/**
	 * Publish one fenced document, resolving anything that cannot be proven.
	 *
	 * A publication that was *refused* never applied, so the namespace still
	 * holds the previously published fenced document and no mapping changed. That
	 * stays a definitive refusal; retiring the marker afterwards is pure cleanup
	 * and may never rewrite the outcome, so its own failure is swallowed.
	 *
	 * A publication that applied without a proven barrier leaves the namespace in
	 * a state this process may not reason about — POSIX does not promise that an
	 * unflushed rename leaves the previous document behind. It is therefore
	 * rolled back rather than trusted: a rollback that is proven durable means
	 * nothing is applied, which is a definitive refusal, and one that is not is
	 * indeterminate.
	 */
	async #publishFenced(
		staged: ConversationStoreDocument<T>,
		document: ConversationStoreDocument<T>,
		provisional: { key: string; previous: T | undefined },
	): Promise<void> {
		try {
			await this.#replaceDocument(staged);
		} catch (error) {
			if (error instanceof ConversationCommitError && error.certainty === "refused") {
				await this.#rollBack(document, provisional).catch(() => undefined);
				throw error;
			}
			await this.#rollBack(document, provisional);
			throw new ConversationCommitRefusedError(this.#file, error);
		}
	}

	/** Run one side of the authority fence, rolling back a proof that refuses. */
	async #prove(
		document: ConversationStoreDocument<T>,
		provisional: { key: string; previous: T | undefined; finalize: () => Promise<boolean> },
	): Promise<boolean> {
		let live: boolean;
		try {
			live = await provisional.finalize();
		} catch (error) {
			await this.#rollBack(document, provisional);
			throw error;
		}
		if (live) return true;
		await this.#rollBack(document, provisional);
		return false;
	}

	/** Restore exactly the mapping the provisional replacement displaced. */
	async #rollBack(
		document: ConversationStoreDocument<T>,
		provisional: { key: string; previous: T | undefined },
	): Promise<void> {
		const conversations = { ...document.conversations };
		if (provisional.previous === undefined) delete conversations[provisional.key];
		else conversations[provisional.key] = provisional.previous;
		try {
			await this.#replaceDocument({ version: CONVERSATION_STORE_VERSION, conversations });
		} catch (error) {
			// The provisional replacement is in the namespace and the rollback is
			// unproven. That is indeterminate, never a definitive refusal.
			throw new ConversationCommitUncertainError(this.#file, error);
		}
	}

	/** Stage, fsync, atomically replace the document, and flush the directory. */
	async #replaceDocument(document: ConversationStoreDocument<T>): Promise<void> {
		const temporary = `${this.#file}.${process.pid}.${randomUUID()}.tmp`;
		let applied = false;
		try {
			await this.#fs.writeFile(temporary, `${JSON.stringify(document)}\n`, { mode: 0o600 });
			await this.#fs.chmod(temporary, 0o600);
			const handle = await this.#fs.open(temporary, "r");
			try {
				await handle.sync();
			} finally {
				await handle.close();
			}
			await this.#fs.rename(temporary, this.#file);
			applied = true;
			await syncParentDirectory(this.#fs, this.#directory, this.#platform);
		} catch (error) {
			if (applied) throw new ConversationCommitUncertainError(this.#file, error);
			await this.#fs.unlink(temporary).catch(() => undefined);
			throw new ConversationCommitRefusedError(this.#file, error);
		}
	}
}

function isMissing(error: unknown): error is NodeJS.ErrnoException {
	return isRecord(error) && error.code === "ENOENT";
}

/**
 * The document as every reader must see it.
 *
 * This is the single resolution rule for the fenced state machine, shared by
 * lock-free reads, recovery, and the writer itself, so no two observers can
 * disagree about what a marked document means:
 *
 * - `staged` and `activating`: no closing proof is recorded, so the replacement
 *   was never confirmed and the key reverts to what it displaced;
 * - `confirmed`: the closing proof is recorded in the document itself, so the
 *   key keeps the replacement and only the marker is dropped.
 */
function resolvePending<T extends ConversationRecord>(
	document: ConversationStoreDocument<T>,
): ConversationStoreDocument<T> {
	const pending = document.pending;
	if (!pending) return document;
	if (pending.phase === "confirmed")
		return { version: CONVERSATION_STORE_VERSION, conversations: document.conversations };
	const conversations = { ...document.conversations };
	if (pending.previous === undefined) delete conversations[pending.key];
	else conversations[pending.key] = pending.previous;
	return { version: CONVERSATION_STORE_VERSION, conversations };
}

/**
 * Whether a marker is complete enough to be resolved at all.
 *
 * Every field the resolution consumes is validated here, including the shape of
 * `previous`: a malformed previous mapping would otherwise be handed back to a
 * caller as an arbitrary `T`. A marker that fails this check fails the whole
 * document closed instead.
 *
 * Confirmation evidence is validated against the document that carries it, not
 * in isolation. `confirmed` without evidence, evidence naming a writer other
 * than the marker's own, evidence for a replacement the document does not
 * carry, and evidence on a phase whose closing proof never ran are all partial
 * states no reader may resolve, so each one fails closed.
 */
function isStorePending(value: unknown, conversations: Record<string, unknown>): boolean {
	if (
		!isRecord(value) ||
		typeof value.key !== "string" ||
		value.key.length === 0 ||
		!PENDING_PHASES.includes(value.phase as ConversationStorePendingPhase) ||
		typeof value.at !== "number" ||
		!Number.isFinite(value.at) ||
		!isStoreOwner(value.owner) ||
		!(value.previous === undefined || isStoredRecord(value.previous))
	)
		return false;
	if (value.phase !== "confirmed") return value.proof === undefined;
	return (
		isActivationProof(value.proof, value.owner) && confirmsVisibleReplacement(value.proof, conversations[value.key])
	);
}

/** The writer identity a marker and its proof must agree on. */
function isStoreOwner(value: unknown): value is { pid: number; incarnation: string } {
	return (
		isRecord(value) &&
		typeof value.pid === "number" &&
		Number.isSafeInteger(value.pid) &&
		value.pid > 0 &&
		typeof value.incarnation === "string" &&
		value.incarnation.length > 0
	);
}

/** Whether closing-proof evidence is complete and was taken by the marker's own writer. */
function isActivationProof(
	value: unknown,
	owner: { pid: number; incarnation: string },
): value is ConversationActivationProof {
	return (
		isRecord(value) &&
		typeof value.generation === "number" &&
		Number.isSafeInteger(value.generation) &&
		value.generation > 0 &&
		typeof value.at === "number" &&
		Number.isFinite(value.at) &&
		isStoreOwner(value.owner) &&
		value.owner.pid === owner.pid &&
		value.owner.incarnation === owner.incarnation
	);
}

/** Whether the evidence attests to the exact replacement the document carries. */
function confirmsVisibleReplacement(proof: ConversationActivationProof, replacement: unknown): boolean {
	return isStoredRecord(replacement) && (replacement as ConversationRecord).generation === proof.generation;
}

/** A displaced mapping is only restorable when it carries a usable generation. */
function isStoredRecord(value: unknown): boolean {
	return (
		isRecord(value) &&
		typeof value.generation === "number" &&
		Number.isSafeInteger(value.generation) &&
		value.generation >= 0
	);
}

async function syncParentDirectory(
	fs: ConversationStoreFs,
	directory: string,
	platform: NodeJS.Platform,
): Promise<void> {
	let handle: ConversationStoreFileHandle;
	try {
		handle = await fs.open(directory, "r");
	} catch (error) {
		if (platform === "win32" && isUnsupportedDirectoryBarrierError(error)) return;
		throw error;
	}
	let syncError: unknown;
	try {
		await handle.sync();
	} catch (error) {
		if (!(platform === "win32" && isUnsupportedDirectoryBarrierError(error))) syncError = error;
	}
	let closeError: unknown;
	try {
		await handle.close();
	} catch (error) {
		closeError = error;
	}
	if (syncError && closeError)
		throw new AggregateError([syncError, closeError], "Parent directory sync and close failed");
	if (syncError) throw syncError;
	if (closeError) throw closeError;
}

function isUnsupportedDirectoryBarrierError(error: unknown): boolean {
	return (
		isRecord(error) &&
		(error.code === "EINVAL" || error.code === "ENOTSUP" || error.code === "EOPNOTSUPP" || error.code === "EPERM")
	);
}
function isAlreadyExists(error: unknown): error is NodeJS.ErrnoException {
	return isRecord(error) && error.code === "EEXIST";
}

function defaultPidAlive(pid: number): boolean {
	try {
		process.kill(pid, 0);
		return true;
	} catch {
		return false;
	}
}

function isConversationStoreLock(value: unknown): value is ConversationStoreLock {
	return (
		isRecord(value) &&
		typeof value.pid === "number" &&
		Number.isSafeInteger(value.pid) &&
		value.pid > 0 &&
		typeof value.incarnation === "string" &&
		typeof value.timestamp === "number"
	);
}

/** Exact tenure identity: the same holder *and* the same acquisition. */
function isSameLockTenure(held: ConversationStoreLock, tenure: ConversationStoreLock): boolean {
	return held.pid === tenure.pid && held.incarnation === tenure.incarnation && held.timestamp === tenure.timestamp;
}
