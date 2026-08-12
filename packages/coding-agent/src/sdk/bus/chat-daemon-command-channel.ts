/**
 * Request/response control channel for chat-daemon commands that must be
 * executed *by* the running owner instead of terminating it.
 *
 * `control.json` is the lifecycle channel: every request the owner recognizes
 * there ends its serving loop (stop/reload). Adopting an existing Slack root
 * must not stop a healthy daemon, so operator commands travel on this separate
 * per-request channel and are answered in place.
 *
 * Invariants:
 * - Every request is addressed to an exact owner (`ownerId`, `pid`,
 *   `incarnation`, daemon `generation`). A daemon that is not that exact owner
 *   answers `owner_changed` and performs no work, so a replaced or restarted
 *   owner can never satisfy a request captured against its predecessor. The
 *   same tuple is re-proven inside the commit fence and echoed in the answer,
 *   so the submitter can verify who actually acted.
 * - `<id>.response.json` is a single-winner arbitration object created with
 *   `O_CREAT|O_EXCL`. The serving daemon must win it *before* it commits, and a
 *   submitter that has given up must win it before reporting failure. Whoever
 *   loses learns so definitively, which removes the commit-vs-timeout race
 *   without any sleep: a definitive `cancelled` answer proves no commit can
 *   follow, and a lost cancellation is reported as `unknown` rather than as a
 *   failure the caller could act on.
 * - Every path operation runs under retained managed-filesystem authority
 *   (`chat-daemon-command-scope`): owner-only, no symlink traversal, no
 *   non-regular entries, exact directory identity re-proven per syscall.
 * - Only identifiers travel through the channel. Tokens, message bodies, and
 *   control secrets are never written here.
 */

import * as crypto from "node:crypto";
import {
	type ChatDaemonCommandScope,
	claimScopedEntry,
	closeChatDaemonCommandScope,
	listScopedEntries,
	openChatDaemonCommandScope,
	publishScopedJsonExclusive,
	readScopedDocument,
	readScopedJson,
	type ScopedPublication,
	scopedEntryExists,
	scopedEntryIdentity,
	unlinkScopedEntry,
	writeScopedJson,
} from "./chat-daemon-command-scope";
import type { ChatDaemonKind } from "./chat-daemon-control";

export const CHAT_DAEMON_COMMAND_VERSION = 1;
export const DEFAULT_CHAT_DAEMON_COMMAND_TIMEOUT_MS = 15_000;
const DEFAULT_POLL_INTERVAL_MS = 25;
/** How long a completed response and an expired request stay readable before sweeping. */
const COMMAND_RETENTION_MS = 60_000;
const DEFAULT_REQUEST_TTL_MS = 30_000;
/** Bounded wait for a daemon that won the response claim before the submitter gave up. */
const DEFAULT_SETTLE_GRACE_MS = 5_000;
const REQUEST_SUFFIX = ".request.json";
const RESPONSE_SUFFIX = ".response.json";
const SETTLEMENT_SUFFIX = ".settled.json";
const REQUEST_ID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const MAX_COMMAND_FIELD_LENGTH = 256;

export type ChatDaemonCommandName = "bind-thread";
export type ChatDaemonCommandStatus = "ok" | "rejected" | "owner_changed" | "expired" | "outcome_unknown";

/** Exact owner authority a request is addressed to. */
export interface ChatDaemonCommandOwner {
	ownerId: string;
	pid: number;
	incarnation: string;
	generation: number;
}

export interface ChatDaemonCommandRequest extends ChatDaemonCommandOwner {
	version: typeof CHAT_DAEMON_COMMAND_VERSION;
	requestId: string;
	kind: ChatDaemonKind;
	command: ChatDaemonCommandName;
	sessionId: string;
	rootTs: string;
	createdAt: number;
	expiresAt: number;
}

/**
 * The answer to exactly one request.
 *
 * The owner tuple, session, and root are the *addressed* request's, echoed
 * verbatim. A response therefore carries the complete request envelope, and a
 * submitter can prove that the document in front of it is correlated with its
 * own request rather than with a replayed or concurrent one.
 *
 * That is correlation, not authentication. Every echoed field is public: it is
 * copied out of the plaintext request published in the same untrusted command
 * directory, so any writer of that directory can build a complete,
 * envelope-correct document — including `status:"ok"` — without the addressed
 * daemon ever running. Nothing in this channel proves authorship, and no
 * companion document in the same directory can, because the same writer forges
 * that too. A caller that needs a definitive outcome must corroborate it against
 * an authority outside this directory (the durable mutation itself).
 */
export interface ChatDaemonCommandResponse extends ChatDaemonCommandOwner {
	version: typeof CHAT_DAEMON_COMMAND_VERSION;
	requestId: string;
	kind: ChatDaemonKind;
	command: ChatDaemonCommandName;
	sessionId: string;
	rootTs: string;
	status: ChatDaemonCommandStatus;
	/** Machine-readable rejection category; never a message body or credential. */
	code?: string;
	endpointGeneration?: number;
	teamId?: string;
	channelId?: string;
	completedAt: number;
}

/**
 * Result of one handler dispatch, carrying explicit commit certainty.
 *
 * A failure is never just a code. `rejected` asserts that no mapping changed, so
 * the caller may be told the binding definitively failed. `unknown` asserts the
 * opposite: commit authority was exercised and the mapping may already be
 * applied, so no definitive rejection may be reported for it. Handlers state
 * this directly instead of leaving the channel to infer it from error text.
 */
export type ChatDaemonCommandOutcome =
	| {
			ok: true;
			sessionId: string;
			endpointGeneration: number;
			teamId: string;
			channelId: string;
			rootTs: string;
	  }
	| { ok: false; certainty: "rejected"; code: string }
	| { ok: false; certainty: "unknown"; code: string };

export interface ChatDaemonCommandBindInput {
	sessionId: string;
	rootTs: string;
	/**
	 * Terminal authority for this exact request. It must be awaited inside the
	 * store fence, immediately before the commit, and the commit must be
	 * abandoned when it answers `false`. It re-proves the exact daemon owner
	 * tuple, that the request is still published and unexpired, and takes the
	 * single-winner response claim, after which no cancellation can succeed.
	 */
	commitAuthority?: () => Promise<boolean>;
}

/** Daemon-side executor. Implemented by the runtime that owns the live transports. */
export interface ChatDaemonCommandHandler {
	bindExistingRoot(input: ChatDaemonCommandBindInput): Promise<ChatDaemonCommandOutcome>;
}

function requestEntry(requestId: string): string {
	return `${requestId}${REQUEST_SUFFIX}`;
}

function responseEntry(requestId: string): string {
	return `${requestId}${RESPONSE_SUFFIX}`;
}

function settlementEntry(requestId: string): string {
	return `${requestId}${SETTLEMENT_SUFFIX}`;
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function boundedString(value: unknown): value is string {
	return typeof value === "string" && value.length > 0 && value.length <= MAX_COMMAND_FIELD_LENGTH;
}

function positiveInteger(value: unknown): value is number {
	return typeof value === "number" && Number.isSafeInteger(value) && value > 0;
}

export function isChatDaemonCommandRequest(value: unknown): value is ChatDaemonCommandRequest {
	if (!isRecord(value)) return false;
	return (
		value.version === CHAT_DAEMON_COMMAND_VERSION &&
		typeof value.requestId === "string" &&
		REQUEST_ID_PATTERN.test(value.requestId) &&
		(value.kind === "discord" || value.kind === "slack") &&
		value.command === "bind-thread" &&
		boundedString(value.ownerId) &&
		positiveInteger(value.pid) &&
		boundedString(value.incarnation) &&
		typeof value.generation === "number" &&
		Number.isSafeInteger(value.generation) &&
		value.generation >= 0 &&
		boundedString(value.sessionId) &&
		boundedString(value.rootTs) &&
		typeof value.createdAt === "number" &&
		Number.isFinite(value.createdAt) &&
		typeof value.expiresAt === "number" &&
		Number.isFinite(value.expiresAt)
	);
}

export function isChatDaemonCommandResponse(value: unknown): value is ChatDaemonCommandResponse {
	if (!isRecord(value)) return false;
	return (
		value.version === CHAT_DAEMON_COMMAND_VERSION &&
		typeof value.requestId === "string" &&
		REQUEST_ID_PATTERN.test(value.requestId) &&
		(value.kind === "discord" || value.kind === "slack") &&
		value.command === "bind-thread" &&
		boundedString(value.ownerId) &&
		positiveInteger(value.pid) &&
		boundedString(value.incarnation) &&
		typeof value.generation === "number" &&
		Number.isSafeInteger(value.generation) &&
		(value.status === "ok" ||
			value.status === "rejected" ||
			value.status === "owner_changed" ||
			value.status === "expired" ||
			value.status === "outcome_unknown") &&
		(value.code === undefined || boundedString(value.code)) &&
		boundedString(value.sessionId) &&
		boundedString(value.rootTs) &&
		(value.endpointGeneration === undefined || positiveInteger(value.endpointGeneration)) &&
		(value.teamId === undefined || boundedString(value.teamId)) &&
		(value.channelId === undefined || boundedString(value.channelId)) &&
		typeof value.completedAt === "number" &&
		Number.isFinite(value.completedAt)
	);
}

/** Two request documents describe the same authorization only when every field agrees. */
function isSameChatDaemonCommandRequest(left: ChatDaemonCommandRequest, right: ChatDaemonCommandRequest): boolean {
	return (
		left.version === right.version &&
		left.requestId === right.requestId &&
		left.kind === right.kind &&
		left.command === right.command &&
		left.ownerId === right.ownerId &&
		left.pid === right.pid &&
		left.incarnation === right.incarnation &&
		left.generation === right.generation &&
		left.sessionId === right.sessionId &&
		left.rootTs === right.rootTs &&
		left.createdAt === right.createdAt &&
		left.expiresAt === right.expiresAt
	);
}

/**
 * A response may influence caller behaviour only when its complete envelope is
 * the request's own.
 *
 * This runs *before* any status is interpreted. A document that agrees on the
 * request id but disagrees on the addressed daemon tuple, the command, or the
 * session/root binding is stale or planted material: it is neither a success
 * nor a trusted business rejection, and it is never allowed to settle the
 * submission as an answer.
 */
function answersRequest(response: ChatDaemonCommandResponse, request: ChatDaemonCommandRequest): boolean {
	return (
		response.version === request.version &&
		response.requestId === request.requestId &&
		response.kind === request.kind &&
		response.command === request.command &&
		response.ownerId === request.ownerId &&
		response.pid === request.pid &&
		response.incarnation === request.incarnation &&
		response.generation === request.generation &&
		response.sessionId === request.sessionId &&
		response.rootTs === request.rootTs
	);
}

/**
 * Terminal arbitration state for one request identifier.
 *
 * A random identifier is not a replay protocol. Once *any* terminal authority
 * has been won for an identifier — a commit, a rejection, a cancellation, an
 * expiry, an ownership change, or an explicit unknown — that fact must outlive
 * the request and response objects, because the submitter deletes both as soon
 * as it has read its answer. Resurrecting the still-unexpired request material
 * afterwards would otherwise buy a second, unauthorized dispatch.
 *
 * The settlement record is that durable, bounded arbitration state. It is
 * created by whoever wins the single-winner response claim, it is never removed
 * by submitter cleanup, and it is retired only by an identity-bound retention
 * sweep once both the request's own expiry and the settlement's retention window
 * have passed.
 */
export type ChatDaemonSettlementOutcome =
	/** Commit authority was taken; the mutation may or may not have applied. */
	"committing" | "ok" | "rejected" | "owner_changed" | "expired" | "cancelled" | "outcome_unknown";

export interface ChatDaemonCommandSettlement extends ChatDaemonCommandOwner {
	version: typeof CHAT_DAEMON_COMMAND_VERSION;
	requestId: string;
	kind: ChatDaemonKind;
	command: ChatDaemonCommandName;
	sessionId: string;
	rootTs: string;
	createdAt: number;
	expiresAt: number;
	outcome: ChatDaemonSettlementOutcome;
	code?: string;
	endpointGeneration?: number;
	teamId?: string;
	channelId?: string;
	settledAt: number;
}

const SETTLEMENT_OUTCOMES: readonly ChatDaemonSettlementOutcome[] = [
	"committing",
	"ok",
	"rejected",
	"owner_changed",
	"expired",
	"cancelled",
	"outcome_unknown",
];

export function isChatDaemonCommandSettlement(value: unknown): value is ChatDaemonCommandSettlement {
	if (!isRecord(value)) return false;
	return (
		value.version === CHAT_DAEMON_COMMAND_VERSION &&
		typeof value.requestId === "string" &&
		REQUEST_ID_PATTERN.test(value.requestId) &&
		(value.kind === "discord" || value.kind === "slack") &&
		value.command === "bind-thread" &&
		boundedString(value.ownerId) &&
		positiveInteger(value.pid) &&
		boundedString(value.incarnation) &&
		typeof value.generation === "number" &&
		Number.isSafeInteger(value.generation) &&
		boundedString(value.sessionId) &&
		boundedString(value.rootTs) &&
		typeof value.createdAt === "number" &&
		Number.isFinite(value.createdAt) &&
		typeof value.expiresAt === "number" &&
		Number.isFinite(value.expiresAt) &&
		SETTLEMENT_OUTCOMES.includes(value.outcome as ChatDaemonSettlementOutcome) &&
		(value.code === undefined || boundedString(value.code)) &&
		(value.endpointGeneration === undefined || positiveInteger(value.endpointGeneration)) &&
		(value.teamId === undefined || boundedString(value.teamId)) &&
		(value.channelId === undefined || boundedString(value.channelId)) &&
		typeof value.settledAt === "number" &&
		Number.isFinite(value.settledAt)
	);
}

/**
 * A settlement only describes a request when the whole authorization identity
 * agrees. A reused identifier that names a different owner, session, root, or
 * command is not a retry of the settled work and must fail closed rather than be
 * suppressed by, or answered from, unrelated stale material.
 */
function settlementDescribes(settlement: ChatDaemonCommandSettlement, request: ChatDaemonCommandRequest): boolean {
	return (
		settlement.requestId === request.requestId &&
		settlement.kind === request.kind &&
		settlement.command === request.command &&
		settlement.ownerId === request.ownerId &&
		settlement.pid === request.pid &&
		settlement.incarnation === request.incarnation &&
		settlement.generation === request.generation &&
		settlement.sessionId === request.sessionId &&
		settlement.rootTs === request.rootTs
	);
}

function settlementFor(
	request: ChatDaemonCommandRequest,
	outcome: ChatDaemonSettlementOutcome,
	settledAt: number,
	extra: Partial<Pick<ChatDaemonCommandSettlement, "code" | "endpointGeneration" | "teamId" | "channelId">> = {},
): ChatDaemonCommandSettlement {
	return {
		version: CHAT_DAEMON_COMMAND_VERSION,
		requestId: request.requestId,
		kind: request.kind,
		command: request.command,
		ownerId: request.ownerId,
		pid: request.pid,
		incarnation: request.incarnation,
		generation: request.generation,
		sessionId: request.sessionId,
		rootTs: request.rootTs,
		createdAt: request.createdAt,
		expiresAt: request.expiresAt,
		outcome,
		settledAt,
		...extra,
	};
}

/**
 * Record a terminal outcome for one identifier.
 *
 * A recorded terminal outcome is final; only the in-flight `committing` marker
 * may be upgraded. A publication whose durability cannot be proven is reported
 * as such rather than as a record the protocol may rely on.
 */
type ChatDaemonSettlementRecord = "recorded" | "conflict" | "durability_unknown";

async function recordSettlement(
	scope: ChatDaemonCommandScope,
	settlement: ChatDaemonCommandSettlement,
): Promise<ChatDaemonSettlementRecord> {
	const name = settlementEntry(settlement.requestId);
	const existing = await readScopedJson(scope, name);
	if (existing === undefined) {
		if (await scopedEntryExists(scope, name)) return "conflict";
		const published = await publishScopedJsonExclusive(scope, name, settlement);
		if (published === "published") return "recorded";
		return published === "exists" ? "conflict" : "durability_unknown";
	}
	if (!isChatDaemonCommandSettlement(existing) || !sameSettlementAuthorization(existing, settlement))
		return "conflict";
	// A recorded terminal outcome is final and may never be downgraded.
	if (existing.outcome !== "committing") return "conflict";
	// The commit fence is re-entered at the linearization point, so re-recording
	// the same in-flight marker is idempotent rather than a conflict.
	if (settlement.outcome === "committing") return "recorded";
	return (await writeScopedJson(scope, name, settlement)) === "written" ? "recorded" : "durability_unknown";
}

/**
 * Replace an unproven definitive record with the indeterminate one the answer
 * now carries.
 *
 * This is the only downgrade the protocol allows, and it is safe because the
 * record it replaces was never proven durable: whatever survives a crash — the
 * `committing` marker, the definitive record, or this one — a reader that finds
 * it is told at most what the durable state supports. Its own durability is
 * equally unprovable, so the result is not consulted: the *answer* has already
 * been made indeterminate, which is what the caller acts on.
 */
async function downgradeSettlement(
	scope: ChatDaemonCommandScope,
	settlement: ChatDaemonCommandSettlement,
): Promise<void> {
	const existing = await readScopedJson(scope, settlementEntry(settlement.requestId));
	if (!isChatDaemonCommandSettlement(existing) || !sameSettlementAuthorization(existing, settlement)) return;
	if (existing.outcome === "outcome_unknown") return;
	await writeScopedJson(scope, settlementEntry(settlement.requestId), settlement).catch(() => undefined);
}

function sameSettlementAuthorization(left: ChatDaemonCommandSettlement, right: ChatDaemonCommandSettlement): boolean {
	return (
		left.requestId === right.requestId &&
		left.kind === right.kind &&
		left.command === right.command &&
		left.ownerId === right.ownerId &&
		left.pid === right.pid &&
		left.incarnation === right.incarnation &&
		left.generation === right.generation &&
		left.sessionId === right.sessionId &&
		left.rootTs === right.rootTs
	);
}

/**
 * Single-winner terminal authority for one request.
 *
 * The claim is the exclusive creation of `<id>.response.json`. Acquiring it is
 * idempotent within a serve, and losing it is permanent for that request: the
 * submitter cancelled, so nothing may be committed or published afterwards.
 *
 * A holder that then discovers it must not act releases the claim instead of
 * publishing, so an abandoned command leaves no orphan behind.
 */
class ChatDaemonResponseClaim {
	#scope: ChatDaemonCommandScope;
	#entry: string;
	#state: "unclaimed" | "held" | "lost" = "unclaimed";

	constructor(scope: ChatDaemonCommandScope, requestId: string) {
		this.#scope = scope;
		this.#entry = responseEntry(requestId);
	}

	get held(): boolean {
		return this.#state === "held";
	}

	async acquire(): Promise<boolean> {
		if (this.#state !== "unclaimed") return this.#state === "held";
		try {
			this.#state = (await claimScopedEntry(this.#scope, this.#entry)) ? "held" : "lost";
		} catch {
			this.#state = "lost";
		}
		return this.#state === "held";
	}

	async release(): Promise<void> {
		if (this.#state !== "held") return;
		this.#state = "lost";
		await unlinkScopedEntry(this.#scope, this.#entry);
	}

	async publish(response: ChatDaemonCommandResponse): Promise<"published" | "durability_unknown" | "skipped"> {
		if (!(await this.acquire())) return "skipped";
		return (await writeScopedJson(this.#scope, this.#entry, response)) === "written"
			? "published"
			: "durability_unknown";
	}
}

/** Build a request addressed to one exact owner. */
export function buildChatDaemonCommandRequest(input: {
	kind: ChatDaemonKind;
	command: ChatDaemonCommandName;
	owner: ChatDaemonCommandOwner;
	sessionId: string;
	rootTs: string;
	now?: number;
	ttlMs?: number;
	requestId?: string;
}): ChatDaemonCommandRequest {
	const createdAt = input.now ?? Date.now();
	return {
		version: CHAT_DAEMON_COMMAND_VERSION,
		requestId: input.requestId ?? crypto.randomUUID(),
		kind: input.kind,
		command: input.command,
		ownerId: input.owner.ownerId,
		pid: input.owner.pid,
		incarnation: input.owner.incarnation,
		generation: input.owner.generation,
		sessionId: input.sessionId,
		rootTs: input.rootTs,
		createdAt,
		expiresAt: createdAt + (input.ttlMs ?? DEFAULT_REQUEST_TTL_MS),
	};
}

export interface SubmitChatDaemonCommandInput {
	agentDir: string;
	kind: ChatDaemonKind;
	owner: ChatDaemonCommandOwner;
	command: ChatDaemonCommandName;
	sessionId: string;
	rootTs: string;
	timeoutMs?: number;
	pollIntervalMs?: number;
	/** Bounded wait after a lost cancellation, before the outcome is reported unknown. */
	settleGraceMs?: number;
	ttlMs?: number;
	now?: () => number;
	sleep?: (ms: number) => Promise<void>;
	requestId?: string;
}

/**
 * Terminal result of one submission.
 *
 * `cancelled` is the only definitive failure: the submitter won the response
 * claim, so the addressed owner can no longer commit. `unknown` means the daemon
 * won that claim first and its answer was not observed; the caller must not
 * report a definitive failure, because a commit may already be applied. Replay
 * of the same command is idempotent, so re-running observes the settled state.
 *
 * `untrusted` means the response object under this identifier is a complete,
 * well-formed document that does not carry this request's envelope. It is
 * channel corruption — stale or planted — so it may not be read as an answer of
 * any status, and because the identifier is now occupied the real outcome is
 * unknowable from here; the caller must treat it exactly like `unknown`.
 */
export type ChatDaemonCommandSubmission =
	| { outcome: "answered"; response: ChatDaemonCommandResponse }
	| { outcome: "cancelled" }
	| { outcome: "unknown" }
	| { outcome: "untrusted"; code: "response_envelope_mismatch" }
	| { outcome: "unavailable"; code: "command_channel_unavailable" | "request_id_unavailable" };

/**
 * Publish a command for the addressed owner and wait for its answer.
 *
 * The wait ends in exactly one of four states, and the commit-vs-timeout race is
 * arbitrated by the exclusive creation of the response object rather than by any
 * timing assumption.
 */
export async function submitChatDaemonCommand(
	input: SubmitChatDaemonCommandInput,
): Promise<ChatDaemonCommandSubmission> {
	const scope = await openChatDaemonCommandScope({ agentDir: input.agentDir, kind: input.kind, create: true });
	if (!scope) return { outcome: "unavailable", code: "command_channel_unavailable" };
	try {
		return await submitAgainstScope(scope, input);
	} finally {
		// The retained descriptor is the channel's authority; release it as soon as
		// the submission settles so a polling caller cannot accumulate descriptors.
		closeChatDaemonCommandScope(scope);
	}
}

async function submitAgainstScope(
	scope: ChatDaemonCommandScope,
	input: SubmitChatDaemonCommandInput,
): Promise<ChatDaemonCommandSubmission> {
	const now = input.now ?? Date.now;
	const sleep = input.sleep ?? (async ms => await Bun.sleep(ms));
	const timeoutMs = Math.max(input.timeoutMs ?? DEFAULT_CHAT_DAEMON_COMMAND_TIMEOUT_MS, 0);
	const pollIntervalMs = Math.max(input.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS, 1);
	const settleGraceMs = Math.max(input.settleGraceMs ?? DEFAULT_SETTLE_GRACE_MS, 0);
	const request = buildChatDaemonCommandRequest({
		kind: input.kind,
		command: input.command,
		owner: input.owner,
		sessionId: input.sessionId,
		rootTs: input.rootTs,
		now: now(),
		ttlMs: input.ttlMs ?? Math.max(timeoutMs * 2, DEFAULT_REQUEST_TTL_MS),
		requestId: input.requestId,
	});
	const requestName = requestEntry(request.requestId);
	const responseName = responseEntry(request.requestId);
	// A settled identifier already has terminal arbitration state. An exact retry
	// of the same authorization discovers that prior outcome here — no dispatch,
	// no provider proof, no second mutation — while any other reuse of the
	// identifier, and any unreadable material under it, fails closed.
	const settlement = await readSettlement(scope, request);
	if (settlement.kind === "untrusted") return { outcome: "unavailable", code: "request_id_unavailable" };
	if (settlement.kind === "settled") return submissionFromSettlement(settlement.settlement);
	// Stale or planted material under this identifier must neither authorize an
	// answer nor silently suppress the command: the submission fails closed and
	// the caller re-runs with a fresh identifier.
	if (await scopedEntryExists(scope, responseName)) return { outcome: "unavailable", code: "request_id_unavailable" };
	let published: ScopedPublication;
	try {
		published = await publishScopedJsonExclusive(scope, requestName, request);
	} catch {
		return { outcome: "unavailable", code: "command_channel_unavailable" };
	}
	// The request itself carries no commit authority, so an unproven barrier over
	// it is not indeterminate: it is published and addressable right now.
	if (published === "exists") return { outcome: "unavailable", code: "request_id_unavailable" };
	try {
		const answered = await awaitChatDaemonCommandResponse({
			scope,
			request,
			responseName,
			deadline: now() + timeoutMs,
			now,
			sleep,
			pollIntervalMs,
		});
		if (answered.kind === "untrusted") return { outcome: "untrusted", code: "response_envelope_mismatch" };
		if (answered.kind === "answer") return { outcome: "answered", response: answered.response };
		// Winning the response claim makes a commit by the addressed owner
		// impossible from here on, which is what turns a timeout into a
		// definitive, mutation-free failure.
		if (await claimScopedEntry(scope, responseName)) {
			// The claim is terminal for this identifier, so the cancellation has to
			// become durable *before* the cleanup below removes the request and the
			// claim itself. Otherwise the still-unexpired request material could be
			// resurrected and buy a second dispatch. A cancellation that cannot be
			// proven durable is therefore never reported as a definitive one.
			const recorded = await recordSettlement(scope, settlementFor(request, "cancelled", now()));
			return recorded === "recorded" ? { outcome: "cancelled" } : { outcome: "unknown" };
		}
		const settled = await awaitChatDaemonCommandResponse({
			scope,
			request,
			responseName,
			deadline: now() + settleGraceMs,
			now,
			sleep,
			pollIntervalMs,
		});
		if (settled.kind === "untrusted") return { outcome: "untrusted", code: "response_envelope_mismatch" };
		return settled.kind === "answer" ? { outcome: "answered", response: settled.response } : { outcome: "unknown" };
	} finally {
		// Order matters: the request is retired before the settled response
		// object. A daemon that re-creates a removed response claim then proves
		// the request is gone and abandons, so no commit can follow a settled
		// submission. The settlement record is deliberately *not* removed here:
		// submitter cleanup may not erase replay authority.
		await unlinkScopedEntry(scope, requestName);
		await unlinkScopedEntry(scope, responseName);
	}
}

/** What the settlement object under one identifier proved for this exact request. */
type ChatDaemonSettlementObservation =
	| { kind: "settled"; settlement: ChatDaemonCommandSettlement }
	| { kind: "untrusted" }
	| { kind: "absent" };

async function readSettlement(
	scope: ChatDaemonCommandScope,
	request: ChatDaemonCommandRequest,
): Promise<ChatDaemonSettlementObservation> {
	const name = settlementEntry(request.requestId);
	if (!(await scopedEntryExists(scope, name))) return { kind: "absent" };
	const document = await readScopedJson(scope, name);
	if (!isChatDaemonCommandSettlement(document) || !settlementDescribes(document, request))
		return { kind: "untrusted" };
	return { kind: "settled", settlement: document };
}

/**
 * Report a prior terminal outcome without repeating any work.
 *
 * `committing` and `outcome_unknown` are indeterminate by construction: terminal
 * authority was taken but the result was never recorded, so the caller must not
 * be told anything definitive about the mapping store.
 */
function submissionFromSettlement(settlement: ChatDaemonCommandSettlement): ChatDaemonCommandSubmission {
	if (settlement.outcome === "cancelled") return { outcome: "cancelled" };
	if (settlement.outcome === "committing" || settlement.outcome === "outcome_unknown") return { outcome: "unknown" };
	return {
		outcome: "answered",
		response: {
			version: CHAT_DAEMON_COMMAND_VERSION,
			requestId: settlement.requestId,
			kind: settlement.kind,
			command: settlement.command,
			ownerId: settlement.ownerId,
			pid: settlement.pid,
			incarnation: settlement.incarnation,
			generation: settlement.generation,
			sessionId: settlement.sessionId,
			rootTs: settlement.rootTs,
			status: settlement.outcome,
			completedAt: settlement.settledAt,
			...(settlement.code === undefined ? {} : { code: settlement.code }),
			...(settlement.endpointGeneration === undefined ? {} : { endpointGeneration: settlement.endpointGeneration }),
			...(settlement.teamId === undefined ? {} : { teamId: settlement.teamId }),
			...(settlement.channelId === undefined ? {} : { channelId: settlement.channelId }),
		},
	};
}

/** What one poll of the response object proved: an answer, corruption, or nothing yet. */
type ChatDaemonResponseObservation =
	| { kind: "answer"; response: ChatDaemonCommandResponse }
	| { kind: "untrusted" }
	| { kind: "pending" };

async function awaitChatDaemonCommandResponse(input: {
	scope: ChatDaemonCommandScope;
	request: ChatDaemonCommandRequest;
	responseName: string;
	deadline: number;
	now: () => number;
	sleep: (ms: number) => Promise<void>;
	pollIntervalMs: number;
}): Promise<ChatDaemonResponseObservation> {
	for (;;) {
		const document = await readScopedJson(input.scope, input.responseName);
		if (document !== undefined) {
			// An empty or unclassifiable entry is the in-flight claim placeholder and
			// must keep the caller waiting. Any *complete* document is terminal
			// material: either it is this request's exact answer, or it is stale or
			// planted content that can never become one, and waiting for the deadline
			// would only delay a fail-closed report.
			if (!isChatDaemonCommandResponse(document) || !answersRequest(document, input.request))
				return { kind: "untrusted" };
			return { kind: "answer", response: document };
		}
		if (input.now() >= input.deadline) return { kind: "pending" };
		await input.sleep(input.pollIntervalMs);
	}
}

export interface ServeChatDaemonCommandsInput {
	agentDir: string;
	kind: ChatDaemonKind;
	/** The serving daemon's own proven authority. */
	ownerId: string;
	pid: number;
	incarnation: string;
	generation: number;
	handler: ChatDaemonCommandHandler;
	/**
	 * Re-proves that this process still holds the persisted owner record. It is
	 * checked before any work is dispatched *and again* inside the commit fence,
	 * so a daemon that lost ownership after dispatch mutates nothing.
	 */
	verifyOwnership?: () => Promise<boolean>;
	now?: () => number;
}

/**
 * Answer every pending request addressed to this exact owner. Requests aimed at
 * any other owner identity are answered `owner_changed` without doing work, and
 * expired requests are answered `expired`; neither performs a mutation.
 */
export async function serveChatDaemonCommandsOnce(input: ServeChatDaemonCommandsInput): Promise<number> {
	const scope = await openChatDaemonCommandScope({ agentDir: input.agentDir, kind: input.kind });
	if (!scope) return 0;
	try {
		return await serveChatDaemonCommandsAgainstScope(scope, input);
	} finally {
		// The serving loop calls this repeatedly; the retained descriptor must not
		// outlive one pass.
		closeChatDaemonCommandScope(scope);
	}
}

/**
 * Answer every pending request against an already-captured command scope.
 *
 * Exposed so a caller that already holds the retained authority — including a
 * durability-injecting harness — drives exactly the production path.
 */
export async function serveChatDaemonCommandsAgainstScope(
	scope: ChatDaemonCommandScope,
	input: ServeChatDaemonCommandsInput,
): Promise<number> {
	const now = input.now ?? Date.now;
	let served = 0;
	for (const name of await listScopedEntries(scope)) {
		if (name.endsWith(SETTLEMENT_SUFFIX)) {
			await sweepSettlement(scope, name, now());
			continue;
		}
		if (name.endsWith(RESPONSE_SUFFIX)) {
			await sweepCompletedResponse(scope, name, now());
			continue;
		}
		if (!name.endsWith(REQUEST_SUFFIX)) continue;
		const requestId = name.slice(0, -REQUEST_SUFFIX.length);
		if (!REQUEST_ID_PATTERN.test(requestId)) continue;
		const document = await readScopedDocument(scope, name);
		const request = document?.value;
		if (!isChatDaemonCommandRequest(request) || request.requestId !== requestId || request.kind !== input.kind) {
			await unlinkScopedEntry(scope, name, document?.identity);
			continue;
		}
		// Terminal arbitration already exists for this identifier: the request is
		// resurrected or replayed material, and it may never authorize a second
		// dispatch. Unreadable or foreign settlement material fails closed the same
		// way — it is left alone and nothing is dispatched.
		const settlement = await readSettlement(scope, request);
		if (settlement.kind !== "absent") {
			if (settlement.kind === "settled" && request.expiresAt + COMMAND_RETENTION_MS <= now())
				await unlinkScopedEntry(scope, name, document?.identity);
			continue;
		}
		// A response object already exists for this identifier: the request is a
		// replay of settled material, or the submitter cancelled. Either way this
		// request may never re-authorize work.
		if (await scopedEntryExists(scope, responseEntry(requestId))) {
			if (request.expiresAt + COMMAND_RETENTION_MS <= now())
				await unlinkScopedEntry(scope, name, document?.identity);
			continue;
		}
		const claim = new ChatDaemonResponseClaim(scope, requestId);
		let answered = await answerChatDaemonCommand({ request, input, scope, claim, now: now() });
		// The terminal record becomes durable *before* the answer is published, so
		// a crash in between leaves replay authority behind rather than an
		// identifier a resurrected request could reuse.
		if (await claim.acquire()) {
			const recorded = await recordSettlement(scope, settlementFromResponse(request, answered));
			// The retained settlement is this identifier's durable outcome, and a
			// settlement that is not provably durable can be lost by a crash. What
			// survives such a crash is the still-unexpired request, which is then
			// dispatched again and may mutate — so *no* terminal status may be
			// published on top of an unproven record, not only a successful one. A
			// definitive `ok`, `rejected`, `owner_changed`, or `expired` would each
			// tell the caller something the durable state does not support.
			if (recorded === "durability_unknown" && answered.status !== "outcome_unknown") {
				answered = { ...answered, status: "outcome_unknown", code: "settlement_durability_unknown" };
				// Keep the retained record consistent with the answer, so an exact
				// replay discovers the same indeterminate outcome this caller got.
				await downgradeSettlement(scope, settlementFromResponse(request, answered));
			}
		}
		await claim.publish(answered);
		await unlinkScopedEntry(scope, name, document?.identity);
		served++;
	}
	return served;
}

/** Why the commit fence refused, so the answer names the real authority failure. */
type CommitFenceFailure = "owner_changed" | "cancelled" | "abandoned" | "unrecordable" | "undurable";

async function answerChatDaemonCommand(context: {
	request: ChatDaemonCommandRequest;
	input: ServeChatDaemonCommandsInput;
	scope: ChatDaemonCommandScope;
	claim: ChatDaemonResponseClaim;
	now: number;
}): Promise<ChatDaemonCommandResponse> {
	const { request, input, scope, claim } = context;
	const now = input.now ?? Date.now;
	// Every answer echoes the *addressed* request envelope, not the responding
	// process's own identity. That is what lets a submitter validate the complete
	// tuple before it interprets any status: an `owner_changed` answer published
	// by a replacement daemon is still provably the answer to this request, while
	// a document carrying some other tuple is provably not.
	const envelope = {
		version: CHAT_DAEMON_COMMAND_VERSION,
		requestId: request.requestId,
		kind: request.kind,
		command: request.command,
		ownerId: request.ownerId,
		pid: request.pid,
		incarnation: request.incarnation,
		generation: request.generation,
		sessionId: request.sessionId,
		rootTs: request.rootTs,
		completedAt: context.now,
	} as const;
	if (
		request.ownerId !== input.ownerId ||
		request.pid !== input.pid ||
		request.incarnation !== input.incarnation ||
		request.generation !== input.generation
	)
		return { ...envelope, status: "owner_changed" };
	if (request.expiresAt <= context.now) return { ...envelope, status: "expired" };
	// Ownership can lapse between capture and execution. Re-prove it here so a
	// daemon that no longer holds the owner record performs no mutation.
	if (input.verifyOwnership && !(await input.verifyOwnership())) return { ...envelope, status: "owner_changed" };
	let fenceFailure: CommitFenceFailure | undefined;
	/**
	 * The commit fence. It runs inside the caller's store lock, immediately
	 * before the mutation, and every check is local: the persisted owner record,
	 * the single-winner claim, then the still-published request material.
	 *
	 * The claim is taken *before* the request is re-read on purpose. A submitter
	 * that gives up claims the response, then removes its request and its own
	 * claim; taking the claim first and only then proving the request is still
	 * published closes the window in which a daemon could re-create the removed
	 * claim and commit behind a caller that already reported cancellation.
	 */
	const commitAuthority = async (): Promise<boolean> => {
		if (input.verifyOwnership && !(await input.verifyOwnership())) {
			fenceFailure = "owner_changed";
			return false;
		}
		if (!(await claim.acquire())) {
			fenceFailure = "cancelled";
			return false;
		}
		const current = await readScopedJson(scope, requestEntry(request.requestId));
		if (
			!isChatDaemonCommandRequest(current) ||
			!isSameChatDaemonCommandRequest(current, request) ||
			current.expiresAt <= now()
		) {
			fenceFailure = "abandoned";
			return false;
		}
		// Terminal authority is now proven, so record it durably *before* the
		// mutation. A crash after this point leaves an indeterminate settlement an
		// exact retry can discover, instead of an identifier that resurrected
		// request material could reuse to commit a second time. The record must be
		// *proven* durable: a marker that may not survive is not replay authority,
		// so it may not authorize a mutation either.
		const recorded = await recordSettlement(scope, settlementFor(request, "committing", now()));
		if (recorded !== "recorded") {
			fenceFailure = recorded === "durability_unknown" ? "undurable" : "unrecordable";
			return false;
		}
		return true;
	};
	const answer = await dispatchChatDaemonCommand({
		input,
		request,
		envelope,
		commitAuthority,
		claim,
		fenceFailure: () => fenceFailure,
	});
	// An abandoned request has no reader left: its submitter already settled.
	// Release the claim rather than publishing an answer nobody can consume.
	if (fenceFailure === "abandoned") await claim.release();
	return answer;
}

async function dispatchChatDaemonCommand(context: {
	input: ServeChatDaemonCommandsInput;
	request: ChatDaemonCommandRequest;
	envelope: Omit<ChatDaemonCommandResponse, "status">;
	commitAuthority: () => Promise<boolean>;
	claim: ChatDaemonResponseClaim;
	fenceFailure: () => CommitFenceFailure | undefined;
}): Promise<ChatDaemonCommandResponse> {
	const { envelope, claim } = context;
	// Whether the fence ever handed out commit authority. Once it has, the
	// handler may have applied a mapping, and an exception carries no evidence
	// either way — so a thrown failure after that point is indeterminate, not a
	// rejection the caller may act on.
	let authorized = false;
	const commitAuthority = async (): Promise<boolean> => {
		const granted = await context.commitAuthority();
		authorized = authorized || granted;
		return granted;
	};
	let outcome: ChatDaemonCommandOutcome;
	try {
		outcome = await context.input.handler.bindExistingRoot({
			sessionId: context.request.sessionId,
			rootTs: context.request.rootTs,
			commitAuthority,
		});
	} catch {
		outcome = authorized
			? { ok: false, certainty: "unknown", code: "binding_outcome_unknown" }
			: { ok: false, certainty: "rejected", code: "binding_failed" };
	}
	if (!outcome.ok) {
		// A handler that reports an indeterminate commit outranks everything: the
		// mapping may already be applied, so no definitive answer may be published.
		if (outcome.certainty === "unknown") return { ...envelope, status: "outcome_unknown", code: outcome.code };
		// The fence is the authority on *why* a refused commit was refused: a
		// handler-level error text can never outrank a proven authority change.
		const failure = context.fenceFailure();
		// Replay authority is recorded but not provably durable. Nothing was
		// mutated, yet the durable state of this identifier is itself unknown, so
		// the answer may not claim a definitive refusal either.
		if (failure === "undurable") return { ...envelope, status: "outcome_unknown", code: "replay_guard_undurable" };
		if (failure === "owner_changed") return { ...envelope, status: "owner_changed" };
		// Replay authority could not be recorded, so nothing was mutated. That is a
		// definitive, mutation-free refusal rather than an ambiguous one.
		if (failure === "unrecordable") return { ...envelope, status: "rejected", code: "replay_guard_unavailable" };
		if (failure !== undefined) return { ...envelope, status: "expired" };
		return { ...envelope, status: "rejected", code: outcome.code };
	}
	// A success is only reportable when this serve holds the terminal claim; a
	// handler that never passed the fence has not proven exact commit authority.
	if (!claim.held) return { ...envelope, status: "rejected", code: "commit_authority_missing" };
	// The answer must describe the binding that was asked for. A handler that
	// reports some other session or root has not answered this request, and
	// publishing it would produce a document the submitter must reject as
	// untrusted rather than a usable outcome.
	if (outcome.sessionId !== context.request.sessionId || outcome.rootTs !== context.request.rootTs)
		return { ...envelope, status: "rejected", code: "binding_mismatch" };
	return {
		...envelope,
		status: "ok",
		endpointGeneration: outcome.endpointGeneration,
		teamId: outcome.teamId,
		channelId: outcome.channelId,
	};
}

/**
 * Retire one completed response, and only the exact object the decision was
 * made about.
 *
 * Every removal here is identity-bound, including the one for material this
 * sweep could not parse: an entry whose identity cannot be read is left alone
 * rather than removed by name.
 */
async function sweepCompletedResponse(scope: ChatDaemonCommandScope, name: string, now: number): Promise<void> {
	const document = await readScopedDocument(scope, name);
	if (document !== undefined && isChatDaemonCommandResponse(document.value)) {
		if (now - document.value.completedAt >= COMMAND_RETENTION_MS)
			await unlinkScopedEntry(scope, name, document.identity);
		return;
	}
	// An unparseable entry can be a live claim placeholder held by an in-flight
	// commit, so only age retires it; deleting it early would break arbitration.
	const identity = document?.identity ?? (await scopedEntryIdentity(scope, name));
	if (identity === undefined) return;
	if (now - identity.mtimeMs >= COMMAND_RETENTION_MS) await unlinkScopedEntry(scope, name, identity);
}

function settlementFromResponse(
	request: ChatDaemonCommandRequest,
	response: ChatDaemonCommandResponse,
): ChatDaemonCommandSettlement {
	return settlementFor(request, response.status, response.completedAt, {
		...(response.code === undefined ? {} : { code: response.code }),
		...(response.endpointGeneration === undefined ? {} : { endpointGeneration: response.endpointGeneration }),
		...(response.teamId === undefined ? {} : { teamId: response.teamId }),
		...(response.channelId === undefined ? {} : { channelId: response.channelId }),
	});
}

/**
 * Retire one settlement, and only the exact object the decision was made about.
 *
 * Replay authority must outlive both the request's own deadline and the
 * retention window, so an exact retry can still discover the settled outcome.
 * The removal is bound to the identity that was read, so a successor that took
 * the same name after the decision is never retired by this sweep.
 */
async function sweepSettlement(scope: ChatDaemonCommandScope, name: string, now: number): Promise<void> {
	const document = await readScopedDocument(scope, name);
	if (document === undefined) {
		const identity = await scopedEntryIdentity(scope, name);
		if (identity !== undefined && now - identity.mtimeMs >= COMMAND_RETENTION_MS)
			await unlinkScopedEntry(scope, name, identity);
		return;
	}
	if (!isChatDaemonCommandSettlement(document.value)) {
		if (now - document.identity.mtimeMs >= COMMAND_RETENTION_MS)
			await unlinkScopedEntry(scope, name, document.identity);
		return;
	}
	const settlement = document.value;
	if (now - settlement.settledAt < COMMAND_RETENTION_MS) return;
	if (now < settlement.expiresAt + COMMAND_RETENTION_MS) return;
	await unlinkScopedEntry(scope, name, document.identity);
}
