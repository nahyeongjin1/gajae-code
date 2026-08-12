/**
 * Adoption of an operator-supplied Slack thread as a live session's root.
 *
 * Three separate authorities have to agree before a mapping exists:
 *  1. *Configuration*: a complete Slack target (both tokens plus workspace and
 *     channel) must already be configured; binding never accepts a target.
 *  2. *Daemon*: only the running owner may mutate mappings, and only while it is
 *     still the exact owner (`ownerId`/`pid`/`incarnation`/daemon generation)
 *     captured before the request was published.
 *  3. *Session*: the SDK session must be attachable right now — indexed, live,
 *     non-terminal, with a readable, non-stale endpoint whose pid matches the
 *     indexed host and whose generation has not rolled.
 *
 * Provider verification happens before any lock is taken, and the mapping commit
 * re-proves session authority inside the store lock, so an authority that
 * changes mid-flight leaves no mapping behind.
 */

import type { Settings } from "../../config/settings";
import type { SessionIndex } from "../broker/session-index";
import { readSdkSessionEndpoint, type SdkSessionEndpoint } from "../client/discovery";
import {
	type ChatDaemonCommandOwner,
	type ChatDaemonCommandSubmission,
	submitChatDaemonCommand,
} from "./chat-daemon-command-channel";
import {
	ChatDaemonController,
	chatDaemonGeneration,
	type EnsureChatDaemonResult,
	ensureSlackDaemon,
	hasSafeChatDaemonStateShape,
	readChatDaemonState,
} from "./chat-daemon-control";
import { getNotificationConfig, isSlackConfigured } from "./config";
import {
	ConversationCommitUncertainError,
	ConversationLockCleanupError,
	ConversationStore,
} from "./conversation-store";
import { normalizeSlackConversation, type SlackConversation, slackConversationKey } from "./slack-conversation";

export type SlackThreadBindingErrorCode =
	| "invalid_root"
	| "target_not_configured"
	| "daemon_unavailable"
	| "daemon_owner_changed"
	| "session_not_live"
	| "root_not_found"
	| "provider_unavailable"
	| "root_conflict"
	| "session_conflict"
	| "binding_outcome_unknown"
	| "binding_failed";

const BINDING_ERROR_CODES: readonly SlackThreadBindingErrorCode[] = [
	"invalid_root",
	"target_not_configured",
	"daemon_unavailable",
	"daemon_owner_changed",
	"session_not_live",
	"root_not_found",
	"provider_unavailable",
	"root_conflict",
	"session_conflict",
	"binding_outcome_unknown",
	"binding_failed",
];

/** A fail-closed rejection while adopting an operator-supplied Slack thread. */
export class SlackThreadBindingError extends Error {
	constructor(
		readonly code: SlackThreadBindingErrorCode,
		message: string,
	) {
		super(message);
		this.name = "SlackThreadBindingError";
	}
}

function bindingErrorCode(value: string | undefined): SlackThreadBindingErrorCode {
	return BINDING_ERROR_CODES.find(candidate => candidate === value) ?? "binding_failed";
}

/**
 * Slack message timestamps are `<seconds>.<fraction>` with ASCII digits on both
 * sides. The bound keeps an operator-supplied value from becoming an unbounded
 * store key or provider argument; the provider remains the final authority on
 * whether the timestamp addresses a real message.
 */
const MAX_SLACK_TS_SEGMENT = 12;

function isAsciiDigits(value: string): boolean {
	if (value.length === 0) return false;
	for (let index = 0; index < value.length; index++) {
		const code = value.charCodeAt(index);
		if (code < 0x30 || code > 0x39) return false;
	}
	return true;
}

export function isBoundedSlackRootTs(value: string): boolean {
	if (typeof value !== "string") return false;
	const separator = value.indexOf(".");
	if (separator <= 0 || separator !== value.lastIndexOf(".")) return false;
	const seconds = value.slice(0, separator);
	const fraction = value.slice(separator + 1);
	if (seconds.length > MAX_SLACK_TS_SEGMENT || fraction.length > MAX_SLACK_TS_SEGMENT) return false;
	return isAsciiDigits(seconds) && isAsciiDigits(fraction);
}

/** Reject a non-addressable root before any authority read or persistence. */
export function assertBoundedSlackRootTs(rootTs: string): void {
	if (!isBoundedSlackRootTs(rootTs))
		throw new SlackThreadBindingError(
			"invalid_root",
			"Slack root timestamp must be a bounded <seconds>.<fraction> message timestamp.",
		);
}

/** Proven right to bind one session at one endpoint generation. */
export interface SlackSessionBindingAuthority {
	sessionId: string;
	endpointGeneration: number;
	pid: number;
	repo: string;
}

export interface SessionBindingAuthorityInput {
	sessionIndex: SessionIndex;
	sessionId: string;
	readEndpoint?: (repo: string, sessionId: string) => Promise<SdkSessionEndpoint | null>;
}

/**
 * Resolve exact discovery/attachment authority for one session.
 *
 * `IndexedSession.live` alone is only pid liveness, which a terminated or
 * unregistered session can still satisfy through pid reuse or a stale record.
 * Adoption additionally requires an intact index replay, a non-terminal record,
 * and a discovery endpoint that is present, well-formed, not marked stale, and
 * owned by the same host pid the index recorded.
 */
export async function resolveSessionBindingAuthority(
	input: SessionBindingAuthorityInput,
): Promise<SlackSessionBindingAuthority | undefined> {
	await input.sessionIndex.refresh();
	const listing = input.sessionIndex.listSessions();
	// A truncated replay cannot prove the tail is free of a terminal or
	// unregistration event, so a degraded index is never binding authority.
	if (listing.warnings.length > 0) return undefined;
	const session = listing.sessions.find(candidate => candidate.sessionId === input.sessionId);
	if (!session?.live || session.terminalUncertain) return undefined;
	if (!Number.isSafeInteger(session.endpointGeneration) || session.endpointGeneration <= 0) return undefined;
	if (!Number.isSafeInteger(session.pid) || session.pid <= 0) return undefined;
	let endpoint: SdkSessionEndpoint | null;
	try {
		endpoint = await (input.readEndpoint ?? readSdkSessionEndpoint)(session.locator.repo, input.sessionId);
	} catch {
		// A malformed discovery record is not authority for anything.
		return undefined;
	}
	if (!endpoint || endpoint.stale === true || !endpoint.url || !endpoint.token) return undefined;
	if (endpoint.pid === undefined || endpoint.pid !== session.pid) return undefined;
	return {
		sessionId: input.sessionId,
		endpointGeneration: session.endpointGeneration,
		pid: session.pid,
		repo: session.locator.repo,
	};
}

/** States in which a mapping still owns its session's root claim. */
function holdsRootClaim(record: SlackConversation): boolean {
	return record.state === "active" || record.state === "posting_root" || record.state === "resumed_root";
}

export interface SlackThreadClaimInput {
	store: ConversationStore<SlackConversation>;
	/** The session root-claim key shared with stock root publication. */
	key: string;
	teamId: string;
	channelId: string;
	sessionId: string;
	rootTs: string;
	endpointGeneration: number;
	/** Re-proves session authority inside the store lock, immediately before commit. */
	revalidate: () => Promise<boolean>;
	now?: () => number;
}

/**
 * Claim an existing Slack root for a session without publishing a replacement.
 *
 * The claim transacts the same `intent:<sessionId>` key that stock root
 * publication uses, so a bind and a concurrent first notification serialize on
 * one invariant: whichever commits first owns the session's single root, and
 * the loser observes it instead of creating a second one.
 */
export async function claimSlackThreadBinding(input: SlackThreadClaimInput): Promise<SlackConversation> {
	assertBoundedSlackRootTs(input.rootTs);
	const now = input.now ?? Date.now;
	let rejection: SlackThreadBindingError | undefined;
	const bound = await claimUnderStoreLock(
		input,
		now,
		() => rejection,
		value => (rejection = value),
	);
	if (rejection) throw rejection;
	if (
		bound?.state !== "active" ||
		bound.sessionId !== input.sessionId ||
		bound.rootTs !== input.rootTs ||
		bound.endpointGeneration !== input.endpointGeneration
	)
		throw new SlackThreadBindingError("binding_failed", "Slack root binding could not be claimed.");
	return bound;
}

/**
 * Run the claim transaction and translate persistence failures by their typed
 * commit certainty.
 *
 * A refused commit never became visible, so it is a definitive rejection. An
 * uncertain commit is already applied and only unproven, so the caller is told
 * the outcome is unknown and the mapping is left exactly as it is.
 *
 * A lock whose cleanup failed is a third, separate thing: the mapping decision
 * itself is certain. A decision that refused stays a definitive rejection,
 * because nothing was applied and nothing can be. A decision that applied is
 * reported as unknown, because the caller cannot tell from here whether the
 * mapping's own guard is still holding.
 */
async function claimUnderStoreLock(
	input: SlackThreadClaimInput,
	now: () => number,
	readRejection: () => SlackThreadBindingError | undefined,
	writeRejection: (value: SlackThreadBindingError) => void,
): Promise<SlackConversation | undefined> {
	try {
		return await claimTransaction(input, now, writeRejection);
	} catch (error) {
		if (error instanceof SlackThreadBindingError) throw error;
		if (error instanceof ConversationCommitUncertainError)
			throw new SlackThreadBindingError(
				"binding_outcome_unknown",
				"The Slack mapping was applied but its durability could not be proven; rerun the binding to observe the settled state.",
			);
		const rejection = readRejection();
		if (error instanceof ConversationLockCleanupError) {
			if (rejection) throw rejection;
			throw new SlackThreadBindingError(
				"binding_outcome_unknown",
				"The Slack mapping was applied but its store lock could not be released; rerun the binding to observe the settled state.",
			);
		}
		if (rejection) throw rejection;
		throw new SlackThreadBindingError("binding_failed", "Slack root binding could not be claimed.");
	}
}

function claimTransaction(
	input: SlackThreadClaimInput,
	now: () => number,
	writeRejection: (value: SlackThreadBindingError) => void,
): Promise<SlackConversation | undefined> {
	const rejectWith = (code: SlackThreadBindingErrorCode, message: string): undefined => {
		writeRejection(new SlackThreadBindingError(code, message));
		return undefined;
	};
	/**
	 * Prove authority at the commit decision.
	 *
	 * The in-callback proof happens before the replacement document is staged and
	 * published, and both of those are asynchronous, so session liveness, endpoint
	 * generation, and the exact daemon owner tuple can all roll in between. The
	 * store therefore runs this proof *after* the replacement is published but
	 * while it is still provisional and unobservable: authority that holds here
	 * commits it, and authority that has already rolled — including a roll that
	 * landed inside the publication itself — rolls it back before any reader can
	 * see it. It is local work only: no provider or network call ever runs under
	 * the store lock.
	 */
	const finalize = async (): Promise<boolean> => {
		if (await input.revalidate()) return true;
		rejectWith("session_not_live", "Slack session authority changed before the binding could commit.");
		return false;
	};
	return input.store.transactWithSnapshot(
		input.key,
		async (current, conversations) => {
			for (const [candidateKey, candidate] of Object.entries(conversations)) {
				if (!holdsRootClaim(candidate)) continue;
				if (
					candidate.teamId === input.teamId &&
					candidate.channelId === input.channelId &&
					candidate.rootTs === input.rootTs &&
					candidate.sessionId !== input.sessionId
				) {
					rejectWith("root_conflict", "Slack root is already bound to another session.");
					return current;
				}
				if (candidateKey !== input.key && candidate.sessionId === input.sessionId) {
					rejectWith("session_conflict", "Slack session already holds a different root claim.");
					return current;
				}
			}
			if (current) {
				const storedGeneration = current.endpointGeneration ?? 0;
				if (
					current.sessionId !== input.sessionId ||
					current.state !== "active" ||
					current.rootTs !== input.rootTs ||
					storedGeneration > input.endpointGeneration
				) {
					rejectWith("session_conflict", "Slack session already holds a different root claim.");
					return current;
				}
				// An exact replay must not mutate, but it still has to prove authority:
				// an unauthorized or cancelled request may not be answered with the
				// record a previous, authorized request created.
				if (storedGeneration === input.endpointGeneration) {
					if (!(await input.revalidate()))
						rejectWith("session_not_live", "Slack session authority changed before the binding could commit.");
					return current;
				}
			}
			if (!(await input.revalidate())) {
				rejectWith("session_not_live", "Slack session authority changed before the binding could commit.");
				return current;
			}
			return normalizeSlackConversation({
				generation: (current?.generation ?? 0) + 1,
				state: "active",
				teamId: input.teamId,
				channelId: input.channelId,
				rootTs: input.rootTs,
				sessionId: input.sessionId,
				endpointGeneration: input.endpointGeneration,
				updatedAt: now(),
				seenEventIds: current?.seenEventIds ?? [],
				seenContextIds: current?.seenContextIds ?? [],
				seenRetryKeys: current?.seenRetryKeys ?? [],
				seenInteractionIds: current?.seenInteractionIds ?? [],
				inboundDispatches: current?.inboundDispatches ?? [],
			});
		},
		{ finalize },
	);
}

export interface ConfiguredSlackThreadBindingInput {
	settings: Settings;
	sessionId: string;
	threadTs: string;
}

export interface ConfiguredSlackThreadBindingDeps {
	ensureDaemon?: (settings: Settings) => Promise<EnsureChatDaemonResult>;
	timeoutMs?: number;
	pollIntervalMs?: number;
	/** Bounded wait for a daemon that took terminal authority before the caller gave up. */
	settleGraceMs?: number;
	now?: () => number;
	sleep?: (ms: number) => Promise<void>;
}

/** Safe confirmation of an applied binding. It carries identifiers only. */
export interface BoundSlackThread {
	sessionId: string;
	endpointGeneration: number;
	teamId: string;
	channelId: string;
	rootTs: string;
	ownerId: string;
	daemonGeneration: number;
}

/**
 * Adopt an existing Slack root for a live session through the running daemon.
 *
 * The CLI never writes the mapping store: it proves the configured target and
 * the exact current owner, then asks that owner to perform the mutation. A
 * daemon that is no longer the captured owner answers `owner_changed`, so a
 * replacement or restart between capture and execution can never apply the
 * request.
 *
 * A submission that is not answered is cancelled through the channel's
 * single-winner arbitration before it is reported as a failure. If that
 * cancellation loses to the daemon's own commit authority, the outcome is
 * reported as unknown instead of as a failure, because a mapping may already
 * exist; re-running the same command observes the settled state idempotently.
 *
 * The command channel proves *correlation*, never authorship. Its documents live
 * in the daemon's own command directory, which this codebase treats as
 * untrusted, and every field a response echoes is copied verbatim out of the
 * plaintext request published beside it. A definitive answer is therefore
 * reported only after this caller itself observes the exact mutation in the
 * conversation store — see `corroborateBoundMapping`.
 */
export async function bindConfiguredSlackThread(
	input: ConfiguredSlackThreadBindingInput,
	deps: ConfiguredSlackThreadBindingDeps = {},
): Promise<BoundSlackThread> {
	assertBoundedSlackRootTs(input.threadTs);
	const config = getNotificationConfig(input.settings);
	if (!isSlackConfigured(config))
		throw new SlackThreadBindingError(
			"target_not_configured",
			"Slack notifications must be fully configured before binding an existing thread.",
		);
	const agentDir = input.settings.getAgentDir();
	const ensured = await (deps.ensureDaemon ?? ensureSlackDaemon)(input.settings);
	if (ensured === "disabled")
		throw new SlackThreadBindingError(
			"target_not_configured",
			"Slack notifications must be fully configured before binding an existing thread.",
		);
	const status = await new ChatDaemonController(input.settings, "slack").status();
	if (status.health !== "running")
		throw new SlackThreadBindingError(
			"daemon_unavailable",
			`Slack daemon must be running to bind an existing thread (current: ${status.health}).`,
		);
	const state = await readChatDaemonState(agentDir, "slack");
	if (
		!hasSafeChatDaemonStateShape(state) ||
		state.kind !== "slack" ||
		state.stoppedAt !== undefined ||
		state.ownerId !== status.ownerId ||
		state.pid !== status.pid ||
		state.generation !== chatDaemonGeneration("slack")
	)
		throw new SlackThreadBindingError(
			"daemon_owner_changed",
			"Slack daemon ownership changed; rerun the binding against the current owner.",
		);
	const owner: ChatDaemonCommandOwner = {
		ownerId: state.ownerId,
		pid: state.pid,
		incarnation: state.incarnation,
		generation: state.generation,
	};
	const submission = await submitChatDaemonCommand({
		agentDir,
		kind: "slack",
		owner,
		command: "bind-thread",
		sessionId: input.sessionId,
		rootTs: input.threadTs,
		...(deps.timeoutMs === undefined ? {} : { timeoutMs: deps.timeoutMs }),
		...(deps.pollIntervalMs === undefined ? {} : { pollIntervalMs: deps.pollIntervalMs }),
		...(deps.settleGraceMs === undefined ? {} : { settleGraceMs: deps.settleGraceMs }),
		...(deps.now === undefined ? {} : { now: deps.now }),
		...(deps.sleep === undefined ? {} : { sleep: deps.sleep }),
	});
	const target = { teamId: config.slack.workspaceId, channelId: config.slack.channelId };
	const bound = interpretBindSubmission(submission, { owner, input, ...target });
	return await corroborateBoundMapping(agentDir, bound, {
		...target,
		sessionId: input.sessionId,
		rootTs: input.threadTs,
	});
}

/**
 * Refuse a reported binding that the durable mapping store does not corroborate.
 *
 * The command channel authenticates nothing. Every field a response echoes is
 * copied verbatim out of the plaintext request that sits beside it, and
 * `<id>.response.json` is a single-winner object inside the daemon's own command
 * directory — a directory `chat-daemon-command-scope` explicitly treats as
 * untrusted, because any process that can write it may create that object first.
 * A complete, envelope-correct `status:"ok"` document is therefore producible
 * with no daemon involvement at all, and so is any companion document in the
 * same directory: a settlement record, a nonce echoed back from the request, or
 * a signature under a key that itself lives in writable state.
 *
 * Authenticating the responder needs an authority the writer cannot copy —
 * kernel-verified peer identity (peer pid plus process incarnation) over a local
 * socket. That is not reachable from this runtime, so nothing here claims to
 * authenticate anything. Instead the one definitive answer a caller can act on,
 * `ok`, is refused unless this process itself observes the exact mutation in the
 * conversation store: an authority outside the command directory, which a forged
 * answer never produces because it performs no mutation.
 *
 * An uncorroborated mapping is reported as indeterminate rather than as a
 * failure. A real daemon may have committed and had its mapping superseded
 * before this read, so the operator is told to rerun and observe the settled
 * state instead of being told the binding definitively failed.
 */
async function corroborateBoundMapping(
	agentDir: string,
	bound: BoundSlackThread,
	requested: { teamId: string; channelId: string; sessionId: string; rootTs: string },
): Promise<BoundSlackThread> {
	const key = slackConversationKey({
		teamId: requested.teamId,
		channelId: requested.channelId,
		rootTs: `intent:${requested.sessionId}`,
	});
	let record: SlackConversation | undefined;
	try {
		record = await new ConversationStore<SlackConversation>({ agentDir, kind: "slack" }).read(key);
	} catch {
		// An unreadable store proves nothing about the mapping, which is exactly
		// the indeterminate outcome reported below.
		record = undefined;
	}
	if (
		record?.state !== "active" ||
		record.sessionId !== requested.sessionId ||
		record.rootTs !== requested.rootTs ||
		record.teamId !== requested.teamId ||
		record.channelId !== requested.channelId ||
		record.endpointGeneration !== bound.endpointGeneration
	)
		throw new SlackThreadBindingError(
			"binding_outcome_unknown",
			"The Slack daemon reported a binding the durable mapping store does not corroborate; rerun the binding to observe the settled state.",
		);
	return bound;
}

function interpretBindSubmission(
	submission: ChatDaemonCommandSubmission,
	context: {
		owner: ChatDaemonCommandOwner;
		input: ConfiguredSlackThreadBindingInput;
		teamId: string;
		channelId: string;
	},
): BoundSlackThread {
	if (submission.outcome === "unavailable")
		throw new SlackThreadBindingError(
			"daemon_unavailable",
			"The Slack daemon command channel is not usable for this request.",
		);
	if (submission.outcome === "cancelled")
		throw new SlackThreadBindingError(
			"daemon_unavailable",
			"The Slack daemon did not answer in time; the request was cancelled before any mapping changed.",
		);
	if (submission.outcome === "unknown")
		throw new SlackThreadBindingError(
			"binding_outcome_unknown",
			"The Slack daemon took commit authority but did not report an outcome; rerun the binding to observe it.",
		);
	if (submission.outcome === "untrusted")
		throw new SlackThreadBindingError(
			"binding_outcome_unknown",
			"The Slack daemon channel holds an answer that does not carry this request's envelope; rerun the binding to observe the settled state.",
		);
	// `submitChatDaemonCommand` has proven that this document carries this exact
	// request's envelope, which is correlation only: the envelope is public, so a
	// forged answer satisfies it too. Nothing below may therefore be treated as
	// proof that the daemon acted — the definitive `ok` branch is corroborated
	// against the durable mapping store by `corroborateBoundMapping`.
	const response = submission.response;
	if (response.status === "owner_changed")
		throw new SlackThreadBindingError(
			"daemon_owner_changed",
			"Slack daemon ownership changed; rerun the binding against the current owner.",
		);
	if (response.status === "expired")
		throw new SlackThreadBindingError(
			"daemon_unavailable",
			"The Slack thread binding request expired before the daemon executed it.",
		);
	if (response.status === "outcome_unknown")
		throw new SlackThreadBindingError(
			"binding_outcome_unknown",
			"The Slack daemon exercised commit authority but could not prove the mapping's durable state; rerun the binding to observe it.",
		);
	if (response.status === "rejected")
		throw new SlackThreadBindingError(
			bindingErrorCode(response.code),
			`The Slack daemon refused the thread binding (${bindingErrorCode(response.code)}).`,
		);
	if (
		response.ownerId !== context.owner.ownerId ||
		response.pid !== context.owner.pid ||
		response.incarnation !== context.owner.incarnation ||
		response.generation !== context.owner.generation ||
		response.sessionId !== context.input.sessionId ||
		response.rootTs !== context.input.threadTs ||
		response.teamId !== context.teamId ||
		response.channelId !== context.channelId ||
		response.endpointGeneration === undefined
	)
		throw new SlackThreadBindingError(
			"binding_failed",
			"The Slack daemon answered with a binding that does not match the request.",
		);
	return {
		sessionId: response.sessionId,
		endpointGeneration: response.endpointGeneration,
		teamId: response.teamId,
		channelId: response.channelId,
		rootTs: response.rootTs,
		ownerId: response.ownerId,
		daemonGeneration: response.generation,
	};
}
