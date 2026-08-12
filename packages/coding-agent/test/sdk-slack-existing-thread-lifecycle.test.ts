import { describe, expect, spyOn, test } from "bun:test";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { SessionIndex } from "../src/sdk/broker/session-index";
import { ChatDaemonRuntime } from "../src/sdk/bus/chat-daemon-runtime";
import { ConversationStore } from "../src/sdk/bus/conversation-store";
import { type NotificationEvent, NotificationPresentationEngine } from "../src/sdk/bus/engine";
import {
	activatePreparedSession,
	createSlackBindingActivationGate,
	EXISTING_THREAD_BIND_ENV,
	isExistingThreadBindingRequested,
} from "../src/sdk/bus/existing-thread-readiness";
import type { SlackConversation } from "../src/sdk/bus/slack-conversation";
import { SlackNotificationDaemon } from "../src/sdk/bus/slack-daemon";
import { SlackProvider } from "../src/sdk/bus/slack-provider";
import { SdkClientError } from "../src/sdk/client/client";
import { SessionSdkHost } from "../src/sdk/host";
import type { SdkFrame } from "../src/sdk/host/types";

const ROOT_TS = "1785573662.132329";
const SESSION_ID = "session-1";
const GENERATION = 4;

/** A fake Slack workspace that records every publication and verifies seeded roots. */
class FakeSlackWorkspace {
	readonly posts: Array<{ channel: string; text: string; threadTs?: string }> = [];
	readonly roots = new Map<string, string>();

	seedRoot(channel: string, ts: string): void {
		this.roots.set(ts, channel);
	}
	rootPosts(): Array<{ channel: string; text: string }> {
		return this.posts.filter(post => post.threadTs === undefined).map(({ channel, text }) => ({ channel, text }));
	}
	async start(): Promise<void> {}
	async stop(): Promise<void> {}
	async ack(): Promise<void> {}
	async postMessage(input: { channel: string; text: string; threadTs?: string; clientMsgId: string }) {
		this.posts.push({ channel: input.channel, text: input.text, threadTs: input.threadTs });
		const ts = `9.${this.posts.length}`;
		this.roots.set(ts, input.channel);
		return { channel: input.channel, ts, client_msg_id: input.clientMsgId };
	}
	async findMessageByClientMsgId() {
		return null;
	}
	async findMessageByTimestamp(input: { channel: string; ts: string }) {
		const channel = this.roots.get(input.ts);
		return channel === input.channel ? { channel, ts: input.ts } : null;
	}
}

interface SessionFixture {
	repo: string;
	agentDir: string;
	index: SessionIndex;
	workspace: FakeSlackWorkspace;
	store: ConversationStore<SlackConversation>;
	daemon: SlackNotificationDaemon;
	/** A daemon bound to one exact endpoint generation over the same mapping store. */
	daemonAt(generation: number): SlackNotificationDaemon;
	conversations(): Promise<SlackConversation[]>;
	/** A runtime whose attached session replays exactly the supplied frames. */
	runtime(replayEvents: SdkFrame[]): ChatDaemonRuntime;
	/** Push a frame down every live client subscription, as the transport would. */
	emitLive(frame: SdkFrame): void;
	/** Every frame the runtime or daemon pushed back down the session endpoint. */
	sentFrames: SdkFrame[];
	/** Resolve once `predicate` holds, or fail with `label` instead of hanging. */
	waitFor(predicate: () => boolean | Promise<boolean>, label: string): Promise<void>;
	cleanup(): Promise<void>;
}

async function sessionFixture(): Promise<SessionFixture> {
	const repo = await fs.mkdtemp(path.join(os.tmpdir(), "gjc-slack-prepare-"));
	const agentDir = path.join(repo, ".gjc", "agent");
	const endpointDir = path.join(repo, ".gjc", "state", "sdk");
	await fs.mkdir(endpointDir, { recursive: true });
	await fs.writeFile(
		path.join(endpointDir, `${SESSION_ID}.json`),
		JSON.stringify({ version: 1, url: "ws://127.0.0.1:1", token: "endpoint-token", pid: process.pid }),
	);
	const index = await new SessionIndex(agentDir).open();
	await index.append({
		type: "host_registered",
		sessionId: SESSION_ID,
		locator: { repo, stateRoot: path.join(repo, ".gjc", "state") },
		endpointGeneration: GENERATION,
		pid: process.pid,
	});
	const workspace = new FakeSlackWorkspace();
	workspace.seedRoot("C1", ROOT_TS);
	const sentFrames: SdkFrame[] = [];
	const daemons: SlackNotificationDaemon[] = [];
	const daemonAt = (generation: number): SlackNotificationDaemon => {
		const created = new SlackNotificationDaemon({
			agentDir,
			repo,
			teamId: "T1",
			channelId: "C1",
			provider: new SlackProvider(workspace),
			createClient: () => ({
				send(frame) {
					sentFrames.push(frame);
				},
			}),
			resolveEndpoint: async sessionId => ({
				sessionId,
				url: "ws://127.0.0.1:1",
				token: "endpoint-token",
				path: "",
				generation,
			}),
		});
		daemons.push(created);
		return created;
	};
	const daemon = daemonAt(GENERATION);
	const store = new ConversationStore<SlackConversation>({ agentDir, kind: "slack" });
	const runtimes: ChatDaemonRuntime[] = [];
	const liveFrameHandlers = new Set<(frame: SdkFrame) => void>();
	return {
		repo,
		agentDir,
		index,
		workspace,
		store,
		sentFrames,
		daemon,
		daemonAt,
		conversations: async () => Object.values((await store.load()).conversations),
		runtime: replayEvents => {
			const runtime = new ChatDaemonRuntime(
				{
					kind: "slack",
					agentDir,
					config: {
						identity: "fingerprint-only",
						notifications: {
							slack: {
								botToken: "xoxb-not-persisted",
								appToken: "xapp-not-persisted",
								workspaceId: "T1",
								channelId: "C1",
								authorizedUserId: "U1",
							},
						},
					},
				},
				{
					createSlackProvider: () => workspace,
					createIndex: () => index,
					createClient: async () => ({
						onFrame: handler => {
							liveFrameHandlers.add(handler);
							return () => liveFrameHandlers.delete(handler);
						},
						request: async () => ({ events: replayEvents }),
						close: async () => {},
						send: frame => {
							sentFrames.push(frame);
						},
					}),
					setInterval: (() => 0) as unknown as typeof setInterval,
					clearInterval: (() => {}) as typeof clearInterval,
				},
			);
			runtimes.push(runtime);
			return runtime;
		},
		emitLive: frame => {
			for (const handler of liveFrameHandlers) handler(frame);
		},
		waitFor: async (predicate, label) => {
			const deadline = Date.now() + 5_000;
			while (!(await predicate())) {
				if (Date.now() > deadline) throw new Error(`Timed out waiting for ${label}`);
				await Bun.sleep(5);
			}
		},
		cleanup: async () => {
			for (const runtime of runtimes) await runtime.stop();
			for (const created of daemons) await created.stop();
			await fs.rm(repo, { recursive: true, force: true });
		},
	};
}

describe("prepared Slack session root ordering", () => {
	test("a replayed session_prepared event publishes no Slack root and no conversation", async () => {
		const fixture = await sessionFixture();
		try {
			// The real prepared startup signal, replayed at the attachment's exact
			// endpoint generation. `session_prepared` is control-plane evidence that
			// the session withheld readiness, so the daemon has nothing to surface.
			await fixture
				.runtime([{ type: "event", name: "session_prepared", sessionId: SESSION_ID, generation: GENERATION }])
				.start();
			expect(fixture.workspace.rootPosts()).toEqual([]);
			expect(fixture.workspace.posts).toEqual([]);
			expect(await fixture.conversations()).toEqual([]);
		} finally {
			await fixture.cleanup();
		}
	});

	test("a live session_prepared frame after attachment causes no Slack post, mapping, or resume", async () => {
		const fixture = await sessionFixture();
		try {
			await fixture.runtime([]).start();
			fixture.emitLive({ type: "event", name: "session_prepared", sessionId: SESSION_ID, generation: GENERATION });
			// A later ordinary notification is serialized behind the prepared frame on
			// the same session's frame tail, so observing it proves the prepared frame
			// was already handled — and that it published nothing of its own.
			fixture.emitLive({
				type: "event",
				name: "notification",
				sessionId: SESSION_ID,
				generation: GENERATION,
				payload: { type: "marker", sessionId: SESSION_ID, text: "ordering-sentinel" },
			});
			// The sentinel's own root publication is durable only once its mapping
			// settles to `active`, so wait on that rather than the raw transport post.
			await fixture.waitFor(
				async () => (await fixture.conversations()).some(record => record.state === "active"),
				"sentinel notification delivery",
			);

			expect(fixture.workspace.rootPosts()).toEqual([{ channel: "C1", text: "GJC marker\nordering-sentinel" }]);
			expect(await fixture.conversations()).toEqual([
				expect.objectContaining({ state: "active", sessionId: SESSION_ID, endpointGeneration: GENERATION }),
			]);
		} finally {
			await fixture.cleanup();
		}
	});

	test("prepared events for a foreign session or a stale generation stay inert", async () => {
		for (const stale of [
			{ label: "foreign session", sessionId: "session-2", generation: GENERATION },
			{ label: "stale generation", sessionId: SESSION_ID, generation: GENERATION - 1 },
		]) {
			const fixture = await sessionFixture();
			try {
				await fixture
					.runtime([
						{
							type: "event",
							name: "session_prepared",
							sessionId: stale.sessionId,
							generation: stale.generation,
						},
					])
					.start();
				expect(fixture.workspace.posts).toEqual([]);
				expect(await fixture.conversations()).toEqual([]);
			} finally {
				await fixture.cleanup();
			}
		}
	});

	test("session_ready after a binding adopts the bound root with zero replacement roots and no duplicate delivery", async () => {
		const fixture = await sessionFixture();
		try {
			await fixture.daemon.bindExistingRoot(SESSION_ID, ROOT_TS);
			// The full prepared lifecycle as the client replays it: the prepared
			// signal, then the readiness the activation published — twice, because a
			// reconnecting daemon replays the retained ring.
			await fixture
				.runtime([
					{ type: "event", name: "session_prepared", sessionId: SESSION_ID, generation: GENERATION },
					{ type: "event", name: "session_ready", sessionId: SESSION_ID, generation: GENERATION },
					{ type: "event", name: "session_ready", sessionId: SESSION_ID, generation: GENERATION },
				])
				.start();

			// The operator's root is adopted silently: no replacement root, and no
			// readiness message republished for the repeated signal.
			expect(fixture.workspace.rootPosts()).toEqual([]);
			expect(fixture.workspace.posts).toEqual([]);
			expect(await fixture.conversations()).toEqual([
				expect.objectContaining({
					state: "active",
					sessionId: SESSION_ID,
					rootTs: ROOT_TS,
					endpointGeneration: GENERATION,
				}),
			]);
		} finally {
			await fixture.cleanup();
		}
	});

	test("binding then activating adopts the exact operator root with zero replacement roots", async () => {
		const fixture = await sessionFixture();
		try {
			await fixture.runtime([]).start();

			// bind: the daemon owner applies the operator-supplied root.
			const bound = await fixture.daemon.bindExistingRoot(SESSION_ID, ROOT_TS);
			expect(bound).toMatchObject({ state: "active", rootTs: ROOT_TS, endpointGeneration: GENERATION });

			// activate: readiness is published, and the daemon adopts the bound root.
			const resumed = await fixture.daemon.resume(SESSION_ID, "GJC session ready.", GENERATION);
			expect(resumed).toMatchObject({ state: "active", rootTs: ROOT_TS, endpointGeneration: GENERATION });
			expect(fixture.workspace.rootPosts()).toEqual([]);
			expect(await fixture.conversations()).toEqual([
				expect.objectContaining({ state: "active", sessionId: SESSION_ID, rootTs: ROOT_TS }),
			]);
		} finally {
			await fixture.cleanup();
		}
	});

	test("an ordinary unbound session still receives exactly one stock root on readiness", async () => {
		const fixture = await sessionFixture();
		try {
			await fixture
				.runtime([{ type: "event", name: "session_ready", sessionId: SESSION_ID, generation: GENERATION }])
				.start();

			expect(fixture.workspace.rootPosts()).toEqual([{ channel: "C1", text: "GJC session ready." }]);
			expect(await fixture.conversations()).toEqual([
				expect.objectContaining({ state: "active", sessionId: SESSION_ID, endpointGeneration: GENERATION }),
			]);
		} finally {
			await fixture.cleanup();
		}
	});

	test("a daemon that attaches after the binding adopts the existing root on late replay", async () => {
		const fixture = await sessionFixture();
		try {
			await fixture.daemon.bindExistingRoot(SESSION_ID, ROOT_TS);
			// The daemon was stopped while the session was prepared and bound; it
			// attaches later and replays the retained readiness signal.
			await fixture
				.runtime([{ type: "event", name: "session_ready", sessionId: SESSION_ID, generation: GENERATION }])
				.start();

			expect(fixture.workspace.rootPosts()).toEqual([]);
			expect(await fixture.conversations()).toEqual([
				expect.objectContaining({ state: "active", sessionId: SESSION_ID, rootTs: ROOT_TS }),
			]);
		} finally {
			await fixture.cleanup();
		}
	});

	/**
	 * The exact ordering the prepare phase exists to remove. A session that
	 * publishes readiness first already owns its single root claim, so the later
	 * binding is a definitive conflict rather than a silent second root.
	 */
	test("readiness published before a binding still refuses the late adoption", async () => {
		const fixture = await sessionFixture();
		try {
			await fixture
				.runtime([{ type: "event", name: "session_ready", sessionId: SESSION_ID, generation: GENERATION }])
				.start();

			await expect(fixture.daemon.bindExistingRoot(SESSION_ID, ROOT_TS)).rejects.toMatchObject({
				name: "SlackThreadBindingError",
				code: "session_conflict",
			});
			expect(fixture.workspace.rootPosts()).toHaveLength(1);
		} finally {
			await fixture.cleanup();
		}
	});
});

/** Every chat mutation verb one delivered frame could reach. */
interface ChatMutationProbe {
	fanoutTypes: string[];
	notifyCount: number;
	resumeCount: number;
	closeCount: number;
	resolveActionCount: number;
	restore: () => void;
}

/**
 * Observe the common presentation fanout and every Slack mutation verb the
 * runtime can reach, so "inert" is proven at the seam rather than inferred
 * from the absence of a post.
 */
function probeChatMutations(): ChatMutationProbe {
	const fanout = spyOn(NotificationPresentationEngine.prototype, "fanout");
	const notify = spyOn(SlackNotificationDaemon.prototype, "notify");
	const resume = spyOn(SlackNotificationDaemon.prototype, "resume");
	const close = spyOn(SlackNotificationDaemon.prototype, "close");
	const resolveAction = spyOn(SlackNotificationDaemon.prototype, "resolveAction");
	return {
		get fanoutTypes() {
			return fanout.mock.calls.map(([event]) => {
				const projected = event as NotificationEvent;
				return projected.type === "frame" ? String(projected.frame.type) : projected.type;
			});
		},
		get notifyCount() {
			return notify.mock.calls.length;
		},
		get resumeCount() {
			return resume.mock.calls.length;
		},
		get closeCount() {
			return close.mock.calls.length;
		},
		get resolveActionCount() {
			return resolveAction.mock.calls.length;
		},
		restore: () => {
			for (const spy of [fanout, notify, resume, close, resolveAction]) spy.mockRestore();
		},
	};
}

/**
 * One event frame carries at most one authority. A wrapper envelope and its
 * payload are two representations of the same event, so a frame that states a
 * different session, generation, or lifecycle identity in each is not a valid
 * event at all — it is an attempt to have the outer envelope pass one filter
 * while the payload supplies the identity a later step consumes.
 */
describe("chat daemon event envelope correlation", () => {
	/** A valid ordinary wrapper: outer transport name, payload event body. */
	function sentinel(text: string): SdkFrame {
		return {
			type: "event",
			name: "notification",
			sessionId: SESSION_ID,
			generation: GENERATION,
			payload: { type: "marker", sessionId: SESSION_ID, text },
		};
	}

	const LIFECYCLE_EVENTS = ["session_prepared", "session_ready", "session_closed", "session_terminated"] as const;

	async function deliver(fixture: SessionFixture, mode: "replay" | "live", frames: SdkFrame[]): Promise<void> {
		if (mode === "replay") {
			await fixture.runtime(frames).start();
			return;
		}
		await fixture.runtime([]).start();
		for (const frame of frames) fixture.emitLive(frame);
	}

	/**
	 * The frame is delivered before an ordinary sentinel on the same session's
	 * serialized frame tail, so the sentinel's own publication proves the frame
	 * under test was already handled — and published nothing of its own.
	 */
	async function expectInert(frame: SdkFrame, mode: "replay" | "live"): Promise<void> {
		const fixture = await sessionFixture();
		try {
			await deliver(fixture, mode, [frame, sentinel("ordering-sentinel")]);
			await fixture.waitFor(
				async () => (await fixture.conversations()).some(record => record.state === "active"),
				"sentinel notification delivery",
			);

			expect(fixture.workspace.rootPosts()).toEqual([{ channel: "C1", text: "GJC marker\nordering-sentinel" }]);
			expect(fixture.workspace.posts).toHaveLength(1);
			expect(await fixture.conversations()).toEqual([
				expect.objectContaining({ state: "active", sessionId: SESSION_ID, endpointGeneration: GENERATION }),
			]);
		} finally {
			await fixture.cleanup();
		}
	}

	/**
	 * Closure and resume are only observable against a mapping that already
	 * exists, so the same frame is replayed behind an established active root.
	 */
	async function expectInertAgainstActiveRoot(frame: SdkFrame, mode: "replay" | "live"): Promise<void> {
		const fixture = await sessionFixture();
		try {
			await deliver(fixture, mode, [sentinel("first"), frame, sentinel("second")]);
			await fixture.waitFor(
				async () => fixture.workspace.posts.some(post => post.text.includes("second")),
				"second sentinel delivery",
			);

			expect(fixture.workspace.rootPosts()).toEqual([{ channel: "C1", text: "GJC marker\nfirst" }]);
			expect(fixture.workspace.posts.map(post => post.text)).toEqual(["GJC marker\nfirst", "GJC marker\nsecond"]);
			expect(await fixture.conversations()).toEqual([
				expect.objectContaining({ state: "active", sessionId: SESSION_ID, endpointGeneration: GENERATION }),
			]);
		} finally {
			await fixture.cleanup();
		}
	}

	for (const mode of ["replay", "live"] as const) {
		/**
		 * The reviewer's split-authority frame: the outer envelope names a foreign
		 * session and an ordinary event, while the payload claims this attachment
		 * and a lifecycle identity. Adopting the payload session id with the outer
		 * event name is exactly what lets a lifecycle marker reach generic fanout.
		 */
		test(`a frame whose envelope and payload disagree on session and lifecycle identity is inert (${mode})`, async () => {
			await expectInert(
				{
					type: "event",
					name: "notification",
					sessionId: "foreign-session",
					generation: GENERATION,
					payload: { type: "session_prepared", sessionId: SESSION_ID, generation: GENERATION - 1 },
				},
				mode,
			);
		});

		test(`a foreign outer session cannot resume through an attached payload session id (${mode})`, async () => {
			await expectInert(
				{
					type: "event",
					name: "session_ready",
					sessionId: "foreign-session",
					generation: GENERATION,
					payload: { type: "session_ready", sessionId: SESSION_ID, generation: GENERATION },
				},
				mode,
			);
		});

		test(`a lifecycle frame whose envelope and payload disagree on generation is inert (${mode})`, async () => {
			await expectInert(
				{
					type: "event",
					name: "session_ready",
					sessionId: SESSION_ID,
					generation: GENERATION,
					payload: { type: "session_ready", sessionId: SESSION_ID, generation: GENERATION - 1 },
				},
				mode,
			);
		});

		for (const lifecycle of LIFECYCLE_EVENTS) {
			test(`a payload-smuggled ${lifecycle} under an ordinary envelope is inert (${mode})`, async () => {
				const smuggled: SdkFrame = {
					type: "event",
					name: "notification",
					sessionId: SESSION_ID,
					generation: GENERATION,
					payload: { type: lifecycle, sessionId: SESSION_ID, generation: GENERATION },
				};
				await expectInert(smuggled, mode);
				await expectInertAgainstActiveRoot(smuggled, mode);
			});
		}

		test(`a canonical prepared-then-ready sequence still publishes exactly one root (${mode})`, async () => {
			const fixture = await sessionFixture();
			try {
				await deliver(fixture, mode, [
					{ type: "event", name: "session_prepared", sessionId: SESSION_ID, generation: GENERATION },
					{ type: "event", name: "session_ready", sessionId: SESSION_ID, generation: GENERATION },
				]);
				await fixture.waitFor(
					async () => (await fixture.conversations()).some(record => record.state === "active"),
					"canonical readiness delivery",
				);

				expect(fixture.workspace.rootPosts()).toEqual([{ channel: "C1", text: "GJC session ready." }]);
				expect(fixture.workspace.posts).toHaveLength(1);
				expect(await fixture.conversations()).toEqual([
					expect.objectContaining({ state: "active", sessionId: SESSION_ID, endpointGeneration: GENERATION }),
				]);
			} finally {
				await fixture.cleanup();
			}
		});

		test(`a valid ordinary notification wrapper still delivers its payload exactly once (${mode})`, async () => {
			const fixture = await sessionFixture();
			try {
				await deliver(fixture, mode, [sentinel("only-once")]);
				await fixture.waitFor(
					async () => (await fixture.conversations()).some(record => record.state === "active"),
					"ordinary notification delivery",
				);

				expect(fixture.workspace.rootPosts()).toEqual([{ channel: "C1", text: "GJC marker\nonly-once" }]);
				expect(fixture.workspace.posts).toHaveLength(1);
			} finally {
				await fixture.cleanup();
			}
		});

		/**
		 * A duplicated identity is one authority tuple, not two, whatever the
		 * event's class. An ordinary wrapper that states one generation in its
		 * envelope and another in its payload is not a usable event: reading
		 * either side lets the frame clear one filter while a later step consumes
		 * the value the other side supplied.
		 */
		test(`an ordinary wrapper that repeats its identity unchanged still delivers exactly once (${mode})`, async () => {
			const fixture = await sessionFixture();
			try {
				await deliver(fixture, mode, [
					{
						type: "event",
						name: "notification",
						sessionId: SESSION_ID,
						generation: GENERATION,
						payload: {
							type: "marker",
							sessionId: SESSION_ID,
							generation: GENERATION,
							text: "agreed-duplicate",
						},
					},
				]);
				await fixture.waitFor(
					async () => (await fixture.conversations()).some(record => record.state === "active"),
					"agreed duplicate identity delivery",
				);

				expect(fixture.workspace.rootPosts()).toEqual([{ channel: "C1", text: "GJC marker\nagreed-duplicate" }]);
				expect(fixture.workspace.posts).toHaveLength(1);
			} finally {
				await fixture.cleanup();
			}
		});

		test(`an ordinary wrapper whose envelope and payload disagree on generation is inert (${mode})`, async () => {
			const conflicting: SdkFrame = {
				type: "event",
				name: "notification",
				sessionId: SESSION_ID,
				generation: GENERATION,
				payload: {
					type: "marker",
					sessionId: SESSION_ID,
					generation: GENERATION - 1,
					text: "conflicting-generation",
				},
			};
			await expectInert(conflicting, mode);
			await expectInertAgainstActiveRoot(conflicting, mode);
		});

		/**
		 * A duplicate that cannot be the identity it claims is never read as an
		 * absent duplicate. Treating it as absent silently promotes the other
		 * representation to sole authority over a frame that stated two.
		 */
		test(`an ordinary wrapper whose duplicated generation is malformed is inert (${mode})`, async () => {
			for (const generation of ["4", 4.5, -1, null, true]) {
				await expectInert(
					{
						type: "event",
						name: "notification",
						sessionId: SESSION_ID,
						generation: GENERATION,
						payload: { type: "marker", sessionId: SESSION_ID, generation, text: "malformed-generation" },
					},
					mode,
				);
			}
		}, 20_000);

		test(`an ordinary wrapper whose duplicated session id is malformed is inert (${mode})`, async () => {
			for (const sessionId of [7, "", null, false]) {
				await expectInert(
					{
						type: "event",
						name: "notification",
						sessionId: SESSION_ID,
						generation: GENERATION,
						payload: { type: "marker", sessionId, text: "malformed-session-id" },
					},
					mode,
				);
			}
		}, 20_000);

		test(`an ordinary wrapper whose envelope and payload disagree on session id is inert (${mode})`, async () => {
			await expectInert(
				{
					type: "event",
					name: "notification",
					sessionId: SESSION_ID,
					generation: GENERATION,
					payload: { type: "marker", sessionId: "session-2", text: "conflicting-session-id" },
				},
				mode,
			);
		});
	}
});

/**
 * A duplicated identity is stated by property *presence*, not by value, and one
 * frame's event identity is stated by `name` and `kind` together.
 *
 * A representation that owns `sessionId` or `generation` has stated it, so a
 * value that cannot be that identity — `undefined` included — is a malformed
 * duplicate rather than an absent one. Reading it as absent silently promotes
 * the other representation to sole authority over a frame that stated two.
 *
 * The same holds for the event name: `name` and `kind` are two spellings of one
 * identity, so a frame that spells a benign transport name in one and a
 * reserved lifecycle or control-plane discriminant in the other is not an event
 * at all. Preferring either alias lets the frame clear one filter while the
 * other supplies the identity a later step consumes.
 */
describe("chat daemon duplicated identity and alias authority", () => {
	/** A valid ordinary wrapper: outer transport name, payload event body. */
	function aliasSentinel(text: string): SdkFrame {
		return {
			type: "event",
			name: "notification",
			sessionId: SESSION_ID,
			generation: GENERATION,
			payload: { type: "marker", sessionId: SESSION_ID, text },
		};
	}

	async function deliver(frames: SdkFrame[], fixture: SessionFixture, mode: "replay" | "live"): Promise<void> {
		if (mode === "replay") {
			await fixture.runtime(frames).start();
			return;
		}
		await fixture.runtime([]).start();
		for (const frame of frames) fixture.emitLive(frame);
	}

	/**
	 * Deliver `frame` ahead of an ordinary sentinel on the same session's
	 * serialized frame tail. The sentinel's own publication proves the frame
	 * under test was already handled, and every counter proves it mutated
	 * nothing: no fanout, no adapter body, no root, no mapping, no resume, no
	 * close, and no action or reply.
	 */
	async function expectInertBeforeSentinel(frame: SdkFrame, mode: "replay" | "live"): Promise<void> {
		const fixture = await sessionFixture();
		const probe = probeChatMutations();
		try {
			await deliver([frame, aliasSentinel("alias-sentinel")], fixture, mode);
			await fixture.waitFor(
				async () => (await fixture.conversations()).some(record => record.state === "active"),
				"sentinel notification delivery",
			);

			expect(probe.fanoutTypes).toEqual(["marker"]);
			expect(probe.notifyCount).toBe(1);
			expect(probe.resumeCount).toBe(0);
			expect(probe.closeCount).toBe(0);
			expect(probe.resolveActionCount).toBe(0);
			expect(fixture.workspace.posts).toEqual([
				{ channel: "C1", text: "GJC marker\nalias-sentinel", threadTs: undefined },
			]);
			expect(fixture.sentFrames).toEqual([]);
			expect(await fixture.conversations()).toEqual([
				expect.objectContaining({ state: "active", sessionId: SESSION_ID, endpointGeneration: GENERATION }),
			]);
		} finally {
			probe.restore();
			await fixture.cleanup();
		}
	}

	/** A compatible frame still reaches presentation exactly once. */
	async function expectDeliveredOnce(frame: SdkFrame, mode: "replay" | "live", text: string): Promise<void> {
		const fixture = await sessionFixture();
		const probe = probeChatMutations();
		try {
			await deliver([frame], fixture, mode);
			await fixture.waitFor(
				async () => (await fixture.conversations()).some(record => record.state === "active"),
				"compatible frame delivery",
			);

			expect(probe.fanoutTypes).toEqual(["marker"]);
			expect(probe.notifyCount).toBe(1);
			expect(fixture.workspace.rootPosts()).toEqual([{ channel: "C1", text: `GJC marker\n${text}` }]);
			expect(fixture.workspace.posts).toHaveLength(1);
			expect(await fixture.conversations()).toHaveLength(1);
		} finally {
			probe.restore();
			await fixture.cleanup();
		}
	}

	for (const mode of ["replay", "live"] as const) {
		test(`a payload that owns sessionId or generation as undefined is a malformed duplicate (${mode})`, async () => {
			for (const payload of [
				{ type: "marker", sessionId: undefined, text: "payload-undefined-session" },
				{ type: "marker", sessionId: SESSION_ID, generation: undefined, text: "payload-undefined-generation" },
			]) {
				await expectInertBeforeSentinel(
					{ type: "event", name: "notification", sessionId: SESSION_ID, generation: GENERATION, payload },
					mode,
				);
			}
		}, 20_000);

		test(`an envelope that owns sessionId or generation as undefined is a malformed duplicate (${mode})`, async () => {
			for (const envelope of [
				{ sessionId: undefined, generation: GENERATION },
				{ sessionId: SESSION_ID, generation: undefined },
				{ sessionId: null, generation: GENERATION },
			]) {
				await expectInertBeforeSentinel(
					{
						type: "event",
						name: "notification",
						...envelope,
						payload: {
							type: "marker",
							sessionId: SESSION_ID,
							generation: GENERATION,
							text: "envelope-undefined-identity",
						},
					},
					mode,
				);
			}
		}, 20_000);

		test(`a lifecycle payload that owns generation as undefined never resumes (${mode})`, async () => {
			await expectInertBeforeSentinel(
				{
					type: "event",
					name: "session_ready",
					sessionId: SESSION_ID,
					generation: GENERATION,
					payload: { type: "session_ready", sessionId: SESSION_ID, generation: undefined },
				},
				mode,
			);
		});

		test(`an ordinary name cannot hide a reserved kind (${mode})`, async () => {
			for (const kind of ["control_response", "event_replay_result", "session_closed", "session_terminated"]) {
				await expectInertBeforeSentinel(
					{
						type: "event",
						name: "notification",
						kind,
						sessionId: SESSION_ID,
						generation: GENERATION,
						payload: { type: "marker", sessionId: SESSION_ID, text: `smuggled-${kind}` },
					},
					mode,
				);
			}
		}, 30_000);

		test(`a reserved name cannot hide behind an ordinary kind (${mode})`, async () => {
			for (const name of ["session_closed", "session_terminated", "control_response"]) {
				await expectInertBeforeSentinel(
					{ type: "event", name, kind: "notification", sessionId: SESSION_ID, generation: GENERATION },
					mode,
				);
			}
		}, 30_000);

		test(`a malformed name or kind alias is inert (${mode})`, async () => {
			for (const alias of [
				{ name: "notification", kind: 42 },
				{ name: undefined, kind: "notification" },
				{ name: "notification", kind: undefined },
				{ name: null, kind: "notification" },
			]) {
				await expectInertBeforeSentinel(
					{
						type: "event",
						...alias,
						sessionId: SESSION_ID,
						generation: GENERATION,
						payload: { type: "marker", sessionId: SESSION_ID, text: "malformed-alias" },
					},
					mode,
				);
			}
		}, 30_000);

		test(`equal name and kind aliases still deliver exactly once (${mode})`, async () => {
			await expectDeliveredOnce(
				{
					type: "event",
					name: "notification",
					kind: "notification",
					sessionId: SESSION_ID,
					generation: GENERATION,
					payload: { type: "marker", sessionId: SESSION_ID, text: "equal-alias" },
				},
				mode,
				"equal-alias",
			);
		});

		test(`a single name or kind alias stays compatible (${mode})`, async () => {
			await expectDeliveredOnce(
				{
					type: "event",
					kind: "notification",
					sessionId: SESSION_ID,
					generation: GENERATION,
					payload: { type: "marker", sessionId: SESSION_ID, text: "kind-only" },
				},
				mode,
				"kind-only",
			);
			await expectDeliveredOnce(aliasSentinel("name-only"), mode, "name-only");
		});

		test(`a one-sided identity and an equal duplicate both deliver exactly once (${mode})`, async () => {
			await expectDeliveredOnce(
				{
					type: "event",
					name: "notification",
					sessionId: SESSION_ID,
					generation: GENERATION,
					payload: { type: "marker", text: "one-sided-identity" },
				},
				mode,
				"one-sided-identity",
			);
			await expectDeliveredOnce(
				{
					type: "event",
					name: "notification",
					sessionId: SESSION_ID,
					generation: GENERATION,
					payload: {
						type: "marker",
						sessionId: SESSION_ID,
						generation: GENERATION,
						text: "equal-duplicate-identity",
					},
				},
				mode,
				"equal-duplicate-identity",
			);
		});
	}
});

/**
 * SDK protocol responses share the one session socket the chat runtime observes
 * for user-visible events: `SdkClient` settles a request's pending promise and
 * still forwards the very same frame to every `onFrame` observer. The runtime
 * therefore sees its own `event_replay` answer, and a runtime that projects
 * unknown frame types into the generic notification path publishes protocol
 * traffic — `GJC event replay result` — as chat content. These frames are
 * control-plane only: they must be completely inert on the live path and the
 * replay path alike.
 */
describe("chat daemon control-plane frame suppression", () => {
	/** A valid ordinary wrapper: outer transport name, payload event body. */
	function controlSentinel(text: string): SdkFrame {
		return {
			type: "event",
			name: "notification",
			sessionId: SESSION_ID,
			generation: GENERATION,
			payload: { type: "marker", sessionId: SESSION_ID, text },
		};
	}

	/** The exact host response to the runtime's own startup `event_replay`. */
	function replayResult(overrides: SdkFrame = {}): SdkFrame {
		return {
			type: "event_replay_result",
			id: "replay-1",
			ok: true,
			generation: GENERATION,
			lastSeq: 3,
			events: [
				{ type: "event", name: "notification", sessionId: SESSION_ID, generation: GENERATION, seq: 1 },
				{ type: "event", kind: "turn_stream", payload: { type: "turn_stream", text: "smuggled-replay-body" } },
			],
			...overrides,
		};
	}

	/** Every text a leaked control frame could render into a chat body. */
	const FORBIDDEN_BODIES = [
		"GJC event replay result",
		"GJC control response",
		"GJC query response",
		"GJC hello",
		"smuggled-replay-body",
	];

	/**
	 * Deliver `frame` ahead of an ordinary sentinel on the same session's
	 * serialized frame tail. The sentinel's own publication proves the frame
	 * under test was already handled, and every counter proves it mutated
	 * nothing: no fanout, no adapter body, no root, no mapping, no resume, no
	 * close, and no action or reply.
	 */
	async function expectControlFrameInert(frame: SdkFrame, mode: "replay" | "live"): Promise<void> {
		const fixture = await sessionFixture();
		const probe = probeChatMutations();
		try {
			const frames = [frame, controlSentinel("control-plane-sentinel")];
			if (mode === "replay") await fixture.runtime(frames).start();
			else {
				await fixture.runtime([]).start();
				for (const delivered of frames) fixture.emitLive(delivered);
			}
			await fixture.waitFor(
				async () => (await fixture.conversations()).some(record => record.state === "active"),
				"sentinel notification delivery",
			);

			// Only the sentinel ever reached presentation fanout or the adapter.
			expect(probe.fanoutTypes).toEqual(["marker"]);
			expect(probe.notifyCount).toBe(1);
			expect(fixture.workspace.posts).toEqual([
				{ channel: "C1", text: "GJC marker\ncontrol-plane-sentinel", threadTs: undefined },
			]);
			expect(fixture.workspace.rootPosts()).toEqual([{ channel: "C1", text: "GJC marker\ncontrol-plane-sentinel" }]);
			for (const forbidden of FORBIDDEN_BODIES)
				expect(fixture.workspace.posts.some(post => post.text.includes(forbidden))).toBe(false);

			// No lifecycle, mapping, or action mutation was reachable from it.
			expect(probe.resumeCount).toBe(0);
			expect(probe.closeCount).toBe(0);
			expect(probe.resolveActionCount).toBe(0);
			expect(fixture.sentFrames).toEqual([]);
			expect(await fixture.conversations()).toEqual([
				expect.objectContaining({ state: "active", sessionId: SESSION_ID, endpointGeneration: GENERATION }),
			]);
		} finally {
			probe.restore();
			await fixture.cleanup();
		}
	}

	for (const mode of ["replay", "live"] as const) {
		test(`a raw event_replay_result response is completely inert (${mode})`, async () => {
			await expectControlFrameInert(replayResult(), mode);
		});

		test(`an ordinary transport wrapper carrying event_replay_result is inert (${mode})`, async () => {
			await expectControlFrameInert(
				{
					type: "event",
					kind: "event_replay_result",
					sessionId: SESSION_ID,
					generation: GENERATION,
					payload: replayResult(),
				},
				mode,
			);
		});

		test(`an event_replay_result smuggled under an ordinary envelope fails closed (${mode})`, async () => {
			await expectControlFrameInert(
				{
					type: "event",
					name: "notification",
					sessionId: SESSION_ID,
					generation: GENERATION,
					payload: replayResult(),
				},
				mode,
			);
		});

		test(`an event_replay_result claiming a foreign session or a stale generation is inert (${mode})`, async () => {
			await expectControlFrameInert(replayResult({ sessionId: "foreign-session" }), mode);
			await expectControlFrameInert(replayResult({ sessionId: SESSION_ID, generation: GENERATION - 1 }), mode);
		});

		test(`a control_response, query_response, or handshake hello is inert (${mode})`, async () => {
			await expectControlFrameInert(
				{ type: "control_response", id: "control-1", ok: true, result: { sessionId: SESSION_ID } },
				mode,
			);
			await expectControlFrameInert({ type: "query_response", id: "query-1", ok: true, result: {} }, mode);
			await expectControlFrameInert(
				{ type: "hello", protocolVersion: 3, connectionId: "connection:1", capabilities: ["threaded"] },
				mode,
			);
		});
	}

	test("a replayed and then re-delivered event_replay_result publishes zero roots", async () => {
		const fixture = await sessionFixture();
		const probe = probeChatMutations();
		try {
			// Startup replay carries the response, then a reconnect re-delivers the
			// identical frame on the live path.
			await fixture.runtime([replayResult(), replayResult()]).start();
			fixture.emitLive(replayResult());
			fixture.emitLive(controlSentinel("after-reconnect"));
			await fixture.waitFor(
				async () => (await fixture.conversations()).some(record => record.state === "active"),
				"post-reconnect sentinel delivery",
			);

			expect(probe.fanoutTypes).toEqual(["marker"]);
			expect(fixture.workspace.rootPosts()).toEqual([{ channel: "C1", text: "GJC marker\nafter-reconnect" }]);
			expect(fixture.workspace.posts).toHaveLength(1);
			expect(await fixture.conversations()).toHaveLength(1);
		} finally {
			probe.restore();
			await fixture.cleanup();
		}
	});

	test("session_prepared stays inert and an exact-generation session_ready still readies once", async () => {
		const fixture = await sessionFixture();
		const probe = probeChatMutations();
		try {
			await fixture
				.runtime([
					replayResult(),
					{ type: "event", name: "session_prepared", sessionId: SESSION_ID, generation: GENERATION },
					{ type: "event", name: "session_ready", sessionId: SESSION_ID, generation: GENERATION },
					{ type: "event", name: "session_ready", sessionId: SESSION_ID, generation: GENERATION },
				])
				.start();

			expect(probe.resumeCount).toBe(2);
			expect(fixture.workspace.rootPosts()).toEqual([{ channel: "C1", text: "GJC session ready." }]);
			expect(fixture.workspace.posts).toHaveLength(1);
			expect(await fixture.conversations()).toEqual([
				expect.objectContaining({ state: "active", sessionId: SESSION_ID, endpointGeneration: GENERATION }),
			]);
		} finally {
			probe.restore();
			await fixture.cleanup();
		}
	});
});

describe("Slack binding activation authority", () => {
	test("activation is refused until an exact mapping for this session and generation exists", async () => {
		const fixture = await sessionFixture();
		const gate = createSlackBindingActivationGate({ store: fixture.store, teamId: "T1", channelId: "C1" });
		try {
			expect(await gate({ sessionId: SESSION_ID, generation: GENERATION })).toBe(false);

			await fixture.daemon.bindExistingRoot(SESSION_ID, ROOT_TS);
			expect(await gate({ sessionId: SESSION_ID, generation: GENERATION })).toBe(true);
			// A different session, a rolled endpoint generation, and a foreign target
			// are all refusals, never authorizations.
			expect(await gate({ sessionId: "session-2", generation: GENERATION })).toBe(false);
			expect(await gate({ sessionId: SESSION_ID, generation: GENERATION + 1 })).toBe(false);
			expect(
				await createSlackBindingActivationGate({ store: fixture.store, teamId: "T9", channelId: "C1" })({
					sessionId: SESSION_ID,
					generation: GENERATION,
				}),
			).toBe(false);
		} finally {
			await fixture.cleanup();
		}
	});

	test("a prepared host publishes readiness only after its binding exists", async () => {
		const fixture = await sessionFixture();
		const readiness: SdkFrame[] = [];
		const host = new SessionSdkHost({
			sessionId: SESSION_ID,
			stateRoot: path.join(fixture.repo, ".gjc", "state"),
			token: "endpoint-token",
			sendFrame: () => undefined,
			onFrame: () => undefined,
			readiness: "deferred",
			activationGate: createSlackBindingActivationGate({ store: fixture.store, teamId: "T1", channelId: "C1" }),
		});
		try {
			await host.start();
			expect(await host.activate(host.generation)).toBe("not_authorized");
			expect(host.events.replay(0).events.filter(frame => frame.name === "session_ready")).toEqual([]);

			// The daemon owner binds at exactly this session's endpoint generation.
			await fixture.daemonAt(host.generation).bindExistingRoot(SESSION_ID, ROOT_TS);
			expect(await host.activate(host.generation)).toBe("activated");
			expect(await host.activate(host.generation)).toBe("already");
			readiness.push(...host.events.replay(0).events.filter(frame => frame.name === "session_ready"));
			expect(readiness).toHaveLength(1);
		} finally {
			await host.stop();
			await fixture.cleanup();
		}
	});

	test("an unreadable mapping store is never read as authorization", async () => {
		const fixture = await sessionFixture();
		const gate = createSlackBindingActivationGate({ store: fixture.store, teamId: "T1", channelId: "C1" });
		try {
			await fs.mkdir(path.dirname(fixture.store.filePath), { recursive: true });
			await fs.writeFile(fixture.store.filePath, "{not json");
			await expect(gate({ sessionId: SESSION_ID, generation: GENERATION })).rejects.toThrow();
		} finally {
			await fixture.cleanup();
		}
	});
});

describe("prepared session activation client", () => {
	function respondingClient(answer: Record<string, unknown> | (() => never)) {
		const sent: Array<Record<string, unknown>> = [];
		return {
			sent,
			connect: async () => ({
				request: async (frame: Record<string, unknown>) => {
					sent.push(frame);
					if (typeof answer === "function") answer();
					return { ...answer, id: frame.id };
				},
				close: async () => {},
			}),
		};
	}

	test("activates through the exact session endpoint and reports the settled status", async () => {
		const fixture = await sessionFixture();
		const client = respondingClient({
			type: "session_activate_result",
			ok: true,
			status: "activated",
			sessionId: SESSION_ID,
			generation: GENERATION,
		});
		try {
			expect(
				await activatePreparedSession(
					{ sessionIndex: fixture.index, sessionId: SESSION_ID },
					{ connect: client.connect },
				),
			).toEqual({ sessionId: SESSION_ID, endpointGeneration: GENERATION, status: "activated" });
			expect(client.sent).toEqual([
				{ type: "session_activate", sessionId: SESSION_ID, endpointGeneration: GENERATION },
			]);
		} finally {
			await fixture.cleanup();
		}
	});

	test("refuses a session without exact discovery authority before any connection", async () => {
		const fixture = await sessionFixture();
		let connects = 0;
		try {
			await expect(
				activatePreparedSession(
					{ sessionIndex: fixture.index, sessionId: "session-2" },
					{
						connect: async () => {
							connects++;
							throw new Error("unreachable");
						},
					},
				),
			).rejects.toMatchObject({ name: "SessionActivationError", code: "session_not_live" });
			expect(connects).toBe(0);
		} finally {
			await fixture.cleanup();
		}
	});

	test("an answer that does not prove the requested session or generation is never a success", async () => {
		const fixture = await sessionFixture();
		const mismatches = [
			{ sessionId: "session-2", generation: GENERATION },
			{ sessionId: SESSION_ID, generation: GENERATION + 1 },
		];
		try {
			for (const mismatch of mismatches) {
				const client = respondingClient({
					type: "session_activate_result",
					ok: true,
					status: "activated",
					...mismatch,
				});
				await expect(
					activatePreparedSession(
						{ sessionIndex: fixture.index, sessionId: SESSION_ID },
						{ connect: client.connect },
					),
				).rejects.toMatchObject({ name: "SessionActivationError", code: "activation_outcome_unknown" });
			}
		} finally {
			await fixture.cleanup();
		}
	});

	test("a refused activation is reported by its exact cause", async () => {
		const fixture = await sessionFixture();
		const refusals: Array<{ code: string; expected: string }> = [
			{ code: "not_authorized", expected: "not_bound" },
			{ code: "not_prepared", expected: "not_prepared" },
			{ code: "generation_changed", expected: "session_not_live" },
			{ code: "session_mismatch", expected: "session_not_live" },
			{ code: "authority_unavailable", expected: "activation_unavailable" },
		];
		try {
			for (const refusal of refusals) {
				const client = {
					connect: async () => ({
						request: async () => {
							throw new SdkClientError(refusal.code, "refused");
						},
						close: async () => {},
					}),
				};
				await expect(
					activatePreparedSession(
						{ sessionIndex: fixture.index, sessionId: SESSION_ID },
						{ connect: client.connect },
					),
				).rejects.toMatchObject({ name: "SessionActivationError", code: refusal.expected });
			}
		} finally {
			await fixture.cleanup();
		}
	});

	test("a lost answer is indeterminate, never a failure, and an exact retry settles it", async () => {
		const fixture = await sessionFixture();
		let attempts = 0;
		const connect = async () => ({
			request: async (frame: Record<string, unknown>) => {
				attempts++;
				if (attempts === 1) throw new SdkClientError("connection_closed", "socket closed");
				return {
					type: "session_activate_result",
					id: frame.id,
					ok: true,
					status: "already",
					sessionId: SESSION_ID,
					generation: GENERATION,
				};
			},
			close: async () => {},
		});
		try {
			await expect(
				activatePreparedSession({ sessionIndex: fixture.index, sessionId: SESSION_ID }, { connect }),
			).rejects.toMatchObject({ name: "SessionActivationError", code: "activation_outcome_unknown" });
			expect(
				await activatePreparedSession({ sessionIndex: fixture.index, sessionId: SESSION_ID }, { connect }),
			).toEqual({ sessionId: SESSION_ID, endpointGeneration: GENERATION, status: "already" });
		} finally {
			await fixture.cleanup();
		}
	});

	test("an endpoint that cannot be reached is unavailable, never an applied activation", async () => {
		const fixture = await sessionFixture();
		try {
			await expect(
				activatePreparedSession(
					{ sessionIndex: fixture.index, sessionId: SESSION_ID },
					{
						connect: async () => {
							throw new SdkClientError("unavailable", "endpoint refused the connection");
						},
					},
				),
			).rejects.toMatchObject({ name: "SessionActivationError", code: "activation_unavailable" });
		} finally {
			await fixture.cleanup();
		}
	});
});

describe("existing-thread preparation opt-in", () => {
	test("preparation is inert unless the session opts in explicitly", () => {
		expect(isExistingThreadBindingRequested({})).toBe(false);
		expect(isExistingThreadBindingRequested({ [EXISTING_THREAD_BIND_ENV]: "" })).toBe(false);
		expect(isExistingThreadBindingRequested({ [EXISTING_THREAD_BIND_ENV]: "0" })).toBe(false);
		expect(isExistingThreadBindingRequested({ [EXISTING_THREAD_BIND_ENV]: "true" })).toBe(false);
		expect(isExistingThreadBindingRequested({ [EXISTING_THREAD_BIND_ENV]: "1" })).toBe(true);
	});
});
