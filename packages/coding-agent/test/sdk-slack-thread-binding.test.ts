import { describe, expect, test } from "bun:test";
import * as crypto from "node:crypto";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { Settings } from "../src/config/settings";
import { processIncarnation } from "../src/sdk/broker/process-incarnation";
import { SessionIndex } from "../src/sdk/broker/session-index";
import { runChatDaemonInternal } from "../src/sdk/bus/chat-daemon-cli";
import {
	buildChatDaemonCommandRequest,
	type ChatDaemonCommandOutcome,
	isChatDaemonCommandRequest,
	isChatDaemonCommandResponse,
	isChatDaemonCommandSettlement,
	serveChatDaemonCommandsAgainstScope,
	serveChatDaemonCommandsOnce,
	submitChatDaemonCommand,
} from "../src/sdk/bus/chat-daemon-command-channel";
import {
	type ChatDaemonCommandScope,
	closeChatDaemonCommandScope,
	openChatDaemonCommandScope,
	publishScopedJsonExclusive,
	readScopedJson,
	scopedEntryExists,
	unlinkScopedEntry,
	writeScopedJson,
} from "../src/sdk/bus/chat-daemon-command-scope";
import {
	acquireChatDaemonOwnership,
	chatDaemonGeneration,
	chatDaemonIdentity,
	chatDaemonPaths,
	hasSafeChatDaemonStateShape,
	readChatDaemonState,
	renewChatDaemonHeartbeat,
	writeChatDaemonControlRequest,
} from "../src/sdk/bus/chat-daemon-control";
import { ChatDaemonRuntime } from "../src/sdk/bus/chat-daemon-runtime";
import { ConversationStore } from "../src/sdk/bus/conversation-store";
import type { SlackConversation } from "../src/sdk/bus/slack-conversation";
import { SlackNotificationDaemon } from "../src/sdk/bus/slack-daemon";
import { SlackProvider } from "../src/sdk/bus/slack-provider";
import {
	bindConfiguredSlackThread,
	claimSlackThreadBinding,
	resolveSessionBindingAuthority,
} from "../src/sdk/bus/slack-thread-binding";
import { MemoryConversationStoreFs } from "./fixtures/chat-daemon-stores";

const SLACK_SETTINGS = {
	"notifications.enabled": true,
	"notifications.slack.botToken": "xoxb-test-not-persisted",
	"notifications.slack.appToken": "xapp-test-not-persisted",
	"notifications.slack.workspaceId": "T1",
	"notifications.slack.channelId": "C1",
	"notifications.slack.authorizedUserId": "U1",
} as const;

function withAgentDir(settings: Settings, agentDir: string): Settings {
	return new Proxy(settings, {
		get(target, property) {
			if (property === "getAgentDir") return () => agentDir;
			const value = Reflect.get(target, property, target);
			return typeof value === "function" ? value.bind(target) : value;
		},
	});
}

/** A fake Slack workspace: it verifies roots by channel and records every publication. */
class FakeSlackWorkspace {
	readonly posts: Array<{ channel: string; text: string; threadTs?: string }> = [];
	readonly roots = new Map<string, string>();
	findCalls = 0;
	failFind = false;
	/** Runs after the root lookup resolves and before the caller commits anything. */
	onFindMessage?: () => Promise<void>;

	seedRoot(channel: string, ts: string): void {
		this.roots.set(ts, channel);
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
		this.findCalls++;
		if (this.failFind) throw new Error("slack unavailable");
		const channel = this.roots.get(input.ts);
		const found = channel === input.channel ? { channel, ts: input.ts } : null;
		await this.onFindMessage?.();
		return found;
	}
}

interface DaemonFixture {
	agentDir: string;
	settings: Settings;
	ownerId: string;
	incarnation: string;
	workspace: FakeSlackWorkspace;
	daemon: SlackNotificationDaemon;
	conversations(): Promise<SlackConversation[]>;
	stateFile(): Promise<string>;
	controlFile(): Promise<string | undefined>;
	commandFiles(): Promise<string[]>;
	/** Command material excluding the durable replay-arbitration records. */
	pendingCommandFiles(): Promise<string[]>;
	cleanup(): Promise<void>;
}

async function daemonFixture(overrides: Record<string, unknown> = {}): Promise<DaemonFixture> {
	const agentDir = await fs.mkdtemp(path.join(os.tmpdir(), "gjc-slack-bind-"));
	const settings = withAgentDir(Settings.isolated({ ...SLACK_SETTINGS, ...overrides }), agentDir);
	const ownerId = `${process.pid}-bind-thread-test`;
	const incarnation = processIncarnation(process.pid)!;
	const identity = chatDaemonIdentity(settings, "slack");
	if (identity)
		await acquireChatDaemonOwnership({ agentDir, kind: "slack", ownerId, pid: process.pid, identity, incarnation });
	if (identity)
		await renewChatDaemonHeartbeat({
			agentDir,
			kind: "slack",
			ownerId,
			pid: process.pid,
			incarnation,
			transportHealthy: true,
		});
	const workspace = new FakeSlackWorkspace();
	const daemon = new SlackNotificationDaemon({
		agentDir,
		repo: agentDir,
		teamId: "T1",
		channelId: "C1",
		provider: new SlackProvider(workspace),
		createClient: () => ({ send() {} }),
		resolveEndpoint: async sessionId => ({
			sessionId,
			url: "ws://localhost",
			token: "not-persisted",
			path: "",
			generation: 7,
		}),
	});
	const paths = chatDaemonPaths(agentDir, "slack");
	const commandFiles = async (): Promise<string[]> =>
		await fs.readdir(path.join(paths.dir, "commands")).catch(() => [] as string[]);
	return {
		agentDir,
		settings,
		ownerId,
		incarnation,
		workspace,
		daemon,
		conversations: async () =>
			Object.values(
				(await new ConversationStore<SlackConversation>({ agentDir, kind: "slack" }).load()).conversations,
			),
		stateFile: async () => await fs.readFile(paths.state, "utf8"),
		controlFile: async () => await fs.readFile(paths.control, "utf8").catch(() => undefined),
		commandFiles,
		pendingCommandFiles: async () => (await commandFiles()).filter(name => !name.endsWith(".settled.json")),
		cleanup: async () => {
			await daemon.stop();
			await fs.rm(agentDir, { recursive: true, force: true });
		},
	};
}

/** Drives the real daemon-side command server the way the owner loop does. */
function pumpCommands(input: {
	agentDir: string;
	ownerId: string;
	pid?: number;
	incarnation: string;
	verifyOwnership?: () => Promise<boolean>;
	bind: (request: {
		sessionId: string;
		rootTs: string;
		commitAuthority?: () => Promise<boolean>;
	}) => Promise<ChatDaemonCommandOutcome>;
}): { stop: () => Promise<void> } {
	let running = true;
	const loop = (async () => {
		while (running) {
			await serveChatDaemonCommandsOnce({
				agentDir: input.agentDir,
				kind: "slack",
				ownerId: input.ownerId,
				pid: input.pid ?? process.pid,
				incarnation: input.incarnation,
				generation: chatDaemonGeneration("slack"),
				handler: { bindExistingRoot: input.bind },
				...(input.verifyOwnership ? { verifyOwnership: input.verifyOwnership } : {}),
			});
			await Bun.sleep(2);
		}
	})();
	return {
		stop: async () => {
			running = false;
			await loop;
		},
	};
}

/** Publishes a bind outcome for a request the fixture daemon actually applies. */
async function bindThroughFixture(
	fixture: DaemonFixture,
	request: { sessionId: string; rootTs: string; commitAuthority?: () => Promise<boolean> },
): Promise<ChatDaemonCommandOutcome> {
	const bound = await fixture.daemon.bindExistingRoot(request.sessionId, request.rootTs, request.commitAuthority);
	if (!bound.rootTs || bound.endpointGeneration === undefined)
		return { ok: false, certainty: "rejected", code: "binding_failed" };
	return {
		ok: true,
		sessionId: request.sessionId,
		endpointGeneration: bound.endpointGeneration,
		teamId: bound.teamId,
		channelId: bound.channelId,
		rootTs: bound.rootTs,
	};
}

function commandsDirectory(agentDir: string): string {
	return path.join(chatDaemonPaths(agentDir, "slack").dir, "commands");
}

describe("gjc notify bind-thread control path", () => {
	test("adopts an existing root through the healthy running daemon without stopping it", async () => {
		const fixture = await daemonFixture();
		fixture.workspace.seedRoot("C1", "1785573662.132329");
		const stateBefore = await fixture.stateFile();
		const server = pumpCommands({
			agentDir: fixture.agentDir,
			ownerId: fixture.ownerId,
			incarnation: fixture.incarnation,
			bind: async request => await bindThroughFixture(fixture, request),
		});
		try {
			const bound = await bindConfiguredSlackThread(
				{ settings: fixture.settings, sessionId: "session-1", threadTs: "1785573662.132329" },
				{ ensureDaemon: async () => "attached" },
			);

			expect(bound).toMatchObject({
				sessionId: "session-1",
				endpointGeneration: 7,
				teamId: "T1",
				channelId: "C1",
				rootTs: "1785573662.132329",
				ownerId: fixture.ownerId,
			});
			expect(await fixture.conversations()).toEqual([
				expect.objectContaining({
					state: "active",
					sessionId: "session-1",
					rootTs: "1785573662.132329",
					endpointGeneration: 7,
				}),
			]);
			expect(fixture.workspace.posts).toEqual([]);
			expect(await fixture.stateFile()).toBe(stateBefore);
			expect(await fixture.controlFile()).toBeUndefined();
		} finally {
			await server.stop();
			await fixture.cleanup();
		}
	});

	test("fails closed when the responding daemon is not the captured owner", async () => {
		const fixture = await daemonFixture();
		fixture.workspace.seedRoot("C1", "1785573662.132329");
		let binds = 0;
		const server = pumpCommands({
			agentDir: fixture.agentDir,
			ownerId: `${fixture.ownerId}-replacement`,
			incarnation: fixture.incarnation,
			bind: async () => {
				binds++;
				return { ok: false, certainty: "rejected", code: "binding_failed" };
			},
		});
		try {
			await expect(
				bindConfiguredSlackThread(
					{ settings: fixture.settings, sessionId: "session-1", threadTs: "1785573662.132329" },
					{ ensureDaemon: async () => "attached", timeoutMs: 2_000 },
				),
			).rejects.toMatchObject({ name: "SlackThreadBindingError", code: "daemon_owner_changed" });
			expect(binds).toBe(0);
			expect(await fixture.conversations()).toEqual([]);
			expect(fixture.workspace.posts).toEqual([]);
		} finally {
			await server.stop();
			await fixture.cleanup();
		}
	});

	test("refuses to bind unless the Slack target is completely configured", async () => {
		const incomplete: Array<{ name: string; overrides: Record<string, unknown> }> = [
			{ name: "disabled", overrides: { "notifications.enabled": false } },
			{ name: "missing bot token", overrides: { "notifications.slack.botToken": "" } },
			{ name: "missing app token", overrides: { "notifications.slack.appToken": "" } },
			{ name: "missing workspace", overrides: { "notifications.slack.workspaceId": "" } },
			{ name: "blank channel", overrides: { "notifications.slack.channelId": "   " } },
		];
		for (const scenario of incomplete) {
			const fixture = await daemonFixture(scenario.overrides);
			let ensured = 0;
			try {
				await expect(
					bindConfiguredSlackThread(
						{ settings: fixture.settings, sessionId: "session-1", threadTs: "1785573662.132329" },
						{
							ensureDaemon: async () => {
								ensured++;
								return "attached";
							},
							timeoutMs: 500,
						},
					),
				).rejects.toMatchObject({ name: "SlackThreadBindingError", code: "target_not_configured" });
				expect(ensured).toBe(0);
				expect(await fixture.conversations()).toEqual([]);
				expect(await fixture.pendingCommandFiles()).toEqual([]);
			} finally {
				await fixture.cleanup();
			}
		}
	});

	test("refuses to bind when no daemon owner is running and leaves no request behind", async () => {
		const fixture = await daemonFixture();
		try {
			await fs.rm(chatDaemonPaths(fixture.agentDir, "slack").state);
			await expect(
				bindConfiguredSlackThread(
					{ settings: fixture.settings, sessionId: "session-1", threadTs: "1785573662.132329" },
					{ ensureDaemon: async () => "attached", timeoutMs: 500 },
				),
			).rejects.toMatchObject({ name: "SlackThreadBindingError", code: "daemon_unavailable" });
			expect(await fixture.conversations()).toEqual([]);
			expect(await fixture.pendingCommandFiles()).toEqual([]);
		} finally {
			await fixture.cleanup();
		}
	});

	test("reports a daemon-side root rejection without persisting anything", async () => {
		const fixture = await daemonFixture();
		const server = pumpCommands({
			agentDir: fixture.agentDir,
			ownerId: fixture.ownerId,
			incarnation: fixture.incarnation,
			bind: async request => {
				try {
					await fixture.daemon.bindExistingRoot(request.sessionId, request.rootTs, request.commitAuthority);
					return { ok: false, certainty: "rejected", code: "binding_failed" };
				} catch (error) {
					return { ok: false, certainty: "rejected", code: (error as { code?: string }).code ?? "binding_failed" };
				}
			},
		});
		try {
			await expect(
				bindConfiguredSlackThread(
					{ settings: fixture.settings, sessionId: "session-1", threadTs: "1785573662.132329" },
					{ ensureDaemon: async () => "attached", timeoutMs: 2_000 },
				),
			).rejects.toMatchObject({ name: "SlackThreadBindingError", code: "root_not_found" });
			expect(await fixture.conversations()).toEqual([]);
			expect(await fixture.pendingCommandFiles()).toEqual([]);
			// The terminal outcome survives the submitter's own cleanup so the
			// identifier cannot be resurrected during the retention window.
			expect(await fixture.commandFiles()).toEqual([expect.stringMatching(/\.settled\.json$/)]);
		} finally {
			await server.stop();
			await fixture.cleanup();
		}
	});
});

describe("Slack session binding authority", () => {
	interface SessionFixture {
		repo: string;
		agentDir: string;
		index: SessionIndex;
		endpointPath: string;
		writeEndpoint(record: unknown): Promise<void>;
		cleanup(): Promise<void>;
	}

	async function sessionFixture(options: { endpointGeneration?: number; pid?: number } = {}): Promise<SessionFixture> {
		const repo = await fs.mkdtemp(path.join(os.tmpdir(), "gjc-slack-bind-repo-"));
		const agentDir = path.join(repo, ".gjc", "agent");
		const endpointDir = path.join(repo, ".gjc", "state", "sdk");
		await fs.mkdir(endpointDir, { recursive: true });
		const endpointPath = path.join(endpointDir, "session-1.json");
		const write = async (record: unknown) => {
			await fs.writeFile(endpointPath, JSON.stringify(record));
		};
		await write({ version: 1, url: "ws://127.0.0.1:1", token: "endpoint-token", pid: options.pid ?? process.pid });
		const index = await new SessionIndex(agentDir).open();
		await index.append({
			type: "host_registered",
			sessionId: "session-1",
			locator: { repo, stateRoot: path.join(repo, ".gjc", "state") },
			endpointGeneration: options.endpointGeneration ?? 3,
			pid: options.pid ?? process.pid,
		});
		return {
			repo,
			agentDir,
			index,
			endpointPath,
			writeEndpoint: write,
			cleanup: async () => {
				await fs.rm(repo, { recursive: true, force: true });
			},
		};
	}

	test("accepts an indexed live session whose discovery endpoint matches its host", async () => {
		const fixture = await sessionFixture();
		try {
			expect(
				await resolveSessionBindingAuthority({ sessionIndex: fixture.index, sessionId: "session-1" }),
			).toMatchObject({ sessionId: "session-1", endpointGeneration: 3, pid: process.pid, repo: fixture.repo });
		} finally {
			await fixture.cleanup();
		}
	});

	test("refuses a missing, malformed, stale, or foreign-pid discovery endpoint", async () => {
		const cases: Array<{ name: string; apply: (fixture: SessionFixture) => Promise<void> }> = [
			{ name: "missing", apply: async fixture => await fs.rm(fixture.endpointPath) },
			{ name: "malformed", apply: async fixture => await fs.writeFile(fixture.endpointPath, "{not json") },
			{
				name: "invalid record",
				apply: async fixture => await fixture.writeEndpoint({ version: 1, token: "t", pid: process.pid }),
			},
			{
				name: "stale",
				apply: async fixture =>
					await fixture.writeEndpoint({ version: 1, url: "ws://127.0.0.1:1", stale: true, pid: process.pid }),
			},
			{
				name: "pid mismatch",
				apply: async fixture =>
					await fixture.writeEndpoint({
						version: 1,
						url: "ws://127.0.0.1:1",
						token: "endpoint-token",
						pid: process.pid + 1,
					}),
			},
			{
				name: "absent pid",
				apply: async fixture =>
					await fixture.writeEndpoint({ version: 1, url: "ws://127.0.0.1:1", token: "endpoint-token" }),
			},
		];
		for (const scenario of cases) {
			const fixture = await sessionFixture();
			try {
				await scenario.apply(fixture);
				expect(
					await resolveSessionBindingAuthority({ sessionIndex: fixture.index, sessionId: "session-1" }),
				).toBeUndefined();
			} finally {
				await fixture.cleanup();
			}
		}
	});

	test("refuses unknown, unregistered, terminal, dead, and truncated-index sessions", async () => {
		const unknown = await sessionFixture();
		try {
			expect(
				await resolveSessionBindingAuthority({ sessionIndex: unknown.index, sessionId: "session-2" }),
			).toBeUndefined();
		} finally {
			await unknown.cleanup();
		}

		const unregistered = await sessionFixture();
		try {
			await unregistered.index.append({
				type: "host_unregistered",
				sessionId: "session-1",
				locator: { repo: unregistered.repo, stateRoot: path.join(unregistered.repo, ".gjc", "state") },
				endpointGeneration: 3,
				pid: process.pid,
			});
			expect(
				await resolveSessionBindingAuthority({ sessionIndex: unregistered.index, sessionId: "session-1" }),
			).toBeUndefined();
		} finally {
			await unregistered.cleanup();
		}

		const terminal = await sessionFixture();
		try {
			await terminal.index.append({
				type: "lifecycle_terminal",
				sessionId: "session-1",
				locator: { repo: terminal.repo, stateRoot: path.join(terminal.repo, ".gjc", "state") },
				endpointGeneration: 3,
				pid: process.pid,
			});
			expect(
				await resolveSessionBindingAuthority({ sessionIndex: terminal.index, sessionId: "session-1" }),
			).toBeUndefined();
		} finally {
			await terminal.cleanup();
		}

		const dead = await sessionFixture({ pid: 2 ** 22 - 1 });
		try {
			expect(
				await resolveSessionBindingAuthority({ sessionIndex: dead.index, sessionId: "session-1" }),
			).toBeUndefined();
		} finally {
			await dead.cleanup();
		}

		const truncated = await sessionFixture();
		try {
			await fs.appendFile(path.join(truncated.agentDir, "sdk", "sessions", "index.jsonl"), "{not json}\n");
			expect(
				await resolveSessionBindingAuthority({ sessionIndex: truncated.index, sessionId: "session-1" }),
			).toBeUndefined();
		} finally {
			await truncated.cleanup();
		}
	});

	test("leaves no mapping when session authority disappears immediately before the commit", async () => {
		const fixture = await daemonFixture();
		fixture.workspace.seedRoot("C1", "1785573662.132329");
		let calls = 0;
		const daemon = new SlackNotificationDaemon({
			agentDir: fixture.agentDir,
			repo: fixture.agentDir,
			teamId: "T1",
			channelId: "C1",
			provider: new SlackProvider(fixture.workspace),
			createClient: () => ({ send() {} }),
			resolveEndpoint: async () => null,
			resolveBindingAuthority: async sessionId => (++calls === 1 ? { sessionId, endpointGeneration: 5 } : undefined),
		});
		try {
			await expect(daemon.bindExistingRoot("session-1", "1785573662.132329")).rejects.toMatchObject({
				name: "SlackThreadBindingError",
				code: "session_not_live",
			});
			expect(calls).toBe(2);
			expect(await fixture.conversations()).toEqual([]);
			expect(fixture.workspace.posts).toEqual([]);
		} finally {
			await daemon.stop();
			await fixture.cleanup();
		}
	});

	test("leaves no mapping when the endpoint generation rolls immediately before the commit", async () => {
		const fixture = await daemonFixture();
		fixture.workspace.seedRoot("C1", "1785573662.132329");
		let calls = 0;
		const daemon = new SlackNotificationDaemon({
			agentDir: fixture.agentDir,
			repo: fixture.agentDir,
			teamId: "T1",
			channelId: "C1",
			provider: new SlackProvider(fixture.workspace),
			createClient: () => ({ send() {} }),
			resolveEndpoint: async () => null,
			resolveBindingAuthority: async sessionId => ({ sessionId, endpointGeneration: ++calls === 1 ? 5 : 6 }),
		});
		try {
			await expect(daemon.bindExistingRoot("session-1", "1785573662.132329")).rejects.toMatchObject({
				name: "SlackThreadBindingError",
				code: "session_not_live",
			});
			expect(await fixture.conversations()).toEqual([]);
		} finally {
			await daemon.stop();
			await fixture.cleanup();
		}
	});

	test("refuses a command whose owner record changed between capture and execution", async () => {
		const fixture = await daemonFixture();
		fixture.workspace.seedRoot("C1", "1785573662.132329");
		let binds = 0;
		let owned = true;
		const server = pumpCommands({
			agentDir: fixture.agentDir,
			ownerId: fixture.ownerId,
			incarnation: fixture.incarnation,
			verifyOwnership: async () => owned,
			bind: async request => {
				binds++;
				return await bindThroughFixture(fixture, request);
			},
		});
		try {
			owned = false;
			await expect(
				bindConfiguredSlackThread(
					{ settings: fixture.settings, sessionId: "session-1", threadTs: "1785573662.132329" },
					{ ensureDaemon: async () => "attached", timeoutMs: 2_000 },
				),
			).rejects.toMatchObject({ name: "SlackThreadBindingError", code: "daemon_owner_changed" });
			expect(binds).toBe(0);
			expect(await fixture.conversations()).toEqual([]);
		} finally {
			await server.stop();
			await fixture.cleanup();
		}
	});
});

describe("chat daemon command channel commit authority", () => {
	const ROOT_TS = "1785573662.132329";

	test("a definitive submit timeout is never followed by a conversation-store mutation", async () => {
		const fixture = await daemonFixture();
		fixture.workspace.seedRoot("C1", ROOT_TS);
		const atProvider = Promise.withResolvers<void>();
		const releaseProvider = Promise.withResolvers<void>();
		fixture.workspace.onFindMessage = async () => {
			atProvider.resolve();
			await releaseProvider.promise;
		};
		let binds = 0;
		const server = pumpCommands({
			agentDir: fixture.agentDir,
			ownerId: fixture.ownerId,
			incarnation: fixture.incarnation,
			bind: async request => {
				binds++;
				return await bindThroughFixture(fixture, request);
			},
		});
		try {
			const submission = bindConfiguredSlackThread(
				{ settings: fixture.settings, sessionId: "session-1", threadTs: ROOT_TS },
				{
					ensureDaemon: async () => "attached",
					timeoutMs: 80,
					pollIntervalMs: 5,
					settleGraceMs: 400,
				},
			);
			await atProvider.promise;
			await expect(submission).rejects.toMatchObject({
				name: "SlackThreadBindingError",
				code: "daemon_unavailable",
			});
			// The handler is resumed only after the caller already reported a
			// definitive failure. It must not be able to commit behind that answer.
			releaseProvider.resolve();
			await server.stop();
			expect(binds).toBe(1);
			expect(await fixture.conversations()).toEqual([]);
			expect(fixture.workspace.posts).toEqual([]);
			expect(await fixture.pendingCommandFiles()).toEqual([]);
		} finally {
			releaseProvider.resolve();
			await server.stop();
			await fixture.cleanup();
		}
	});

	test("a commit fence blocked past the deadline settles as cancelled with zero mutation", async () => {
		const fixture = await daemonFixture();
		fixture.workspace.seedRoot("C1", ROOT_TS);
		const atFence = Promise.withResolvers<void>();
		const releaseFence = Promise.withResolvers<void>();
		const server = pumpCommands({
			agentDir: fixture.agentDir,
			ownerId: fixture.ownerId,
			incarnation: fixture.incarnation,
			bind: async request =>
				await bindThroughFixture(fixture, {
					sessionId: request.sessionId,
					rootTs: request.rootTs,
					commitAuthority: async () => {
						atFence.resolve();
						await releaseFence.promise;
						return (await request.commitAuthority?.()) ?? true;
					},
				}),
		});
		try {
			const submission = bindConfiguredSlackThread(
				{ settings: fixture.settings, sessionId: "session-1", threadTs: ROOT_TS },
				{ ensureDaemon: async () => "attached", timeoutMs: 80, pollIntervalMs: 5, settleGraceMs: 400 },
			);
			await atFence.promise;
			await expect(submission).rejects.toMatchObject({
				name: "SlackThreadBindingError",
				code: "daemon_unavailable",
			});
			releaseFence.resolve();
			await server.stop();
			expect(await fixture.conversations()).toEqual([]);
			expect(await fixture.pendingCommandFiles()).toEqual([]);
		} finally {
			releaseFence.resolve();
			await server.stop();
			await fixture.cleanup();
		}
	});

	test("owner loss after provider verification answers owner_changed and mutates nothing", async () => {
		const fixture = await daemonFixture();
		fixture.workspace.seedRoot("C1", ROOT_TS);
		const stateBefore = await fixture.stateFile();
		let owned = true;
		// Ownership lapses after the pre-dispatch check and the provider proof,
		// while the binding transaction is still in flight.
		fixture.workspace.onFindMessage = async () => {
			owned = false;
		};
		let binds = 0;
		const server = pumpCommands({
			agentDir: fixture.agentDir,
			ownerId: fixture.ownerId,
			incarnation: fixture.incarnation,
			verifyOwnership: async () => owned,
			bind: async request => {
				binds++;
				return await bindThroughFixture(fixture, request);
			},
		});
		try {
			await expect(
				bindConfiguredSlackThread(
					{ settings: fixture.settings, sessionId: "session-1", threadTs: ROOT_TS },
					{ ensureDaemon: async () => "attached", timeoutMs: 2_000, pollIntervalMs: 5 },
				),
			).rejects.toMatchObject({ name: "SlackThreadBindingError", code: "daemon_owner_changed" });
			expect(binds).toBe(1);
			expect(await fixture.conversations()).toEqual([]);
			// The superseded owner never publishes a root and never touches the
			// lifecycle channel of its replacement.
			expect(fixture.workspace.posts).toEqual([]);
			expect(await fixture.stateFile()).toBe(stateBefore);
			expect(await fixture.controlFile()).toBeUndefined();
			expect(await fixture.pendingCommandFiles()).toEqual([]);
		} finally {
			await server.stop();
			await fixture.cleanup();
		}
	});

	test("a request addressed to a different pid is refused without dispatch or mutation", async () => {
		const fixture = await daemonFixture();
		fixture.workspace.seedRoot("C1", ROOT_TS);
		const state = await readChatDaemonState(fixture.agentDir, "slack");
		let binds = 0;
		const server = pumpCommands({
			agentDir: fixture.agentDir,
			ownerId: fixture.ownerId,
			incarnation: fixture.incarnation,
			bind: async request => {
				binds++;
				return await bindThroughFixture(fixture, request);
			},
		});
		try {
			const submission = await submitChatDaemonCommand({
				agentDir: fixture.agentDir,
				kind: "slack",
				owner: {
					ownerId: state!.ownerId,
					pid: state!.pid + 1,
					incarnation: state!.incarnation,
					generation: state!.generation,
				},
				command: "bind-thread",
				sessionId: "session-1",
				rootTs: ROOT_TS,
				timeoutMs: 2_000,
				pollIntervalMs: 5,
			});
			expect(submission).toMatchObject({
				outcome: "answered",
				response: { status: "owner_changed", ownerId: state!.ownerId, pid: state!.pid + 1 },
			});
			expect(binds).toBe(0);
			expect(await fixture.conversations()).toEqual([]);
			expect(await fixture.pendingCommandFiles()).toEqual([]);
		} finally {
			await server.stop();
			await fixture.cleanup();
		}
	});

	test("a success response that proves a different pid is untrusted material, never an answer", async () => {
		const fixture = await daemonFixture();
		fixture.workspace.seedRoot("C1", ROOT_TS);
		const directory = commandsDirectory(fixture.agentDir);
		let forging = true;
		const forger = (async () => {
			while (forging) {
				const names = await fs.readdir(directory).catch(() => [] as string[]);
				const requestName = names.find(name => name.endsWith(".request.json"));
				if (requestName) {
					const raw = await fs.readFile(path.join(directory, requestName), "utf8").catch(() => undefined);
					if (raw) {
						const request = JSON.parse(raw) as Record<string, unknown>;
						await fs.writeFile(
							path.join(directory, `${String(request.requestId)}.response.json`),
							`${JSON.stringify({
								version: 1,
								requestId: request.requestId,
								kind: "slack",
								command: "bind-thread",
								ownerId: request.ownerId,
								pid: (request.pid as number) + 1,
								incarnation: request.incarnation,
								generation: request.generation,
								status: "ok",
								sessionId: request.sessionId,
								endpointGeneration: 7,
								teamId: "T1",
								channelId: "C1",
								rootTs: request.rootTs,
								completedAt: Date.now(),
							})}\n`,
							{ mode: 0o600 },
						);
						return;
					}
				}
				await Bun.sleep(2);
			}
		})();
		try {
			await expect(
				bindConfiguredSlackThread(
					{ settings: fixture.settings, sessionId: "session-1", threadTs: ROOT_TS },
					{ ensureDaemon: async () => "attached", timeoutMs: 2_000, pollIntervalMs: 5 },
				),
			).rejects.toMatchObject({ name: "SlackThreadBindingError", code: "binding_outcome_unknown" });
			expect(await fixture.conversations()).toEqual([]);
		} finally {
			forging = false;
			await forger;
			await fixture.cleanup();
		}
	});

	test("a symlinked commands directory is refused before any external write or dispatch", async () => {
		const fixture = await daemonFixture();
		fixture.workspace.seedRoot("C1", ROOT_TS);
		const daemonDir = chatDaemonPaths(fixture.agentDir, "slack").dir;
		const outside = await fs.mkdtemp(path.join(os.tmpdir(), "gjc-slack-bind-escape-"));
		await fs.rm(path.join(daemonDir, "commands"), { recursive: true, force: true });
		await fs.symlink(outside, path.join(daemonDir, "commands"), "dir");
		let binds = 0;
		const server = pumpCommands({
			agentDir: fixture.agentDir,
			ownerId: fixture.ownerId,
			incarnation: fixture.incarnation,
			bind: async request => {
				binds++;
				return await bindThroughFixture(fixture, request);
			},
		});
		try {
			await expect(
				bindConfiguredSlackThread(
					{ settings: fixture.settings, sessionId: "session-1", threadTs: ROOT_TS },
					{ ensureDaemon: async () => "attached", timeoutMs: 200, pollIntervalMs: 5, settleGraceMs: 100 },
				),
			).rejects.toMatchObject({ name: "SlackThreadBindingError", code: "daemon_unavailable" });
			expect(await fs.readdir(outside)).toEqual([]);
			expect(binds).toBe(0);
			expect(await fixture.conversations()).toEqual([]);
		} finally {
			await server.stop();
			await fs.rm(outside, { recursive: true, force: true });
			await fixture.cleanup();
		}
	});

	test("an existing group/world-accessible commands directory is repaired to owner-only", async () => {
		const fixture = await daemonFixture();
		fixture.workspace.seedRoot("C1", ROOT_TS);
		const directory = commandsDirectory(fixture.agentDir);
		await fs.mkdir(directory, { recursive: true });
		await fs.chmod(directory, 0o777);
		const server = pumpCommands({
			agentDir: fixture.agentDir,
			ownerId: fixture.ownerId,
			incarnation: fixture.incarnation,
			bind: async request => await bindThroughFixture(fixture, request),
		});
		try {
			await bindConfiguredSlackThread(
				{ settings: fixture.settings, sessionId: "session-1", threadTs: ROOT_TS },
				{ ensureDaemon: async () => "attached", timeoutMs: 2_000, pollIntervalMs: 5 },
			);
			expect((await fs.stat(directory)).mode & 0o777).toBe(0o700);
		} finally {
			await server.stop();
			await fixture.cleanup();
		}
	});

	test("a non-regular commands directory is refused outright", async () => {
		const fixture = await daemonFixture();
		fixture.workspace.seedRoot("C1", ROOT_TS);
		const daemonDir = chatDaemonPaths(fixture.agentDir, "slack").dir;
		await fs.rm(path.join(daemonDir, "commands"), { recursive: true, force: true });
		await fs.writeFile(path.join(daemonDir, "commands"), "not a directory", { mode: 0o600 });
		try {
			await expect(
				bindConfiguredSlackThread(
					{ settings: fixture.settings, sessionId: "session-1", threadTs: ROOT_TS },
					{ ensureDaemon: async () => "attached", timeoutMs: 200, pollIntervalMs: 5, settleGraceMs: 100 },
				),
			).rejects.toMatchObject({ name: "SlackThreadBindingError", code: "daemon_unavailable" });
			expect(await fixture.conversations()).toEqual([]);
		} finally {
			await fixture.cleanup();
		}
	});

	test("symlinked and non-regular request entries never reach the handler", async () => {
		const fixture = await daemonFixture();
		fixture.workspace.seedRoot("C1", ROOT_TS);
		const state = await readChatDaemonState(fixture.agentDir, "slack");
		const directory = commandsDirectory(fixture.agentDir);
		await fs.mkdir(directory, { recursive: true, mode: 0o700 });
		const outside = await fs.mkdtemp(path.join(os.tmpdir(), "gjc-slack-bind-planted-"));
		const linkedId = crypto.randomUUID();
		const payload = path.join(outside, "payload.json");
		await fs.writeFile(
			payload,
			JSON.stringify({
				version: 1,
				requestId: linkedId,
				kind: "slack",
				command: "bind-thread",
				ownerId: state!.ownerId,
				pid: state!.pid,
				incarnation: state!.incarnation,
				generation: state!.generation,
				sessionId: "session-1",
				rootTs: ROOT_TS,
				createdAt: Date.now(),
				expiresAt: Date.now() + 60_000,
			}),
		);
		await fs.symlink(payload, path.join(directory, `${linkedId}.request.json`));
		await fs.mkdir(path.join(directory, `${crypto.randomUUID()}.request.json`));
		let binds = 0;
		try {
			const served = await serveChatDaemonCommandsOnce({
				agentDir: fixture.agentDir,
				kind: "slack",
				ownerId: fixture.ownerId,
				pid: process.pid,
				incarnation: fixture.incarnation,
				generation: chatDaemonGeneration("slack"),
				handler: {
					bindExistingRoot: async request => {
						binds++;
						return await bindThroughFixture(fixture, request);
					},
				},
			});
			expect(served).toBe(0);
			expect(binds).toBe(0);
			expect(await fixture.conversations()).toEqual([]);
		} finally {
			await fs.rm(outside, { recursive: true, force: true });
			await fixture.cleanup();
		}
	});

	test("a stale response for a reused request id can neither authorize nor suppress a command", async () => {
		const fixture = await daemonFixture();
		fixture.workspace.seedRoot("C1", ROOT_TS);
		const state = await readChatDaemonState(fixture.agentDir, "slack");
		const directory = commandsDirectory(fixture.agentDir);
		await fs.mkdir(directory, { recursive: true, mode: 0o700 });
		const requestId = crypto.randomUUID();
		await fs.writeFile(
			path.join(directory, `${requestId}.response.json`),
			`${JSON.stringify({
				version: 1,
				requestId,
				kind: "slack",
				command: "bind-thread",
				ownerId: state!.ownerId,
				pid: state!.pid,
				incarnation: state!.incarnation,
				generation: state!.generation,
				status: "ok",
				sessionId: "session-stale",
				endpointGeneration: 3,
				teamId: "T1",
				channelId: "C1",
				rootTs: "1700000000.000100",
				completedAt: Date.now() - 5_000,
			})}\n`,
			{ mode: 0o600 },
		);
		let binds = 0;
		const server = pumpCommands({
			agentDir: fixture.agentDir,
			ownerId: fixture.ownerId,
			incarnation: fixture.incarnation,
			bind: async request => {
				binds++;
				return await bindThroughFixture(fixture, request);
			},
		});
		try {
			const submission = await submitChatDaemonCommand({
				agentDir: fixture.agentDir,
				kind: "slack",
				owner: {
					ownerId: state!.ownerId,
					pid: state!.pid,
					incarnation: state!.incarnation,
					generation: state!.generation,
				},
				command: "bind-thread",
				sessionId: "session-1",
				rootTs: ROOT_TS,
				requestId,
				timeoutMs: 200,
				pollIntervalMs: 5,
				settleGraceMs: 100,
			});
			expect(submission).toEqual({ outcome: "unavailable", code: "request_id_unavailable" });
			expect(binds).toBe(0);
			expect(await fixture.conversations()).toEqual([]);
		} finally {
			await server.stop();
			await fixture.cleanup();
		}
	});

	test("a settled response blocks a replayed request from re-authorizing a mutation", async () => {
		const fixture = await daemonFixture();
		fixture.workspace.seedRoot("C1", ROOT_TS);
		const state = await readChatDaemonState(fixture.agentDir, "slack");
		const directory = commandsDirectory(fixture.agentDir);
		await fs.mkdir(directory, { recursive: true, mode: 0o700 });
		const requestId = crypto.randomUUID();
		const owner = {
			ownerId: state!.ownerId,
			pid: state!.pid,
			incarnation: state!.incarnation,
			generation: state!.generation,
		};
		await fs.writeFile(
			path.join(directory, `${requestId}.response.json`),
			`${JSON.stringify({
				version: 1,
				requestId,
				kind: "slack",
				command: "bind-thread",
				...owner,
				status: "rejected",
				code: "session_not_live",
				completedAt: Date.now(),
			})}\n`,
			{ mode: 0o600 },
		);
		await fs.writeFile(
			path.join(directory, `${requestId}.request.json`),
			`${JSON.stringify({
				version: 1,
				requestId,
				kind: "slack",
				command: "bind-thread",
				...owner,
				sessionId: "session-1",
				rootTs: ROOT_TS,
				createdAt: Date.now(),
				expiresAt: Date.now() + 60_000,
			})}\n`,
			{ mode: 0o600 },
		);
		let binds = 0;
		try {
			const served = await serveChatDaemonCommandsOnce({
				agentDir: fixture.agentDir,
				kind: "slack",
				ownerId: fixture.ownerId,
				pid: process.pid,
				incarnation: fixture.incarnation,
				generation: chatDaemonGeneration("slack"),
				handler: {
					bindExistingRoot: async request => {
						binds++;
						return await bindThroughFixture(fixture, request);
					},
				},
			});
			expect(served).toBe(0);
			expect(binds).toBe(0);
			expect(await fixture.conversations()).toEqual([]);
		} finally {
			await fixture.cleanup();
		}
	});

	test("a lost cancellation is reported as an unknown outcome, never as a failure", async () => {
		const fixture = await daemonFixture();
		fixture.workspace.seedRoot("C1", ROOT_TS);
		const directory = commandsDirectory(fixture.agentDir);
		await fs.mkdir(directory, { recursive: true, mode: 0o700 });
		let claimed = false;
		// Take the single-winner response object exactly once, after the request
		// is published and before the caller's deadline, then answer nothing.
		const sleep = async (ms: number): Promise<void> => {
			if (!claimed) {
				const names = await fs.readdir(directory).catch(() => [] as string[]);
				const requestName = names.find(name => name.endsWith(".request.json"));
				if (requestName) {
					const requestId = requestName.slice(0, -".request.json".length);
					const handle = await fs.open(path.join(directory, `${requestId}.response.json`), "wx", 0o600);
					await handle.close();
					claimed = true;
				}
			}
			await Bun.sleep(ms);
		};
		try {
			await expect(
				bindConfiguredSlackThread(
					{ settings: fixture.settings, sessionId: "session-1", threadTs: ROOT_TS },
					{
						ensureDaemon: async () => "attached",
						timeoutMs: 40,
						pollIntervalMs: 1,
						settleGraceMs: 20,
						sleep,
					},
				),
			).rejects.toMatchObject({ name: "SlackThreadBindingError", code: "binding_outcome_unknown" });
			expect(claimed).toBe(true);
			expect(await fixture.conversations()).toEqual([]);
		} finally {
			await fixture.cleanup();
		}
	});

	test("a persisted owner pid roll is caught by the commit fence with zero mutation", async () => {
		const fixture = await daemonFixture();
		fixture.workspace.seedRoot("C1", ROOT_TS);
		const paths = chatDaemonPaths(fixture.agentDir, "slack");
		// The production owner-loop predicate: the exact persisted tuple,
		// including pid, plus a record that is not stopped.
		const stillOwner = async (): Promise<boolean> => {
			const current = await readChatDaemonState(fixture.agentDir, "slack");
			return (
				hasSafeChatDaemonStateShape(current) &&
				current.kind === "slack" &&
				current.ownerId === fixture.ownerId &&
				current.pid === process.pid &&
				current.incarnation === fixture.incarnation &&
				current.generation === chatDaemonGeneration("slack") &&
				current.stoppedAt === undefined
			);
		};
		// The persisted owner pid rolls after the provider proof, while the
		// binding transaction is still in flight.
		fixture.workspace.onFindMessage = async () => {
			const state = JSON.parse(await fs.readFile(paths.state, "utf8")) as Record<string, unknown>;
			await fs.writeFile(paths.state, `${JSON.stringify({ ...state, pid: (state.pid as number) + 1 })}\n`, {
				mode: 0o600,
			});
		};
		let binds = 0;
		const server = pumpCommands({
			agentDir: fixture.agentDir,
			ownerId: fixture.ownerId,
			incarnation: fixture.incarnation,
			verifyOwnership: stillOwner,
			bind: async request => {
				binds++;
				return await bindThroughFixture(fixture, request);
			},
		});
		try {
			await expect(
				bindConfiguredSlackThread(
					{ settings: fixture.settings, sessionId: "session-1", threadTs: ROOT_TS },
					{ ensureDaemon: async () => "attached", timeoutMs: 2_000, pollIntervalMs: 5 },
				),
			).rejects.toMatchObject({ name: "SlackThreadBindingError", code: "daemon_owner_changed" });
			expect(binds).toBe(1);
			expect(await fixture.conversations()).toEqual([]);
			expect(fixture.workspace.posts).toEqual([]);
			expect(await fixture.controlFile()).toBeUndefined();
			expect(await fixture.pendingCommandFiles()).toEqual([]);
		} finally {
			await server.stop();
			await fixture.cleanup();
		}
	});
});

describe("Slack daemon worker binding integration", () => {
	function slackWorkerConfig(): string {
		return [
			"notifications:",
			"  enabled: true",
			"  slack:",
			"    botToken: xoxb-worker-not-persisted",
			"    appToken: xapp-worker-not-persisted",
			"    workspaceId: T1",
			"    channelId: C1",
			"    authorizedUserId: U1",
			"",
		].join("\n");
	}

	test("answers a bind command from the owner loop without stopping the daemon", async () => {
		const agentDir = await fs.mkdtemp(path.join(os.tmpdir(), "gjc-slack-bind-worker-"));
		const ownerId = `${process.pid}-bind-worker-test`;
		const started = Promise.withResolvers<void>();
		const binds: Array<{ sessionId: string; rootTs: string; authorized: boolean }> = [];
		let stops = 0;
		await Bun.write(path.join(agentDir, "config.yml"), slackWorkerConfig());
		const worker = runChatDaemonInternal("slack", ["--agent-dir", agentDir, "--owner-id", ownerId], {
			createRuntime: () => ({
				start: async () => started.resolve(),
				stop: async () => {
					stops++;
				},
				transportHealthy: () => true,
				bindExistingRoot: async request => {
					// A runtime that reports success must pass the channel's commit
					// fence first; a stub that skips it is refused by design.
					const authorized = (await request.commitAuthority?.()) ?? false;
					binds.push({ sessionId: request.sessionId, rootTs: request.rootTs, authorized });
					if (!authorized) return { ok: false, certainty: "rejected", code: "binding_failed" };
					return {
						ok: true,
						sessionId: request.sessionId,
						endpointGeneration: 4,
						teamId: "T1",
						channelId: "C1",
						rootTs: request.rootTs,
					};
				},
			}),
		});
		try {
			await started.promise;
			const state = await readChatDaemonState(agentDir, "slack");
			expect(state?.ownerId).toBe(ownerId);

			const response = await submitChatDaemonCommand({
				agentDir,
				kind: "slack",
				owner: {
					ownerId: state!.ownerId,
					pid: state!.pid,
					incarnation: state!.incarnation,
					generation: state!.generation,
				},
				command: "bind-thread",
				sessionId: "session-1",
				rootTs: "1785573662.132329",
				timeoutMs: 5_000,
				pollIntervalMs: 10,
			});

			expect(response).toMatchObject({
				outcome: "answered",
				response: {
					status: "ok",
					sessionId: "session-1",
					rootTs: "1785573662.132329",
					endpointGeneration: 4,
					teamId: "T1",
					channelId: "C1",
					ownerId,
					pid: state!.pid,
				},
			});
			expect(binds).toEqual([{ sessionId: "session-1", rootTs: "1785573662.132329", authorized: true }]);
			// The owner loop must still be serving: a bind is not a lifecycle request.
			expect(stops).toBe(0);
			expect((await readChatDaemonState(agentDir, "slack"))?.stoppedAt).toBeUndefined();
		} finally {
			const state = await readChatDaemonState(agentDir, "slack");
			if (state)
				await writeChatDaemonControlRequest(agentDir, "slack", {
					version: 1,
					requestId: "stop-request",
					action: "stop",
					ownerId: state.ownerId,
					pid: state.pid,
					incarnation: state.incarnation,
					createdAt: Date.now(),
				});
			await worker;
			await fs.rm(agentDir, { recursive: true, force: true });
		}
	});

	test("binds only sessions the runtime has attached with exact discovery authority", async () => {
		const repo = await fs.mkdtemp(path.join(os.tmpdir(), "gjc-slack-runtime-bind-"));
		const agentDir = path.join(repo, ".gjc", "agent");
		const endpointDir = path.join(repo, ".gjc", "state", "sdk");
		await fs.mkdir(endpointDir, { recursive: true });
		await fs.writeFile(
			path.join(endpointDir, "session-1.json"),
			JSON.stringify({ version: 1, url: "ws://127.0.0.1:1", token: "endpoint-token", pid: process.pid }),
		);
		const index = await new SessionIndex(agentDir).open();
		await index.append({
			type: "host_registered",
			sessionId: "session-1",
			locator: { repo, stateRoot: path.join(repo, ".gjc", "state") },
			endpointGeneration: 4,
			pid: process.pid,
		});
		const workspace = new FakeSlackWorkspace();
		workspace.seedRoot("C1", "1785573662.132329");
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
					onFrame: () => () => {},
					request: async () => ({ events: [] }),
					close: async () => {},
					send: () => {},
				}),
				setInterval: (() => 0) as unknown as typeof setInterval,
				clearInterval: (() => {}) as typeof clearInterval,
			},
		);
		try {
			await runtime.start();

			expect(await runtime.bindExistingRoot({ sessionId: "session-2", rootTs: "1785573662.132329" })).toEqual({
				ok: false,
				certainty: "rejected",
				code: "session_not_live",
			});
			expect(await runtime.bindExistingRoot({ sessionId: "session-1", rootTs: "1785573662.132329" })).toEqual({
				ok: true,
				sessionId: "session-1",
				endpointGeneration: 4,
				teamId: "T1",
				channelId: "C1",
				rootTs: "1785573662.132329",
			});

			await index.append({
				type: "host_unregistered",
				sessionId: "session-1",
				locator: { repo, stateRoot: path.join(repo, ".gjc", "state") },
				endpointGeneration: 4,
				pid: process.pid,
			});
			expect(await runtime.bindExistingRoot({ sessionId: "session-1", rootTs: "1785573662.132329" })).toEqual({
				ok: false,
				certainty: "rejected",
				code: "session_not_live",
			});
			expect(workspace.posts.filter(post => post.threadTs === undefined)).toEqual([]);
			const stored = Object.values(
				(await new ConversationStore<SlackConversation>({ agentDir, kind: "slack" }).load()).conversations,
			);
			expect(stored).toEqual([
				expect.objectContaining({
					sessionId: "session-1",
					rootTs: "1785573662.132329",
					endpointGeneration: 4,
				}),
			]);
		} finally {
			await runtime.stop();
			await fs.rm(repo, { recursive: true, force: true });
		}
	});
});
describe("chat daemon response envelope authority", () => {
	const ROOT_TS = "1785573662.132329";
	const SESSION_ID = "session-1";

	interface EnvelopeFixture {
		agentDir: string;
		directory: string;
		owner: { ownerId: string; pid: number; incarnation: string; generation: number };
		cleanup(): Promise<void>;
	}

	async function envelopeFixture(): Promise<EnvelopeFixture> {
		const agentDir = await fs.mkdtemp(path.join(os.tmpdir(), "gjc-slack-envelope-"));
		const directory = commandsDirectory(agentDir);
		await fs.mkdir(directory, { recursive: true, mode: 0o700 });
		return {
			agentDir,
			directory,
			owner: {
				ownerId: `${process.pid}-envelope-owner`,
				pid: process.pid,
				incarnation: processIncarnation(process.pid)!,
				generation: chatDaemonGeneration("slack"),
			},
			cleanup: async () => await fs.rm(agentDir, { recursive: true, force: true }),
		};
	}

	async function awaitRequestId(directory: string): Promise<string> {
		for (let attempt = 0; attempt < 2_000; attempt++) {
			const names = await fs.readdir(directory).catch(() => [] as string[]);
			const requestName = names.find(name => name.endsWith(".request.json"));
			if (requestName) return requestName.slice(0, -".request.json".length);
			await Bun.sleep(1);
		}
		throw new Error("the submitter never published a request");
	}

	/** Plant a well-formed response whose envelope differs from the request in exactly one field. */
	async function submitAgainstPlantedResponse(
		fixture: EnvelopeFixture,
		mutate: (response: Record<string, unknown>) => void,
	) {
		const submission = submitChatDaemonCommand({
			agentDir: fixture.agentDir,
			kind: "slack",
			owner: fixture.owner,
			command: "bind-thread",
			sessionId: SESSION_ID,
			rootTs: ROOT_TS,
			timeoutMs: 4_000,
			pollIntervalMs: 2,
			settleGraceMs: 40,
		});
		const requestId = await awaitRequestId(fixture.directory);
		const response: Record<string, unknown> = {
			version: 1,
			requestId,
			kind: "slack",
			command: "bind-thread",
			...fixture.owner,
			sessionId: SESSION_ID,
			rootTs: ROOT_TS,
			status: "rejected",
			code: "root_not_found",
			completedAt: Date.now(),
		};
		mutate(response);
		await fs.writeFile(path.join(fixture.directory, `${requestId}.response.json`), `${JSON.stringify(response)}\n`, {
			mode: 0o600,
		});
		return await submission;
	}

	test("a rejected response with a foreign pid is never reported as the request answer", async () => {
		const fixture = await envelopeFixture();
		try {
			const submission = await submitAgainstPlantedResponse(fixture, response => {
				response.pid = fixture.owner.pid + 1;
			});
			expect(submission).toEqual({ outcome: "untrusted", code: "response_envelope_mismatch" });
		} finally {
			await fixture.cleanup();
		}
	});

	const MISMATCHES: ReadonlyArray<{
		field: string;
		mutate: (r: Record<string, unknown>, f: EnvelopeFixture) => void;
	}> = [
		{ field: "requestId", mutate: r => (r.requestId = "22222222-2222-4222-8222-222222222222") },
		{ field: "kind", mutate: r => (r.kind = "discord") },
		{ field: "command", mutate: r => (r.command = "unbind-thread") },
		{ field: "ownerId", mutate: (r, f) => (r.ownerId = `${f.owner.ownerId}-other`) },
		{ field: "pid", mutate: (r, f) => (r.pid = f.owner.pid + 1) },
		{ field: "incarnation", mutate: (r, f) => (r.incarnation = `${f.owner.incarnation}9`) },
		{ field: "generation", mutate: (r, f) => (r.generation = f.owner.generation + 1) },
		{ field: "sessionId", mutate: r => (r.sessionId = "session-2") },
		{ field: "rootTs", mutate: r => (r.rootTs = "1785573662.132330") },
	];

	for (const status of ["ok", "rejected", "owner_changed", "expired", "outcome_unknown"] as const) {
		for (const mismatch of MISMATCHES) {
			test(`a ${status} response whose ${mismatch.field} differs is untrusted material`, async () => {
				const fixture = await envelopeFixture();
				try {
					const submission = await submitAgainstPlantedResponse(fixture, response => {
						response.status = status;
						if (status === "ok") {
							response.endpointGeneration = 7;
							response.teamId = "T1";
							response.channelId = "C1";
							delete response.code;
						}
						if (status === "outcome_unknown") response.code = "binding_outcome_unknown";
						mismatch.mutate(response, fixture);
					});
					expect(submission).toEqual({ outcome: "untrusted", code: "response_envelope_mismatch" });
				} finally {
					await fixture.cleanup();
				}
			});
		}
	}

	test("an exact-envelope rejection is still delivered as the definitive answer", async () => {
		const fixture = await envelopeFixture();
		try {
			const submission = await submitAgainstPlantedResponse(fixture, () => undefined);
			expect(submission).toMatchObject({
				outcome: "answered",
				response: { status: "rejected", code: "root_not_found", pid: fixture.owner.pid },
			});
		} finally {
			await fixture.cleanup();
		}
	});
});

describe("chat daemon settled-request replay authority", () => {
	const ROOT_TS = "1785573662.132329";
	const SESSION_ID = "session-1";
	const REQUEST_ID = "11111111-1111-4111-8111-111111111111";

	interface ReplayFixture {
		agentDir: string;
		scope: ChatDaemonCommandScope;
		owner: { ownerId: string; pid: number; incarnation: string; generation: number };
		request: Record<string, unknown>;
		cleanup(): Promise<void>;
	}

	async function replayFixture(now = 1_000): Promise<ReplayFixture> {
		const agentDir = await fs.mkdtemp(path.join(os.tmpdir(), "gjc-slack-replay-"));
		const scope = await openChatDaemonCommandScope({ agentDir, kind: "slack", create: true });
		if (!scope) throw new Error("the retained command scope must be available on this host");
		const owner = {
			ownerId: `${process.pid}-replay-owner`,
			pid: process.pid,
			incarnation: processIncarnation(process.pid)!,
			generation: chatDaemonGeneration("slack"),
		};
		return {
			agentDir,
			scope,
			owner,
			request: {
				version: 1,
				requestId: REQUEST_ID,
				kind: "slack",
				command: "bind-thread",
				...owner,
				sessionId: SESSION_ID,
				rootTs: ROOT_TS,
				createdAt: now,
				expiresAt: now + 100_000,
			},
			cleanup: async () => {
				closeChatDaemonCommandScope(scope);
				await fs.rm(agentDir, { recursive: true, force: true });
			},
		};
	}

	interface ServeTally {
		calls: number;
		authorized: number;
		mutations: number;
	}

	function serveOnce(fixture: ReplayFixture, tally: ServeTally, now: number, outcome: "reject" | "commit") {
		return serveChatDaemonCommandsOnce({
			agentDir: fixture.agentDir,
			kind: "slack",
			...fixture.owner,
			verifyOwnership: async () => true,
			now: () => now,
			handler: {
				bindExistingRoot: async request => {
					tally.calls++;
					if (outcome === "reject") return { ok: false, certainty: "rejected", code: "root_not_found" };
					if (!(await request.commitAuthority?.()))
						return { ok: false, certainty: "rejected", code: "binding_failed" };
					tally.authorized++;
					tally.mutations++;
					return {
						ok: true,
						sessionId: request.sessionId,
						endpointGeneration: 7,
						teamId: "T1",
						channelId: "C1",
						rootTs: request.rootTs,
					};
				},
			},
		});
	}

	test("a resurrected request with a settled identifier never re-authorizes a handler", async () => {
		const fixture = await replayFixture();
		const tally: ServeTally = { calls: 0, authorized: 0, mutations: 0 };
		try {
			await writeScopedJson(fixture.scope, `${REQUEST_ID}.request.json`, fixture.request);
			expect(await serveOnce(fixture, tally, 2_000, "reject")).toBe(1);
			expect(await scopedEntryExists(fixture.scope, `${REQUEST_ID}.response.json`)).toBe(true);

			// Model the submitter's cleanup: it retires the request and the settled
			// response object once it has reported the rejection.
			await unlinkScopedEntry(fixture.scope, `${REQUEST_ID}.request.json`);
			await unlinkScopedEntry(fixture.scope, `${REQUEST_ID}.response.json`);

			// Model resurrection of the still-unexpired request material.
			await writeScopedJson(fixture.scope, `${REQUEST_ID}.request.json`, fixture.request);
			await serveOnce(fixture, tally, 3_000, "commit");
			expect(tally).toEqual({ calls: 1, authorized: 0, mutations: 0 });
		} finally {
			await fixture.cleanup();
		}
	});

	test("a cancelled submission leaves replay authority that survives its own cleanup", async () => {
		const fixture = await replayFixture();
		const tally: ServeTally = { calls: 0, authorized: 0, mutations: 0 };
		try {
			const submission = await submitChatDaemonCommand({
				agentDir: fixture.agentDir,
				kind: "slack",
				owner: fixture.owner,
				command: "bind-thread",
				sessionId: SESSION_ID,
				rootTs: ROOT_TS,
				requestId: REQUEST_ID,
				timeoutMs: 0,
				pollIntervalMs: 1,
				settleGraceMs: 0,
			});
			expect(submission).toEqual({ outcome: "cancelled" });
			expect(await scopedEntryExists(fixture.scope, `${REQUEST_ID}.request.json`)).toBe(false);

			await writeScopedJson(fixture.scope, `${REQUEST_ID}.request.json`, fixture.request);
			await serveOnce(fixture, tally, 3_000, "commit");
			expect(tally).toEqual({ calls: 0, authorized: 0, mutations: 0 });
		} finally {
			await fixture.cleanup();
		}
	});

	test("an exact idempotent retry discovers the prior terminal outcome without another dispatch", async () => {
		const fixture = await replayFixture();
		const tally: ServeTally = { calls: 0, authorized: 0, mutations: 0 };
		try {
			await writeScopedJson(fixture.scope, `${REQUEST_ID}.request.json`, fixture.request);
			expect(await serveOnce(fixture, tally, 2_000, "reject")).toBe(1);
			await unlinkScopedEntry(fixture.scope, `${REQUEST_ID}.request.json`);
			await unlinkScopedEntry(fixture.scope, `${REQUEST_ID}.response.json`);

			const retry = await submitChatDaemonCommand({
				agentDir: fixture.agentDir,
				kind: "slack",
				owner: fixture.owner,
				command: "bind-thread",
				sessionId: SESSION_ID,
				rootTs: ROOT_TS,
				requestId: REQUEST_ID,
				timeoutMs: 0,
				pollIntervalMs: 1,
				settleGraceMs: 0,
			});
			expect(retry).toMatchObject({
				outcome: "answered",
				response: { status: "rejected", code: "root_not_found", requestId: REQUEST_ID },
			});
			expect(tally).toEqual({ calls: 1, authorized: 0, mutations: 0 });
			expect(await scopedEntryExists(fixture.scope, `${REQUEST_ID}.request.json`)).toBe(false);
		} finally {
			await fixture.cleanup();
		}
	});

	const REUSE_MISMATCHES: ReadonlyArray<{ field: string; mutate: (request: Record<string, unknown>) => void }> = [
		{ field: "ownerId", mutate: request => (request.ownerId = "other-owner") },
		{ field: "pid", mutate: request => (request.pid = (request.pid as number) + 1) },
		{ field: "incarnation", mutate: request => (request.incarnation = `${String(request.incarnation)}9`) },
		{ field: "generation", mutate: request => (request.generation = (request.generation as number) + 1) },
		{ field: "sessionId", mutate: request => (request.sessionId = "session-2") },
		{ field: "rootTs", mutate: request => (request.rootTs = "1785573662.132330") },
	];

	for (const mismatch of REUSE_MISMATCHES) {
		test(`a settled identifier reused with a different ${mismatch.field} fails closed`, async () => {
			const fixture = await replayFixture();
			const tally: ServeTally = { calls: 0, authorized: 0, mutations: 0 };
			try {
				await writeScopedJson(fixture.scope, `${REQUEST_ID}.request.json`, fixture.request);
				expect(await serveOnce(fixture, tally, 2_000, "reject")).toBe(1);
				await unlinkScopedEntry(fixture.scope, `${REQUEST_ID}.request.json`);
				await unlinkScopedEntry(fixture.scope, `${REQUEST_ID}.response.json`);

				const reused: Record<string, unknown> = { ...fixture.request };
				mismatch.mutate(reused);
				await writeScopedJson(fixture.scope, `${REQUEST_ID}.request.json`, reused);
				await serveOnce(fixture, tally, 3_000, "commit");
				expect(tally).toEqual({ calls: 1, authorized: 0, mutations: 0 });

				const submission = await submitChatDaemonCommand({
					agentDir: fixture.agentDir,
					kind: "slack",
					owner: {
						ownerId: String(reused.ownerId),
						pid: reused.pid as number,
						incarnation: String(reused.incarnation),
						generation: reused.generation as number,
					},
					command: "bind-thread",
					sessionId: String(reused.sessionId),
					rootTs: String(reused.rootTs),
					requestId: REQUEST_ID,
					timeoutMs: 0,
					pollIntervalMs: 1,
					settleGraceMs: 0,
				});
				expect(submission).toEqual({ outcome: "unavailable", code: "request_id_unavailable" });
			} finally {
				await fixture.cleanup();
			}
		});
	}

	test("a crash between commit authority and result publication leaves an indeterminate outcome", async () => {
		const fixture = await replayFixture();
		let authorized = 0;
		try {
			await writeScopedJson(fixture.scope, `${REQUEST_ID}.request.json`, fixture.request);
			// The handler takes terminal authority and then the process dies before
			// the answer is published: the mapping may already be applied.
			await expect(
				serveChatDaemonCommandsOnce({
					agentDir: fixture.agentDir,
					kind: "slack",
					...fixture.owner,
					verifyOwnership: async () => true,
					now: () => 2_000,
					handler: {
						bindExistingRoot: async request => {
							if (await request.commitAuthority?.()) authorized++;
							throw Object.assign(new Error("process died after the commit"), { code: "ECRASH" });
						},
					},
				}),
			).resolves.toBeGreaterThanOrEqual(0);
			expect(authorized).toBe(1);

			await unlinkScopedEntry(fixture.scope, `${REQUEST_ID}.request.json`);
			await unlinkScopedEntry(fixture.scope, `${REQUEST_ID}.response.json`);
			const retry = await submitChatDaemonCommand({
				agentDir: fixture.agentDir,
				kind: "slack",
				owner: fixture.owner,
				command: "bind-thread",
				sessionId: SESSION_ID,
				rootTs: ROOT_TS,
				requestId: REQUEST_ID,
				timeoutMs: 0,
				pollIntervalMs: 1,
				settleGraceMs: 0,
			});
			expect(retry).toEqual({ outcome: "unknown" });
		} finally {
			await fixture.cleanup();
		}
	});

	test("the retention sweep retires only the exact stale identity, never a successor", async () => {
		const fixture = await replayFixture();
		const tally: ServeTally = { calls: 0, authorized: 0, mutations: 0 };
		try {
			await writeScopedJson(fixture.scope, `${REQUEST_ID}.request.json`, fixture.request);
			expect(await serveOnce(fixture, tally, 2_000, "reject")).toBe(1);
			await unlinkScopedEntry(fixture.scope, `${REQUEST_ID}.request.json`);
			await unlinkScopedEntry(fixture.scope, `${REQUEST_ID}.response.json`);
			expect(await scopedEntryExists(fixture.scope, `${REQUEST_ID}.settled.json`)).toBe(true);

			// Still inside the retention window: replay authority is retained.
			await serveOnce(fixture, tally, 3_000, "commit");
			expect(await scopedEntryExists(fixture.scope, `${REQUEST_ID}.settled.json`)).toBe(true);

			// Past retention: the stale identity is retired and the identifier is
			// usable again.
			await serveOnce(fixture, tally, 100_000 + 1_000 + 60_000 + 1, "commit");
			expect(await scopedEntryExists(fixture.scope, `${REQUEST_ID}.settled.json`)).toBe(false);
			expect(tally).toEqual({ calls: 1, authorized: 0, mutations: 0 });
		} finally {
			await fixture.cleanup();
		}
	});
});

describe("Slack thread binding commit certainty", () => {
	const ROOT_TS = "1785573662.132329";
	const KEY = "T1:C1:intent:session-1";

	function claimInput(store: ConversationStore<SlackConversation>, revalidate = async () => true) {
		return {
			store,
			key: KEY,
			teamId: "T1",
			channelId: "C1",
			sessionId: "session-1",
			rootTs: ROOT_TS,
			endpointGeneration: 7,
			revalidate,
			now: () => 1_000,
		};
	}

	test("a durability failure over the whole fence is never a definitive rejection", async () => {
		const memory = new MemoryConversationStoreFs();
		const store = new ConversationStore<SlackConversation>({
			agentDir: "/agent",
			kind: "slack",
			fs: memory,
			platform: "linux",
		});
		memory.failDirectorySync = true;

		await expect(claimSlackThreadBinding(claimInput(store))).rejects.toMatchObject({
			name: "SlackThreadBindingError",
			code: "binding_outcome_unknown",
		});
		// A publication whose barrier cannot be proven is never assumed to have
		// left the previous document behind, so it is rolled back rather than
		// trusted — but the rollback is unproven too, which is exactly why the
		// caller is told the outcome is unknown instead of definitively refused.
		expect(memory.files.has(store.filePath)).toBe(true);
		const resolved = await store.load();
		expect(resolved.pending).toBeUndefined();
		expect(resolved.conversations[KEY]).toBeUndefined();
	});

	test("a mapping applied under a proven fence survives a barrier failure that only retires the marker", async () => {
		const memory = new MemoryConversationStoreFs();
		const store = new ConversationStore<SlackConversation>({
			agentDir: "/agent",
			kind: "slack",
			fs: memory,
			platform: "linux",
		});
		let publications = 0;
		memory.onRename = (_from, to) => {
			if (to === store.filePath) publications++;
		};
		// Only the publication that retires the state machine fails. The mapping
		// is already confirmed and durable — its closing proof is in the namespace
		// — so nothing about it is uncertain.
		memory.failRenameWhen = (_from, to) => to === store.filePath && publications >= 4;

		await expect(claimSlackThreadBinding(claimInput(store))).resolves.toMatchObject({
			state: "active",
			sessionId: "session-1",
			rootTs: ROOT_TS,
			endpointGeneration: 7,
		});
		expect(await store.read(KEY)).toMatchObject({
			state: "active",
			sessionId: "session-1",
			rootTs: ROOT_TS,
			endpointGeneration: 7,
		});
	});

	test("a durability failure before the mapping is applied stays a definitive rejection", async () => {
		const memory = new MemoryConversationStoreFs();
		const store = new ConversationStore<SlackConversation>({
			agentDir: "/agent",
			kind: "slack",
			fs: memory,
			platform: "linux",
		});
		memory.failFileSync = true;

		await expect(claimSlackThreadBinding(claimInput(store))).rejects.toMatchObject({
			name: "SlackThreadBindingError",
			code: "binding_failed",
		});
		expect(await store.read(KEY)).toBeUndefined();
	});

	test("a handler that fails after taking commit authority answers an unknown outcome", async () => {
		const agentDir = await fs.mkdtemp(path.join(os.tmpdir(), "gjc-slack-certainty-"));
		const owner = {
			ownerId: `${process.pid}-certainty-owner`,
			pid: process.pid,
			incarnation: processIncarnation(process.pid)!,
			generation: chatDaemonGeneration("slack"),
		};
		const server = pumpCommands({
			agentDir,
			ownerId: owner.ownerId,
			incarnation: owner.incarnation,
			bind: async request => {
				if (!(await request.commitAuthority?.()))
					return { ok: false, certainty: "rejected", code: "binding_failed" };
				throw Object.assign(new Error("durability barrier failed after the rename"), { code: "EIO" });
			},
		});
		try {
			const submission = await submitChatDaemonCommand({
				agentDir,
				kind: "slack",
				owner,
				command: "bind-thread",
				sessionId: "session-1",
				rootTs: ROOT_TS,
				timeoutMs: 4_000,
				pollIntervalMs: 2,
			});
			expect(submission).toMatchObject({ outcome: "answered", response: { status: "outcome_unknown" } });
		} finally {
			await server.stop();
			await fs.rm(agentDir, { recursive: true, force: true });
		}
	});

	test("an explicitly indeterminate handler outcome is reported as an unknown binding", async () => {
		const agentDir = await fs.mkdtemp(path.join(os.tmpdir(), "gjc-slack-certainty-typed-"));
		const owner = {
			ownerId: `${process.pid}-certainty-typed`,
			pid: process.pid,
			incarnation: processIncarnation(process.pid)!,
			generation: chatDaemonGeneration("slack"),
		};
		const server = pumpCommands({
			agentDir,
			ownerId: owner.ownerId,
			incarnation: owner.incarnation,
			bind: async request => {
				await request.commitAuthority?.();
				return { ok: false, certainty: "unknown", code: "binding_outcome_unknown" };
			},
		});
		try {
			const submission = await submitChatDaemonCommand({
				agentDir,
				kind: "slack",
				owner,
				command: "bind-thread",
				sessionId: "session-1",
				rootTs: ROOT_TS,
				timeoutMs: 4_000,
				pollIntervalMs: 2,
			});
			expect(submission).toMatchObject({
				outcome: "answered",
				response: { status: "outcome_unknown", code: "binding_outcome_unknown" },
			});
		} finally {
			await server.stop();
			await fs.rm(agentDir, { recursive: true, force: true });
		}
	});
});

describe("Slack thread binding commit linearization", () => {
	const ROOT_TS = "1785573662.132329";
	const KEY = "T1:C1:intent:session-1";

	/**
	 * Holds the claim after the staged document is written and before the
	 * replacement rename — the exact seam at which authority may still roll.
	 */
	class PauseBeforeRenameFs extends MemoryConversationStoreFs {
		readonly staged = Promise.withResolvers<void>();
		readonly release = Promise.withResolvers<void>();
		paused = false;

		override async writeFile(file: string, data: string, options: { mode: number }): Promise<void> {
			await super.writeFile(file, data, options);
			if (this.paused || !file.includes("conversations.json.") || !file.endsWith(".tmp")) return;
			this.paused = true;
			this.staged.resolve();
			await this.release.promise;
		}
	}

	function claimInput(
		store: ConversationStore<SlackConversation>,
		revalidate: () => Promise<boolean>,
	): Parameters<typeof claimSlackThreadBinding>[0] {
		return {
			store,
			key: KEY,
			teamId: "T1",
			channelId: "C1",
			sessionId: "session-1",
			rootTs: ROOT_TS,
			endpointGeneration: 7,
			revalidate,
			now: () => 1_000,
		};
	}

	test("session authority that rolls before the rename never yields a stale success", async () => {
		const memory = new PauseBeforeRenameFs();
		const store = new ConversationStore<SlackConversation>({ agentDir: "/agent", kind: "slack", fs: memory });
		let live = true;
		let revalidations = 0;
		const binding = claimSlackThreadBinding(
			claimInput(store, async () => {
				revalidations++;
				return live;
			}),
		);
		await memory.staged.promise;
		live = false;
		memory.release.resolve();

		await expect(binding).rejects.toMatchObject({ name: "SlackThreadBindingError", code: "session_not_live" });
		// Authority is proven again at the logical linearization point, which sits
		// after the provisional namespace replacement, so a roll that lands in the
		// staging window is still observed and rolled back.
		expect(revalidations).toBeGreaterThanOrEqual(2);
		expect(await store.read(KEY)).toBeUndefined();
	});

	test("a daemon owner tuple that rolls before the rename never yields a stale success", async () => {
		const memory = new PauseBeforeRenameFs();
		const store = new ConversationStore<SlackConversation>({ agentDir: "/agent", kind: "slack", fs: memory });
		let ownerPid = process.pid;
		// Models the daemon's own revalidate: exact endpoint generation *and* the
		// exact owner tuple must still hold at the commit.
		const binding = claimSlackThreadBinding(claimInput(store, async () => ownerPid === process.pid));
		await memory.staged.promise;
		ownerPid = process.pid + 1;
		memory.release.resolve();

		await expect(binding).rejects.toMatchObject({ name: "SlackThreadBindingError", code: "session_not_live" });
		expect(await store.read(KEY)).toBeUndefined();
		expect(await store.load()).toMatchObject({ conversations: {} });
	});

	test("authority valid at the linearization point commits, and a later roll is a lifecycle event", async () => {
		const memory = new PauseBeforeRenameFs();
		const store = new ConversationStore<SlackConversation>({ agentDir: "/agent", kind: "slack", fs: memory });
		let live = true;
		const binding = claimSlackThreadBinding(claimInput(store, async () => live));
		await memory.staged.promise;
		memory.release.resolve();
		await expect(binding).resolves.toMatchObject({ state: "active", endpointGeneration: 7 });

		// Rolling afterwards does not retroactively invalidate the commit; it is a
		// subsequent lifecycle event handled by ordinary generation fencing.
		live = false;
		expect(await store.read(KEY)).toMatchObject({ state: "active", endpointGeneration: 7 });
		await expect(
			claimSlackThreadBinding({ ...claimInput(store, async () => live), endpointGeneration: 8 }),
		).rejects.toMatchObject({ name: "SlackThreadBindingError", code: "session_not_live" });
		expect(await store.read(KEY)).toMatchObject({ state: "active", endpointGeneration: 7 });
	});

	test("no provider or network call is made while the mapping store lock is held", async () => {
		const memory = new PauseBeforeRenameFs();
		const store = new ConversationStore<SlackConversation>({ agentDir: "/agent", kind: "slack", fs: memory });
		const underLock: string[] = [];
		const binding = claimSlackThreadBinding(
			claimInput(store, async () => {
				underLock.push(...memory.calls.filter(call => call.startsWith("http")));
				return true;
			}),
		);
		await memory.staged.promise;
		memory.release.resolve();
		await binding;
		expect(underLock).toEqual([]);
	});

	/**
	 * The four contracts of the mapping publication boundary. `onRename` fires at
	 * the actual rename entry — the exact seam a proof taken beforehand cannot
	 * cover — and `afterRename` fires the instant the namespace change is live.
	 */
	test("an endpoint generation that goes stale at the actual rename hook never yields a stale success", async () => {
		const memory = new MemoryConversationStoreFs();
		const store = new ConversationStore<SlackConversation>({ agentDir: "/agent", kind: "slack", fs: memory });
		let liveGeneration = 7;
		memory.onRename = (_from, to) => {
			if (to === store.filePath) liveGeneration = 8;
		};

		await expect(claimSlackThreadBinding(claimInput(store, async () => liveGeneration === 7))).rejects.toMatchObject({
			name: "SlackThreadBindingError",
			code: "session_not_live",
		});
		expect(await store.read(KEY)).toBeUndefined();
		expect(await store.load()).toMatchObject({ conversations: {} });
	});

	test("a daemon owner tuple that goes stale at the actual rename hook never yields a stale success", async () => {
		const memory = new MemoryConversationStoreFs();
		const store = new ConversationStore<SlackConversation>({ agentDir: "/agent", kind: "slack", fs: memory });
		let ownerPid = process.pid;
		memory.onRename = (_from, to) => {
			if (to === store.filePath) ownerPid = process.pid + 1;
		};

		await expect(
			claimSlackThreadBinding(claimInput(store, async () => ownerPid === process.pid)),
		).rejects.toMatchObject({ name: "SlackThreadBindingError", code: "session_not_live" });
		expect(await store.read(KEY)).toBeUndefined();
		expect(await store.load()).toMatchObject({ conversations: {} });
	});

	test("no reader observes a mapping inside the authority fence, only the decided state", async () => {
		const memory = new MemoryConversationStoreFs();
		const store = new ConversationStore<SlackConversation>({ agentDir: "/agent", kind: "slack", fs: memory });
		const reader = new ConversationStore<SlackConversation>({ agentDir: "/agent", kind: "slack", fs: memory });
		const order: string[] = [];
		let live = true;
		let publications = 0;
		let fenced: Promise<SlackConversation | undefined> | undefined;
		memory.onRename = (_from, to) => {
			if (to !== store.filePath) return;
			publications++;
			// Roll authority at the activating publication, so the fence must
			// observe it and roll the mapping back.
			if (publications === 2) live = false;
		};
		memory.afterRename = (_from, to) => {
			// A reader that starts while the writer is inside its fence. It must not
			// resolve until the transaction has decided.
			if (to !== store.filePath || fenced) return;
			fenced = reader.read(KEY).then(value => {
				order.push("read");
				return value;
			});
		};

		await expect(claimSlackThreadBinding(claimInput(store, async () => live))).rejects.toMatchObject({
			code: "session_not_live",
		});
		order.push("decided");
		expect(await fenced).toBeUndefined();
		// The read is fenced behind the decision, so no provisional or
		// mid-activation mapping is ever observable.
		expect(order).toEqual(["decided", "read"]);
		expect(await store.read(KEY)).toBeUndefined();
	});

	test("a rollback that cannot be proven complete is an unknown outcome, never a rejection", async () => {
		const memory = new MemoryConversationStoreFs();
		const store = new ConversationStore<SlackConversation>({ agentDir: "/agent", kind: "slack", fs: memory });
		let live = true;
		let publications = 0;
		memory.onRename = (_from, to) => {
			if (to !== store.filePath) return;
			publications++;
			if (publications === 1) live = false;
		};
		memory.failRenameWhen = (_from, to) => to === store.filePath && publications >= 2;

		await expect(claimSlackThreadBinding(claimInput(store, async () => live))).rejects.toMatchObject({
			name: "SlackThreadBindingError",
			code: "binding_outcome_unknown",
		});
		// An unproven rollback may never surface a provisional mapping as active.
		expect(await store.read(KEY)).toBeUndefined();
	});

	/**
	 * The final activation window. The provisional publication is not the last
	 * physical publication that changes what a reader resolves: the publication
	 * that carries the mapping into its *activating* state is. Rolling authority
	 * there must be observed, not reported as a success.
	 */
	test("an endpoint generation that goes stale at the activating publication never yields a stale success", async () => {
		const memory = new MemoryConversationStoreFs();
		const store = new ConversationStore<SlackConversation>({ agentDir: "/agent", kind: "slack", fs: memory });
		let liveGeneration = 7;
		let publications = 0;
		let staleAtActivation = false;
		memory.onRename = (_from, to) => {
			if (to !== store.filePath) return;
			publications++;
			// The second publication is the one that carries the mapping into its
			// activating state; roll authority exactly there.
			if (publications === 2) liveGeneration = 8;
		};
		memory.afterRename = (_from, to) => {
			if (to === store.filePath && publications === 2) staleAtActivation = liveGeneration !== 7;
		};

		await expect(claimSlackThreadBinding(claimInput(store, async () => liveGeneration === 7))).rejects.toMatchObject({
			name: "SlackThreadBindingError",
			code: "session_not_live",
		});
		expect(staleAtActivation).toBe(true);
		expect(await store.read(KEY)).toBeUndefined();
		expect(await store.load()).toMatchObject({ conversations: {} });
	});

	test("a daemon owner tuple that goes stale at the activating publication never yields a stale success", async () => {
		const memory = new MemoryConversationStoreFs();
		const store = new ConversationStore<SlackConversation>({ agentDir: "/agent", kind: "slack", fs: memory });
		let ownerPid = process.pid;
		let publications = 0;
		memory.onRename = (_from, to) => {
			if (to !== store.filePath) return;
			publications++;
			if (publications === 2) ownerPid = process.pid + 1;
		};

		await expect(
			claimSlackThreadBinding(claimInput(store, async () => ownerPid === process.pid)),
		).rejects.toMatchObject({ name: "SlackThreadBindingError", code: "session_not_live" });
		expect(await store.read(KEY)).toBeUndefined();
		expect(await store.load()).toMatchObject({ conversations: {} });
	});

	test("authority is proven again after the activating publication, never only before it", async () => {
		const memory = new MemoryConversationStoreFs();
		const store = new ConversationStore<SlackConversation>({ agentDir: "/agent", kind: "slack", fs: memory });
		let publications = 0;
		const proofsAfterActivation: number[] = [];
		memory.onRename = (_from, to) => {
			if (to === store.filePath) publications++;
		};

		await expect(
			claimSlackThreadBinding(
				claimInput(store, async () => {
					if (publications >= 2) proofsAfterActivation.push(publications);
					return true;
				}),
			),
		).resolves.toMatchObject({ state: "active", endpointGeneration: 7 });
		// At least one authority proof runs strictly after the activating
		// publication, which is what closes the final activation window.
		expect(proofsAfterActivation.length).toBeGreaterThanOrEqual(1);
	});

	test("a rollback of the activating publication that cannot be proven is unknown, never a rejection", async () => {
		const memory = new MemoryConversationStoreFs();
		const store = new ConversationStore<SlackConversation>({ agentDir: "/agent", kind: "slack", fs: memory });
		let live = true;
		let publications = 0;
		memory.onRename = (_from, to) => {
			if (to !== store.filePath) return;
			publications++;
			if (publications === 2) live = false;
		};
		memory.failRenameWhen = (_from, to) => to === store.filePath && publications >= 3;

		await expect(claimSlackThreadBinding(claimInput(store, async () => live))).rejects.toMatchObject({
			name: "SlackThreadBindingError",
			code: "binding_outcome_unknown",
		});
	});

	test("a provisional publication whose barrier fails is rolled back, never assumed to have survived", async () => {
		const memory = new MemoryConversationStoreFs();
		const store = new ConversationStore<SlackConversation>({
			agentDir: "/agent",
			kind: "slack",
			fs: memory,
			platform: "linux",
		});
		let publications = 0;
		const barrierAttempts: number[] = [];
		memory.onRename = (_from, to) => {
			if (to === store.filePath) publications++;
		};
		memory.failDirectorySync = true;
		memory.afterRename = (_from, to) => {
			if (to === store.filePath) barrierAttempts.push(publications);
		};

		await expect(claimSlackThreadBinding(claimInput(store, async () => true))).rejects.toMatchObject({
			name: "SlackThreadBindingError",
			code: "binding_outcome_unknown",
		});
		// The very first publication is barriered: nothing assumes an unflushed
		// rename leaves the previous document behind, so the staged publication is
		// rolled back rather than trusted.
		expect(barrierAttempts[0]).toBe(1);
		expect(publications).toBe(2);
		expect(await store.read(KEY)).toBeUndefined();
	});

	test("a crash between the activating publication and its confirmation recovers deterministically", async () => {
		const memory = new MemoryConversationStoreFs();
		const store = new ConversationStore<SlackConversation>({ agentDir: "/agent", kind: "slack", fs: memory });
		const crash = new Error("process died inside the authority fence");
		let publications = 0;
		memory.onRename = (_from, to) => {
			if (to === store.filePath) publications++;
		};

		await expect(
			claimSlackThreadBinding(
				claimInput(store, async () => {
					if (publications >= 2) throw crash;
					return true;
				}),
			),
		).rejects.toMatchObject({ name: "SlackThreadBindingError" });
		// Whatever survives is resolvable without the crashed writer: a reader
		// observes exactly one decided state and never a half-applied one.
		const recovered = await store.load();
		expect(recovered.pending).toBeUndefined();
		const observed = await store.read(KEY);
		if (observed !== undefined) expect(observed).toMatchObject({ state: "active", endpointGeneration: 7 });
	});

	test("malformed pending marker state fails the store closed instead of surfacing an arbitrary record", async () => {
		const memory = new MemoryConversationStoreFs();
		const store = new ConversationStore<SlackConversation>({ agentDir: "/agent", kind: "slack", fs: memory });
		const malformed = [
			{ key: "", owner: { pid: 1, incarnation: "i" }, at: 1, phase: "staged" },
			{ key: KEY, owner: { pid: 0, incarnation: "i" }, at: 1, phase: "staged" },
			{ key: KEY, owner: { pid: 1, incarnation: "" }, at: 1, phase: "staged" },
			{ key: KEY, owner: { pid: 1, incarnation: "i" }, at: Number.NaN, phase: "staged" },
			{ key: KEY, owner: { pid: 1, incarnation: "i" }, at: 1, phase: "not-a-phase" },
			{ key: KEY, owner: { pid: 1, incarnation: "i" }, at: 1, phase: "staged", previous: { generation: "one" } },
			{ key: KEY, owner: { pid: 1, incarnation: "i" }, at: 1, phase: "staged", previous: { generation: -1 } },
			{ key: KEY, owner: { pid: 1, incarnation: "i" }, at: 1, phase: "staged", previous: [] },
		];
		for (const pending of malformed) {
			memory.files.set(store.filePath, `${JSON.stringify({ version: 1, conversations: {}, pending })}\n`);
			await expect(store.load()).rejects.toThrow();
			await expect(store.read(KEY)).rejects.toThrow();
		}
	});
});

describe("Slack thread binding activation crash recovery", () => {
	const ROOT_TS = "1785573662.132329";
	const PREVIOUS_ROOT_TS = "1785573000.100100";
	const KEY = "T1:C1:intent:session-1";
	const CRASHED_OWNER = { pid: 4242, incarnation: "crashed-writer" };

	function memoryStore(memory: MemoryConversationStoreFs): ConversationStore<SlackConversation> {
		return new ConversationStore<SlackConversation>({
			agentDir: "/agent",
			kind: "slack",
			fs: memory,
			platform: "linux",
		});
	}

	function mapping(overrides: Partial<SlackConversation> = {}): SlackConversation {
		return {
			generation: 1,
			state: "active",
			teamId: "T1",
			channelId: "C1",
			rootTs: ROOT_TS,
			sessionId: "session-1",
			endpointGeneration: 7,
			updatedAt: 1_000,
			seenEventIds: [],
			seenContextIds: [],
			seenRetryKeys: [],
			seenInteractionIds: [],
			inboundDispatches: [],
			...overrides,
		};
	}

	const previousMapping = (): SlackConversation =>
		mapping({ generation: 1, rootTs: PREVIOUS_ROOT_TS, endpointGeneration: 6, updatedAt: 900 });

	function claimInput(
		store: ConversationStore<SlackConversation>,
		revalidate: () => Promise<boolean> = async () => true,
		overrides: { endpointGeneration?: number } = {},
	): Parameters<typeof claimSlackThreadBinding>[0] {
		return {
			store,
			key: KEY,
			teamId: "T1",
			channelId: "C1",
			sessionId: "session-1",
			rootTs: ROOT_TS,
			endpointGeneration: overrides.endpointGeneration ?? 7,
			revalidate,
			now: () => 1_000,
		};
	}

	/**
	 * The exact durable namespace state a writer leaves behind when it dies
	 * after the activating document is published and flushed but before the
	 * closing authority proof ever ran.
	 */
	function seedUnconfirmedActivation(
		memory: MemoryConversationStoreFs,
		file: string,
		previous: SlackConversation | undefined,
	): void {
		const replacement = mapping({ generation: (previous?.generation ?? 0) + 1 });
		memory.files.set(
			file,
			`${JSON.stringify({
				version: 1,
				conversations: { [KEY]: replacement },
				pending: {
					key: KEY,
					phase: "activating",
					owner: CRASHED_OWNER,
					at: 1_000,
					...(previous === undefined ? {} : { previous }),
				},
			})}\n`,
		);
	}

	test("an endpoint generation that disappeared before the closing proof is never recovered as active", async () => {
		const memory = new MemoryConversationStoreFs();
		const store = memoryStore(memory);
		seedUnconfirmedActivation(memory, store.filePath, previousMapping());

		const recovered = await store.load();
		expect(recovered.pending).toBeUndefined();
		// The activating marker attests that an activation was attempted, never
		// that it was allowed, so the displaced mapping is restored instead.
		expect(await store.read(KEY)).toMatchObject({
			generation: 1,
			rootTs: PREVIOUS_ROOT_TS,
			endpointGeneration: 6,
		});
	});

	test("an unconfirmed activation over an absent key recovers to no mapping at all", async () => {
		const memory = new MemoryConversationStoreFs();
		const store = memoryStore(memory);
		seedUnconfirmedActivation(memory, store.filePath, undefined);

		expect(await store.read(KEY)).toBeUndefined();
		expect(await store.load()).toMatchObject({ conversations: {} });
	});

	test("a daemon owner tuple that disappears in the activation window rolls back after a real crash", async () => {
		const memory = new MemoryConversationStoreFs();
		const writer = memoryStore(memory);
		let ownerPid = process.pid;
		let publications = 0;
		memory.onRename = (_from, to) => {
			if (to !== writer.filePath) return;
			publications++;
			// The owner tuple rolls the instant the activating document lands.
			if (publications === 2) ownerPid = process.pid + 1;
		};
		// The writer dies inside its fence: nothing it would have published after
		// the activating document ever reaches the namespace.
		memory.failRenameWhen = (_from, to) => to === writer.filePath && publications >= 3;

		await expect(
			claimSlackThreadBinding(claimInput(writer, async () => ownerPid === process.pid)),
		).rejects.toMatchObject({ name: "SlackThreadBindingError" });

		const durable = JSON.parse(memory.files.get(writer.filePath)!);
		expect(durable.pending?.phase).toBe("activating");
		expect(durable.conversations[KEY]).toMatchObject({ rootTs: ROOT_TS });

		memory.onRename = undefined;
		memory.failRenameWhen = undefined;
		const restarted = memoryStore(memory);
		expect(await restarted.read(KEY)).toBeUndefined();
	});

	test("a recovered unconfirmed activation is never surfaced as an active mapping to any reader", async () => {
		const memory = new MemoryConversationStoreFs();
		const reader = memoryStore(memory);
		const other = memoryStore(memory);
		const writer = memoryStore(memory);
		seedUnconfirmedActivation(memory, reader.filePath, previousMapping());

		const snapshots: Record<string, SlackConversation>[] = [];
		await writer.transactWithSnapshot(KEY, (current, conversations) => {
			snapshots.push({ ...conversations });
			return current;
		});
		expect(snapshots[0]?.[KEY]).toMatchObject({ rootTs: PREVIOUS_ROOT_TS, endpointGeneration: 6 });
		expect(await reader.read(KEY)).toMatchObject({ rootTs: PREVIOUS_ROOT_TS, endpointGeneration: 6 });
		expect(await other.read(KEY)).toMatchObject({ rootTs: PREVIOUS_ROOT_TS, endpointGeneration: 6 });
	});

	test("a restore that cannot be proven durable is indeterminate, never a stale active mapping", async () => {
		const memory = new MemoryConversationStoreFs();
		const store = memoryStore(memory);
		seedUnconfirmedActivation(memory, store.filePath, previousMapping());
		memory.failDirectorySync = true;

		await expect(
			claimSlackThreadBinding(claimInput(store, async () => true, { endpointGeneration: 8 })),
		).rejects.toMatchObject({ name: "SlackThreadBindingError", code: "binding_outcome_unknown" });

		memory.failDirectorySync = false;
		// Whatever survived the failed restore, the unconfirmed replacement is
		// still not a mapping any reader may observe.
		expect(await store.read(KEY)).toMatchObject({ rootTs: PREVIOUS_ROOT_TS, endpointGeneration: 6 });
	});

	test("a confirmed activation records its closing proof and survives a restart", async () => {
		const memory = new MemoryConversationStoreFs();
		const store = memoryStore(memory);
		let publications = 0;
		memory.onRename = (_from, to) => {
			if (to === store.filePath) publications++;
		};
		// Only the publication that retires the state machine fails, so the
		// document that survives is exactly the confirmed one.
		memory.failRenameWhen = (_from, to) => to === store.filePath && publications >= 4;

		await expect(claimSlackThreadBinding(claimInput(store))).resolves.toMatchObject({
			state: "active",
			rootTs: ROOT_TS,
			endpointGeneration: 7,
		});

		const durable = JSON.parse(memory.files.get(store.filePath)!);
		expect(durable.pending).toMatchObject({ key: KEY, phase: "confirmed" });
		expect(durable.pending.proof).toMatchObject({ generation: durable.conversations[KEY].generation });
		expect(durable.pending.proof.owner).toEqual(durable.pending.owner);

		memory.onRename = undefined;
		memory.failRenameWhen = undefined;
		const restarted = memoryStore(memory);
		expect(await restarted.read(KEY)).toMatchObject({
			state: "active",
			rootTs: ROOT_TS,
			endpointGeneration: 7,
		});
	});

	test("authority that rolls at the confirming publication never yields a stale success", async () => {
		const memory = new MemoryConversationStoreFs();
		const store = memoryStore(memory);
		let live = true;
		let publications = 0;
		let rolledAt = 0;
		memory.onRename = (_from, to) => {
			if (to !== store.filePath) return;
			publications++;
			// The third publication is the first document in which the replacement
			// is the mapping; roll authority exactly there.
			if (publications === 3) {
				rolledAt = publications;
				live = false;
			}
		};

		await expect(claimSlackThreadBinding(claimInput(store, async () => live))).rejects.toMatchObject({
			name: "SlackThreadBindingError",
			code: "session_not_live",
		});
		expect(rolledAt).toBe(3);
		expect(await store.read(KEY)).toBeUndefined();
		expect(await store.load()).toMatchObject({ conversations: {} });
	});

	test("a store lock reclaimed mid-transaction is indeterminate and never rolled back over", async () => {
		const memory = new MemoryConversationStoreFs();
		const store = memoryStore(memory);
		let publications = 0;
		memory.afterRename = (_from, to) => {
			if (to !== store.filePath) return;
			publications++;
			// A stale-lock reclaim hands the fence's exclusion to another writer
			// exactly while this transaction is still inside it.
			if (publications === 2)
				memory.files.set(
					`${store.filePath}.lock`,
					`${JSON.stringify({ pid: process.pid + 1, incarnation: "other-writer", timestamp: 5 })}\n`,
				);
		};

		await expect(claimSlackThreadBinding(claimInput(store))).rejects.toMatchObject({
			name: "SlackThreadBindingError",
			code: "binding_outcome_unknown",
		});
		// The transaction stops before the confirming publication and does not
		// publish a rollback over a namespace another writer may now own.
		expect(publications).toBe(2);
		// What survives is an unconfirmed activation, which no reader resolves as
		// an active mapping.
		memory.afterRename = undefined;
		expect(await store.read(KEY)).toBeUndefined();
	});

	test("malformed or partial confirmation evidence fails the store closed", async () => {
		const memory = new MemoryConversationStoreFs();
		const store = memoryStore(memory);
		const conversations = { [KEY]: mapping({ generation: 1 }) };
		const proof = { generation: 1, at: 1_000, owner: CRASHED_OWNER };
		const malformed = [
			// Confirmation claimed with no evidence at all.
			{ key: KEY, owner: CRASHED_OWNER, at: 1_000, phase: "confirmed" },
			// Evidence that never names the writer that took the proof.
			{ key: KEY, owner: CRASHED_OWNER, at: 1_000, phase: "confirmed", proof: { generation: 1, at: 1_000 } },
			// Evidence taken by a writer other than the one the marker names.
			{
				key: KEY,
				owner: CRASHED_OWNER,
				at: 1_000,
				phase: "confirmed",
				proof: { ...proof, owner: { pid: 99, incarnation: "someone-else" } },
			},
			// Evidence for a replacement this document does not carry.
			{ key: KEY, owner: CRASHED_OWNER, at: 1_000, phase: "confirmed", proof: { ...proof, generation: 9 } },
			// Evidence with an unusable replacement generation.
			{ key: KEY, owner: CRASHED_OWNER, at: 1_000, phase: "confirmed", proof: { ...proof, generation: 0 } },
			// Evidence attached to a phase whose closing proof never ran.
			{ key: KEY, owner: CRASHED_OWNER, at: 1_000, phase: "staged", proof },
			{ key: KEY, owner: CRASHED_OWNER, at: 1_000, phase: "activating", proof },
		];
		for (const pending of malformed) {
			memory.files.set(store.filePath, `${JSON.stringify({ version: 1, conversations, pending })}\n`);
			await expect(store.load()).rejects.toThrow();
			await expect(store.read(KEY)).rejects.toThrow();
		}

		// A confirmation naming a replacement that is absent entirely.
		memory.files.set(
			store.filePath,
			`${JSON.stringify({
				version: 1,
				conversations: {},
				pending: { key: KEY, owner: CRASHED_OWNER, at: 1_000, phase: "confirmed", proof },
			})}\n`,
		);
		await expect(store.load()).rejects.toThrow();
	});
});

describe("Slack thread binding lock cleanup certainty", () => {
	const ROOT_TS = "1785573662.132329";
	const KEY = "T1:C1:intent:session-1";

	function claimInput(store: ConversationStore<SlackConversation>, revalidate = async () => true) {
		return {
			store,
			key: KEY,
			teamId: "T1",
			channelId: "C1",
			sessionId: "session-1",
			rootTs: ROOT_TS,
			endpointGeneration: 7,
			revalidate,
			now: () => 1_000,
		};
	}

	for (const failure of ["failLockUnlink", "failLockClose"] as const) {
		test(`a post-commit lock ${failure === "failLockClose" ? "close" : "unlink"} failure never becomes a definitive rejection`, async () => {
			const memory = new MemoryConversationStoreFs();
			memory[failure] = true;
			const store = new ConversationStore<SlackConversation>({
				agentDir: "/agent",
				kind: "slack",
				fs: memory,
				platform: "linux",
			});

			await expect(claimSlackThreadBinding(claimInput(store))).rejects.toMatchObject({
				name: "SlackThreadBindingError",
				code: "binding_outcome_unknown",
			});
			// The mapping commit itself is certain; only the lock's own cleanup is not.
			expect(await store.read(KEY)).toMatchObject({
				state: "active",
				sessionId: "session-1",
				rootTs: ROOT_TS,
				endpointGeneration: 7,
			});
			// A lock that could not be released is never force-reclaimed afterwards.
			expect(memory.files.has(`${store.filePath}.lock`)).toBe(true);
		});
	}

	test("a lock cleanup failure after a refused commit keeps the definitive rejection", async () => {
		const memory = new MemoryConversationStoreFs();
		memory.failLockUnlink = true;
		const store = new ConversationStore<SlackConversation>({
			agentDir: "/agent",
			kind: "slack",
			fs: memory,
			platform: "linux",
		});

		await expect(claimSlackThreadBinding(claimInput(store, async () => false))).rejects.toMatchObject({
			name: "SlackThreadBindingError",
			code: "session_not_live",
		});
		expect(await store.read(KEY)).toBeUndefined();
	});
});

describe("chat daemon command durability certainty", () => {
	const ROOT_TS = "1785573662.132329";
	const SESSION_ID = "session-1";

	interface DurabilityFixture {
		agentDir: string;
		scope: ChatDaemonCommandScope;
		/** The same retained authority with a directory barrier that always fails. */
		unsyncable: ChatDaemonCommandScope;
		owner: { ownerId: string; pid: number; incarnation: string; generation: number };
		cleanup(): Promise<void>;
	}

	async function durabilityFixture(): Promise<DurabilityFixture> {
		const agentDir = await fs.mkdtemp(path.join(os.tmpdir(), "gjc-slack-durability-"));
		const scope = await openChatDaemonCommandScope({ agentDir, kind: "slack", create: true });
		if (!scope) throw new Error("the retained command scope must be available on this host");
		const authority = scope.authority;
		const unsyncable: ChatDaemonCommandScope = {
			directory: scope.directory,
			authority: {
				identity: () => authority.identity(),
				list: () => authority.list(),
				statEntry: name => authority.statEntry(name),
				createExclusive: (name, data, mode) => authority.createExclusive(name, data, mode),
				readEntry: name => authority.readEntry(name),
				renameEntry: (from, to) => authority.renameEntry(from, to),
				linkEntry: (from, to) => authority.linkEntry(from, to),
				unlinkEntry: (name, dev, ino) => authority.unlinkEntry(name, dev, ino),
				// A real host that answers the directory barrier with an I/O error.
				syncDir: () => ({ ok: false, code: "io_error" }),
				close: () => undefined,
			},
		};
		return {
			agentDir,
			scope,
			unsyncable,
			owner: {
				ownerId: `${process.pid}-durability-owner`,
				pid: process.pid,
				incarnation: processIncarnation(process.pid)!,
				generation: chatDaemonGeneration("slack"),
			},
			cleanup: async () => {
				closeChatDaemonCommandScope(scope);
				await fs.rm(agentDir, { recursive: true, force: true });
			},
		};
	}

	test("a directory barrier failure after a rename is never an ordinary write success", async () => {
		const fixture = await durabilityFixture();
		try {
			expect(await writeScopedJson(fixture.unsyncable, "durability.json", { marker: "written" })).toBe(
				"durability_unknown",
			);
			// The namespace change is already applied, so it is reported as
			// applied-but-not-proven rather than discarded.
			expect(await readScopedJson(fixture.scope, "durability.json")).toEqual({ marker: "written" });
		} finally {
			await fixture.cleanup();
		}
	});

	test("a directory barrier failure after an exclusive publish is never an ordinary publish success", async () => {
		const fixture = await durabilityFixture();
		try {
			expect(await publishScopedJsonExclusive(fixture.unsyncable, "published.json", { marker: "published" })).toBe(
				"durability_unknown",
			);
			expect(await readScopedJson(fixture.scope, "published.json")).toEqual({ marker: "published" });
			// A taken name is still a definitive loss, independent of durability.
			expect(await publishScopedJsonExclusive(fixture.scope, "published.json", { marker: "again" })).toBe("exists");
		} finally {
			await fixture.cleanup();
		}
	});

	test("a settlement that cannot be proven durable never answers a definitive status", async () => {
		const fixture = await durabilityFixture();
		try {
			const request = buildChatDaemonCommandRequest({
				kind: "slack",
				command: "bind-thread",
				owner: fixture.owner,
				sessionId: SESSION_ID,
				rootTs: ROOT_TS,
				now: 1_000,
				ttlMs: 100_000,
			});
			await writeScopedJson(fixture.scope, `${request.requestId}.request.json`, request);
			let authorized = 0;
			await serveChatDaemonCommandsAgainstScope(fixture.unsyncable, {
				agentDir: fixture.agentDir,
				kind: "slack",
				...fixture.owner,
				verifyOwnership: async () => true,
				now: () => 2_000,
				handler: {
					bindExistingRoot: async input => {
						if (!(await input.commitAuthority?.()))
							return { ok: false, certainty: "rejected", code: "binding_failed" };
						authorized++;
						return {
							ok: true,
							sessionId: input.sessionId,
							rootTs: input.rootTs,
							endpointGeneration: 7,
							teamId: "T1",
							channelId: "C1",
						};
					},
				},
			});

			const answer = await readScopedJson(fixture.scope, `${request.requestId}.response.json`);
			expect(isChatDaemonCommandResponse(answer)).toBe(true);
			// A commit whose replay record is not provably durable may never be
			// published as a definitive `ok`, `rejected`, or `cancelled`.
			expect((answer as { status: string }).status).toBe("outcome_unknown");
			const settled = await readScopedJson(fixture.scope, `${request.requestId}.settled.json`);
			expect(isChatDaemonCommandSettlement(settled)).toBe(true);
			expect(["committing", "outcome_unknown"]).toContain((settled as { outcome: string }).outcome);
			expect(authorized).toBeLessThanOrEqual(1);
		} finally {
			await fixture.cleanup();
		}
	});

	/**
	 * Every terminal settlement, not only a committed one, must be durable
	 * before the answer that depends on it is published. A crash that loses an
	 * unproven settlement restores the still-unexpired request, which can be
	 * dispatched again and mutate — so a definitive answer published on top of
	 * one is unsafe for every status, not just `ok`.
	 */
	for (const terminal of [
		{
			label: "rejected",
			ttlMs: 100_000,
			ownerShift: 0,
			handler: async (): Promise<ChatDaemonCommandOutcome> => ({
				ok: false,
				certainty: "rejected",
				code: "root_not_found",
			}),
		},
		{
			label: "owner_changed",
			ttlMs: 100_000,
			ownerShift: 1,
			handler: async (): Promise<ChatDaemonCommandOutcome> => ({
				ok: false,
				certainty: "rejected",
				code: "binding_failed",
			}),
		},
		{
			label: "expired",
			ttlMs: 1,
			ownerShift: 0,
			handler: async (): Promise<ChatDaemonCommandOutcome> => ({
				ok: false,
				certainty: "rejected",
				code: "binding_failed",
			}),
		},
	] as const) {
		test(`a terminal ${terminal.label} settlement that cannot be proven durable is never definitive`, async () => {
			const fixture = await durabilityFixture();
			try {
				const request = buildChatDaemonCommandRequest({
					kind: "slack",
					command: "bind-thread",
					owner: { ...fixture.owner, pid: fixture.owner.pid + terminal.ownerShift },
					sessionId: SESSION_ID,
					rootTs: ROOT_TS,
					now: 1_000,
					ttlMs: terminal.ttlMs,
				});
				await writeScopedJson(fixture.scope, `${request.requestId}.request.json`, request);
				await serveChatDaemonCommandsAgainstScope(fixture.unsyncable, {
					agentDir: fixture.agentDir,
					kind: "slack",
					...fixture.owner,
					verifyOwnership: async () => true,
					now: () => 2_000,
					handler: { bindExistingRoot: terminal.handler },
				});

				const answer = await readScopedJson(fixture.scope, `${request.requestId}.response.json`);
				expect(isChatDaemonCommandResponse(answer)).toBe(true);
				expect((answer as { status: string }).status).toBe("outcome_unknown");
				const settled = await readScopedJson(fixture.scope, `${request.requestId}.settled.json`);
				if (settled !== undefined) {
					expect(isChatDaemonCommandSettlement(settled)).toBe(true);
					expect(["committing", "outcome_unknown"]).toContain((settled as { outcome: string }).outcome);
				}
			} finally {
				await fixture.cleanup();
			}
		});
	}

	test("a durable terminal settlement keeps its exact status", async () => {
		const fixture = await durabilityFixture();
		try {
			const request = buildChatDaemonCommandRequest({
				kind: "slack",
				command: "bind-thread",
				owner: fixture.owner,
				sessionId: SESSION_ID,
				rootTs: ROOT_TS,
				now: 1_000,
				ttlMs: 100_000,
			});
			await writeScopedJson(fixture.scope, `${request.requestId}.request.json`, request);
			await serveChatDaemonCommandsAgainstScope(fixture.scope, {
				agentDir: fixture.agentDir,
				kind: "slack",
				...fixture.owner,
				verifyOwnership: async () => true,
				now: () => 2_000,
				handler: {
					bindExistingRoot: async () => ({ ok: false, certainty: "rejected", code: "root_not_found" }),
				},
			});

			const answer = await readScopedJson(fixture.scope, `${request.requestId}.response.json`);
			expect(answer).toMatchObject({ status: "rejected", code: "root_not_found" });
			const settled = await readScopedJson(fixture.scope, `${request.requestId}.settled.json`);
			expect(settled).toMatchObject({ outcome: "rejected", code: "root_not_found" });
		} finally {
			await fixture.cleanup();
		}
	});
});

/**
 * The command channel's response object is correlation material, never proof of
 * who produced it.
 *
 * Every field a response echoes — the owner tuple, the session, the root — is
 * copied verbatim out of the plaintext request that sits in the same untrusted
 * command directory, and `<id>.response.json` is a single-winner object any
 * writer of that directory can create first. A complete, envelope-correct
 * `status:"ok"` document is therefore forgeable without the daemon ever running,
 * and so is any companion document in that directory, including the settlement.
 *
 * These regressions pin the exact observed attack: the forged answer must never
 * become a reported binding, no mapping may be created, and the suppressed
 * request may not buy a later unauthorized dispatch.
 */
describe("gjc notify bind-thread forged command response", () => {
	const FORGED_ROOT = "1785573662.132329";

	/** Everything an untrusted command-directory writer can build from the request alone. */
	async function forgeAnswer(
		agentDir: string,
		options: { settlement?: boolean; endpointGeneration?: number } = {},
	): Promise<boolean> {
		const scope = await openChatDaemonCommandScope({ agentDir, kind: "slack" });
		if (!scope) return false;
		try {
			const names = await fs.readdir(commandsDirectory(agentDir)).catch(() => [] as string[]);
			const requestName = names.find(name => name.endsWith(".request.json"));
			if (!requestName) return false;
			const request = await readScopedJson(scope, requestName);
			if (!isChatDaemonCommandRequest(request)) return false;
			const envelope = {
				version: request.version,
				requestId: request.requestId,
				kind: request.kind,
				command: request.command,
				ownerId: request.ownerId,
				pid: request.pid,
				incarnation: request.incarnation,
				generation: request.generation,
				sessionId: request.sessionId,
				rootTs: request.rootTs,
				// The configured target is public configuration, not daemon evidence.
				teamId: "T1",
				channelId: "C1",
				endpointGeneration: options.endpointGeneration ?? 99,
			};
			if (options.settlement)
				await publishScopedJsonExclusive(scope, `${request.requestId}.settled.json`, {
					...envelope,
					createdAt: request.createdAt,
					expiresAt: request.expiresAt,
					outcome: "ok",
					settledAt: Date.now(),
				});
			const published = await publishScopedJsonExclusive(scope, `${request.requestId}.response.json`, {
				...envelope,
				status: "ok",
				completedAt: Date.now(),
			});
			return published === "published" || published === "durability_unknown";
		} finally {
			closeChatDaemonCommandScope(scope);
		}
	}

	for (const scenario of [
		{ label: "response", settlement: false },
		{ label: "response and a forged same-directory settlement", settlement: true },
	] as const) {
		test(`refuses a forged ${scenario.label} and creates no mapping`, async () => {
			const fixture = await daemonFixture();
			fixture.workspace.seedRoot("C1", FORGED_ROOT);
			let forged = false;
			let dispatches = 0;
			try {
				await expect(
					bindConfiguredSlackThread(
						{ settings: fixture.settings, sessionId: "session-1", threadTs: FORGED_ROOT },
						{
							ensureDaemon: async () => "attached",
							timeoutMs: 5_000,
							pollIntervalMs: 1,
							// The writer wins the single-winner response object between two
							// polls, which is the exact race the attack relies on.
							sleep: async () => {
								if (!forged) forged = await forgeAnswer(fixture.agentDir, { settlement: scenario.settlement });
							},
						},
					),
				).rejects.toMatchObject({ name: "SlackThreadBindingError", code: "binding_outcome_unknown" });
				expect(forged).toBe(true);
				// No mapping exists, and the provider was never asked to verify a root,
				// so nothing about this binding was ever performed.
				expect(await fixture.conversations()).toEqual([]);
				expect(fixture.workspace.findCalls).toBe(0);
				expect(fixture.workspace.posts).toEqual([]);

				// The suppressed request may not buy a later unauthorized dispatch.
				await serveChatDaemonCommandsOnce({
					agentDir: fixture.agentDir,
					kind: "slack",
					ownerId: fixture.ownerId,
					pid: process.pid,
					incarnation: fixture.incarnation,
					generation: chatDaemonGeneration("slack"),
					verifyOwnership: async () => true,
					handler: {
						bindExistingRoot: async request => {
							dispatches++;
							return await bindThroughFixture(fixture, request);
						},
					},
				});
				expect(dispatches).toBe(0);
				expect(await fixture.conversations()).toEqual([]);
			} finally {
				await fixture.cleanup();
			}
		});
	}

	test("still reports a binding the daemon actually committed", async () => {
		const fixture = await daemonFixture();
		fixture.workspace.seedRoot("C1", FORGED_ROOT);
		const server = pumpCommands({
			agentDir: fixture.agentDir,
			ownerId: fixture.ownerId,
			incarnation: fixture.incarnation,
			bind: async request => await bindThroughFixture(fixture, request),
		});
		try {
			const bound = await bindConfiguredSlackThread(
				{ settings: fixture.settings, sessionId: "session-1", threadTs: FORGED_ROOT },
				{ ensureDaemon: async () => "attached" },
			);
			expect(bound).toMatchObject({ sessionId: "session-1", endpointGeneration: 7, rootTs: FORGED_ROOT });
			expect(await fixture.conversations()).toEqual([
				expect.objectContaining({ state: "active", sessionId: "session-1", endpointGeneration: 7 }),
			]);
		} finally {
			await server.stop();
			await fixture.cleanup();
		}
	});

	test("refuses an answer whose endpoint generation is not the committed one", async () => {
		const fixture = await daemonFixture();
		fixture.workspace.seedRoot("C1", FORGED_ROOT);
		const server = pumpCommands({
			agentDir: fixture.agentDir,
			ownerId: fixture.ownerId,
			incarnation: fixture.incarnation,
			bind: async request => {
				const outcome = await bindThroughFixture(fixture, request);
				// A daemon that commits one generation but answers another has not
				// described the mapping that exists.
				return outcome.ok ? { ...outcome, endpointGeneration: outcome.endpointGeneration + 1 } : outcome;
			},
		});
		try {
			await expect(
				bindConfiguredSlackThread(
					{ settings: fixture.settings, sessionId: "session-1", threadTs: FORGED_ROOT },
					{ ensureDaemon: async () => "attached", timeoutMs: 5_000 },
				),
			).rejects.toMatchObject({ name: "SlackThreadBindingError", code: "binding_outcome_unknown" });
		} finally {
			await server.stop();
			await fixture.cleanup();
		}
	});
});
