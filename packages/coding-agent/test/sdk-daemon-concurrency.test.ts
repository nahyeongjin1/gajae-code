import { describe, expect, test, vi } from "bun:test";
import * as nodeFs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { isProcessIncarnation, processIncarnation } from "../src/sdk/broker/process-incarnation";
import {
	claimScopedEntry,
	listScopedEntries,
	openChatDaemonCommandScope,
	publishScopedJsonExclusive,
	readScopedJson,
	scopedEntryAgeMs,
	scopedEntryExists,
	scopedEntryIdentity,
	unlinkScopedEntry,
	writeScopedJson,
} from "../src/sdk/bus/chat-daemon-command-scope";
import { ChatEffectJournal, MAX_TERMINAL_CHAT_EFFECTS } from "../src/sdk/bus/chat-effect-journal";
import {
	boundedDedupe,
	ConversationLockTimeoutError,
	type ConversationRecord,
	ConversationStore,
	conversationStorePath,
	MAX_DEDUPE_IDS,
} from "../src/sdk/bus/conversation-store";
import type { SlackConversation } from "../src/sdk/bus/slack-conversation";
import { MemoryConversationStoreFs } from "./fixtures/chat-daemon-stores";

interface TestConversation extends ConversationRecord {
	state: "creating" | "active";
	seenEventIds: string[];
}

function record(generation: number, state: TestConversation["state"] = "creating"): TestConversation {
	return { generation, state, seenEventIds: [] };
}

describe("ConversationStore", () => {
	test("creates the transport store under the SDK daemon path and permits one concurrent creator", async () => {
		const fs = new MemoryConversationStoreFs();
		const store = new ConversationStore<TestConversation>({ agentDir: "/agent", kind: "discord", fs, now: () => 1 });
		expect(store.filePath).toBe(conversationStorePath("/agent", "discord"));
		const [first, second] = await Promise.all([
			store.write("mapping", undefined, record(1)),
			store.write("mapping", undefined, record(1)),
		]);
		expect([first, second].filter(Boolean)).toHaveLength(1);
		expect(await store.read("mapping")).toEqual(record(1));
		expect(fs.modes.get(store.filePath)).toBe(0o600);
		expect(fs.modes.get("/agent/sdk/daemons/discord")).toBe(0o700);
	});

	test("does not reclaim a newly created lock before its owner publishes metadata", async () => {
		const entered = Promise.withResolvers<void>();
		const observedEmptyLock = Promise.withResolvers<void>();
		const release = Promise.withResolvers<void>();
		let paused = false;
		class PausingFs extends MemoryConversationStoreFs {
			override async open(file: string, flags: string) {
				const handle = await super.open(file, flags);
				if (flags === "wx" && !paused) {
					paused = true;
					entered.resolve();
					await release.promise;
				}
				return handle;
			}
			override async readFile(file: string, encoding: "utf8") {
				const value = await super.readFile(file, encoding);
				if (paused && file.endsWith(".lock") && value === "") observedEmptyLock.resolve();
				return value;
			}
		}
		const fs = new PausingFs();
		const first = new ConversationStore<TestConversation>({
			agentDir: "/agent",
			kind: "discord",
			fs,
			pid: 101,
			pidAlive: () => true,
		});
		const second = new ConversationStore<TestConversation>({
			agentDir: "/agent",
			kind: "discord",
			fs,
			pid: 202,
			pidAlive: () => true,
		});
		const firstWrite = first.write("mapping", undefined, record(1));
		await entered.promise;
		let secondSettled = false;
		const secondWrite = second.write("mapping", undefined, record(1)).finally(() => {
			secondSettled = true;
		});
		await observedEmptyLock.promise;
		expect(secondSettled).toBe(false);
		release.resolve();
		expect((await Promise.all([firstWrite, secondWrite])).filter(Boolean)).toHaveLength(1);
	});

	test("recovers a lock whose recorded owner is dead", async () => {
		const fs = new MemoryConversationStoreFs();
		const store = new ConversationStore<TestConversation>({
			agentDir: "/agent",
			kind: "discord",
			fs,
			pid: 202,
			pidAlive: pid => pid === 202,
		});
		fs.files.set(`${store.filePath}.lock`, JSON.stringify({ pid: 101, incarnation: "old", timestamp: 1 }));
		expect(await store.write("mapping", undefined, record(1))).toBe(true);
	});
	test("recovers an abandoned reclaim lock owned by a reused PID", async () => {
		const fs = new MemoryConversationStoreFs();
		const store = new ConversationStore<TestConversation>({
			agentDir: "/agent",
			kind: "discord",
			fs,
			pid: 202,
			pidAlive: () => true,
			pidIncarnation: pid => (pid === 101 ? "current" : "writer"),
		});
		fs.files.set(`${store.filePath}.lock`, JSON.stringify({ pid: 101, incarnation: "old", timestamp: 1 }));
		fs.files.set(`${store.filePath}.lock.reclaim`, JSON.stringify({ pid: 101, incarnation: "old", timestamp: 1 }));
		await expect(store.write("mapping", undefined, record(1))).resolves.toBe(true);
		expect(fs.files.has(`${store.filePath}.lock.reclaim`)).toBe(false);
	});

	test("does not steal a fresh live reclaim lock or bypass the lock timeout", async () => {
		const fs = new MemoryConversationStoreFs();
		const store = new ConversationStore<TestConversation>({
			agentDir: "/agent",
			kind: "discord",
			fs,
			pid: 202,
			pidAlive: pid => pid !== 101,
			pidIncarnation: pid => (pid === 303 ? "darwin:1700000000:123456" : "darwin:1700000000:654321"),
			lockTimeoutMs: 0,
		});
		fs.files.set(
			`${store.filePath}.lock`,
			JSON.stringify({ pid: 101, incarnation: "darwin:1700000000:999999", timestamp: 1 }),
		);
		fs.files.set(
			`${store.filePath}.lock.reclaim`,
			JSON.stringify({ pid: 303, incarnation: "darwin:1700000000:123456", timestamp: 1 }),
		);
		await expect(store.write("mapping", undefined, record(1))).rejects.toBeInstanceOf(ConversationLockTimeoutError);
		expect(fs.files.get(`${store.filePath}.lock.reclaim`)).toBe(
			JSON.stringify({ pid: 303, incarnation: "darwin:1700000000:123456", timestamp: 1 }),
		);
	});

	test("default-path lock uses canonical processIncarnation format", async () => {
		let capturedLock: string | undefined;
		class CapturingFs extends MemoryConversationStoreFs {
			override async open(file: string, flags: string) {
				const handle = await super.open(file, flags);
				if (flags === "wx" && file.endsWith(".lock")) {
					return {
						...handle,
						writeFile: async (data: string, encoding: "utf8") => {
							capturedLock = data.trim();
							await handle.writeFile(data, encoding);
						},
					};
				}
				return handle;
			}
		}
		const capturingFs = new CapturingFs();
		const store = new ConversationStore<TestConversation>({
			agentDir: "/agent",
			kind: "discord",
			fs: capturingFs,
			pid: process.pid,
			pidAlive: () => true,
		});
		await store.write("mapping", undefined, record(1));
		expect(capturedLock).toBeDefined();
		const lock = JSON.parse(capturedLock!);
		expect(lock.pid).toBe(process.pid);
		// The incarnation must be canonical (not a locale-dependent lstart string).
		expect(isProcessIncarnation(lock.incarnation)).toBe(true);
		expect(lock.incarnation).toBe(processIncarnation(process.pid));
	});

	test("reclaims a non-canonical locale-dependent Darwin lock as not-owned", async () => {
		const fs = new MemoryConversationStoreFs();
		const store = new ConversationStore<TestConversation>({
			agentDir: "/agent",
			kind: "discord",
			fs,
			pid: 202,
			pidAlive: () => true,
			pidIncarnation: () => "darwin:1700000000:123456",
		});
		// Simulate a stale lock written by the old locale-dependent defaultPidIncarnation.
		fs.files.set(
			`${store.filePath}.lock`,
			JSON.stringify({ pid: 101, incarnation: "darwin:Thu Jul 17 10:00:00 2025", timestamp: 1 }),
		);
		expect(await store.write("mapping", undefined, record(1))).toBe(true);
	});

	test("serializes separate store instances so independent mapping updates do not overwrite one another", async () => {
		const fs = new MemoryConversationStoreFs();
		const first = new ConversationStore<TestConversation>({ agentDir: "/agent", kind: "slack", fs, now: () => 2 });
		const second = new ConversationStore<TestConversation>({ agentDir: "/agent", kind: "slack", fs, now: () => 2 });
		await Promise.all([first.write("one", undefined, record(1)), second.write("two", undefined, record(1))]);
		expect((await first.load()).conversations).toEqual({ one: record(1), two: record(1) });
	});

	test("atomically observes other mappings while claiming a different key", async () => {
		const fs = new MemoryConversationStoreFs();
		const first = new ConversationStore<TestConversation>({ agentDir: "/agent", kind: "slack", fs, now: () => 2 });
		const second = new ConversationStore<TestConversation>({ agentDir: "/agent", kind: "slack", fs, now: () => 2 });
		const claim = (store: ConversationStore<TestConversation>, key: string) =>
			store.transactWithSnapshot(key, (current, conversations) => {
				if (Object.values(conversations).some(candidate => candidate.state === "active")) return current;
				return record((current?.generation ?? 0) + 1, "active");
			});

		const claimed = await Promise.all([claim(first, "one"), claim(second, "two")]);
		expect(claimed.filter(candidate => candidate?.state === "active")).toHaveLength(1);
		expect(Object.values((await first.load()).conversations)).toHaveLength(1);
	});

	test("keeps an asynchronous authority fence inside the lock and writes nothing when it fails", async () => {
		const fs = new MemoryConversationStoreFs();
		const store = new ConversationStore<TestConversation>({ agentDir: "/agent", kind: "slack", fs, now: () => 2 });
		const peer = new ConversationStore<TestConversation>({ agentDir: "/agent", kind: "slack", fs, now: () => 2 });
		let observedDuringFence: TestConversation | undefined;

		const rejected = await store.transactWithSnapshot("mapping", async current => {
			// Another writer must not be able to interleave while the fence runs.
			observedDuringFence = await peer.read("mapping");
			return current;
		});
		expect(rejected).toBeUndefined();
		expect(observedDuringFence).toBeUndefined();
		expect(Object.values((await store.load()).conversations)).toEqual([]);

		const committed = await store.transactWithSnapshot("mapping", async current =>
			record((current?.generation ?? 0) + 1, "active"),
		);
		expect(committed).toMatchObject({ state: "active", generation: 1 });
	});

	test("rejects a stale generation and restores persisted mappings after restart", async () => {
		const fs = new MemoryConversationStoreFs();
		const initial = new ConversationStore<TestConversation>({ agentDir: "/agent", kind: "slack", fs, now: () => 2 });
		expect(await initial.write("mapping", undefined, record(1))).toBe(true);
		expect(await initial.write("mapping", 1, record(2, "active"))).toBe(true);
		expect(await initial.write("mapping", 1, record(2, "active"))).toBe(false);
		const restarted = new ConversationStore<TestConversation>({
			agentDir: "/agent",
			kind: "slack",
			fs,
			now: () => 3,
		});
		expect(await restarted.read("mapping")).toEqual(record(2, "active"));
	});

	test("bounds durable dedupe identifiers without retaining duplicate values", () => {
		const ids = Array.from({ length: MAX_DEDUPE_IDS + 2 }, (_, index) => `event-${index}`);
		const bounded = boundedDedupe(["event-0", ...ids, "event-1"]);
		expect(bounded).toHaveLength(MAX_DEDUPE_IDS);
		expect(bounded[0]).toBe("event-2");
		expect(bounded.at(-1)).toBe(`event-${MAX_DEDUPE_IDS + 1}`);
	});

	test("keeps the prior document intact when fsync or rename fails", async () => {
		const fs = new MemoryConversationStoreFs();
		const store = new ConversationStore<TestConversation>({ agentDir: "/agent", kind: "discord", fs, now: () => 4 });
		await store.write("mapping", undefined, record(1));
		// Both failures happen before the replacement rename, so the commit is
		// definitively refused rather than uncertain and the prior document stands.
		const staged = new TemporarySyncFailureFs();
		const stagedStore = new ConversationStore<TestConversation>({
			agentDir: "/agent",
			kind: "discord",
			fs: staged,
			now: () => 4,
		});
		await stagedStore.write("mapping", undefined, record(1));
		staged.armed = true;
		await expect(stagedStore.write("mapping", 1, record(2))).rejects.toMatchObject({
			name: "ConversationCommitRefusedError",
			certainty: "refused",
		});
		expect(await stagedStore.read("mapping")).toEqual(record(1));

		fs.failRename = true;
		await expect(store.write("mapping", 1, record(2))).rejects.toMatchObject({
			name: "ConversationCommitRefusedError",
			certainty: "refused",
		});
		expect(await store.read("mapping")).toEqual(record(1));
		expect(staged.calls.some(call => call.startsWith("sync:/agent/sdk/daemons/discord/conversations.json."))).toBe(
			true,
		);
	});
	/** Fails the durability barrier of the staged document only, before any rename. */
	class TemporarySyncFailureFs extends MemoryConversationStoreFs {
		armed = false;

		override async open(file: string, flags: string) {
			const handle = await super.open(file, flags);
			if (!this.armed || !file.includes("conversations.json.") || !file.endsWith(".tmp")) return handle;
			return {
				...handle,
				sync: async () => {
					await handle.sync();
					throw new Error("temporary sync failed");
				},
			};
		}
	}
	class DirectoryBarrierFs extends MemoryConversationStoreFs {
		directoryOpenError?: Error;
		directorySyncError?: Error;
		directoryCloseError?: Error;

		override async open(file: string, flags: string) {
			if (file === "/agent/sdk/daemons/discord" && this.directoryOpenError) throw this.directoryOpenError;
			const handle = await super.open(file, flags);
			if (file !== "/agent/sdk/daemons/discord") return handle;
			return {
				...handle,
				sync: async () => {
					await handle.sync();
					if (this.directorySyncError) throw this.directorySyncError;
				},
				close: async () => {
					await handle.close();
					if (this.directoryCloseError) throw this.directoryCloseError;
				},
			};
		}
	}

	function barrierError(message: string, code?: string): Error {
		return code === undefined ? new Error(message) : Object.assign(new Error(message), { code });
	}

	test("tolerates unsupported Windows parent open and sync errors after durable temp sync and rename", async () => {
		for (const code of ["EINVAL", "ENOTSUP", "EOPNOTSUPP", "EPERM"]) {
			for (const phase of ["open", "sync"] as const) {
				const fs = new DirectoryBarrierFs();
				const error = barrierError(`${phase} ${code}`, code);
				fs[phase === "open" ? "directoryOpenError" : "directorySyncError"] = error;
				const store = new ConversationStore<TestConversation>({
					agentDir: "/agent",
					kind: "discord",
					fs,
					now: () => 5,
					platform: "win32",
				});
				await expect(store.write("mapping", undefined, record(1))).resolves.toBe(true);
				const tempSync = fs.calls.findIndex(call =>
					call.startsWith("sync:/agent/sdk/daemons/discord/conversations.json."),
				);
				const rename = fs.calls.findIndex(call => call.startsWith("rename:"));
				expect(tempSync).toBeGreaterThanOrEqual(0);
				expect(rename).toBeGreaterThan(tempSync);
				if (phase === "sync") {
					const parentSync = fs.calls.indexOf("sync:/agent/sdk/daemons/discord");
					const parentClose = fs.calls.indexOf("close:/agent/sdk/daemons/discord");
					expect(parentSync).toBeGreaterThan(rename);
					expect(parentClose).toBeGreaterThan(parentSync);
				}
			}
		}
	});

	test("rejects parent open and sync errors without supported Windows codes", async () => {
		for (const phase of ["open", "sync"] as const) {
			const fs = new DirectoryBarrierFs();
			const error = barrierError(`no code ${phase}`);
			fs[phase === "open" ? "directoryOpenError" : "directorySyncError"] = error;
			const store = new ConversationStore<TestConversation>({
				agentDir: "/agent",
				kind: "discord",
				fs,
				platform: "win32",
			});
			await expect(store.write("mapping", undefined, record(1))).rejects.toMatchObject({
				name: "ConversationCommitUncertainError",
				certainty: "uncertain",
				reason: error,
			});
			// The replacement already applied, so the applied document is kept
			// exactly as written rather than compensated away.
			expect(await store.read("mapping")).toEqual(record(1));
			const tempSync = fs.calls.findIndex(call =>
				call.startsWith("sync:/agent/sdk/daemons/discord/conversations.json."),
			);
			const rename = fs.calls.findIndex(call => call.startsWith("rename:"));
			expect(rename).toBeGreaterThan(tempSync);
			if (phase === "sync") expect(fs.calls).toContain("close:/agent/sdk/daemons/discord");
		}
	});

	test("rejects EACCES parent open and sync errors on Windows", async () => {
		for (const phase of ["open", "sync"] as const) {
			const fs = new DirectoryBarrierFs();
			const error = barrierError(`access ${phase}`, "EACCES");
			fs[phase === "open" ? "directoryOpenError" : "directorySyncError"] = error;
			const store = new ConversationStore<TestConversation>({
				agentDir: "/agent",
				kind: "discord",
				fs,
				platform: "win32",
			});
			await expect(store.write("mapping", undefined, record(1))).rejects.toMatchObject({
				name: "ConversationCommitUncertainError",
				certainty: "uncertain",
				reason: error,
			});
			// The replacement already applied, so the applied document is kept
			// exactly as written rather than compensated away.
			expect(await store.read("mapping")).toEqual(record(1));
			const tempSync = fs.calls.findIndex(call =>
				call.startsWith("sync:/agent/sdk/daemons/discord/conversations.json."),
			);
			expect(fs.calls.findIndex(call => call.startsWith("rename:"))).toBeGreaterThan(tempSync);
			if (phase === "sync") expect(fs.calls).toContain("close:/agent/sdk/daemons/discord");
		}
	});

	test("rejects unsupported Linux parent open and sync errors", async () => {
		for (const code of ["EINVAL", "ENOTSUP", "EOPNOTSUPP", "EPERM"]) {
			for (const phase of ["open", "sync"] as const) {
				const fs = new DirectoryBarrierFs();
				const error = barrierError(`linux ${phase} ${code}`, code);
				fs[phase === "open" ? "directoryOpenError" : "directorySyncError"] = error;
				const store = new ConversationStore<TestConversation>({
					agentDir: "/agent",
					kind: "discord",
					fs,
					platform: "linux",
				});
				await expect(store.write("mapping", undefined, record(1))).rejects.toMatchObject({
					name: "ConversationCommitUncertainError",
					certainty: "uncertain",
					reason: error,
				});
				const tempSync = fs.calls.findIndex(call =>
					call.startsWith("sync:/agent/sdk/daemons/discord/conversations.json."),
				);
				const rename = fs.calls.findIndex(call => call.startsWith("rename:"));
				expect(tempSync).toBeGreaterThanOrEqual(0);
				expect(rename).toBeGreaterThan(tempSync);
				if (phase === "sync") {
					const parentSync = fs.calls.indexOf("sync:/agent/sdk/daemons/discord");
					const parentClose = fs.calls.indexOf("close:/agent/sdk/daemons/discord");
					expect(parentSync).toBeGreaterThan(rename);
					expect(parentClose).toBeGreaterThan(parentSync);
				}
			}
		}
	});

	test("rejects parent close errors", async () => {
		const fs = new DirectoryBarrierFs();
		const error = barrierError("close failed", "EIO");
		fs.directoryCloseError = error;
		const store = new ConversationStore<TestConversation>({
			agentDir: "/agent",
			kind: "discord",
			fs,
			platform: "win32",
		});
		await expect(store.write("mapping", undefined, record(1))).rejects.toMatchObject({
			name: "ConversationCommitUncertainError",
			certainty: "uncertain",
			reason: error,
		});
	});

	test("rejects parent close errors after tolerating supported Windows parent sync errors", async () => {
		const fs = new DirectoryBarrierFs();
		const syncError = barrierError("sync unsupported", "EPERM");
		const closeError = barrierError("close failed", "EIO");
		fs.directorySyncError = syncError;
		fs.directoryCloseError = closeError;
		const store = new ConversationStore<TestConversation>({
			agentDir: "/agent",
			kind: "discord",
			fs,
			platform: "win32",
		});
		await expect(store.write("mapping", undefined, record(1))).rejects.toMatchObject({
			name: "ConversationCommitUncertainError",
			certainty: "uncertain",
			reason: closeError,
		});
		expect(fs.calls).toContain("close:/agent/sdk/daemons/discord");
	});
	test("aggregates unexpected parent sync and close errors", async () => {
		const fs = new DirectoryBarrierFs();
		const syncError = barrierError("sync failed", "EIO");
		const closeError = barrierError("close failed", "EIO");
		fs.directorySyncError = syncError;
		fs.directoryCloseError = closeError;
		const store = new ConversationStore<TestConversation>({
			agentDir: "/agent",
			kind: "discord",
			fs,
			platform: "win32",
		});
		await expect(store.write("mapping", undefined, record(1))).rejects.toMatchObject({
			name: "ConversationCommitUncertainError",
			certainty: "uncertain",
			reason: { errors: [syncError, closeError] },
		});
		const rename = fs.calls.findIndex(call => call.startsWith("rename:"));
		const parentSync = fs.calls.indexOf("sync:/agent/sdk/daemons/discord");
		const parentClose = fs.calls.indexOf("close:/agent/sdk/daemons/discord");
		expect(parentSync).toBeGreaterThan(rename);
		expect(parentClose).toBeGreaterThan(parentSync);
	});
});
describe("ChatEffectJournal", () => {
	test("keeps provider payloads out of mappings while replaying the protected journal after restart", async () => {
		const fs = new MemoryConversationStoreFs();
		const mappings = new ConversationStore<SlackConversation>({ agentDir: "/agent", kind: "slack", fs });
		const journal = new ChatEffectJournal({ agentDir: "/agent", transport: "slack", fs, now: () => 1 });
		await journal.enqueue({
			id: "inbound:evt-1",
			kind: "command",
			transport: "slack",
			sessionId: "session",
			endpointGeneration: 4,
			payload: { content: "/sdk secret-command", token: "super-secret" },
		});
		await mappings.write("team:channel:root", undefined, {
			generation: 1,
			state: "active",
			teamId: "team",
			channelId: "channel",
			rootTs: "root",
			sessionId: "session",
			endpointGeneration: 4,
			updatedAt: 1,
			seenEventIds: [],
			seenContextIds: [],
			seenRetryKeys: [],
			seenInteractionIds: [],
			inboundDispatches: [
				{
					key: "evt-1",
					eventId: "evt-1",
					interactionId: "interaction",
					retryKey: "retry",
					kind: "command",
					endpointGeneration: 4,
					effectId: "inbound:evt-1",
					idempotencyKey: "inbound:evt-1",
				},
			],
		});
		const mappingBody = fs.files.get(mappings.filePath) ?? "";
		expect(mappingBody).not.toContain("secret-command");
		expect(mappingBody).not.toContain("super-secret");
		expect(fs.modes.get(journal.filePath)).toBe(0o600);
		const restarted = new ChatEffectJournal({ agentDir: "/agent", transport: "slack", fs, now: () => 2 });
		expect(await restarted.replayable("slack", 4)).toEqual([
			expect.objectContaining({
				id: "inbound:evt-1",
				payload: { content: "/sdk secret-command", token: "super-secret" },
			}),
		]);
	});

	test("takes over expired leases and fences stale owners from terminal commits", async () => {
		const fs = new MemoryConversationStoreFs();
		const first = new ChatEffectJournal({ agentDir: "/agent", transport: "discord", fs, now: () => 1 });
		await first.enqueue({
			id: "effect",
			kind: "reply",
			transport: "discord",
			endpointGeneration: 2,
			payload: { answer: "body" },
		});
		const oldLease = await first.claim("effect", "old", 5);
		expect(oldLease).toMatchObject({ state: "leased", epoch: 1 });
		const second = new ChatEffectJournal({ agentDir: "/agent", transport: "discord", fs, now: () => 7 });
		const newLease = await second.claim("effect", "new", 5);
		expect(newLease).toMatchObject({ state: "leased", owner: "new", epoch: 2 });
		expect(await first.record("effect", { owner: "old", epoch: oldLease!.epoch }, "terminal")).toBeUndefined();
		expect(
			await second.record("effect", { owner: "new", epoch: newLease!.epoch }, "terminal", { messageId: "remote" }),
		).toMatchObject({ state: "terminal", receipt: { messageId: "remote" } });
	});

	test("retains more than 128 nonterminal effects while bounding terminal history", async () => {
		const fs = new MemoryConversationStoreFs();
		const journal = new ChatEffectJournal({ agentDir: "/agent", transport: "discord", fs, now: () => 1 });
		for (let index = 0; index < 130; index++)
			await journal.enqueue({
				id: `pending-${index}`,
				kind: "reply",
				transport: "discord",
				endpointGeneration: 1,
				payload: { index },
			});
		for (let index = 0; index < 130; index++) {
			await journal.enqueue({
				id: `terminal-${index}`,
				kind: "reply",
				transport: "discord",
				endpointGeneration: 1,
				payload: { index },
			});
			const lease = await journal.claim(`terminal-${index}`, "owner", 10);
			await journal.record(`terminal-${index}`, { owner: "owner", epoch: lease!.epoch }, "terminal");
		}
		const effects = await journal.list();
		expect(effects.filter(effect => effect.state !== "terminal")).toHaveLength(130);
		expect(effects.filter(effect => effect.state === "terminal")).toHaveLength(MAX_TERMINAL_CHAT_EFFECTS);
	});
});

describe("chat daemon command scope retained authority", () => {
	interface ScopeFixture {
		root: string;
		agentDir: string;
		commands: string;
		retained: string;
		outside: string;
		cleanup(): Promise<void>;
	}

	async function scopeFixture(): Promise<ScopeFixture> {
		const root = await nodeFs.mkdtemp(path.join(os.tmpdir(), "gjc-command-scope-"));
		const agentDir = path.join(root, "agent");
		const daemonDir = path.join(agentDir, "sdk", "daemons", "slack");
		const outside = path.join(root, "outside");
		await nodeFs.mkdir(outside, { recursive: true, mode: 0o700 });
		return {
			root,
			agentDir,
			commands: path.join(daemonDir, "commands"),
			retained: path.join(daemonDir, "commands-retained"),
			outside,
			cleanup: async () => await nodeFs.rm(root, { recursive: true, force: true }),
		};
	}

	async function names(directory: string): Promise<string[]> {
		return (await nodeFs.readdir(directory).catch(() => [] as string[])).sort();
	}

	/**
	 * Move the real command directory aside and put `replacement` at its pathname.
	 * Everything the scope does afterwards must still reach the moved directory.
	 */
	async function replacePathname(fixture: ScopeFixture, replacement: "symlink" | "directory"): Promise<void> {
		await nodeFs.rename(fixture.commands, fixture.retained);
		if (replacement === "symlink") await nodeFs.symlink(fixture.outside, fixture.commands, "dir");
		else await nodeFs.mkdir(fixture.commands, { mode: 0o700 });
	}

	const ENTRY = "00000000-0000-0000-0000-000000000000.response.json";
	const OTHER = "00000000-0000-0000-0000-000000000001.response.json";

	for (const replacement of ["symlink", "directory"] as const) {
		test(`every operation stays on the retained directory after the pathname becomes a ${replacement}`, async () => {
			const fixture = await scopeFixture();
			try {
				const scope = await openChatDaemonCommandScope({ agentDir: fixture.agentDir, kind: "slack", create: true });
				expect(scope).toBeDefined();
				await replacePathname(fixture, replacement);

				expect(await claimScopedEntry(scope!, ENTRY)).toBe(true);
				expect(await claimScopedEntry(scope!, ENTRY)).toBe(false);
				await writeScopedJson(scope!, ENTRY, { marker: "retained" });
				expect(await readScopedJson(scope!, ENTRY)).toEqual({ marker: "retained" });
				expect(await scopedEntryExists(scope!, ENTRY)).toBe(true);
				expect(await listScopedEntries(scope!)).toContain(ENTRY);
				expect(await publishScopedJsonExclusive(scope!, OTHER, { marker: "published" })).toBe("published");
				expect(await publishScopedJsonExclusive(scope!, OTHER, { marker: "again" })).toBe("exists");
				expect(await scopedEntryAgeMs(scope!, ENTRY, Date.now())).toBeGreaterThanOrEqual(0);
				expect(await unlinkScopedEntry(scope!, OTHER)).toBe("removed");
				expect(await scopedEntryExists(scope!, OTHER)).toBe(false);

				// The retained identity absorbed every operation, and neither the
				// replacement pathname nor the external directory was ever touched.
				expect(await names(fixture.retained)).toEqual([ENTRY]);
				expect(await names(fixture.outside)).toEqual([]);
				if (replacement === "directory") expect(await names(fixture.commands)).toEqual([]);
			} finally {
				await fixture.cleanup();
			}
		});
	}

	test("no pathname recheck seam remains between authority capture and the operation", async () => {
		const fixture = await scopeFixture();
		const realLstat: (target: unknown, options: unknown) => Promise<unknown> = (target, options) =>
			(nodeFs.lstat as unknown as (t: unknown, o: unknown) => Promise<unknown>)(target, options);
		let lstat: ReturnType<typeof vi.spyOn> | undefined;
		let swaps = 0;
		try {
			const scope = await openChatDaemonCommandScope({ agentDir: fixture.agentDir, kind: "slack", create: true });
			expect(scope).toBeDefined();
			// Arm a deterministic replacement barrier at the classic recheck seam: the
			// instant anything re-reads the command directory *by pathname*, the
			// pathname stops describing the retained directory. A `lstat` recheck
			// followed by an ordinary pathname syscall therefore escapes here.
			const barrier = async (target: unknown, options: unknown): Promise<unknown> => {
				const observed = await realLstat(target, options);
				if (String(target) === fixture.commands && swaps === 0) {
					swaps++;
					await nodeFs.rename(fixture.commands, fixture.retained);
					await nodeFs.symlink(fixture.outside, fixture.commands, "dir");
				}
				return observed;
			};
			lstat = vi.spyOn(nodeFs, "lstat").mockImplementation(barrier as unknown as typeof nodeFs.lstat);
			await claimScopedEntry(scope!, ENTRY).catch(() => undefined);
			lstat.mockRestore();
			lstat = undefined;
			// Retained authority does not re-resolve the pathname at all, so the
			// barrier never fires and nothing can be redirected outside the root.
			expect(swaps).toBe(0);
			expect(await names(fixture.outside)).toEqual([]);
		} finally {
			lstat?.mockRestore();
			await fixture.cleanup();
		}
	});

	test("a planted symlink, directory, or hard link under an entry name is never read or claimed", async () => {
		const fixture = await scopeFixture();
		try {
			const scope = await openChatDaemonCommandScope({ agentDir: fixture.agentDir, kind: "slack", create: true });
			expect(scope).toBeDefined();
			const secret = path.join(fixture.outside, "secret.json");
			await nodeFs.writeFile(secret, `${JSON.stringify({ stolen: true })}\n`, { mode: 0o600 });

			await nodeFs.symlink(secret, path.join(fixture.commands, ENTRY));
			expect(await readScopedJson(scope!, ENTRY)).toBeUndefined();
			expect(await claimScopedEntry(scope!, ENTRY)).toBe(false);
			await unlinkScopedEntry(scope!, ENTRY);

			await nodeFs.mkdir(path.join(fixture.commands, ENTRY), { mode: 0o700 });
			expect(await readScopedJson(scope!, ENTRY)).toBeUndefined();
			await nodeFs.rmdir(path.join(fixture.commands, ENTRY));

			await nodeFs.link(secret, path.join(fixture.commands, ENTRY));
			expect(await readScopedJson(scope!, ENTRY)).toBeUndefined();
			await nodeFs.unlink(path.join(fixture.commands, ENTRY));

			await nodeFs.writeFile(path.join(fixture.commands, ENTRY), `${JSON.stringify({ loose: true })}\n`, {
				mode: 0o644,
			});
			expect(await readScopedJson(scope!, ENTRY)).toBeUndefined();
		} finally {
			await fixture.cleanup();
		}
	});

	test("a group-writable command directory is repaired or refused, never used as captured", async () => {
		const fixture = await scopeFixture();
		try {
			expect(
				await openChatDaemonCommandScope({ agentDir: fixture.agentDir, kind: "slack", create: true }),
			).toBeDefined();
			await nodeFs.chmod(fixture.commands, 0o777);
			const scope = await openChatDaemonCommandScope({ agentDir: fixture.agentDir, kind: "slack" });
			expect(scope).toBeDefined();
			expect((await nodeFs.lstat(fixture.commands)).mode & 0o077).toBe(0);
		} finally {
			await fixture.cleanup();
		}
	});

	test("an unsupported host fails closed instead of downgrading to pathname operations", async () => {
		const fixture = await scopeFixture();
		try {
			expect(
				await openChatDaemonCommandScope({
					agentDir: fixture.agentDir,
					kind: "slack",
					create: true,
					platform: "win32",
				}),
			).toBeUndefined();
			expect(await names(fixture.root)).toEqual(["outside"]);
		} finally {
			await fixture.cleanup();
		}
	});

	test("an identity-bound removal defers to a successor that took the same name", async () => {
		const fixture = await scopeFixture();
		try {
			const scope = await openChatDaemonCommandScope({ agentDir: fixture.agentDir, kind: "slack", create: true });
			expect(scope).toBeDefined();
			await writeScopedJson(scope!, ENTRY, { outcome: "ok" });
			const decided = await scopedEntryIdentity(scope!, ENTRY);
			expect(decided).toBeDefined();

			// A successor takes the same name after the sweep decided about the
			// object it read, which is exactly what a retained terminal settlement
			// republished under the same identifier looks like.
			await writeScopedJson(scope!, ENTRY, { outcome: "rejected" });
			const successor = await scopedEntryIdentity(scope!, ENTRY);
			expect(successor!.ino).not.toBe(decided!.ino);

			expect(await unlinkScopedEntry(scope!, ENTRY, decided)).toBe("identity_mismatch");
			// The successor survives with its replay authority intact, so no
			// same-identifier redispatch becomes possible.
			expect(await scopedEntryExists(scope!, ENTRY)).toBe(true);
			expect(await readScopedJson(scope!, ENTRY)).toEqual({ outcome: "rejected" });
			expect(await scopedEntryIdentity(scope!, ENTRY)).toMatchObject({ ino: successor!.ino });

			// The exact object the decision was about is still retirable.
			expect(await unlinkScopedEntry(scope!, ENTRY, successor)).toBe("removed");
			expect(await scopedEntryExists(scope!, ENTRY)).toBe(false);
			expect(await unlinkScopedEntry(scope!, ENTRY, successor)).toBe("absent");
		} finally {
			await fixture.cleanup();
		}
	});

	test("the protocol leaves no addressable residue behind an identity-bound removal", async () => {
		const fixture = await scopeFixture();
		try {
			const scope = await openChatDaemonCommandScope({ agentDir: fixture.agentDir, kind: "slack", create: true });
			expect(scope).toBeDefined();
			await writeScopedJson(scope!, ENTRY, { outcome: "ok" });
			const decided = await scopedEntryIdentity(scope!, ENTRY);
			expect(await unlinkScopedEntry(scope!, ENTRY, decided)).toBe("removed");
			expect(await listScopedEntries(scope!)).toEqual([]);
			expect(await names(fixture.commands)).toEqual([]);
		} finally {
			await fixture.cleanup();
		}
	});
});
