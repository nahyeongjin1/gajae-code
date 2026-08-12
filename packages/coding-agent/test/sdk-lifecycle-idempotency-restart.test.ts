import { describe, expect, it } from "bun:test";
import { createHash } from "node:crypto";
import * as fs from "node:fs/promises";
import path from "node:path";
import { Broker } from "../src/sdk/broker/broker";
import { LifecycleLedger } from "../src/sdk/broker/lifecycle-ledger";

describe("SDK lifecycle ledger", () => {
	it("replays terminal responses and rejects conflicts across restarts", async () => {
		const dir = await fs.mkdtemp(path.join(process.env.TMPDIR ?? "/tmp", "gjc-ledger-"));
		const ledger = await new LifecycleLedger(dir).open();
		const begun = await ledger.begin("i", "a");
		if (begun.kind !== "new") throw new Error("expected new");
		await ledger.transition("i", "terminal_ok", { response: { ok: true, result: { sessionId: "s" } } });
		const resumed = await new LifecycleLedger(dir).open();
		expect((await resumed.begin("i", "a")).kind).toBe("replay");
		expect((await resumed.begin("i", "b")).kind).toBe("idempotency_conflict");
	});
	it("retries a clean accepted row after restart", async () => {
		const dir = await fs.mkdtemp(path.join(process.env.TMPDIR ?? "/tmp", "gjc-ledger-accepted-"));
		const ledger = await new LifecycleLedger(dir).open();
		await ledger.begin("i", "a");

		const resumed = await new LifecycleLedger(dir).open();
		expect((await resumed.begin("i", "a")).kind).toBe("new");
		expect((await resumed.begin("i", "b")).kind).toBe("idempotency_conflict");
		await resumed.transition("i", "terminal_ok", { response: { ok: true, result: { sessionId: "s" } } });
		expect((await new LifecycleLedger(dir).open()).get("i")?.state).toBe("terminal_ok");
	});
	it("seals a valid row missing its final newline before appending", async () => {
		const dir = await fs.mkdtemp(path.join(process.env.TMPDIR ?? "/tmp", "gjc-ledger-unsealed-"));
		const ledgerPath = path.join(dir, "sdk", "lifecycle-ledger.jsonl");
		const ledger = await new LifecycleLedger(dir).open();
		await ledger.begin("i", "a");
		const source = await fs.readFile(ledgerPath, "utf8");
		await fs.writeFile(ledgerPath, source.slice(0, -1));

		const resumed = await new LifecycleLedger(dir).open();
		expect((await resumed.begin("i", "a")).kind).toBe("new");
		await resumed.transition("i", "terminal_ok", { response: { ok: true, result: { sessionId: "s" } } });
		const lines = (await fs.readFile(ledgerPath, "utf8")).trimEnd().split("\n");
		expect(lines.map(line => JSON.parse(line))).toHaveLength(2);
	});
	it("quarantines corrupt middle rows and replays later valid rows", async () => {
		const dir = await fs.mkdtemp(path.join(process.env.TMPDIR ?? "/tmp", "gjc-ledger-"));
		const ledger = await new LifecycleLedger(dir).open();
		await ledger.begin("first", "a");
		await fs.appendFile(path.join(dir, "sdk", "lifecycle-ledger.jsonl"), "not json\n");
		const resumed = await new LifecycleLedger(dir).open();
		expect((await resumed.begin("first", "a")).kind).toBe("terminal_uncertain");
		await resumed.begin("later", "b");
		expect(resumed.get("first")).toBeDefined();
		expect(resumed.get("later")).toBeDefined();
		expect(resumed.warnings).not.toHaveLength(0);
		expect(await fs.readFile(path.join(dir, "sdk", "lifecycle-ledger.jsonl.corrupt"), "utf8")).toContain("not json");
	});
	it("fails closed when a torn row may hide side-effect authority", async () => {
		const dir = await fs.mkdtemp(path.join(process.env.TMPDIR ?? "/tmp", "gjc-ledger-torn-"));
		const ledgerPath = path.join(dir, "sdk", "lifecycle-ledger.jsonl");
		const ledger = await new LifecycleLedger(dir).open();
		await ledger.begin("i", "a");
		await fs.appendFile(
			ledgerPath,
			`${JSON.stringify({ version: 1, identity: "i", requestHash: "a", state: "effect_started" }).slice(0, -1)}`,
		);

		const resumed = await new LifecycleLedger(dir).open();
		expect((await resumed.begin("i", "a")).kind).toBe("terminal_uncertain");
		expect(resumed.get("i")?.state).toBe("terminal_uncertain");
		const recoveredLines = (await fs.readFile(ledgerPath, "utf8")).trimEnd().split("\n");
		expect(() => JSON.parse(recoveredLines.at(-2)!)).toThrow();
		expect(JSON.parse(recoveredLines.at(-1)!)).toMatchObject({ identity: "i", state: "terminal_uncertain" });
		expect((await new LifecycleLedger(dir).open()).get("i")?.state).toBe("terminal_uncertain");
	});
	it("does not let a later terminal row clear uncertainty from corrupt middle history", async () => {
		const dir = await fs.mkdtemp(path.join(process.env.TMPDIR ?? "/tmp", "gjc-ledger-corrupt-"));
		const ledgerPath = path.join(dir, "sdk", "lifecycle-ledger.jsonl");
		const ledger = await new LifecycleLedger(dir).open();
		await ledger.begin("i", "a");
		await fs.appendFile(ledgerPath, "not json\n");
		await fs.appendFile(
			ledgerPath,
			`${JSON.stringify({
				version: 1,
				identity: "i",
				requestHash: "a",
				state: "terminal_ok",
				response: { ok: true, result: { sessionId: "s" } },
				responseDigest: createHash("sha256").update('{"ok":true,"result":{"sessionId":"s"}}').digest("hex"),
				ts: Date.now(),
			})}\n`,
		);

		const resumed = await new LifecycleLedger(dir).open();
		expect((await resumed.begin("i", "a")).kind).toBe("terminal_uncertain");
		expect(resumed.get("i")?.state).toBe("terminal_uncertain");
		const quarantined = await fs.readFile(`${ledgerPath}.corrupt`, "utf8");
		expect(quarantined).toContain('"state":"terminal_ok"');
	});
	it("persists complete multibyte rows through durable appends", async () => {
		const dir = await fs.mkdtemp(path.join(process.env.TMPDIR ?? "/tmp", "gjc-ledger-large-"));
		const ledgerPath = path.join(dir, "sdk", "lifecycle-ledger.jsonl");
		const response = { ok: true, result: { payload: "界".repeat(128 * 1024) } };
		const ledger = await new LifecycleLedger(dir).open();
		await ledger.begin("i", "a");
		await ledger.transition("i", "terminal_ok", { response });

		const lines = (await fs.readFile(ledgerPath, "utf8")).trimEnd().split("\n");
		expect(lines.map(line => JSON.parse(line))).toHaveLength(2);
		expect((await new LifecycleLedger(dir).open()).get("i")?.response).toEqual(response);
	});
	it("reads concurrent terminal proof without mutating an unrelated torn append", async () => {
		const dir = await fs.mkdtemp(path.join(process.env.TMPDIR ?? "/tmp", "gjc-ledger-read-terminal-"));
		const ledgerPath = path.join(dir, "sdk", "lifecycle-ledger.jsonl");
		const ledger = await new LifecycleLedger(dir).open();
		await Promise.all([ledger.begin("first", "first-request"), ledger.begin("second", "second-request")]);
		await Promise.all([
			ledger.transition("first", "terminal_ok", { response: { ok: true, result: { sessionId: "first" } } }),
			ledger.transition("second", "terminal_ok", { response: { ok: true, result: { sessionId: "second" } } }),
		]);
		await fs.appendFile(
			ledgerPath,
			JSON.stringify(lifecycleRow("unrelated", "unrelated-request", "effect_started", Date.now())).slice(0, -1),
		);
		const before = await fs.readFile(ledgerPath, "utf8");
		const verifier = new LifecycleLedger(dir);

		await expect(verifier.readTerminal("first", "first-request")).resolves.toMatchObject({
			state: "terminal_ok",
			response: { ok: true, result: { sessionId: "first" } },
		});
		await expect(verifier.readTerminal("second", "second-request")).resolves.toMatchObject({
			state: "terminal_ok",
			response: { ok: true, result: { sessionId: "second" } },
		});
		expect(await fs.readFile(ledgerPath, "utf8")).toBe(before);
		expect(await fs.stat(`${ledgerPath}.corrupt`).catch(() => undefined)).toBeUndefined();
	});

	it("withholds terminal proof for incomplete or conflicting target history", async () => {
		const dir = await fs.mkdtemp(path.join(process.env.TMPDIR ?? "/tmp", "gjc-ledger-read-terminal-target-"));
		const ledgerPath = path.join(dir, "sdk", "lifecycle-ledger.jsonl");
		const ledger = await new LifecycleLedger(dir).open();
		await ledger.begin("target", "request");
		await ledger.transition("target", "terminal_ok", { response: { ok: true, result: { sessionId: "target" } } });
		const terminalSource = await fs.readFile(ledgerPath, "utf8");
		await fs.writeFile(ledgerPath, terminalSource.slice(0, -1));
		await expect(new LifecycleLedger(dir).readTerminal("target", "request")).resolves.toBeUndefined();

		const conflictingDir = await fs.mkdtemp(
			path.join(process.env.TMPDIR ?? "/tmp", "gjc-ledger-read-terminal-conflict-"),
		);
		const conflictingPath = path.join(conflictingDir, "sdk", "lifecycle-ledger.jsonl");
		await fs.mkdir(path.dirname(conflictingPath), { recursive: true });
		await fs.writeFile(
			conflictingPath,
			[
				lifecycleRow("target", "request", "accepted", 1),
				lifecycleRow("target", "request", "terminal_ok", 2, {
					response: { ok: true, result: { sessionId: "target" } },
					responseDigest: "invalid",
				}),
			]
				.map(row => `${JSON.stringify(row)}\n`)
				.join(""),
		);
		await expect(new LifecycleLedger(conflictingDir).readTerminal("target", "request")).resolves.toBeUndefined();
	});
});

function lifecycleRow(
	identity: string,
	requestHash: string,
	state: "accepted" | "effect_started" | "awaiting_ready" | "terminal_ok" | "terminal_error" | "terminal_uncertain",
	ts: number,
	fields: Record<string, unknown> = {},
): Record<string, unknown> {
	return { version: 1, identity, requestHash, state, ts, ...fields };
}

describe("SDK lifecycle ledger history validation", () => {
	it("quarantines a request hash substitution without exposing its effect intent", async () => {
		const dir = await fs.mkdtemp(path.join(process.env.TMPDIR ?? "/tmp", "gjc-ledger-history-"));
		const ledgerPath = path.join(dir, "sdk", "lifecycle-ledger.jsonl");
		await fs.mkdir(path.dirname(ledgerPath), { recursive: true });
		await fs.writeFile(
			ledgerPath,
			[
				lifecycleRow("i", "original", "accepted", 1),
				lifecycleRow("i", "substituted", "effect_started", 2, {
					effectIntent: { sessionId: "untrusted", stateRoot: "/untrusted" },
				}),
			]
				.map(row => `${JSON.stringify(row)}\n`)
				.join(""),
		);

		const ledger = await new LifecycleLedger(dir).open();
		expect(await ledger.begin("i", "original")).toMatchObject({ kind: "terminal_uncertain" });
		expect(ledger.get("i")).toMatchObject({ state: "terminal_uncertain", requestHash: "original" });
		expect(ledger.get("i")?.effectIntent).toBeUndefined();
		expect(await fs.readFile(`${ledgerPath}.corrupt`, "utf8")).toContain("substituted");
	});

	it("quarantines every row after a terminal entry for the same identity", async () => {
		const dir = await fs.mkdtemp(path.join(process.env.TMPDIR ?? "/tmp", "gjc-ledger-history-"));
		const ledgerPath = path.join(dir, "sdk", "lifecycle-ledger.jsonl");
		const response = { ok: true, result: { sessionId: "s" } };
		const responseDigest = createHash("sha256").update(JSON.stringify(response)).digest("hex");
		await fs.mkdir(path.dirname(ledgerPath), { recursive: true });
		await fs.writeFile(
			ledgerPath,
			[
				lifecycleRow("i", "request", "accepted", 1),
				lifecycleRow("i", "request", "terminal_ok", 2, { response, responseDigest }),
				lifecycleRow("i", "request", "accepted", 3),
				lifecycleRow("i", "request", "terminal_error", 4, { response, responseDigest }),
			]
				.map(row => `${JSON.stringify(row)}\n`)
				.join(""),
		);

		const ledger = await new LifecycleLedger(dir).open();
		expect(await ledger.begin("i", "request")).toMatchObject({ kind: "terminal_uncertain" });
		// A quarantined history proves nothing about which outcome happened, so the
		// recovered uncertainty must not carry the prior row's replayable response.
		expect(ledger.get("i")?.response).toBeUndefined();
		expect(ledger.get("i")?.responseDigest).toBeUndefined();
		expect(ledger.get("i")).toMatchObject({ state: "terminal_uncertain", requestHash: "request" });
		const quarantined = await fs.readFile(`${ledgerPath}.corrupt`, "utf8");
		expect(quarantined).toContain('"state":"accepted"');
		expect(quarantined).toContain('"state":"terminal_error"');
	});

	it("accepts repeated and interleaved durable effect markers before a terminal entry", async () => {
		const dir = await fs.mkdtemp(path.join(process.env.TMPDIR ?? "/tmp", "gjc-ledger-history-"));
		const ledgerPath = path.join(dir, "sdk", "lifecycle-ledger.jsonl");
		const response = { ok: true, result: { sessionId: "s" } };
		await fs.mkdir(path.dirname(ledgerPath), { recursive: true });
		await fs.writeFile(
			ledgerPath,
			[
				lifecycleRow("i", "request", "accepted", 1),
				lifecycleRow("i", "request", "accepted", 2),
				lifecycleRow("i", "request", "effect_started", 3),
				lifecycleRow("i", "request", "awaiting_ready", 4),
				lifecycleRow("i", "request", "effect_started", 5),
				lifecycleRow("i", "request", "awaiting_ready", 6),
				lifecycleRow("i", "request", "terminal_ok", 7, {
					response,
					responseDigest: createHash("sha256").update(JSON.stringify(response)).digest("hex"),
				}),
			]
				.map(row => `${JSON.stringify(row)}\n`)
				.join(""),
		);

		const ledger = await new LifecycleLedger(dir).open();
		expect(await ledger.begin("i", "request")).toMatchObject({ kind: "replay", entry: { response } });
		expect(await fs.stat(`${ledgerPath}.corrupt`).catch(() => undefined)).toBeUndefined();
	});

	it("quarantines standalone cleanup authority without appending lifecycle authority", async () => {
		const dir = await fs.mkdtemp(path.join(process.env.TMPDIR ?? "/tmp", "gjc-ledger-unanchored-cleanup-"));
		const ledgerPath = path.join(dir, "sdk", "lifecycle-ledger.jsonl");
		await fs.mkdir(path.dirname(ledgerPath), { recursive: true });
		const cleanupOnly = lifecycleRow("cleanup", "request", "effect_started", 1, {
			response: { ok: false, error: { code: "cleanup_pending", cleanup: { target: "outside" } } },
		});
		const terminalOnly = lifecycleRow("terminal", "request", "terminal_ok", 2, {
			response: { ok: true, result: { sessionId: "untrusted" } },
			responseDigest: createHash("sha256").update('{"ok":true,"result":{"sessionId":"untrusted"}}').digest("hex"),
		});
		const source = [cleanupOnly, terminalOnly].map(row => `${JSON.stringify(row)}\n`).join("");
		await fs.writeFile(ledgerPath, source);

		const ledger = await new LifecycleLedger(dir).open();
		expect(await ledger.begin("cleanup", "request")).toMatchObject({ kind: "terminal_uncertain" });
		expect(await ledger.begin("terminal", "request")).toMatchObject({ kind: "terminal_uncertain" });
		await expect(new LifecycleLedger(dir).readTerminal("cleanup", "request")).resolves.toBeUndefined();
		await expect(new LifecycleLedger(dir).readTerminal("terminal", "request")).resolves.toBeUndefined();
		expect(await fs.readFile(ledgerPath, "utf8")).toBe(source);
		expect(await fs.readFile(`${ledgerPath}.corrupt`, "utf8")).toContain('"effect_started"');
	});

	it("rejects ledger and corrupt-sidecar symlink swaps without modifying their targets", async () => {
		const dir = await fs.mkdtemp(path.join(process.env.TMPDIR ?? "/tmp", "gjc-ledger-symlink-write-"));
		const sdkDir = path.join(dir, "sdk");
		const ledgerPath = path.join(sdkDir, "lifecycle-ledger.jsonl");
		const outside = path.join(dir, "outside");
		await new LifecycleLedger(dir).open();
		await fs.writeFile(outside, "outside-ledger");
		await fs.symlink(outside, ledgerPath);
		await expect(new LifecycleLedger(dir).begin("swap", "request")).rejects.toThrow();
		expect(await fs.readFile(outside, "utf8")).toBe("outside-ledger");

		await fs.unlink(ledgerPath);
		await fs.writeFile(ledgerPath, "not json\n");
		const corruptOutside = path.join(dir, "outside-corrupt");
		await fs.writeFile(corruptOutside, "outside-corrupt");
		await fs.symlink(corruptOutside, `${ledgerPath}.corrupt`);
		await expect(new LifecycleLedger(dir).open()).rejects.toThrow();
		expect(await fs.readFile(corruptOutside, "utf8")).toBe("outside-corrupt");
	});
});

describe("SDK lifecycle ledger bounded writer", () => {
	it("compacts before a writer-generated row threshold and reopens the terminal authority", async () => {
		const dir = await fs.mkdtemp(path.join(process.env.TMPDIR ?? "/tmp", "gjc-ledger-compact-"));
		const ledger = await new LifecycleLedger(dir, { maxRows: 2 }).open();
		await ledger.begin("i", "request");
		await ledger.transition("i", "effect_started");
		const response = { ok: true, result: { sessionId: "survives-compaction" } };
		await ledger.transition("i", "terminal_ok", { response });

		const resumed = await new LifecycleLedger(dir, { maxRows: 2 }).open();
		expect(await resumed.begin("i", "request")).toMatchObject({ kind: "replay", entry: { response } });
		const rows = (await fs.readFile(path.join(dir, "sdk", "lifecycle-ledger.jsonl"), "utf8"))
			.trimEnd()
			.split("\n")
			.map(line => JSON.parse(line));
		expect(rows).toHaveLength(2);
		expect(rows.at(-1)).toMatchObject({ state: "terminal_ok", response });
		expect(rows.at(0)).toMatchObject({ identity: "i", requestHash: "request", state: "accepted" });
	});

	it("compacts an accepted anchor with its latest nonterminal authority", async () => {
		const dir = await fs.mkdtemp(path.join(process.env.TMPDIR ?? "/tmp", "gjc-ledger-compact-effect-"));
		const ledgerPath = path.join(dir, "sdk", "lifecycle-ledger.jsonl");
		const ledger = await new LifecycleLedger(dir, { maxRows: 3 }).open();
		await ledger.begin("first", "request");
		await ledger.transition("first", "accepted");
		await ledger.transition("first", "effect_started");
		await ledger.begin("second", "request");

		const rows = (await fs.readFile(ledgerPath, "utf8"))
			.trimEnd()
			.split("\n")
			.map(line => JSON.parse(line));
		expect(rows).toMatchObject([
			{ identity: "first", requestHash: "request", state: "accepted" },
			{ identity: "first", requestHash: "request", state: "effect_started" },
			{ identity: "second", requestHash: "request", state: "accepted" },
		]);
		expect((await new LifecycleLedger(dir, { maxRows: 3 }).open()).get("first")?.state).toBe("terminal_uncertain");
	});

	it("rejects before writing when compaction cannot make room for the next identity transition", async () => {
		const dir = await fs.mkdtemp(path.join(process.env.TMPDIR ?? "/tmp", "gjc-ledger-compact-full-"));
		const ledgerPath = path.join(dir, "sdk", "lifecycle-ledger.jsonl");
		const ledger = await new LifecycleLedger(dir, { maxRows: 2 }).open();
		await ledger.begin("first", "a");
		await ledger.begin("second", "b");
		const before = await fs.readFile(ledgerPath, "utf8");

		await expect(
			ledger.transition("first", "terminal_ok", { response: { ok: true, result: { sessionId: "first" } } }),
		).rejects.toThrow("Lifecycle ledger compaction exceeds configured bounds.");
		expect(await fs.readFile(ledgerPath, "utf8")).toBe(before);
	});

	it("leaves the prior ledger authoritative when a torn compaction temporary file exists", async () => {
		const dir = await fs.mkdtemp(path.join(process.env.TMPDIR ?? "/tmp", "gjc-ledger-compact-temp-"));
		const ledger = await new LifecycleLedger(dir).open();
		await ledger.begin("i", "request");
		await ledger.transition("i", "terminal_ok", { response: { ok: true, result: { sessionId: "stable" } } });
		const sdkDir = path.join(dir, "sdk");
		await fs.writeFile(path.join(sdkDir, ".lifecycle-ledger.crash.tmp"), "{torn");

		const resumed = await new LifecycleLedger(dir).open();
		expect(await resumed.begin("i", "request")).toMatchObject({
			kind: "replay",
			entry: { response: { ok: true, result: { sessionId: "stable" } } },
		});
	});
});

it("serializes concurrent distinct-identity compactions and reopens both terminal responses", async () => {
	const dir = await fs.mkdtemp(path.join(process.env.TMPDIR ?? "/tmp", "gjc-ledger-compact-fifo-"));
	const ledger = await new LifecycleLedger(dir, { maxRows: 4 }).open();
	await Promise.all([ledger.begin("first", "first-request"), ledger.begin("second", "second-request")]);
	await Promise.all([ledger.transition("first", "effect_started"), ledger.transition("second", "effect_started")]);
	await Promise.all([
		ledger.transition("first", "terminal_ok", { response: { ok: true, result: { sessionId: "first" } } }),
		ledger.transition("second", "terminal_ok", { response: { ok: true, result: { sessionId: "second" } } }),
	]);

	const reopened = await new LifecycleLedger(dir, { maxRows: 4 }).open();
	expect(await reopened.begin("first", "first-request")).toMatchObject({
		kind: "replay",
		entry: { response: { ok: true, result: { sessionId: "first" } } },
	});
	expect(await reopened.begin("second", "second-request")).toMatchObject({
		kind: "replay",
		entry: { response: { ok: true, result: { sessionId: "second" } } },
	});
});
// A corrupt digest can never be produced by the writer — it refuses any row that
// is not broker outcome evidence — so the damaged rows are planted directly in
// the durable source, which is the only way they can actually occur.
it("quarantines terminal-uncertain replay rows with corrupt response or durable-effect digests", async () => {
	const dir = await fs.mkdtemp(path.join(process.env.TMPDIR ?? "/tmp", "gjc-ledger-uncertain-digest-"));
	const ledgerPath = path.join(dir, "sdk", "lifecycle-ledger.jsonl");
	await fs.mkdir(path.dirname(ledgerPath), { recursive: true });
	const response = { ok: false, error: { code: "terminal_uncertain", message: "unproven" } };
	await fs.writeFile(
		ledgerPath,
		[
			lifecycleRow("response", "request-response", "accepted", 1),
			lifecycleRow("response", "request-response", "terminal_uncertain", 2, {
				response,
				responseDigest: "corrupt",
			}),
			lifecycleRow("effects", "request-effects", "accepted", 3),
			lifecycleRow("effects", "request-effects", "terminal_uncertain", 4, {
				response,
				responseDigest: createHash("sha256")
					.update('{"error":{"code":"terminal_uncertain","message":"unproven"},"ok":false}')
					.digest("hex"),
				durableEffects: {
					worktree: { cwdDigest: "worktree", created: true, reused: false },
					digest: "corrupt",
				},
			}),
		]
			.map(row => `${JSON.stringify(row)}\n`)
			.join(""),
	);

	const reopened = await new LifecycleLedger(dir).open();
	expect((await reopened.begin("response", "request-response")).kind).toBe("terminal_uncertain");
	expect((await reopened.begin("effects", "request-effects")).kind).toBe("terminal_uncertain");
	expect(reopened.get("response")?.response).toBeUndefined();
	expect(reopened.get("effects")?.response).toBeUndefined();
	expect(await fs.readFile(`${ledgerPath}.corrupt`, "utf8")).toContain("corrupt");
});

/**
 * A second final row after a valid `terminal_ok` is unexplainable history: the
 * ledger cannot tell which final actually happened, so the recovered uncertainty
 * must drop the prior success entirely. It must also stay stable — no synthetic
 * final appended behind an existing one, no growth across reopen, and no
 * compaction that normalizes the success back into a replayable row.
 */
describe("SDK lifecycle ledger second-final recovery", () => {
	const leakedResponse = { ok: true, result: { sessionId: "leaked-success" } };
	const leakedDigest = createHash("sha256").update(JSON.stringify(leakedResponse)).digest("hex");

	async function writeSecondFinalHistory(dir: string, identity: string, requestHash: string): Promise<string> {
		const ledgerPath = path.join(dir, "sdk", "lifecycle-ledger.jsonl");
		await fs.mkdir(path.dirname(ledgerPath), { recursive: true });
		await fs.writeFile(
			ledgerPath,
			[
				lifecycleRow(identity, requestHash, "accepted", 1),
				lifecycleRow(identity, requestHash, "terminal_ok", 2, {
					response: leakedResponse,
					responseDigest: leakedDigest,
				}),
				lifecycleRow(identity, requestHash, "terminal_error", 3, {
					response: leakedResponse,
					responseDigest: leakedDigest,
				}),
			]
				.map(row => `${JSON.stringify(row)}\n`)
				.join(""),
		);
		return ledgerPath;
	}

	it("recovers a second final into uncertainty that carries no prior success across reopen", async () => {
		const dir = await fs.mkdtemp(path.join(process.env.TMPDIR ?? "/tmp", "gjc-ledger-second-final-"));
		const ledgerPath = await writeSecondFinalHistory(dir, "i", "request");
		const rowsOf = async (file: string) =>
			(await fs.readFile(file, "utf8"))
				.split("\n")
				.filter(Boolean)
				.map(line => JSON.parse(line) as { state: string; response?: unknown });

		const first = await new LifecycleLedger(dir).open();
		expect(await first.begin("i", "request")).toMatchObject({ kind: "terminal_uncertain" });
		expect(first.get("i")?.response).toBeUndefined();
		expect(first.get("i")?.responseDigest).toBeUndefined();
		expect(first.get("i")).toMatchObject({ state: "terminal_uncertain", requestHash: "request" });
		// The prior final stays the only final: no synthetic row is appended behind it.
		expect((await rowsOf(ledgerPath)).filter(row => row.state === "terminal_uncertain")).toHaveLength(0);
		const afterFirst = await fs.readFile(ledgerPath, "utf8");
		const corruptAfterFirst = await fs.readFile(`${ledgerPath}.corrupt`);
		expect(corruptAfterFirst.toString("utf8")).toContain('"state":"terminal_error"');

		// Repeated reopen re-derives the same fence from the same evidence, and
		// re-quarantining byte-identical evidence must not grow the sidecar.
		for (const _ of [0, 1, 2]) {
			const reopened = await new LifecycleLedger(dir).open();
			expect(await reopened.begin("i", "request")).toMatchObject({ kind: "terminal_uncertain" });
			expect(reopened.get("i")?.response).toBeUndefined();
			expect(await fs.readFile(ledgerPath, "utf8")).toBe(afterFirst);
			const corruptNow = await fs.readFile(`${ledgerPath}.corrupt`);
			expect(corruptNow.equals(corruptAfterFirst)).toBe(true);
			expect((await fs.stat(`${ledgerPath}.corrupt`)).size).toBe(corruptAfterFirst.length);
		}

		// Read-back must not bless a history it cannot explain.
		await expect(new LifecycleLedger(dir).readTerminal("i", "request")).resolves.toBeUndefined();
	});

	it("cannot compact a quarantined second final into a replayable success", async () => {
		const dir = await fs.mkdtemp(path.join(process.env.TMPDIR ?? "/tmp", "gjc-ledger-second-final-compact-"));
		const ledgerPath = await writeSecondFinalHistory(dir, "i", "request");

		const ledger = await new LifecycleLedger(dir, { maxRows: 3 }).open();
		await ledger.begin("other", "other-request");
		const compacted = await fs.readFile(ledgerPath, "utf8");
		expect(compacted).not.toContain('"ok":true');
		expect(compacted).not.toContain("leaked-success");
		const rows = compacted
			.split("\n")
			.filter(Boolean)
			.map(line => JSON.parse(line) as { identity: string; state: string; response?: unknown });
		expect(rows.filter(row => row.identity === "i" && row.state === "terminal_uncertain")).toHaveLength(1);
		expect(rows.find(row => row.identity === "i" && row.state === "terminal_uncertain")?.response).toBeUndefined();

		const reopened = await new LifecycleLedger(dir, { maxRows: 3 }).open();
		expect(await reopened.begin("i", "request")).toMatchObject({ kind: "terminal_uncertain" });
		expect(reopened.get("i")?.response).toBeUndefined();
		// The rewritten history is now a legitimately recorded uncertainty, so it reads
		// back as itself — proving uncertainty, never the discarded success.
		const proven = await new LifecycleLedger(dir).readTerminal("i", "request");
		expect(proven).toMatchObject({ state: "terminal_uncertain", requestHash: "request" });
		expect(proven?.response).toBeUndefined();
		expect(proven?.responseDigest).toBeUndefined();
	});

	it("refuses to replay the quarantined success through same-key broker handling", async () => {
		const dir = await fs.mkdtemp(path.join(process.env.TMPDIR ?? "/tmp", "gjc-ledger-second-final-broker-"));
		const ledgerPath = path.join(dir, "sdk", "lifecycle-ledger.jsonl");
		const idempotencyKey = "second-final-leak";
		const input = { sessionId: "leak-probe" };
		const opened = new Broker({ agentDir: dir });
		try {
			// Let the broker mint its own identity and request hash for this exact key,
			// then replace that identity's history with a second final it cannot explain.
			await opened.start();
			await opened.handleRequest("session.unknown", input, idempotencyKey);
			await opened.stop();
			const accepted = (await fs.readFile(ledgerPath, "utf8"))
				.split("\n")
				.filter(Boolean)
				.map(line => JSON.parse(line) as { identity: string; requestHash: string; state: string })
				.find(row => row.state === "accepted");
			if (!accepted) throw new Error("Expected the broker to anchor an accepted lifecycle row");
			await writeSecondFinalHistory(dir, accepted.identity, accepted.requestHash);
		} finally {
			await opened.stop();
		}

		const broker = new Broker({ agentDir: dir });
		try {
			await broker.start();
			const replayed = await broker.handleRequest("session.unknown", input, idempotencyKey);
			expect(replayed).toMatchObject({ ok: false, error: { code: "terminal_uncertain" } });
			expect(JSON.stringify(replayed)).not.toContain("leaked-success");
			// A retry sees the same fence, never the quarantined success.
			const retried = await broker.handleRequest("session.unknown", input, idempotencyKey);
			expect(retried).toMatchObject({ ok: false, error: { code: "terminal_uncertain" } });
			expect(JSON.stringify(retried)).not.toContain("leaked-success");
		} finally {
			await broker.stop();
			await fs.rm(dir, { recursive: true, force: true });
		}
	});
});
/**
 * A final row and the response it carries must agree. `lifecycleResponseState`
 * is the only mapping the broker ever writes, so a row whose state contradicts
 * its own response is unexplainable history: it cannot prove which outcome
 * happened and must never hand a caller the response it carries.
 */
describe("SDK lifecycle ledger final state and response agreement", () => {
	// Keys are written in canonical (sorted) order so `JSON.stringify` reproduces
	// the ledger's own canonical digest input exactly.
	const successResponse = { ok: true, result: { sessionId: "leaked-agreement" } };
	const definitiveFailure = { error: { code: "spawn_failed", message: "leaked-agreement" }, ok: false };
	const uncertainFailure = { error: { code: "terminal_uncertain", message: "leaked-agreement" }, ok: false };
	const mismatches = [
		{
			name: "terminal_uncertain carrying a success response",
			state: "terminal_uncertain",
			response: successResponse,
		},
		{ name: "terminal_error carrying a success response", state: "terminal_error", response: successResponse },
		{ name: "terminal_ok carrying a definitive failure", state: "terminal_ok", response: definitiveFailure },
		{ name: "terminal_error carrying terminal uncertainty", state: "terminal_error", response: uncertainFailure },
	] as const;

	function writeMismatchHistory(
		ledgerPath: string,
		identity: string,
		requestHash: string,
		mismatch: (typeof mismatches)[number],
	): Promise<void> {
		return fs.writeFile(
			ledgerPath,
			[
				lifecycleRow(identity, requestHash, "accepted", 1),
				lifecycleRow(identity, requestHash, mismatch.state, 2, {
					response: mismatch.response,
					responseDigest: createHash("sha256").update(JSON.stringify(mismatch.response)).digest("hex"),
				}),
			]
				.map(row => `${JSON.stringify(row)}\n`)
				.join(""),
		);
	}

	for (const mismatch of mismatches) {
		it(`quarantines ${mismatch.name} without a replayable response`, async () => {
			const dir = await fs.mkdtemp(path.join(process.env.TMPDIR ?? "/tmp", "gjc-ledger-state-response-"));
			const ledgerPath = path.join(dir, "sdk", "lifecycle-ledger.jsonl");
			await fs.mkdir(path.dirname(ledgerPath), { recursive: true });
			await writeMismatchHistory(ledgerPath, "i", "request", mismatch);

			const ledger = await new LifecycleLedger(dir).open();
			expect(await ledger.begin("i", "request")).toMatchObject({ kind: "terminal_uncertain" });
			expect(ledger.get("i")?.response).toBeUndefined();
			expect(ledger.get("i")?.responseDigest).toBeUndefined();
			expect(ledger.get("i")).toMatchObject({ state: "terminal_uncertain", requestHash: "request" });
			await expect(new LifecycleLedger(dir).readTerminal("i", "request")).resolves.toBeUndefined();
			expect(await fs.readFile(`${ledgerPath}.corrupt`, "utf8")).toContain("leaked-agreement");
		});
	}

	it("refuses to replay any state/response mismatch through same-key broker handling", async () => {
		for (const mismatch of mismatches) {
			const dir = await fs.mkdtemp(path.join(process.env.TMPDIR ?? "/tmp", "gjc-broker-state-response-"));
			const ledgerPath = path.join(dir, "sdk", "lifecycle-ledger.jsonl");
			const idempotencyKey = "state-response-mismatch";
			const input = { sessionId: "agreement-probe" };
			const opened = new Broker({ agentDir: dir });
			try {
				await opened.start();
				await opened.handleRequest("session.unknown", input, idempotencyKey);
			} finally {
				await opened.stop();
			}
			const accepted = (await fs.readFile(ledgerPath, "utf8"))
				.split("\n")
				.filter(Boolean)
				.map(line => JSON.parse(line) as { identity: string; requestHash: string; state: string })
				.find(row => row.state === "accepted");
			if (!accepted) throw new Error("Expected the broker to anchor an accepted lifecycle row");
			await writeMismatchHistory(ledgerPath, accepted.identity, accepted.requestHash, mismatch);

			const broker = new Broker({ agentDir: dir });
			try {
				await broker.start();
				const replayed = await broker.handleRequest("session.unknown", input, idempotencyKey);
				expect(replayed).toMatchObject({ ok: false, error: { code: "terminal_uncertain" } });
				expect(JSON.stringify(replayed)).not.toContain("leaked-agreement");
			} finally {
				await broker.stop();
				await fs.rm(dir, { recursive: true, force: true });
			}
		}
	}, 30_000);
});

/**
 * A complete line that cannot be parsed or schema-validated at all is
 * unattributable corruption. It may be a final row for any identity already
 * present in the same source, so every known identity — including one that
 * already reached a durable final — must fail closed, and no later compaction
 * may normalize the discarded success back into a replayable row.
 */
describe("SDK lifecycle ledger parse-impossible corruption after a durable final", () => {
	const leakedResponse = { ok: true, result: { sessionId: "leaked-after-final" } };
	const leakedDigest = createHash("sha256").update(JSON.stringify(leakedResponse)).digest("hex");
	// A complete (newline-terminated) line that claims this identity but can never
	// be decoded: neither JSON nor the ledger schema can attribute it.
	const malformedFinal = (identity: string, requestHash: string) =>
		`{"version":1,"identity":${JSON.stringify(identity)},"requestHash":${JSON.stringify(
			requestHash,
		)},"state":"terminal_error","response":{"ok":true,\n`;

	async function writeCorruptAfterFinal(dir: string, identity: string, requestHash: string): Promise<string> {
		const ledgerPath = path.join(dir, "sdk", "lifecycle-ledger.jsonl");
		await fs.mkdir(path.dirname(ledgerPath), { recursive: true });
		await fs.writeFile(
			ledgerPath,
			`${[
				lifecycleRow(identity, requestHash, "accepted", 1),
				lifecycleRow(identity, requestHash, "terminal_ok", 2, {
					response: leakedResponse,
					responseDigest: leakedDigest,
				}),
			]
				.map(row => `${JSON.stringify(row)}\n`)
				.join("")}${malformedFinal(identity, requestHash)}`,
		);
		return ledgerPath;
	}

	it("fails a durable final closed and keeps the corrupt sidecar byte-stable across reopens", async () => {
		const dir = await fs.mkdtemp(path.join(process.env.TMPDIR ?? "/tmp", "gjc-ledger-corrupt-after-final-"));
		const ledgerPath = await writeCorruptAfterFinal(dir, "i", "request");
		const corruptPath = `${ledgerPath}.corrupt`;

		const first = await new LifecycleLedger(dir).open();
		expect(await first.begin("i", "request")).toMatchObject({ kind: "terminal_uncertain" });
		expect(first.get("i")?.response).toBeUndefined();
		expect(first.get("i")?.responseDigest).toBeUndefined();
		expect(first.get("i")).toMatchObject({ state: "terminal_uncertain", requestHash: "request" });
		const afterFirst = await fs.readFile(ledgerPath, "utf8");
		// No synthetic final is appended behind the durable final it fenced.
		expect(
			afterFirst
				.split("\n")
				.filter(Boolean)
				.filter(line => line.includes('"state":"terminal_uncertain"')),
		).toHaveLength(0);
		const corruptAfterFirst = await fs.readFile(corruptPath);
		expect(corruptAfterFirst.toString("utf8")).toContain('"response":{"ok":true,');

		for (const _ of [0, 1, 2]) {
			const reopened = await new LifecycleLedger(dir).open();
			expect(await reopened.begin("i", "request")).toMatchObject({ kind: "terminal_uncertain" });
			expect(reopened.get("i")?.response).toBeUndefined();
			expect(await fs.readFile(ledgerPath, "utf8")).toBe(afterFirst);
			const corruptNow = await fs.readFile(corruptPath);
			expect(corruptNow.equals(corruptAfterFirst)).toBe(true);
			expect((await fs.stat(corruptPath)).size).toBe(corruptAfterFirst.length);
			expect(createHash("sha256").update(corruptNow).digest("hex")).toBe(
				createHash("sha256").update(corruptAfterFirst).digest("hex"),
			);
		}

		// Distinct corrupt evidence is never suppressed by that idempotency.
		await fs.appendFile(ledgerPath, "also not json\n");
		await new LifecycleLedger(dir).open();
		const grown = await fs.readFile(corruptPath);
		expect(grown.subarray(0, corruptAfterFirst.length).equals(corruptAfterFirst)).toBe(true);
		expect(grown.toString("utf8")).toContain("also not json");
		expect(grown.length).toBeGreaterThan(corruptAfterFirst.length);

		await expect(new LifecycleLedger(dir).readTerminal("i", "request")).resolves.toBeUndefined();
		await fs.rm(dir, { recursive: true, force: true });
	});

	it("cannot compact parse-impossible corruption into a replayable success", async () => {
		const dir = await fs.mkdtemp(path.join(process.env.TMPDIR ?? "/tmp", "gjc-ledger-corrupt-compact-"));
		const ledgerPath = await writeCorruptAfterFinal(dir, "i", "request");

		const ledger = await new LifecycleLedger(dir, { maxRows: 3 }).open();
		await ledger.begin("other", "other-request");
		const compacted = await fs.readFile(ledgerPath, "utf8");
		expect(compacted).not.toContain("leaked-after-final");
		expect(compacted).not.toContain('"state":"terminal_ok"');
		const rows = compacted
			.split("\n")
			.filter(Boolean)
			.map(line => JSON.parse(line) as { identity: string; state: string; response?: unknown });
		expect(rows.filter(row => row.identity === "i" && row.state === "terminal_uncertain")).toHaveLength(1);
		expect(rows.find(row => row.identity === "i" && row.state === "terminal_uncertain")?.response).toBeUndefined();

		const proven = await new LifecycleLedger(dir).readTerminal("i", "request");
		expect(proven).toMatchObject({ state: "terminal_uncertain", requestHash: "request" });
		expect(proven?.response).toBeUndefined();
		await fs.rm(dir, { recursive: true, force: true });
	});

	it("refuses to replay the fenced success through same-key broker handling", async () => {
		const dir = await fs.mkdtemp(path.join(process.env.TMPDIR ?? "/tmp", "gjc-broker-corrupt-after-final-"));
		const ledgerPath = path.join(dir, "sdk", "lifecycle-ledger.jsonl");
		const idempotencyKey = "corrupt-after-final";
		const input = { sessionId: "corrupt-after-final-probe" };
		const opened = new Broker({ agentDir: dir });
		try {
			await opened.start();
			await opened.handleRequest("session.unknown", input, idempotencyKey);
		} finally {
			await opened.stop();
		}
		const accepted = (await fs.readFile(ledgerPath, "utf8"))
			.split("\n")
			.filter(Boolean)
			.map(line => JSON.parse(line) as { identity: string; requestHash: string; state: string })
			.find(row => row.state === "accepted");
		if (!accepted) throw new Error("Expected the broker to anchor an accepted lifecycle row");
		await writeCorruptAfterFinal(dir, accepted.identity, accepted.requestHash);

		const broker = new Broker({ agentDir: dir });
		try {
			await broker.start();
			const replayed = await broker.handleRequest("session.unknown", input, idempotencyKey);
			expect(replayed).toMatchObject({ ok: false, error: { code: "terminal_uncertain" } });
			expect(JSON.stringify(replayed)).not.toContain("leaked-after-final");
			const retried = await broker.handleRequest("session.unknown", input, idempotencyKey);
			expect(retried).toMatchObject({ ok: false, error: { code: "terminal_uncertain" } });
			expect(JSON.stringify(retried)).not.toContain("leaked-after-final");
		} finally {
			await broker.stop();
			await fs.rm(dir, { recursive: true, force: true });
		}
	});
});

/**
 * The `session.delete` shorthand exists so a caller that lost its locator can
 * still reconcile an owned cleanup. It is not an escape hatch around the
 * idempotency fence or durable terminal revalidation: a remembered terminal
 * outcome is never proof, and a shorthand retry is a different request than the
 * full-locator delete that anchored the key.
 */
describe("SDK broker session.delete shorthand terminal revalidation", () => {
	it("fences a full-locator key against a shorthand retry and revalidates minimal replays", async () => {
		const dir = await fs.mkdtemp(path.join(process.env.TMPDIR ?? "/tmp", "gjc-broker-delete-shorthand-"));
		const ledgerPath = path.join(dir, "sdk", "lifecycle-ledger.jsonl");
		const workspace = path.join(dir, "workspace");
		await fs.mkdir(path.join(workspace, ".gjc", "state"), { recursive: true });
		const broker = new Broker({ agentDir: dir });
		try {
			await broker.start();

			// A full-locator delete anchors this key with its own request hash.
			const full = await broker.handleRequest(
				"session.delete",
				{
					sessionId: "shorthand-fenced",
					cwd: workspace,
					sessionPath: path.join(workspace, "shorthand-fenced.jsonl"),
				},
				"delete-shorthand-fence",
			);
			expect(full).toMatchObject({ ok: false });
			const anchoredState = (await fs.readFile(ledgerPath, "utf8"))
				.split("\n")
				.filter(Boolean)
				.map(line => JSON.parse(line) as { state: string })
				.at(-1)?.state;
			expect(anchoredState).toBe("terminal_error");

			// The same key retried with only `{sessionId}` is a different request and
			// must never promote the remembered outcome.
			const shorthand = await broker.handleRequest(
				"session.delete",
				{ sessionId: "shorthand-fenced" },
				"delete-shorthand-fence",
			);
			expect(shorthand).toMatchObject({ ok: false, error: { code: "idempotency_conflict" } });

			// A shorthand delete under its own key anchors that exact minimal request.
			const minimalKey = "delete-shorthand-minimal";
			const minimalInput = { sessionId: "shorthand-minimal" };
			const first = await broker.handleRequest("session.delete", minimalInput, minimalKey);
			expect(first).toMatchObject({ ok: true });
			const anchored = await fs.readFile(ledgerPath, "utf8");

			// While the durable terminal row cannot be re-read, the exact same request
			// stays generically uncertain and writes no second final.
			await fs.appendFile(ledgerPath, "not json\n");
			const unverified = await broker.handleRequest("session.delete", minimalInput, minimalKey);
			expect(unverified).toMatchObject({ ok: false, error: { code: "terminal_uncertain" } });
			expect(await fs.readFile(ledgerPath, "utf8")).toBe(`${anchored}not json\n`);

			// A later authoritative read may replay the original exact request.
			await fs.writeFile(ledgerPath, anchored);
			const replayed = await broker.handleRequest("session.delete", minimalInput, minimalKey);
			expect(replayed).toMatchObject({ ok: true });
			expect(await fs.readFile(ledgerPath, "utf8")).toBe(anchored);

			const rows = anchored
				.split("\n")
				.filter(Boolean)
				.map(line => JSON.parse(line) as { identity: string; state: string });
			for (const identity of new Set(rows.map(row => row.identity)))
				expect(
					rows.filter(
						row =>
							row.identity === identity &&
							(row.state === "terminal_ok" ||
								row.state === "terminal_error" ||
								row.state === "terminal_uncertain"),
					),
				).toHaveLength(1);
			expect(await fs.stat(`${ledgerPath}.corrupt`).catch(() => undefined)).toBeUndefined();
		} finally {
			await broker.stop();
			await fs.rm(dir, { recursive: true, force: true });
		}
	}, 30_000);
});
