import { describe, expect, it } from "bun:test";
import { createHash } from "node:crypto";
import * as fs from "node:fs/promises";
import path from "node:path";
import { LifecycleLedger, type LifecycleState } from "../src/sdk/broker/lifecycle-ledger";

/**
 * A lifecycle ledger row is broker outcome evidence, never a generic durable log
 * line.
 *
 * `lifecycleResponseState` is the only mapping the broker ever writes, so a row
 * that reaches a final state must carry the exact `BrokerResponse` that mapping
 * accepts: a success with its own boolean `ok:true`, a definitive failure with
 * its own boolean `ok:false` and a well-formed error, or — for uncertainty — no
 * response at all, an explicit `terminal_uncertain` failure, or retained cleanup
 * authority.
 *
 * Anything else is an opaque payload. It cannot prove which outcome happened, so
 * it may never be normalized into success, replayed to a caller, or read back as
 * terminal proof. These regressions pin the exact forged row observed in the
 * external repro plus every neighbouring `ok`/error discriminant.
 */

function canonicalJson(value: unknown): string {
	if (value === null || typeof value !== "object") return JSON.stringify(value);
	if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
	const record = value as Record<string, unknown>;
	return `{${Object.keys(record)
		.filter(key => record[key] !== undefined)
		.sort()
		.map(key => `${JSON.stringify(key)}:${canonicalJson(record[key])}`)
		.join(",")}}`;
}

function responseDigest(response: unknown): string {
	return createHash("sha256").update(canonicalJson(response)).digest("hex");
}

async function ledgerDir(prefix: string): Promise<string> {
	return fs.mkdtemp(path.join(process.env.TMPDIR ?? "/tmp", prefix));
}

function ledgerFile(dir: string): string {
	return path.join(dir, "sdk", "lifecycle-ledger.jsonl");
}

function row(
	identity: string,
	requestHash: string,
	state: LifecycleState,
	fields: Record<string, unknown> = {},
): Record<string, unknown> {
	return { version: 1, identity, requestHash, state, ts: Date.now(), ...fields };
}

function finalRow(identity: string, requestHash: string, state: LifecycleState, response: unknown) {
	return row(identity, requestHash, state, { response, responseDigest: responseDigest(response) });
}

async function writeRows(dir: string, rows: Record<string, unknown>[]): Promise<string> {
	const file = ledgerFile(dir);
	await fs.mkdir(path.dirname(file), { recursive: true, mode: 0o700 });
	await fs.writeFile(file, rows.map(entry => `${JSON.stringify(entry)}\n`).join(""), { mode: 0o600 });
	return file;
}

/** The exact opaque payload the external repro replayed as a successful outcome. */
const HERMES_OPAQUE_RESPONSE = { sessionId: "not-a-broker-response" };

const OK_RESPONSE = { ok: true, result: { sessionId: "s" } };
const ERROR_RESPONSE = { ok: false, error: { code: "spawn_failed", message: "boom" } };
const UNCERTAIN_RESPONSE = { ok: false, error: { code: "terminal_uncertain", message: "unproven" } };
const CLEANUP_RESPONSE = {
	ok: false,
	error: { code: "cleanup_pending", message: "retained", cleanup: { sessionId: "s" } },
};

describe("SDK lifecycle ledger terminal response authenticity", () => {
	it("refuses to persist the opaque terminal_ok response from the external repro", async () => {
		const dir = await ledgerDir("gjc-ledger-opaque-write-");
		const ledger = await new LifecycleLedger(dir).open();
		expect((await ledger.begin("i", "hash")).kind).toBe("new");

		await expect(ledger.transition("i", "terminal_ok", { response: HERMES_OPAQUE_RESPONSE })).rejects.toThrow();

		// Nothing durable may describe the refused outcome, and the identity must
		// stay retryable rather than become a replayable success.
		const persisted = await fs.readFile(ledgerFile(dir), "utf8");
		expect(persisted).not.toContain("not-a-broker-response");
		const reopened = await new LifecycleLedger(dir).open();
		expect((await reopened.begin("i", "hash")).kind).toBe("new");
		await expect(new LifecycleLedger(dir).readTerminal("i", "hash")).resolves.toBeUndefined();
	});

	it("never replays a forged opaque terminal_ok row planted in the durable source", async () => {
		const dir = await ledgerDir("gjc-ledger-opaque-plant-");
		const file = await writeRows(dir, [
			row("i", "hash", "accepted"),
			finalRow("i", "hash", "terminal_ok", HERMES_OPAQUE_RESPONSE),
		]);

		const reopened = await new LifecycleLedger(dir).open();
		const begun = await reopened.begin("i", "hash");
		expect(begun.kind).toBe("terminal_uncertain");
		expect(reopened.get("i")?.response).toBeUndefined();
		expect(reopened.get("i")?.responseDigest).toBeUndefined();
		await expect(new LifecycleLedger(dir).readTerminal("i", "hash")).resolves.toBeUndefined();
		expect(await fs.readFile(`${file}.corrupt`, "utf8")).toContain("not-a-broker-response");

		// Recovery is re-derived from the same source on every reopen, so a second
		// pass must reach the same fail-closed conclusion.
		const again = await new LifecycleLedger(dir).open();
		expect((await again.begin("i", "hash")).kind).toBe("terminal_uncertain");
		expect(again.get("i")?.response).toBeUndefined();
	});

	it("refuses an inherited ok discriminant that serializes to an opaque payload", async () => {
		const dir = await ledgerDir("gjc-ledger-inherited-ok-");
		const ledger = await new LifecycleLedger(dir).open();
		await ledger.begin("i", "hash");

		// `Object.create` puts `ok` on the prototype: `"ok" in response` is true, but
		// nothing about it survives serialization, so it is not own evidence.
		const inherited = Object.create({ ok: true }) as Record<string, unknown>;
		await expect(ledger.transition("i", "terminal_ok", { response: inherited })).rejects.toThrow();
		expect(await fs.readFile(ledgerFile(dir), "utf8")).not.toContain("terminal_ok");
	});

	const forgedTerminalOk: readonly [string, unknown][] = [
		["missing ok", HERMES_OPAQUE_RESPONSE],
		["JSON-inherited ok", JSON.parse('{"__proto__":{"ok":true}}')],
		["empty payload", {}],
		["string ok", { ok: "true" }],
		["numeric ok", { ok: 1 }],
		["null ok", { ok: null }],
		["array payload", [{ ok: true }]],
		["null payload", null],
		["primitive payload", "ok"],
		["success carrying an error", { ok: true, error: { code: "spawn_failed", message: "boom" } }],
		["definitive failure", ERROR_RESPONSE],
		["uncertainty", UNCERTAIN_RESPONSE],
	];

	for (const [label, response] of forgedTerminalOk) {
		it(`refuses a terminal_ok row whose response is ${label}`, async () => {
			const dir = await ledgerDir("gjc-ledger-forged-ok-");
			await writeRows(dir, [row("i", "hash", "accepted"), finalRow("i", "hash", "terminal_ok", response)]);

			const reopened = await new LifecycleLedger(dir).open();
			expect((await reopened.begin("i", "hash")).kind).toBe("terminal_uncertain");
			expect(reopened.get("i")?.response).toBeUndefined();
			await expect(new LifecycleLedger(dir).readTerminal("i", "hash")).resolves.toBeUndefined();
		});
	}

	const forgedTerminalError: readonly [string, unknown][] = [
		["missing ok", { error: { code: "spawn_failed", message: "boom" } }],
		["success", OK_RESPONSE],
		["string ok", { ok: "false", error: { code: "spawn_failed", message: "boom" } }],
		["a failure with no error object", { ok: false }],
		["a failure with a string error", { ok: false, error: "boom" }],
		["a failure with a null error", { ok: false, error: null }],
		["a failure with an array error", { ok: false, error: [{ code: "spawn_failed", message: "boom" }] }],
		["a failure with a non-string code", { ok: false, error: { code: 7, message: "boom" } }],
		["a failure with no message", { ok: false, error: { code: "spawn_failed" } }],
		["uncertainty", UNCERTAIN_RESPONSE],
		["retained cleanup authority", CLEANUP_RESPONSE],
	];

	for (const [label, response] of forgedTerminalError) {
		it(`refuses a terminal_error row whose response is ${label}`, async () => {
			const dir = await ledgerDir("gjc-ledger-forged-error-");
			await writeRows(dir, [row("i", "hash", "accepted"), finalRow("i", "hash", "terminal_error", response)]);

			const reopened = await new LifecycleLedger(dir).open();
			expect((await reopened.begin("i", "hash")).kind).toBe("terminal_uncertain");
			expect(reopened.get("i")?.response).toBeUndefined();
			await expect(new LifecycleLedger(dir).readTerminal("i", "hash")).resolves.toBeUndefined();
		});
	}

	const forgedTerminalUncertain: readonly [string, unknown][] = [
		["an opaque payload", HERMES_OPAQUE_RESPONSE],
		["a success", OK_RESPONSE],
		["a JSON-inherited ok", JSON.parse('{"__proto__":{"ok":true}}')],
		["a non-boolean ok", { ok: "false", error: { code: "terminal_uncertain", message: "unproven" } }],
		["a definitive failure", ERROR_RESPONSE],
		["a malformed uncertainty", { ok: false, error: { code: "terminal_uncertain" } }],
	];

	for (const [label, response] of forgedTerminalUncertain) {
		it(`refuses a terminal_uncertain row carrying ${label}`, async () => {
			const dir = await ledgerDir("gjc-ledger-forged-uncertain-");
			await writeRows(dir, [row("i", "hash", "accepted"), finalRow("i", "hash", "terminal_uncertain", response)]);

			const reopened = await new LifecycleLedger(dir).open();
			const begun = await reopened.begin("i", "hash");
			expect(begun.kind).toBe("terminal_uncertain");
			// The quarantined row's own response must never survive as replay material.
			expect(reopened.get("i")?.response).toBeUndefined();
			await expect(new LifecycleLedger(dir).readTerminal("i", "hash")).resolves.toBeUndefined();
		});
	}

	it("refuses an opaque response on a non-final row that recovery would carry forward", async () => {
		const dir = await ledgerDir("gjc-ledger-forged-effect-");
		await writeRows(dir, [
			row("i", "hash", "accepted"),
			row("i", "hash", "effect_started", {
				response: OK_RESPONSE,
				responseDigest: responseDigest(OK_RESPONSE),
			}),
		]);

		const reopened = await new LifecycleLedger(dir).open();
		expect((await reopened.begin("i", "hash")).kind).toBe("terminal_uncertain");
		expect(reopened.get("i")?.response).toBeUndefined();
	});

	it("still replays every valid broker outcome", async () => {
		const dir = await ledgerDir("gjc-ledger-valid-");
		const ledger = await new LifecycleLedger(dir).open();
		await ledger.begin("ok", "h1");
		await ledger.transition("ok", "terminal_ok", { response: OK_RESPONSE });
		await ledger.begin("err", "h2");
		await ledger.transition("err", "terminal_error", { response: ERROR_RESPONSE });
		await ledger.begin("unc", "h3");
		await ledger.transition("unc", "terminal_uncertain", { response: UNCERTAIN_RESPONSE });
		await ledger.begin("cleanup", "h4");
		await ledger.transition("cleanup", "effect_started", {
			intendedSessionId: "s",
			response: CLEANUP_RESPONSE,
		});

		const reopened = await new LifecycleLedger(dir).open();
		const okReplay = await reopened.begin("ok", "h1");
		expect(okReplay.kind).toBe("replay");
		expect(okReplay.kind === "replay" ? okReplay.entry.response : undefined).toEqual(OK_RESPONSE);
		const errorReplay = await reopened.begin("err", "h2");
		expect(errorReplay.kind).toBe("replay");
		expect(errorReplay.kind === "replay" ? errorReplay.entry.response : undefined).toEqual(ERROR_RESPONSE);
		expect((await reopened.begin("unc", "h3")).kind).toBe("terminal_uncertain");
		const cleanupReplay = await reopened.begin("cleanup", "h4");
		expect(cleanupReplay.kind).toBe("replay");
		expect(cleanupReplay.kind === "replay" ? cleanupReplay.entry.response : undefined).toEqual(CLEANUP_RESPONSE);

		await expect(new LifecycleLedger(dir).readTerminal("ok", "h1")).resolves.toMatchObject({
			state: "terminal_ok",
			response: OK_RESPONSE,
		});
		await expect(new LifecycleLedger(dir).readTerminal("err", "h2")).resolves.toMatchObject({
			state: "terminal_error",
			response: ERROR_RESPONSE,
		});
	});

	it("never compacts a forged terminal row into replayable success", async () => {
		const dir = await ledgerDir("gjc-ledger-forged-compaction-");
		const file = await writeRows(dir, [
			row("i", "hash", "accepted"),
			finalRow("i", "hash", "terminal_ok", HERMES_OPAQUE_RESPONSE),
		]);
		// Bounded so the appends below force a real compaction pass: the rewritten
		// snapshot may only contain rows the ledger itself validated.
		const ledger = await new LifecycleLedger(dir, { maxRows: 8 }).open();
		await ledger.begin("a", "h-a");
		await ledger.transition("a", "effect_started", { intendedSessionId: "s", response: CLEANUP_RESPONSE });
		await ledger.transition("a", "terminal_error", { response: ERROR_RESPONSE });
		await ledger.begin("b", "h-b");
		await ledger.transition("b", "terminal_ok", { response: OK_RESPONSE });
		await ledger.begin("c", "h-c");
		await ledger.transition("c", "terminal_ok", { response: OK_RESPONSE });

		const compacted = await fs.readFile(file, "utf8");
		expect(compacted).not.toContain("not-a-broker-response");
		const reopened = await new LifecycleLedger(dir).open();
		expect((await reopened.begin("i", "hash")).kind).toBe("terminal_uncertain");
		expect(reopened.get("i")?.response).toBeUndefined();
	});
});
