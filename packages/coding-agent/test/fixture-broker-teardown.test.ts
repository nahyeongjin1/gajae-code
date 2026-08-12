import { afterEach, describe, expect, spyOn, test } from "bun:test";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { Process } from "@gajae-code/natives";
import type { Subprocess } from "bun";
import { brokerDiscoveryPath, writeBrokerDiscovery } from "../src/sdk/broker/discovery";
import {
	type FixtureBrokerDiscovery,
	fixtureBrokerTeardownProblem,
	type ProcessPresence,
	type RetainedProcess,
	type RetainedProcessOpen,
	rememberFixtureBrokerAt,
	settleFixtureBrokerAt,
	settleFixtureBrokerTargets,
	settleFixtureBrokerWithOwner,
	settleRememberedFixtureBroker,
} from "./helpers/owned-children";

const SIGTERM = os.constants.signals.SIGTERM;
const SIGKILL = os.constants.signals.SIGKILL;
/** Platforms whose kernel exposes a root-only signal primitive to a pinned reference. */
const HAS_ROOT_SIGNAL_PRIMITIVE = process.platform !== "darwin";

const roots: string[] = [];

async function agentRoot(): Promise<string> {
	const dir = await fs.mkdtemp(path.join(os.tmpdir(), "gjc-fixture-teardown-"));
	const canonical = await fs.realpath(dir);
	roots.push(canonical);
	return path.join(canonical, "agent");
}

async function writeDiscoveryText(agentDir: string, text: string): Promise<void> {
	const file = brokerDiscoveryPath(agentDir);
	await fs.mkdir(path.dirname(file), { recursive: true });
	await fs.writeFile(file, text);
}

/**
 * Run `body` against a long-lived child this test owns outright.
 *
 * The child is always stopped through its own launch-time handle — including
 * when `body` throws — so a failing assertion can never leave a process behind
 * and teardown is never asked to reach for a pid it did not launch.
 */
async function withIdleChild(body: (child: Subprocess & { pid: number }) => Promise<void>): Promise<void> {
	const child = Bun.spawn([process.execPath, "-e", "setInterval(() => {}, 1_000)"], {
		stdout: "ignore",
		stderr: "ignore",
		stdin: "ignore",
	});
	if (!child.pid) throw new Error("fixture child has no pid");
	try {
		await body(child as Subprocess & { pid: number });
	} finally {
		if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
		await child.exited;
	}
}

async function publishFor(agentDir: string, pid: number): Promise<void> {
	await writeBrokerDiscovery(agentDir, {
		version: 1,
		protocolVersion: 3,
		packageGeneration: "test",
		ownerId: "fixture-teardown-test",
		pid,
		host: "127.0.0.1",
		port: 1,
		url: "ws://127.0.0.1:1",
		token: "fixture-teardown-token",
		startedAt: Date.now(),
		heartbeatAt: Date.now(),
	});
}

/** Every signal a spy observed that was not the read-only existence probe. */
function deliveredSignals(kill: { mock: { calls: unknown[][] } }): unknown[] {
	return kill.mock.calls.filter(([, signal]) => signal !== 0).map(([, signal]) => signal);
}

/**
 * A retained reference with scripted kernel behaviour.
 *
 * Every signal is recorded on the exact reference it was delivered through, so
 * a test can prove which incarnation — if any — a teardown authorized. The
 * descendants and the tree-terminating method exist only so a test can prove
 * teardown never reaches for either: neither is part of {@link RetainedProcess}.
 */
class FakeRetainedProcess implements RetainedProcess {
	readonly delivered: number[] = [];
	readonly descendants: FakeRetainedProcess[] = [];
	terminateCalls = 0;
	#alive = true;
	constructor(
		readonly pid: number,
		readonly incarnation: string,
		readonly behavior: {
			refuseSignal?: boolean;
			exitOnSignal?: number;
			liveness?: "running" | "exited" | "unprovable";
			wait?: "exited" | "running" | "unprovable";
		} = {},
	) {}
	signalRoot(signal: number): boolean {
		if (this.behavior.refuseSignal) return false;
		this.delivered.push(signal);
		if (this.behavior.exitOnSignal === signal) this.#alive = false;
		return true;
	}
	liveness(): "running" | "exited" | "unprovable" {
		return this.behavior.liveness ?? (this.#alive ? "running" : "exited");
	}
	async waitForExit(): Promise<"exited" | "running" | "unprovable"> {
		return this.behavior.wait ?? (this.#alive ? "running" : "exited");
	}
	/** Never reachable through the retained authority; a call here is a defect. */
	async terminate(): Promise<boolean> {
		this.terminateCalls += 1;
		for (const descendant of this.descendants) descendant.delivered.push(SIGTERM, SIGKILL);
		this.#alive = false;
		return true;
	}
}

const opened = (reference: RetainedProcess): RetainedProcessOpen => ({ kind: "opened", process: reference });

/** Hands out one scripted open result per acquisition attempt, in order. */
function opener(...results: RetainedProcessOpen[]) {
	const remaining = [...results];
	return {
		acquisitions: 0,
		open(): RetainedProcessOpen {
			this.acquisitions += 1;
			return (remaining.length > 1 ? remaining.shift() : remaining[0]) ?? { kind: "unprovable" };
		},
	};
}

afterEach(async () => {
	for (const root of roots.splice(0)) await fs.rm(root, { recursive: true, force: true });
});

describe("fixture broker identity capture", () => {
	test("a missing discovery record is identity_unavailable, never proof of cleanup", async () => {
		const agentDir = await agentRoot();
		expect(await rememberFixtureBrokerAt(agentDir)).toEqual({ kind: "identity_unavailable", agentDir });
		expect(await settleFixtureBrokerAt(agentDir)).toEqual({ outcome: "identity_unavailable", agentDir });
	});

	test("an unreadable or invalid discovery record is identity_malformed, never proof of cleanup", async () => {
		for (const text of [
			"{not json",
			"[]",
			JSON.stringify({ version: 1, protocolVersion: 3, pid: 0, incarnation: "linux:1" }),
			JSON.stringify({ version: 1, protocolVersion: 3, pid: 4_242, incarnation: "not-an-incarnation" }),
			JSON.stringify({ version: 1, protocolVersion: 3, pid: 4_242 }),
			JSON.stringify({ version: 1, protocolVersion: 2, pid: 4_242, incarnation: "linux:1" }),
		]) {
			const agentDir = await agentRoot();
			await writeDiscoveryText(agentDir, text);
			expect(await rememberFixtureBrokerAt(agentDir)).toEqual({ kind: "identity_malformed", agentDir });
			expect(await settleFixtureBrokerAt(agentDir)).toEqual({ outcome: "identity_malformed", agentDir });
		}
	});

	test("a record naming this process is reported as self-owned and is never signalled", async () => {
		const agentDir = await agentRoot();
		await publishFor(agentDir, process.pid);
		const kill = spyOn(process, "kill");
		try {
			expect(await rememberFixtureBrokerAt(agentDir)).toEqual({ kind: "self_owned", agentDir, pid: process.pid });
			expect(await settleFixtureBrokerAt(agentDir)).toEqual({ outcome: "self_owned", agentDir, pid: process.pid });
			expect(kill).not.toHaveBeenCalled();
		} finally {
			kill.mockRestore();
		}
	});

	test("a live exact record captures the running process identity", async () => {
		const agentDir = await agentRoot();
		await withIdleChild(async child => {
			await publishFor(agentDir, child.pid);
			const captured = await rememberFixtureBrokerAt(agentDir);
			expect(captured).toMatchObject({ kind: "identity", identity: { agentDir, pid: child.pid } });
			if (captured.kind !== "identity") throw new Error("identity capture failed");
			expect(captured.identity.incarnation).toMatch(/^(?:linux:|darwin:|windows:)/);
		});
	});
});

describe("fixture broker teardown through retained process authority", () => {
	test("a live exact identity is settled only through the retained root primitive", async () => {
		const agentDir = await agentRoot();
		await withIdleChild(async child => {
			await publishFor(agentDir, child.pid);
			const captured = await rememberFixtureBrokerAt(agentDir);
			if (captured.kind !== "identity") throw new Error("identity capture failed");
			const kill = spyOn(process, "kill");
			try {
				const teardown = await settleRememberedFixtureBroker(captured.identity, { gracePeriodMs: 5_000 });
				expect(teardown).toEqual(
					HAS_ROOT_SIGNAL_PRIMITIVE
						? { outcome: "settled", pid: child.pid }
						: // macOS exposes no stable root-only signal primitive, so teardown
							// fails closed here instead of reaching for the pid, the process
							// group, or the descendant tree. Only the launch-time owner —
							// this test's own child handle — may stop it.
							{ outcome: "survived", pid: child.pid, reason: "signal_refused" },
				);
				// A raw pid is never signalled on either branch; the only permitted
				// `process.kill` use is the read-only signal-0 existence probe.
				expect(deliveredSignals(kill)).toEqual([]);
				if (HAS_ROOT_SIGNAL_PRIMITIVE) await child.exited;
				else expect(child.exitCode).toBeNull();
			} finally {
				kill.mockRestore();
			}
		});
	}, 20_000);

	test("an identity whose process already exited settles without delivering a signal", async () => {
		const agentDir = await agentRoot();
		let pid = 0;
		await withIdleChild(async child => {
			pid = child.pid;
			await publishFor(agentDir, child.pid);
			const captured = await rememberFixtureBrokerAt(agentDir);
			if (captured.kind !== "identity") throw new Error("identity capture failed");
			child.kill("SIGKILL");
			await child.exited;
			const kill = spyOn(process, "kill");
			try {
				expect(await settleRememberedFixtureBroker(captured.identity)).toEqual({
					outcome: "already_exited",
					pid,
				});
				expect(deliveredSignals(kill)).toEqual([]);
			} finally {
				kill.mockRestore();
			}
		});
	}, 20_000);

	test("an incarnation that no longer matches is never signalled", async () => {
		const stranger = new FakeRetainedProcess(4_242, "linux:999", { exitOnSignal: SIGTERM });
		const references = opener(opened(stranger));
		const kill = spyOn(process, "kill");
		try {
			expect(
				await settleRememberedFixtureBroker(
					{ agentDir: "/tmp/absent", pid: 4_242, incarnation: "linux:111" },
					{ gracePeriodMs: 10, openProcess: () => references.open() },
				),
			).toEqual({ outcome: "already_exited", pid: 4_242 });
			expect(stranger.delivered).toEqual([]);
			expect(deliveredSignals(kill)).toEqual([]);
		} finally {
			kill.mockRestore();
		}
	});

	test("an unreadable incarnation is typed uncertainty, never a settled claim", async () => {
		const unreadable = new FakeRetainedProcess(4_242, "", { exitOnSignal: SIGTERM });
		const references = opener(opened(unreadable));
		expect(
			await settleRememberedFixtureBroker(
				{ agentDir: "/tmp/absent", pid: 4_242, incarnation: "linux:111" },
				{ gracePeriodMs: 10, openProcess: () => references.open() },
			),
		).toEqual({ outcome: "survived", pid: 4_242, reason: "authority_unreadable" });
		expect(unreadable.delivered).toEqual([]);
	});

	test("an exact authority that exits on the polite signal settles after one signal", async () => {
		const exact = new FakeRetainedProcess(4_242, "linux:111", { exitOnSignal: SIGTERM });
		const references = opener(opened(exact));
		expect(
			await settleRememberedFixtureBroker(
				{ agentDir: "/tmp/absent", pid: 4_242, incarnation: "linux:111" },
				{ gracePeriodMs: 10, openProcess: () => references.open() },
			),
		).toEqual({ outcome: "settled", pid: 4_242 });
		expect(exact.delivered).toEqual([SIGTERM]);
	});

	test("an exact authority that ignores the polite signal is re-proven before escalation", async () => {
		const first = new FakeRetainedProcess(4_242, "linux:111");
		const second = new FakeRetainedProcess(4_242, "linux:111", { exitOnSignal: SIGKILL });
		const references = opener(opened(first), opened(second));
		expect(
			await settleRememberedFixtureBroker(
				{ agentDir: "/tmp/absent", pid: 4_242, incarnation: "linux:111" },
				{ gracePeriodMs: 10, openProcess: () => references.open() },
			),
		).toEqual({ outcome: "settled", pid: 4_242 });
		expect(references.acquisitions).toBe(2);
		expect(first.delivered).toEqual([SIGTERM]);
		expect(second.delivered).toEqual([SIGKILL]);
	});

	test("a pid replaced between the polite signal and escalation signals neither process again", async () => {
		const original = new FakeRetainedProcess(4_242, "linux:111");
		const replacement = new FakeRetainedProcess(4_242, "linux:222", { exitOnSignal: SIGKILL });
		const references = opener(opened(original), opened(replacement));
		expect(
			await settleRememberedFixtureBroker(
				{ agentDir: "/tmp/absent", pid: 4_242, incarnation: "linux:111" },
				{ gracePeriodMs: 10, openProcess: () => references.open() },
			),
		).toEqual({ outcome: "settled", pid: 4_242 });
		expect(original.delivered).toEqual([SIGTERM]);
		expect(replacement.delivered).toEqual([]);
	});

	/**
	 * The retained root primitive is the only delivery path. A platform that
	 * refuses it has no authority to fall back to: a tree termination signals
	 * descendants this suite never captured, and a pid signal targets whoever
	 * holds the slot now.
	 */
	test("a refused root signal never reaches a tree, a descendant, or a raw pid", async () => {
		const exact = new FakeRetainedProcess(4_242, "linux:111", { refuseSignal: true });
		const descendant = new FakeRetainedProcess(4_243, "linux:112");
		exact.descendants.push(descendant);
		const references = opener(opened(exact));
		const kill = spyOn(process, "kill");
		try {
			expect(
				await settleRememberedFixtureBroker(
					{ agentDir: "/tmp/absent", pid: 4_242, incarnation: "linux:111" },
					{ gracePeriodMs: 10, openProcess: () => references.open() },
				),
			).toEqual({ outcome: "survived", pid: 4_242, reason: "signal_refused" });
			expect(exact.delivered).toEqual([]);
			expect(exact.terminateCalls).toBe(0);
			expect(descendant.delivered).toEqual([]);
			expect(deliveredSignals(kill)).toEqual([]);
		} finally {
			kill.mockRestore();
		}
	});

	test("a process that outlives both signals is reported as survival, never success", async () => {
		const stubborn = new FakeRetainedProcess(4_242, "linux:111");
		const references = opener(opened(stubborn));
		expect(
			await settleRememberedFixtureBroker(
				{ agentDir: "/tmp/absent", pid: 4_242, incarnation: "linux:111" },
				{ gracePeriodMs: 10, openProcess: () => references.open() },
			),
		).toEqual({ outcome: "survived", pid: 4_242, reason: "ignored_signals" });
		expect(stubborn.delivered).toEqual([SIGTERM, SIGKILL]);
	});
});

/**
 * Native authority answers are evidence, not verdicts.
 *
 * An exception, a permission refusal, an unreadable status, and macOS's
 * ambiguous `from_pid` null all mean "unknown" — never "gone". Collapsing any of
 * them into an exit turns a broker this suite may still be running into a clean
 * teardown report, so each one must surface as typed uncertainty and authorize
 * no signal at all.
 */
describe("fixture broker teardown authority evidence", () => {
	const identity = { agentDir: "/tmp/absent", pid: 4_242, incarnation: "linux:111" };
	const presence =
		(...answers: ProcessPresence[]) =>
		(): ProcessPresence =>
			(answers.length > 1 ? answers.shift() : answers[0]) ?? "unknown";

	test("a native authority that throws is unreadable, never an exit", async () => {
		const fromPid = spyOn(Process, "fromPid").mockImplementation(() => {
			throw new Error("proc_pidinfo refused");
		});
		const kill = spyOn(process, "kill");
		try {
			expect(await settleRememberedFixtureBroker(identity, { gracePeriodMs: 10 })).toEqual({
				outcome: "survived",
				pid: 4_242,
				reason: "authority_unreadable",
			});
			expect(deliveredSignals(kill)).toEqual([]);
		} finally {
			kill.mockRestore();
			fromPid.mockRestore();
		}
	});

	test("an ambiguous native null is an exit only when process absence is proven", async () => {
		const fromPid = spyOn(Process, "fromPid").mockImplementation(() => null);
		const kill = spyOn(process, "kill");
		try {
			for (const [answer, expected] of [
				["absent", { outcome: "already_exited", pid: 4_242 }],
				["occupied", { outcome: "survived", pid: 4_242, reason: "authority_unreadable" }],
				["unknown", { outcome: "survived", pid: 4_242, reason: "authority_unreadable" }],
			] as const) {
				expect(
					await settleRememberedFixtureBroker(identity, {
						gracePeriodMs: 10,
						probeProcessPresence: presence(answer),
					}),
				).toEqual(expected);
			}
			expect(deliveredSignals(kill)).toEqual([]);
		} finally {
			kill.mockRestore();
			fromPid.mockRestore();
		}
	});

	test("a presence probe that throws is unreadable authority, never an exit", async () => {
		expect(
			await settleRememberedFixtureBroker(identity, {
				gracePeriodMs: 10,
				openProcess: () => ({ kind: "unprovable" }),
				probeProcessPresence: () => {
					throw new Error("probe refused");
				},
			}),
		).toEqual({ outcome: "survived", pid: 4_242, reason: "authority_unreadable" });
	});

	test("proven process absence settles as an exit without probing further", async () => {
		let probes = 0;
		expect(
			await settleRememberedFixtureBroker(identity, {
				gracePeriodMs: 10,
				openProcess: () => ({ kind: "absent" }),
				probeProcessPresence: () => {
					probes += 1;
					return "absent";
				},
			}),
		).toEqual({ outcome: "already_exited", pid: 4_242 });
		expect(probes).toBe(0);
	});

	test("a liveness failure after a refused signal is uncertainty, never a settled teardown", async () => {
		const unreadable = new FakeRetainedProcess(4_242, "linux:111", {
			refuseSignal: true,
			liveness: "unprovable",
		});
		const references = opener(opened(unreadable));
		expect(
			await settleRememberedFixtureBroker(identity, {
				gracePeriodMs: 10,
				openProcess: () => references.open(),
			}),
		).toEqual({ outcome: "survived", pid: 4_242, reason: "authority_unreadable" });
		expect(unreadable.delivered).toEqual([]);
		expect(unreadable.terminateCalls).toBe(0);
	});

	test("a wait failure after a delivered signal is uncertainty, never a settled teardown", async () => {
		const unreadable = new FakeRetainedProcess(4_242, "linux:111", { wait: "unprovable" });
		const references = opener(opened(unreadable));
		expect(
			await settleRememberedFixtureBroker(identity, {
				gracePeriodMs: 10,
				openProcess: () => references.open(),
			}),
		).toEqual({ outcome: "survived", pid: 4_242, reason: "authority_unreadable" });
		expect(unreadable.delivered).toEqual([SIGTERM]);
	});

	test("an authority that becomes unreadable before escalation is never settled", async () => {
		const first = new FakeRetainedProcess(4_242, "linux:111");
		const references = opener(opened(first), { kind: "unprovable" });
		expect(
			await settleRememberedFixtureBroker(identity, {
				gracePeriodMs: 10,
				openProcess: () => references.open(),
				probeProcessPresence: () => "occupied",
			}),
		).toEqual({ outcome: "survived", pid: 4_242, reason: "authority_unreadable" });
		expect(first.delivered).toEqual([SIGTERM]);
	});

	test("a live process the default authority cannot signal is never reported as exited", async () => {
		const agentDir = await agentRoot();
		await withIdleChild(async child => {
			await publishFor(agentDir, child.pid);
			const captured = await rememberFixtureBrokerAt(agentDir);
			if (captured.kind !== "identity") throw new Error("identity capture failed");
			// The default opener is asked for an incarnation the live pid cannot
			// have. The replacement is never signalled, and the captured identity is
			// judged against the process that actually holds the slot.
			expect(
				await settleRememberedFixtureBroker(
					{ ...captured.identity, incarnation: `${captured.identity.incarnation}9` },
					{ gracePeriodMs: 10 },
				),
			).toEqual({ outcome: "already_exited", pid: child.pid });
			expect(child.exitCode).toBeNull();
		});
	}, 20_000);
});

describe("fixture broker teardown proof for call sites", () => {
	test("missing or malformed discovery is not cleanup proof for a call site that may have launched a broker", () => {
		for (const teardown of [
			{ outcome: "identity_unavailable", agentDir: "/tmp/absent" },
			{ outcome: "identity_malformed", agentDir: "/tmp/absent" },
		] as const) {
			expect(fixtureBrokerTeardownProblem(teardown, { launched: true })).toBeString();
			expect(fixtureBrokerTeardownProblem(teardown, { launched: false })).toBeUndefined();
		}
	});

	test("survival is never cleanup proof, whether or not a launch was attempted", () => {
		for (const launched of [true, false]) {
			expect(
				fixtureBrokerTeardownProblem({ outcome: "survived", pid: 7, reason: "ignored_signals" }, { launched }),
			).toBeString();
		}
	});

	test("a settled, already-exited, or self-owned broker is proven cleanup", () => {
		for (const teardown of [
			{ outcome: "settled", pid: 7 },
			{ outcome: "already_exited", pid: 7 },
			{ outcome: "self_owned", agentDir: "/tmp/absent", pid: process.pid },
		] as const) {
			expect(fixtureBrokerTeardownProblem(teardown, { launched: true })).toBeUndefined();
		}
	});
});

describe("fixture broker teardown around a retained owner", () => {
	test("the broker identity is captured before the owner release destroys the record", async () => {
		const agentDir = await agentRoot();
		await withIdleChild(async child => {
			await publishFor(agentDir, child.pid);
			const result = await settleFixtureBrokerWithOwner(agentDir, {
				launched: true,
				// A real owner close removes the discovery record *and* stops the
				// process it owns. Capturing after it would leave a detached broker
				// with no authority naming it at all.
				owner: async () => {
					await fs.rm(brokerDiscoveryPath(agentDir), { force: true });
					child.kill("SIGKILL");
					await child.exited;
				},
			});
			expect(result).toEqual({ problem: undefined, failures: [] });
		});
	}, 20_000);

	test("an owner release that throws is reported without losing the broker teardown proof", async () => {
		const agentDir = await agentRoot();
		const result = await settleFixtureBrokerWithOwner(agentDir, {
			launched: false,
			owner: async () => {
				throw new Error("owner stop failed");
			},
		});
		expect(result.problem).toBeUndefined();
		expect(result.failures).toHaveLength(1);
	});

	test("a record naming this process stays proven cleanup even after the owner removes it", async () => {
		const agentDir = await agentRoot();
		await publishFor(agentDir, process.pid);
		const result = await settleFixtureBrokerWithOwner(agentDir, {
			launched: true,
			owner: async () => void (await fs.rm(brokerDiscoveryPath(agentDir), { force: true })),
		});
		expect(result).toEqual({ problem: undefined, failures: [] });
	});

	test("a launch with no discovery record and no retained owner fails teardown", async () => {
		const agentDir = await agentRoot();
		const result = await settleFixtureBrokerWithOwner(agentDir, {
			launched: true,
			owner: async () => {},
		});
		expect(result.problem).toBeString();
		expect(result.failures).toEqual([]);
	});
});

/**
 * Suite teardown owns every target it registered, not the first one that fails.
 *
 * Stopping at the first failure leaves the remaining directories, remembered
 * identities, and owners unsettled — precisely the brokers a failing run is most
 * likely to have leaked. Every target is therefore processed, and the
 * accumulated verdict is reported once at the end.
 */
describe("fixture broker teardown across every owned target", () => {
	test("every target and remembered identity is processed after an early failure", async () => {
		const released: string[] = [];
		const failing = await agentRoot();
		const mayHaveLaunched = await agentRoot();
		const proven = await agentRoot();
		await publishFor(proven, process.pid);
		const remembered: FixtureBrokerDiscovery[] = [
			{ kind: "identity_malformed", agentDir: "/tmp/remembered-malformed" },
			{ kind: "self_owned", agentDir: "/tmp/remembered-self", pid: process.pid },
		];

		const settled = await settleFixtureBrokerTargets({
			targets: [
				{
					agentDir: failing,
					launched: false,
					owner: async () => {
						released.push(failing);
						throw new Error("owner stop failed");
					},
				},
				{ agentDir: mayHaveLaunched, launched: true, owner: async () => void released.push(mayHaveLaunched) },
				{ agentDir: proven, launched: true, owner: async () => void released.push(proven) },
			],
			remembered,
		});

		// Every owner ran, in registration order, despite the first one throwing.
		expect(released).toEqual([failing, mayHaveLaunched, proven]);
		expect(settled.failures).toHaveLength(1);
		expect(settled.problems).toEqual([
			expect.stringContaining(mayHaveLaunched),
			expect.stringContaining("/tmp/remembered-malformed"),
		]);
	});

	test("a fully settled set of targets reports no problem at all", async () => {
		const agentDir = await agentRoot();
		const settled = await settleFixtureBrokerTargets({
			targets: [{ agentDir, launched: false, owner: async () => {} }],
			remembered: [{ kind: "self_owned", agentDir, pid: process.pid }],
		});
		expect(settled).toEqual({ problems: [], failures: [] });
	});

	test("a remembered identity is settled through retained authority, never skipped", async () => {
		const agentDir = await agentRoot();
		let pid = 0;
		await withIdleChild(async child => {
			pid = child.pid;
			await publishFor(agentDir, child.pid);
			const captured = await rememberFixtureBrokerAt(agentDir);
			if (captured.kind !== "identity") throw new Error("identity capture failed");
			child.kill("SIGKILL");
			await child.exited;
			expect(await settleFixtureBrokerTargets({ targets: [], remembered: [captured] })).toEqual({
				problems: [],
				failures: [],
			});
			expect(pid).toBeGreaterThan(0);
		});
	}, 20_000);
});
