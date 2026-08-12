import * as fs from "node:fs/promises";
import * as os from "node:os";
import { Process, ProcessStatus } from "@gajae-code/natives";
import type { Subprocess } from "bun";
import { type BrokerDiscovery, brokerDiscoveryPath } from "../../src/sdk/broker/discovery";
import { isProcessIncarnation } from "../../src/sdk/broker/process-incarnation";
import { SDK_STATE_VERSION } from "../../src/sdk/broker/state-version";

const TERM_SIGNAL = os.constants.signals.SIGTERM;
const KILL_SIGNAL = os.constants.signals.SIGKILL;

/**
 * Exact, owner-scoped tracking for the child processes one suite spawns.
 *
 * Teardown terminates only these handles. It never matches on a process name,
 * a command-line pattern, or another suite's descendants, so a suite can settle
 * what it owns without reaping a peer's live broker or session host. `settle`
 * reports the pids that refused to die, which is the suite's own orphan count.
 */
export class OwnedChildren {
	readonly #children = new Set<Subprocess>();

	/** Adopt a freshly spawned child. Returns it unchanged for inline use. */
	track<T extends Subprocess>(child: T): T {
		this.#children.add(child);
		void child.exited.then(
			() => this.#children.delete(child),
			() => this.#children.delete(child),
		);
		return child;
	}

	/** Whether this owner is currently holding any unsettled child. */
	get size(): number {
		return this.#children.size;
	}

	/**
	 * Terminate every still-running owned child through its own handle and
	 * return the pids that survived both signals.
	 */
	async settle(gracePeriodMs = 2_000): Promise<number[]> {
		const owned = [...this.#children];
		this.#children.clear();
		const survivors: number[] = [];
		for (const child of owned) {
			if (child.exitCode !== null || child.signalCode !== null) continue;
			const exited = async (): Promise<boolean> => {
				await child.exited;
				return true;
			};
			child.kill("SIGTERM");
			if (!(await Promise.race([exited(), Bun.sleep(gracePeriodMs).then(() => false)]))) {
				child.kill("SIGKILL");
				await Promise.race([exited(), Bun.sleep(gracePeriodMs).then(() => false)]);
			}
			if (child.exitCode === null && child.signalCode === null) survivors.push(child.pid);
		}
		return survivors;
	}
}

/** What one exact pinned process's own authority proves about its liveness. */
export type RetainedLiveness = "running" | "exited" | "unprovable";

/**
 * A retained, kernel-pinned reference to one exact process incarnation.
 *
 * A pid is a reusable slot, not an identity. Everything below is expressed
 * against this reference rather than the number it was opened from, so a pid
 * recycled between proof and delivery cannot redirect a signal at a stranger.
 *
 * There is deliberately no termination method. `signalRoot` is the only
 * delivery primitive: it is root-only, bound to this incarnation, and atomic.
 * A tree termination would signal descendants this suite never captured, and a
 * platform without a root-only primitive must fail closed instead of escalating
 * behind the caller's back.
 */
export interface RetainedProcess {
	readonly pid: number;
	/** Kernel-derived identity evidence for this exact incarnation. */
	readonly incarnation: string;
	/** Deliver `signal` only to this pinned root process. */
	signalRoot(signal: number): boolean;
	/** What this pinned reference proves about its own liveness right now. */
	liveness(): RetainedLiveness;
	/** What this pinned reference proves after waiting up to `timeoutMs`. */
	waitForExit(timeoutMs: number): Promise<RetainedLiveness>;
}

/**
 * The result of asking the platform for authority over one pid.
 *
 * `absent` is a positive statement that no process holds the pid. It is
 * separate from `unprovable` because most platforms cannot tell "gone" apart
 * from "refused" or "unreadable", and collapsing the two turns an unreadable
 * kernel answer into a clean teardown report.
 */
export type RetainedProcessOpen =
	| { kind: "opened"; process: RetainedProcess }
	| { kind: "absent" }
	| { kind: "unprovable" };

/** Opens a retained reference, or reports what it could prove instead. */
export type RetainedProcessOpener = (pid: number) => RetainedProcessOpen;

/** Read-only evidence about whether a pid names any live process at all. */
export type ProcessPresence = "absent" | "occupied" | "unknown";

/** A read-only existence probe. It never delivers a signal. */
export type ProcessPresenceProbe = (pid: number) => ProcessPresence;

function nativeIncarnation(reference: Process): string {
	try {
		return reference.incarnation;
	} catch {
		// An unreadable incarnation is never "not ours" and never "gone": it is
		// unproven, and the caller must treat it as uncertainty.
		return "";
	}
}

/**
 * Open the platform's stable process authority.
 *
 * `Process.fromPid` pins the kernel process object — a pidfd on Linux, a
 * process handle on Windows, the `(pid, start-time)` triple on macOS — so the
 * reference keeps naming the same incarnation even if the pid is recycled.
 *
 * A native `null` is a normal but ambiguous answer: macOS `from_pid` collapses
 * "no such process" and "`proc_pidinfo` refused" into it, so absence is never
 * inferred from it here, and only the typed presence probe may establish it.
 * A thrown error is not an answer at all — it says this authority layer could
 * not be consulted — so it is deliberately propagated to the caller, which
 * classifies it as unreadable without falling back to weaker evidence.
 */
function openRetainedProcess(pid: number): RetainedProcessOpen {
	const native = Process.fromPid(pid);
	if (!native) return { kind: "unprovable" };
	const reference = native;
	return {
		kind: "opened",
		process: {
			pid: reference.pid,
			incarnation: nativeIncarnation(reference),
			signalRoot: signal => {
				try {
					return reference.signalRoot(signal);
				} catch {
					return false;
				}
			},
			liveness: () => {
				try {
					return reference.status() === ProcessStatus.Running ? "running" : "exited";
				} catch {
					// A status that cannot be read proves nothing at all. Reading it as
					// `false` is what turns an unreadable authority into a settled claim.
					return "unprovable";
				}
			},
			waitForExit: async timeoutMs => {
				try {
					return (await reference.waitForExit({ timeoutMs })) ? "exited" : "running";
				} catch {
					return "unprovable";
				}
			},
		},
	};
}

/**
 * Affirmative process-absence evidence, and nothing else.
 *
 * Signal `0` delivers no signal: it runs the kernel's existence and permission
 * checks and returns. `ESRCH` is the only answer that proves the pid names no
 * process; `EPERM` proves the opposite — somebody else's process holds the slot
 * — and every other answer proves nothing. The result may therefore only
 * *permit* an "already exited" verdict for an unreadable authority. It never
 * authorizes delivery, and a recycled pid reads as `occupied`, which fails
 * closed into `authority_unreadable`.
 */
function probeProcessPresence(pid: number): ProcessPresence {
	try {
		process.kill(pid, 0);
		return "occupied";
	} catch (error) {
		const code = (error as NodeJS.ErrnoException).code;
		if (code === "ESRCH") return "absent";
		return code === "EPERM" ? "occupied" : "unknown";
	}
}

/** One fixture broker's proven identity, captured while its record existed. */
export interface FixtureBrokerIdentity {
	agentDir: string;
	pid: number;
	incarnation: string;
}

/**
 * What a temp agent directory's discovery record proves about a fixture broker.
 *
 * Missing and malformed are kept apart from each other and from a captured
 * identity: after a launch may have happened, neither absence nor corruption is
 * evidence that nothing is running, and collapsing them into "no identity"
 * turns an unreadable record into a clean teardown report.
 */
export type FixtureBrokerDiscovery =
	| { kind: "identity"; identity: FixtureBrokerIdentity }
	| { kind: "self_owned"; agentDir: string; pid: number }
	| { kind: "identity_unavailable"; agentDir: string }
	| { kind: "identity_malformed"; agentDir: string };

/** The proven end state of one fixture broker teardown. */
export type FixtureBrokerTeardown =
	| { outcome: "settled"; pid: number }
	| { outcome: "already_exited"; pid: number }
	| { outcome: "survived"; pid: number; reason: "signal_refused" | "ignored_signals" | "authority_unreadable" }
	| { outcome: "self_owned"; agentDir: string; pid: number }
	| { outcome: "identity_unavailable"; agentDir: string }
	| { outcome: "identity_malformed"; agentDir: string };

/**
 * Capture the exact identity of a broker one of a suite's own children spawned
 * inside a temp agent directory.
 *
 * A detached broker started by a child process leaves no owner handle in this
 * process, so the only authority available is that temp directory's own
 * discovery record. Its `pid` names exactly the broker this suite caused to
 * exist — never a process-name or command-line match, and a directory created
 * by `mkdtemp` cannot name a pre-existing broker.
 *
 * Capturing is separate from settling because a test that removes its temp root
 * in its own `finally` destroys the discovery record before any `afterEach`
 * runs. Reading the identity while the record still exists is what lets teardown
 * settle that exact process afterwards instead of silently reporting nothing.
 *
 * Liveness is deliberately not consulted here. A fixture broker that stopped
 * heartbeating, or exited, is precisely what teardown must classify, and that
 * classification belongs to the retained process authority rather than to a
 * freshness window on a file.
 */
export async function rememberFixtureBrokerAt(agentDir: string): Promise<FixtureBrokerDiscovery> {
	let raw: string;
	try {
		raw = await fs.readFile(brokerDiscoveryPath(agentDir), "utf8");
	} catch (error) {
		return (error as NodeJS.ErrnoException).code === "ENOENT"
			? { kind: "identity_unavailable", agentDir }
			: { kind: "identity_malformed", agentDir };
	}
	let parsed: unknown;
	try {
		parsed = JSON.parse(raw);
	} catch {
		return { kind: "identity_malformed", agentDir };
	}
	if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return { kind: "identity_malformed", agentDir };
	const record = parsed as Partial<BrokerDiscovery>;
	if (record.version !== SDK_STATE_VERSION || record.protocolVersion !== 3)
		return { kind: "identity_malformed", agentDir };
	const { pid, incarnation } = record;
	if (typeof pid !== "number" || !Number.isSafeInteger(pid) || pid <= 0)
		return { kind: "identity_malformed", agentDir };
	if (!isProcessIncarnation(incarnation)) return { kind: "identity_malformed", agentDir };
	// Never adopt this process. A suite that publishes an in-process broker owns
	// it through that object's own `stop()`, and signalling here would kill the
	// test runner itself.
	if (pid === process.pid) return { kind: "self_owned", agentDir, pid };
	return { kind: "identity", identity: { agentDir, pid, incarnation } };
}

type ProvenAuthority = { kind: "proven"; authority: RetainedProcess } | { kind: "exited" } | { kind: "unprovable" };

/**
 * Re-open the platform authority and prove it still names the captured
 * incarnation.
 *
 * Only two things prove the captured process is gone: the platform stating
 * outright that no process holds the pid, or a *valid, different* incarnation
 * holding it — which additionally proves that whoever holds it now must not be
 * signalled. Everything else — an exception, a permission refusal, an
 * unreadable incarnation, macOS's ambiguous `null` — proves nothing at all and
 * therefore authorizes nothing.
 */
function proveAuthority(
	open: RetainedProcessOpener,
	probe: ProcessPresenceProbe,
	identity: FixtureBrokerIdentity,
): ProvenAuthority {
	let opened: RetainedProcessOpen;
	try {
		opened = open(identity.pid);
	} catch {
		return { kind: "unprovable" };
	}
	if (opened.kind === "absent") return { kind: "exited" };
	if (opened.kind === "unprovable") return proveAbsence(probe, identity.pid);
	const authority = opened.process;
	if (authority.pid !== identity.pid) return { kind: "unprovable" };
	if (!isProcessIncarnation(authority.incarnation)) return { kind: "unprovable" };
	return authority.incarnation === identity.incarnation ? { kind: "proven", authority } : { kind: "exited" };
}

/** Ask the read-only probe whether an unreadable authority is simply gone. */
function proveAbsence(probe: ProcessPresenceProbe, pid: number): ProvenAuthority {
	let presence: ProcessPresence;
	try {
		presence = probe(pid);
	} catch {
		return { kind: "unprovable" };
	}
	return presence === "absent" ? { kind: "exited" } : { kind: "unprovable" };
}

type SignalPhase = { signal: number; timeoutMs: number };
type SignalResult = "exited" | "running" | "refused" | "unprovable";

/**
 * Deliver one phase through the retained reference and report what it proved.
 *
 * `signalRoot` is the only delivery primitive, and it has no fallback. It
 * returns `false` both when the pinned process is already gone and when the
 * platform exposes no root-only authority — macOS deliberately fails closed
 * there — so the reference itself is asked which of the two happened. A
 * liveness answer it cannot prove stays uncertain rather than becoming an exit,
 * and no pid, process group, name, or descendant is ever signalled on any
 * branch.
 */
async function deliverPhase(authority: RetainedProcess, phase: SignalPhase): Promise<SignalResult> {
	if (!authority.signalRoot(phase.signal)) {
		const liveness = authority.liveness();
		return liveness === "exited" ? "exited" : liveness === "running" ? "refused" : "unprovable";
	}
	const waited = await authority.waitForExit(phase.timeoutMs);
	return waited === "unprovable" ? "unprovable" : waited;
}

export interface SettleFixtureBrokerOptions {
	gracePeriodMs?: number;
	/** Test seam for the platform's stable process authority. */
	openProcess?: RetainedProcessOpener;
	/** Test seam for the read-only process-absence probe. */
	probeProcessPresence?: ProcessPresenceProbe;
}

/**
 * Terminate a captured broker identity through retained process authority.
 *
 * Identity authorizes every signal, and the authority that proves the identity
 * is the same object that delivers it: a pid is never handed to `process.kill`,
 * so a pid recycled between proof and delivery cannot redirect a signal. The
 * escalation to the hard signal re-opens and re-proves a fresh reference, because
 * the polite phase's proof says nothing about who holds the pid afterwards.
 *
 * Uncertainty is never rounded down to success. An authority that cannot be
 * opened, read, or waited on reports `survived(authority_unreadable)`, and a
 * platform that refuses the root-only primitive for a live process reports
 * `survived(signal_refused)` — both of which fail the caller's teardown proof
 * instead of quietly claiming the broker is gone.
 */
export async function settleRememberedFixtureBroker(
	identity: FixtureBrokerIdentity,
	options: SettleFixtureBrokerOptions = {},
): Promise<FixtureBrokerTeardown> {
	const gracePeriodMs = options.gracePeriodMs ?? 2_000;
	const open = options.openProcess ?? openRetainedProcess;
	const probe = options.probeProcessPresence ?? probeProcessPresence;
	const { pid } = identity;
	const survived = (result: SignalResult): FixtureBrokerTeardown => ({
		outcome: "survived",
		pid,
		reason:
			result === "refused" ? "signal_refused" : result === "unprovable" ? "authority_unreadable" : "ignored_signals",
	});
	const polite = proveAuthority(open, probe, identity);
	if (polite.kind === "exited") return { outcome: "already_exited", pid };
	if (polite.kind === "unprovable") return { outcome: "survived", pid, reason: "authority_unreadable" };
	const politeResult = await deliverPhase(polite.authority, { signal: TERM_SIGNAL, timeoutMs: gracePeriodMs });
	if (politeResult === "exited") return { outcome: "settled", pid };
	if (politeResult !== "running") return survived(politeResult);
	const hard = proveAuthority(open, probe, identity);
	if (hard.kind === "exited") return { outcome: "settled", pid };
	if (hard.kind === "unprovable") return { outcome: "survived", pid, reason: "authority_unreadable" };
	const hardResult = await deliverPhase(hard.authority, { signal: KILL_SIGNAL, timeoutMs: gracePeriodMs });
	return hardResult === "exited" ? { outcome: "settled", pid } : survived(hardResult);
}

/** Capture and settle in one step, for a directory that still exists. */
export async function settleFixtureBrokerAt(
	agentDir: string,
	options: SettleFixtureBrokerOptions = {},
): Promise<FixtureBrokerTeardown> {
	const discovery = await rememberFixtureBrokerAt(agentDir);
	if (discovery.kind === "identity") return await settleRememberedFixtureBroker(discovery.identity, options);
	if (discovery.kind === "self_owned")
		return { outcome: "self_owned", agentDir: discovery.agentDir, pid: discovery.pid };
	return { outcome: discovery.kind, agentDir: discovery.agentDir };
}

/**
 * Describe why a teardown outcome is not proof that a suite left nothing behind.
 *
 * `launched` is the call site's own statement about whether it may have caused a
 * detached broker to exist. When it may have, an absent or unreadable discovery
 * record is the one authority over that process going missing — not evidence it
 * never existed — so it fails teardown. A call site that can prove no launch
 * happened (its own retained owner and children account for everything it
 * started) states `launched: false` and is allowed an empty directory.
 */
export function fixtureBrokerTeardownProblem(
	teardown: FixtureBrokerTeardown,
	options: { launched: boolean },
): string | undefined {
	switch (teardown.outcome) {
		case "settled":
		case "already_exited":
		case "self_owned":
			return undefined;
		case "survived":
			return `fixture broker pid ${teardown.pid} survived teardown (${teardown.reason})`;
		default:
			return options.launched
				? `fixture broker teardown could not prove cleanup at ${teardown.agentDir} (${teardown.outcome})`
				: undefined;
	}
}

export interface FixtureBrokerOwnerRelease extends SettleFixtureBrokerOptions {
	/**
	 * Release this directory's retained owner — a broker lease, an in-process
	 * broker, or an ensure-owned detached child.
	 */
	owner: () => Promise<void>;
	/**
	 * Whether this call site may have caused a detached broker it holds no
	 * retained owner handle for. A retained owner is the alternative proof; with
	 * neither, a missing record cannot be read as cleanup.
	 */
	launched: boolean;
}

/**
 * Settle one temp agent directory's fixture broker around its retained owner.
 *
 * The identity is captured *before* the owner is released, because releasing an
 * owner — like removing the temp root afterwards — destroys the discovery
 * record that is this process's only authority over a detached broker. Every
 * step is isolated so one failure cannot strand the others, and the returned
 * `problem` is the call site's typed teardown verdict.
 */
export async function settleFixtureBrokerWithOwner(
	agentDir: string,
	release: FixtureBrokerOwnerRelease,
): Promise<{ problem: string | undefined; failures: unknown[] }> {
	const failures: unknown[] = [];
	let captured: FixtureBrokerDiscovery;
	try {
		captured = await rememberFixtureBrokerAt(agentDir);
	} catch (error) {
		failures.push(error);
		captured = { kind: "identity_malformed", agentDir };
	}
	try {
		await release.owner();
	} catch (error) {
		failures.push(error);
	}
	let teardown: FixtureBrokerTeardown;
	try {
		teardown =
			captured.kind === "identity"
				? await settleRememberedFixtureBroker(captured.identity, release)
				: captured.kind === "self_owned"
					? { outcome: "self_owned", agentDir, pid: captured.pid }
					: { outcome: captured.kind, agentDir };
	} catch (error) {
		failures.push(error);
		teardown = { outcome: "identity_malformed", agentDir };
	}
	return { problem: fixtureBrokerTeardownProblem(teardown, { launched: release.launched }), failures };
}

/** One temp agent directory a suite must settle, with its retained owner. */
export interface FixtureBrokerTarget extends FixtureBrokerOwnerRelease {
	agentDir: string;
}

/**
 * Settle every registered target and every remembered identity, then report
 * once.
 *
 * Stopping at the first failure leaves every later directory, remembered
 * identity, and retained owner unsettled — exactly the brokers a failing run is
 * most likely to have leaked. Each target is therefore isolated: its problems
 * and its thrown errors are accumulated, and the caller asserts on the complete
 * verdict rather than on whichever target happened to fail first.
 */
export async function settleFixtureBrokerTargets(input: {
	targets: readonly FixtureBrokerTarget[];
	remembered?: readonly FixtureBrokerDiscovery[];
	options?: SettleFixtureBrokerOptions;
}): Promise<{ problems: string[]; failures: unknown[] }> {
	const problems: string[] = [];
	const failures: unknown[] = [];
	for (const target of input.targets) {
		try {
			const settled = await settleFixtureBrokerWithOwner(target.agentDir, { ...input.options, ...target });
			if (settled.problem) problems.push(settled.problem);
			failures.push(...settled.failures);
		} catch (error) {
			failures.push(error);
		}
	}
	// Identities captured while their temp root still existed: each one is proof
	// that a detached broker was launched, so nothing short of a settled or
	// already-exited process counts as cleanup.
	for (const remembered of input.remembered ?? []) {
		try {
			const teardown =
				remembered.kind === "identity"
					? await settleRememberedFixtureBroker(remembered.identity, input.options)
					: remembered.kind === "self_owned"
						? ({ outcome: "self_owned", agentDir: remembered.agentDir, pid: remembered.pid } as const)
						: ({ outcome: remembered.kind, agentDir: remembered.agentDir } as const);
			const problem = fixtureBrokerTeardownProblem(teardown, { launched: true });
			if (problem) problems.push(problem);
		} catch (error) {
			failures.push(error);
		}
	}
	return { problems, failures };
}
