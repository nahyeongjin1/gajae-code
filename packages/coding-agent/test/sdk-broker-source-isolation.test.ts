import { afterEach, expect, it } from "bun:test";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { Broker } from "../src/sdk/broker/broker";
import { readBrokerDiscovery } from "../src/sdk/broker/discovery";
import { type FixtureBrokerLease, startFixtureBrokerWithLeaseForTest } from "../src/sdk/broker/ensure";
import { OwnedChildren, settleFixtureBrokerWithOwner } from "./helpers/owned-children";

const roots = new Set<string>();
const brokerLeases = new Map<string, FixtureBrokerLease>();
/**
 * Roots where this suite began a step that can bring a detached broker into
 * existence — starting the fixture lease, or spawning a CLI child that ensures
 * a broker for the same agent dir.
 *
 * It is recorded before that step starts and never derived from the lease map:
 * a start that throws after spawning, and a child that ensures its own broker,
 * both leave no lease here at all, and reading the map would report exactly
 * those runs as "nothing was launched".
 */
const mayHaveLaunchedBroker = new Set<string>();
const cliEntrypoint = path.resolve(import.meta.dir, "../src/cli.ts");

async function tempRoot(): Promise<string> {
	const root = await fs.mkdtemp(path.join(process.env.TMPDIR ?? "/tmp", "gjc-sdk-isolation-"));
	roots.add(root);
	return root;
}

const isolationChildren = new OwnedChildren();

afterEach(async () => {
	// Owner-scoped: this suite settles only the CLI children it spawned and only
	// the brokers published inside its own temp roots. One failing release never
	// strands the rest.
	const survivors = await isolationChildren.settle();
	const failures: unknown[] = [];
	const unsettled: string[] = [];
	for (const root of roots) {
		const lease = brokerLeases.get(root);
		// The lease is this suite's retained handle over the detached broker it
		// started; the may-launch record is the separate statement that one can
		// exist at all. With no retained lease, a root that may have launched one
		// must still name it in discovery, so a missing or unreadable record fails
		// teardown instead of reading as nothing to settle.
		const settled = await settleFixtureBrokerWithOwner(path.join(root, "agent"), {
			launched: mayHaveLaunchedBroker.has(root) && lease === undefined,
			owner: async () => await (lease?.close() ?? Promise.resolve()),
		});
		if (settled.problem) unsettled.push(settled.problem);
		failures.push(...settled.failures);
		try {
			await fs.rm(root, { recursive: true, force: true });
		} catch (error) {
			failures.push(error);
		}
	}
	roots.clear();
	brokerLeases.clear();
	mayHaveLaunchedBroker.clear();
	expect(survivors).toEqual([]);
	expect(unsettled).toEqual([]);
	if (failures.length > 0) throw new AggregateError(failures, "SDK broker isolation fixture teardown failed.");
});

it("starts a fresh detached source broker without loading hostile cwd bunfig or dotenv", async () => {
	const root = await tempRoot();
	// This test starts a detached broker and then runs a CLI child that would
	// ensure one of its own for the same agent dir. Either can exist before this
	// process holds a lease over it, so the root is accounted for from here on.
	mayHaveLaunchedBroker.add(root);
	const hostileCwd = path.join(root, "hostile project ü");
	const agentDir = path.join(root, "agent");
	const preloadSentinel = path.join(root, "preload-sentinel");
	const dotenvSentinel = path.join(root, "dotenv-sentinel");
	const preload = path.join(root, "preload.ts");
	const pathSentinel = path.join(root, "path-sentinel");
	const hostileBin = path.join(root, "hostile-bin");
	await fs.mkdir(hostileCwd, { recursive: true });
	await fs.mkdir(hostileBin, { recursive: true });
	const fakeBun = path.join(hostileBin, process.platform === "win32" ? "bun.cmd" : "bun");
	await Bun.write(
		fakeBun,
		process.platform === "win32"
			? `@echo path-hijack>${JSON.stringify(pathSentinel)}\r\n`
			: `#!/bin/sh\nprintf path-hijack > ${JSON.stringify(pathSentinel)}\n`,
	);
	if (process.platform !== "win32") await fs.chmod(fakeBun, 0o755);
	await fs.mkdir(agentDir, { recursive: true });
	brokerLeases.set(
		root,
		(
			await startFixtureBrokerWithLeaseForTest({
				agentDir,
				env: {
					...process.env,
					BUN_OPTIONS: "--no-env-file --config=/dev/null",
					PI_COMPILED: "1",
					GJC_COMPILED: "1",
					PATH: `${hostileBin}${path.delimiter}${process.env.PATH ?? ""}`,
				},
			})
		).lease,
	);
	await Bun.write(path.join(hostileCwd, "bunfig.toml"), `preload = [${JSON.stringify(preload)}]\n`);
	await Bun.write(path.join(hostileCwd, ".env"), "GJC_2178_DOTENV=dotenv-loaded\n");
	await Bun.write(
		preload,
		[
			`await Bun.write(${JSON.stringify(preloadSentinel)}, "preload-loaded");`,
			`if (process.env.GJC_2178_DOTENV) await Bun.write(${JSON.stringify(dotenvSentinel)}, process.env.GJC_2178_DOTENV);`,
		].join("\n"),
	);

	// Owned before the first await so a hung or slow CLI child is settled by this
	// suite's teardown rather than surviving as a `gjc-sdk-isolation-*` orphan.
	const child = isolationChildren.track(
		Bun.spawn(
			[
				process.execPath,
				"--no-env-file",
				"--config=/dev/null",
				cliEntrypoint,
				"daemon",
				"session",
				"global",
				"--op=session.list",
				"--json-input={}",
				`--agent-dir=${agentDir}`,
			],
			{
				cwd: hostileCwd,
				env: {
					...process.env,
					BUN_OPTIONS: "--no-env-file --config=/dev/null",
					PI_COMPILED: "1",
					GJC_COMPILED: "1",
					PATH: `${hostileBin}${path.delimiter}${process.env.PATH ?? ""}`,
				},
				stdout: "pipe",
				stderr: "pipe",
			},
		),
	);
	const [exitCode, stdout, stderr] = await Promise.all([
		child.exited,
		new Response(child.stdout).text(),
		new Response(child.stderr).text(),
	]);
	expect(exitCode, stderr).toBe(0);
	const response = JSON.parse(stdout.trim()) as { ok?: boolean; result?: { sessions?: unknown[] } };
	expect(response.ok).toBe(true);
	expect(response.result?.sessions).toEqual([]);
	expect(await readBrokerDiscovery(agentDir)).not.toBeNull();
	expect(brokerLeases.get(root)).toBeDefined();
	expect(await Bun.file(preloadSentinel).exists()).toBe(false);
	expect(await Bun.file(dotenvSentinel).exists()).toBe(false);
	expect(await Bun.file(pathSentinel).exists()).toBe(false);
	// A full CLI child that spawns its own detached broker, exactly like the
	// sibling below: the default 5s budget is a per-test limit rather than a
	// correctness bound, and under load this child alone can exceed it.
}, 30_000);

it("starts the default source session host with isolated bootstrap policy and workspace cwd", async () => {
	const root = await tempRoot();
	const workspace = path.join(root, "workspace ü");
	const agentDir = path.join(root, "agent");
	const stateRoot = path.join(workspace, ".gjc", "state");
	const sentinel = path.join(root, "host-preload-sentinel");
	const preload = path.join(root, "host-preload.ts");
	await fs.mkdir(workspace, { recursive: true });
	await fs.mkdir(agentDir, { recursive: true });
	await Bun.write(path.join(workspace, "bunfig.toml"), `preload = [${JSON.stringify(preload)}]\n`);
	await Bun.write(preload, `await Bun.write(${JSON.stringify(sentinel)}, process.cwd());\n`);
	const previousCommand = process.env.GJC_SDK_SESSION_COMMAND;
	delete process.env.GJC_SDK_SESSION_COMMAND;
	const broker = new Broker({ agentDir });
	// The broker spawns a real session host; the id is captured as soon as it
	// exists so `finally` can close that exact session even when a later
	// assertion throws. `broker.stop()` alone does not settle a spawned host.
	let createdSessionId: string | undefined;
	try {
		await broker.start();
		const created = await broker.handleRequest(
			"session.create",
			{ cwd: workspace, stateRoot, readinessTimeoutMs: 10_000 },
			"source-host-isolation",
		);
		expect(created.ok).toBe(true);
		if (!created.ok) throw new Error(created.error.message);
		const sessionId = (created.result as { sessionId?: unknown }).sessionId;
		if (typeof sessionId === "string") createdSessionId = sessionId;
		expect(typeof sessionId).toBe("string");
		if (typeof sessionId !== "string") throw new Error("session.create did not return a session id");
		expect(await Bun.file(sentinel).exists()).toBe(false);
		expect(await broker.handleRequest("session.close", { sessionId }, "source-host-close")).toMatchObject({
			ok: true,
			result: { sessionId },
		});
		createdSessionId = undefined;
	} finally {
		if (previousCommand === undefined) delete process.env.GJC_SDK_SESSION_COMMAND;
		else process.env.GJC_SDK_SESSION_COMMAND = previousCommand;
		if (createdSessionId !== undefined)
			await broker
				.handleRequest("session.close", { sessionId: createdSessionId }, "source-host-close-teardown")
				.catch(() => undefined);
		await broker.stop();
	}
}, 30_000);
