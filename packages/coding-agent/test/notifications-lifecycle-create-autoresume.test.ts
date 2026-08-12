import { afterEach, expect, test } from "bun:test";
import * as fs from "node:fs";
import * as fsp from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";

import { getAgentDir, setAgentDir } from "@gajae-code/utils";
import type { Args } from "../src/cli/args";
import { Settings } from "../src/config/settings";
import { createSessionManager } from "../src/main";
import { SessionManager } from "../src/session/session-manager";

const LIFECYCLE_ENV = ["GJC_LIFECYCLE_REQUEST_ID", "GJC_SESSION_ID"] as const;

/**
 * `setAgentDir` rewrites a process-global that every later suite in the same
 * `bun test` cohort resolves through. Leaving it pointed at a removed temp
 * directory is exactly the kind of unrestored global mutation that makes an
 * unrelated suite's agent-directory behaviour order-dependent, so the entry
 * value is captured once and restored after every test.
 */
const AMBIENT_AGENT_DIR = getAgentDir();
const owned: Array<{ agentDir: string; sessions: SessionManager[] }> = [];

/** Own one temp agent directory and every session manager opened against it. */
async function ownedAgentDir(prefix: string): Promise<{ agentDir: string; sessions: SessionManager[] }> {
	const entry = { agentDir: await fsp.mkdtemp(path.join(os.tmpdir(), prefix)), sessions: [] as SessionManager[] };
	owned.push(entry);
	setAgentDir(entry.agentDir);
	return entry;
}

afterEach(async () => {
	for (const k of LIFECYCLE_ENV) delete process.env[k];
	const failures: unknown[] = [];
	for (const entry of owned.splice(0)) {
		// Session managers hold transcript file handles; close each through its own
		// handle before the directory it lives in is removed.
		for (const session of entry.sessions.splice(0)) {
			try {
				await session.close();
			} catch (error) {
				failures.push(error);
			}
		}
		try {
			await fsp.rm(entry.agentDir, { recursive: true, force: true });
		} catch (error) {
			failures.push(error);
		}
	}
	setAgentDir(AMBIENT_AGENT_DIR);
	expect(getAgentDir()).toBe(AMBIENT_AGENT_DIR);
	if (failures.length > 0) throw new AggregateError(failures, "Lifecycle autoresume fixture teardown failed.");
});

test("normal root launch creates a current SessionManager for root token logs", async () => {
	const fixture = await ownedAgentDir("gjc-root-token-session-");
	const cwd = path.join(fixture.agentDir, "repo");
	fs.mkdirSync(cwd, { recursive: true });

	const settings = Settings.isolated();
	settings.set("autoResume", false);

	const created = await createSessionManager({} as Args, cwd, settings);
	if (created) fixture.sessions.push(created);
	expect(created).toBeDefined();
	expect(created?.getSessionId()).toBeTruthy();
	expect(created?.getCwd()).toBe(cwd);
});

// Regression for the PR #1148 stage-17 blocker: a `/session_create` child is a
// bare `gjc` launch with GJC_SESSION_ID/GJC_LIFECYCLE_REQUEST_ID. With autoResume
// enabled and existing history in the cwd, the child must NOT auto-resume the old
// session (which would diverge the daemon/tmux id from the header id); it must
// create a fresh session that adopts the pre-allocated id.
test("lifecycle /session_create bypasses autoResume; normal launch still resumes", async () => {
	const fixture = await ownedAgentDir("gjc-lc-autoresume-");
	const cwd = path.join(fixture.agentDir, "repo");
	fs.mkdirSync(cwd, { recursive: true });

	const settings = Settings.isolated();
	settings.set("autoResume", true);

	// Seed a prior persisted session in cwd so autoResume has something to resume.
	const prior = SessionManager.create(cwd);
	fixture.sessions.push(prior);
	const priorId = prior.getSessionId();
	await prior.ensureOnDisk();
	await prior.flush();

	// Control: a normal launch (no lifecycle env) auto-resumes the prior session.
	delete process.env.GJC_LIFECYCLE_REQUEST_ID;
	delete process.env.GJC_SESSION_ID;
	const resumed = await createSessionManager({} as Args, cwd, settings);
	if (resumed) fixture.sessions.push(resumed);
	expect(resumed?.getSessionId()).toBe(priorId);

	// Lifecycle create: the guard returns undefined (the SDK then creates a fresh
	// session that adopts the pre-allocated id), never auto-resuming the old one.
	process.env.GJC_LIFECYCLE_REQUEST_ID = "lc-autoresume-1";
	process.env.GJC_SESSION_ID = "s-prealloc-autoresume-1";
	const created = await createSessionManager({} as Args, cwd, settings);
	if (created) fixture.sessions.push(created);
	expect(created).toBeUndefined();

	// The freshly created SDK session under the same env adopts the pre-allocated id.
	const fresh = SessionManager.inMemory(cwd);
	fixture.sessions.push(fresh);
	expect(fresh.getSessionId()).toBe("s-prealloc-autoresume-1");
});
