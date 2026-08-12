import { describe, expect, test } from "bun:test";
import { SessionSdkHost } from "../src/sdk/host";
import { SESSION_PREPARED_EVENT } from "../src/sdk/host/host";
import type { BrokerIndexWriter, SdkFrame } from "../src/sdk/host/types";

interface HostFixture {
	host: SessionSdkHost;
	sent: SdkFrame[];
	registrations: Array<{ sessionId: string; endpointGeneration: number }>;
	writer: BrokerIndexWriter;
	deliver(frame: SdkFrame): void;
	readyFrames(): SdkFrame[];
	preparedFrames(): SdkFrame[];
}

function hostFixture(
	options: {
		readiness?: "immediate" | "deferred";
		activationGate?: (input: { sessionId: string; generation: number }) => boolean | Promise<boolean>;
	} = {},
): HostFixture {
	const sent: SdkFrame[] = [];
	const registrations: Array<{ sessionId: string; endpointGeneration: number }> = [];
	let handler: ((connectionId: string, frame: SdkFrame) => void) | undefined;
	const host = new SessionSdkHost({
		sessionId: "session-1",
		stateRoot: "/state",
		token: "endpoint-token",
		sendFrame: (_connectionId, frame) => {
			sent.push(frame);
		},
		onFrame: value => {
			handler = value;
			return () => {
				handler = undefined;
			};
		},
		...(options.readiness ? { readiness: options.readiness } : {}),
		...(options.activationGate ? { activationGate: options.activationGate } : {}),
	});
	return {
		host,
		sent,
		registrations,
		writer: {
			register: input => {
				registrations.push({ sessionId: input.sessionId, endpointGeneration: input.endpointGeneration });
			},
			unregister: () => undefined,
		},
		deliver: frame => handler?.("connection-1", frame),
		readyFrames: () => host.events.replay(0).events.filter(frame => frame.name === "session_ready"),
		preparedFrames: () => host.events.replay(0).events.filter(frame => frame.name === SESSION_PREPARED_EVENT),
	};
}

/** Settles the host's fire-and-forget frame handling without a timing assumption. */
async function drainFrames(): Promise<void> {
	for (let turn = 0; turn < 8; turn++) await Promise.resolve();
}

describe("prepared session readiness", () => {
	test("an ordinary start publishes session_ready immediately", async () => {
		const fixture = hostFixture();
		await fixture.host.registerWithBroker(fixture.writer);
		expect(await fixture.host.start()).toBe("started");

		expect(fixture.readyFrames()).toEqual([
			expect.objectContaining({
				name: "session_ready",
				sessionId: "session-1",
				generation: fixture.host.generation,
			}),
		]);
		expect(fixture.host.prepared).toBe(false);
		expect(fixture.host.ready).toBe(true);
		expect(fixture.registrations).toEqual([{ sessionId: "session-1", endpointGeneration: fixture.host.generation }]);
	});

	test("a prepared start publishes endpoint authority without any readiness signal", async () => {
		const fixture = hostFixture({ readiness: "deferred" });
		await fixture.host.registerWithBroker(fixture.writer);
		expect(await fixture.host.start()).toBe("started");

		// Session id and endpoint generation are discoverable authority, but no
		// replayable readiness exists for a chat daemon to act on.
		expect(fixture.registrations).toEqual([{ sessionId: "session-1", endpointGeneration: fixture.host.generation }]);
		expect(fixture.readyFrames()).toEqual([]);
		expect(fixture.host.prepared).toBe(true);
		expect(fixture.host.ready).toBe(false);
	});

	/**
	 * A prepared session still has to be observable as fully initialized, so it
	 * publishes its own replayable signal. It is deliberately a different event
	 * name from readiness: a broker wait can authenticate it, and a chat daemon
	 * that only acts on `session_ready` stays inert and publishes no root.
	 */
	test("a prepared start publishes a distinct replayable prepared signal, never readiness", async () => {
		const fixture = hostFixture({ readiness: "deferred" });
		await fixture.host.start();
		const generation = fixture.host.generation;

		expect(fixture.preparedFrames()).toEqual([
			expect.objectContaining({ name: SESSION_PREPARED_EVENT, sessionId: "session-1", generation }),
		]);
		expect(SESSION_PREPARED_EVENT).not.toBe("session_ready");
		expect(fixture.readyFrames()).toEqual([]);

		// Activation adds readiness exactly once and never a second prepared signal.
		expect(await fixture.host.activate(generation)).toBe("activated");
		expect(fixture.preparedFrames()).toHaveLength(1);
		expect(fixture.readyFrames()).toHaveLength(1);
	});

	test("an ordinary start never publishes a prepared signal", async () => {
		const fixture = hostFixture();
		await fixture.host.start();

		expect(fixture.preparedFrames()).toEqual([]);
		expect(fixture.readyFrames()).toHaveLength(1);
	});

	test("activation emits readiness exactly once and an exact retry never republishes it", async () => {
		const fixture = hostFixture({ readiness: "deferred" });
		await fixture.host.start();
		const generation = fixture.host.generation;

		expect(await fixture.host.activate(generation)).toBe("activated");
		expect(await fixture.host.activate(generation)).toBe("already");
		expect(await fixture.host.activate()).toBe("already");
		expect(fixture.readyFrames()).toEqual([
			expect.objectContaining({ name: "session_ready", sessionId: "session-1", generation }),
		]);
		expect(fixture.host.ready).toBe(true);
		expect(fixture.host.prepared).toBe(false);
	});

	test("activation against a rolled or unstarted generation fails closed", async () => {
		const fixture = hostFixture({ readiness: "deferred" });
		expect(await fixture.host.activate()).toBe("not_prepared");
		expect(fixture.readyFrames()).toEqual([]);

		await fixture.host.start();
		expect(await fixture.host.activate(fixture.host.generation + 1)).toBe("generation_changed");
		expect(await fixture.host.activate(0)).toBe("generation_changed");
		expect(fixture.readyFrames()).toEqual([]);

		await fixture.host.stop();
		expect(await fixture.host.activate()).toBe("not_prepared");
		expect(fixture.readyFrames()).toEqual([]);
	});

	test("an immediate host answers activation as already without a second readiness", async () => {
		const fixture = hostFixture();
		await fixture.host.start();
		expect(await fixture.host.activate(fixture.host.generation)).toBe("already");
		expect(fixture.readyFrames()).toHaveLength(1);
	});

	test("a settled activation never suppresses a legitimate later generation", async () => {
		const authorized: number[] = [];
		let allow = true;
		const fixture = hostFixture({
			readiness: "deferred",
			activationGate: input => {
				authorized.push(input.generation);
				return allow;
			},
		});
		await fixture.host.start();
		const first = fixture.host.generation;
		expect(await fixture.host.activate(first)).toBe("activated");

		// A rolled endpoint is a newly prepared session: readiness is withheld
		// again, the earlier activation cannot answer for it, and its own
		// authorization is proven separately.
		await fixture.host.stop();
		await fixture.host.start();
		const second = fixture.host.generation;
		expect(second).toBeGreaterThan(first);
		expect(fixture.host.prepared).toBe(true);
		expect(await fixture.host.activate(first)).toBe("generation_changed");

		allow = false;
		expect(await fixture.host.activate(second)).toBe("not_authorized");
		allow = true;
		expect(await fixture.host.activate(second)).toBe("activated");
		expect(authorized).toEqual([first, second, second]);
		expect(fixture.readyFrames()).toEqual([
			expect.objectContaining({ name: "session_ready", sessionId: "session-1", generation: second }),
		]);
	});
});

describe("prepared session activation authority", () => {
	test("a gate that refuses leaves the session prepared and silent until it authorizes", async () => {
		let bound = false;
		const seen: Array<{ sessionId: string; generation: number }> = [];
		const fixture = hostFixture({
			readiness: "deferred",
			activationGate: input => {
				seen.push(input);
				return bound;
			},
		});
		await fixture.host.start();

		expect(await fixture.host.activate(fixture.host.generation)).toBe("not_authorized");
		expect(fixture.readyFrames()).toEqual([]);
		expect(fixture.host.prepared).toBe(true);

		bound = true;
		expect(await fixture.host.activate(fixture.host.generation)).toBe("activated");
		expect(fixture.readyFrames()).toHaveLength(1);
		expect(seen).toEqual([
			{ sessionId: "session-1", generation: fixture.host.generation },
			{ sessionId: "session-1", generation: fixture.host.generation },
		]);
	});

	test("a gate that fails is never read as authorization", async () => {
		const fixture = hostFixture({
			readiness: "deferred",
			activationGate: () => {
				throw new Error("mapping store unreadable");
			},
		});
		await fixture.host.start();

		expect(await fixture.host.activate(fixture.host.generation)).toBe("authority_unavailable");
		expect(fixture.readyFrames()).toEqual([]);
	});

	test("a session that stops while its gate is in flight never publishes readiness", async () => {
		const atGate = Promise.withResolvers<void>();
		const releaseGate = Promise.withResolvers<void>();
		const fixture = hostFixture({
			readiness: "deferred",
			activationGate: async () => {
				atGate.resolve();
				await releaseGate.promise;
				return true;
			},
		});
		await fixture.host.start();
		const activation = fixture.host.activate(fixture.host.generation);
		await atGate.promise;
		await fixture.host.stop();
		releaseGate.resolve();

		expect(await activation).toBe("not_prepared");
		expect(fixture.readyFrames()).toEqual([]);
	});
});

describe("remote prepared-session activation", () => {
	function activationResults(sent: SdkFrame[]): SdkFrame[] {
		return sent.filter(frame => frame.type === "session_activate_result");
	}

	test("an exact request activates the addressed session at its exact generation", async () => {
		const fixture = hostFixture({ readiness: "deferred" });
		await fixture.host.start();
		const generation = fixture.host.generation;

		fixture.deliver({ type: "session_activate", id: "a1", sessionId: "session-1", endpointGeneration: generation });
		await drainFrames();

		expect(activationResults(fixture.sent)).toEqual([
			{
				type: "session_activate_result",
				id: "a1",
				ok: true,
				status: "activated",
				sessionId: "session-1",
				generation,
			},
		]);
		expect(fixture.readyFrames()).toHaveLength(1);
	});

	test("an exact retry answers already and never republishes readiness", async () => {
		const fixture = hostFixture({ readiness: "deferred" });
		await fixture.host.start();
		const generation = fixture.host.generation;

		fixture.deliver({ type: "session_activate", id: "a1", sessionId: "session-1", endpointGeneration: generation });
		await drainFrames();
		fixture.deliver({ type: "session_activate", id: "a2", sessionId: "session-1", endpointGeneration: generation });
		await drainFrames();

		expect(activationResults(fixture.sent).map(frame => frame.status)).toEqual(["activated", "already"]);
		expect(fixture.readyFrames()).toHaveLength(1);
	});

	test("a foreign session or generation is refused without publishing readiness", async () => {
		const fixture = hostFixture({ readiness: "deferred" });
		await fixture.host.start();
		const generation = fixture.host.generation;

		fixture.deliver({ type: "session_activate", id: "a1", sessionId: "session-2", endpointGeneration: generation });
		fixture.deliver({
			type: "session_activate",
			id: "a2",
			sessionId: "session-1",
			endpointGeneration: generation + 1,
		});
		await drainFrames();

		expect(activationResults(fixture.sent)).toEqual([
			expect.objectContaining({ id: "a1", ok: false, status: "session_mismatch" }),
			expect.objectContaining({ id: "a2", ok: false, status: "generation_changed" }),
		]);
		expect(fixture.readyFrames()).toEqual([]);
	});

	test("a refused gate is reported to the caller as an unauthorized activation", async () => {
		const fixture = hostFixture({ readiness: "deferred", activationGate: () => false });
		await fixture.host.start();

		fixture.deliver({
			type: "session_activate",
			id: "a1",
			sessionId: "session-1",
			endpointGeneration: fixture.host.generation,
		});
		await drainFrames();

		expect(activationResults(fixture.sent)).toEqual([
			expect.objectContaining({
				id: "a1",
				ok: false,
				status: "not_authorized",
				error: { code: "not_authorized", message: expect.any(String) },
			}),
		]);
		expect(fixture.readyFrames()).toEqual([]);
	});
});
