import { describe, expect, spyOn, test } from "bun:test";
import type { CliConfig } from "@gajae-code/utils/cli";
import {
	assertStrictActivateThreadInvocation,
	assertStrictBindThreadInvocation,
	parseNotifyArgs,
	runNotifyCommand,
} from "../src/cli/notify-cli";
import Notify from "../src/commands/notify";
import { Settings } from "../src/config/settings";
import {
	parseInThreadConfigCommand,
	parseRichToggleCommand,
	parseTelegramControlCommand,
	parseToolActivityToggleCommand,
} from "../src/sdk/bus/config-commands";

const NOTIFY_TEST_CONFIG: CliConfig = { bin: "gjc", version: "0.0.0-test", commands: new Map() };

describe("parseInThreadConfigCommand", () => {
	test("/verbose and /lean toggle verbosity", () => {
		expect(parseInThreadConfigCommand("/verbose")).toEqual({ verbosity: "verbose" });
		expect(parseInThreadConfigCommand("/lean")).toEqual({ verbosity: "lean" });
	});

	test("/verbosity <arg> sets verbosity, rejects bad args", () => {
		expect(parseInThreadConfigCommand("/verbosity verbose")).toEqual({ verbosity: "verbose" });
		expect(parseInThreadConfigCommand("/verbosity lean")).toEqual({ verbosity: "lean" });
		expect(parseInThreadConfigCommand("/verbosity loud")).toBeUndefined();
	});

	test("/redact on|off|true|false|1|0 toggles redaction", () => {
		expect(parseInThreadConfigCommand("/redact on")).toEqual({ redact: true });
		expect(parseInThreadConfigCommand("/redact off")).toEqual({ redact: false });
		expect(parseInThreadConfigCommand("/redact true")).toEqual({ redact: true });
		expect(parseInThreadConfigCommand("/redact 0")).toEqual({ redact: false });
		expect(parseInThreadConfigCommand("/redact maybe")).toBeUndefined();
	});

	test("non-commands and free text return undefined (treated as injection)", () => {
		expect(parseInThreadConfigCommand("keep going")).toBeUndefined();
		expect(parseInThreadConfigCommand("/answer s1 yes")).toBeUndefined();
		expect(parseInThreadConfigCommand("/unknown")).toBeUndefined();
		expect(parseInThreadConfigCommand("")).toBeUndefined();
	});

	test("is case-insensitive and tolerant of extra whitespace", () => {
		expect(parseInThreadConfigCommand("  /VERBOSE  ")).toEqual({ verbosity: "verbose" });
		expect(parseInThreadConfigCommand("/Redact   ON")).toEqual({ redact: true });
	});
});

describe("parseRichToggleCommand", () => {
	test("/rich on|true|1 -> true", () => {
		expect(parseRichToggleCommand("/rich on")).toBe(true);
		expect(parseRichToggleCommand("/rich true")).toBe(true);
		expect(parseRichToggleCommand("/rich 1")).toBe(true);
	});

	test("/rich off|false|0 -> false", () => {
		expect(parseRichToggleCommand("/rich off")).toBe(false);
		expect(parseRichToggleCommand("/rich false")).toBe(false);
		expect(parseRichToggleCommand("/rich 0")).toBe(false);
	});

	test("case-insensitive and whitespace-tolerant", () => {
		expect(parseRichToggleCommand("  /RICH   On ")).toBe(true);
		expect(parseRichToggleCommand("/Rich OFF")).toBe(false);
	});

	test("accepts the /rich@botname group form", () => {
		expect(parseRichToggleCommand("/rich@GajaeCodeBot off")).toBe(false);
		expect(parseRichToggleCommand("/rich@GajaeCodeBot on")).toBe(true);
		expect(parseRichToggleCommand("/RICH@GajaeCodeBot ON")).toBe(true);
	});

	test("missing/invalid arg and non-rich commands -> undefined", () => {
		expect(parseRichToggleCommand("/rich")).toBeUndefined();
		expect(parseRichToggleCommand("/rich maybe")).toBeUndefined();
		expect(parseRichToggleCommand("/richfoo on")).toBeUndefined();
		expect(parseRichToggleCommand("/verbose")).toBeUndefined();
		expect(parseRichToggleCommand("rich on")).toBeUndefined();
		expect(parseRichToggleCommand("")).toBeUndefined();
	});
});

describe("parseToolActivityToggleCommand", () => {
	test("parses exact on/off aliases and matching bot suffixes", () => {
		expect(parseToolActivityToggleCommand("/toolactivity on")).toBe(true);
		expect(parseToolActivityToggleCommand("/toolactivity true")).toBe(true);
		expect(parseToolActivityToggleCommand("/toolactivity 1")).toBe(true);
		expect(parseToolActivityToggleCommand("/toolactivity off")).toBe(false);
		expect(parseToolActivityToggleCommand("/toolactivity false")).toBe(false);
		expect(parseToolActivityToggleCommand("/toolactivity 0")).toBe(false);
		expect(parseToolActivityToggleCommand("/TOOLACTIVITY@GajaeCodeBot OFF", "GajaeCodeBot")).toBe(false);
	});

	test("fails closed for malformed, trailing, unrelated, and foreign-addressed commands", () => {
		expect(parseToolActivityToggleCommand("/toolactivity")).toBeUndefined();
		expect(parseToolActivityToggleCommand("/toolactivity maybe")).toBeUndefined();
		expect(parseToolActivityToggleCommand("/toolactivity off accidental")).toBeUndefined();
		expect(parseToolActivityToggleCommand("/toolactivity@OtherBot off", "GajaeCodeBot")).toBeUndefined();
		expect(parseToolActivityToggleCommand("/toolactivity@ off", "GajaeCodeBot")).toBeUndefined();
		expect(parseToolActivityToggleCommand("/toolactivity@@ off", "GajaeCodeBot")).toBeUndefined();
		expect(parseToolActivityToggleCommand("/toolactivity@GajaeCodeBot@OtherBot off", "GajaeCodeBot")).toBeUndefined();
		expect(parseToolActivityToggleCommand("/toolactivity@GajaeCodeBot off")).toBeUndefined();
		expect(parseToolActivityToggleCommand("/tools off")).toBeUndefined();
		expect(parseToolActivityToggleCommand("toolactivity off")).toBeUndefined();
	});
});

describe("parseTelegramControlCommand", () => {
	test("parses command roots and bot suffixes", () => {
		expect(parseTelegramControlCommand("/context@GajaeCodeBot", "GajaeCodeBot")).toEqual({
			kind: "command",
			command: { name: "context" },
		});
		expect(parseTelegramControlCommand("/usage", "GajaeCodeBot")).toEqual({
			kind: "command",
			command: { name: "usage" },
		});
		expect(parseTelegramControlCommand("/compact keep architecture notes", "GajaeCodeBot")).toEqual({
			kind: "command",
			command: { name: "compact", instructions: "keep architecture notes" },
		});
	});

	test("parses reasoning status, cycle, and levels", () => {
		expect(parseTelegramControlCommand("/reasoning")).toEqual({
			kind: "command",
			command: { name: "reasoning", action: "status" },
		});
		expect(parseTelegramControlCommand("/reasoning cycle")).toEqual({
			kind: "command",
			command: { name: "reasoning", action: "cycle" },
		});
		expect(parseTelegramControlCommand("/reasoning HIGH")).toEqual({
			kind: "command",
			command: { name: "reasoning", action: "set", level: "high" },
		});
	});

	test("normalizes reasoning aliases and accepts global set and display mutations", () => {
		expect(parseTelegramControlCommand("/reasoning NONE --global")).toEqual({
			kind: "command",
			command: { name: "reasoning", action: "set", level: "off", global: true },
		});
		expect(parseTelegramControlCommand("/reasoning reset --global")).toEqual({
			kind: "command",
			command: { name: "reasoning", action: "set", level: "inherit", global: true },
		});
		expect(parseTelegramControlCommand("/reasoning show")).toEqual({
			kind: "command",
			command: { name: "reasoning", action: "show" },
		});
		expect(parseTelegramControlCommand("/reasoning hide --global")).toEqual({
			kind: "command",
			command: { name: "reasoning", action: "hide", global: true },
		});
	});

	test("parses model lists and exact model selectors", () => {
		expect(parseTelegramControlCommand("/model")).toEqual({
			kind: "command",
			command: { name: "model", action: "list" },
		});
		expect(parseTelegramControlCommand("/model OpenAI/GPT-5")).toEqual({
			kind: "command",
			command: { name: "model", action: "set", selector: "OpenAI/GPT-5" },
		});
	});

	test("recognized invalid forms fail closed", () => {
		expect(parseTelegramControlCommand("/usage now")).toMatchObject({ kind: "invalid", commandName: "usage" });
		expect(parseTelegramControlCommand("/context extra")).toMatchObject({ kind: "invalid", commandName: "context" });
		expect(parseTelegramControlCommand("/reasoning enormous")).toMatchObject({
			kind: "invalid",
			commandName: "reasoning",
		});
		for (const text of [
			"/reasoning cycle --global",
			"/reasoning --global high",
			"/reasoning show later",
			"/reasoning high --global extra",
			"/model provider/model extra",
		]) {
			expect(parseTelegramControlCommand(text)).toMatchObject({ kind: "invalid" });
		}
	});

	test("unknown commands and wrong bot suffix fall through", () => {
		expect(parseTelegramControlCommand("/unknown")).toEqual({ kind: "none" });
		expect(parseTelegramControlCommand("/btw why is this happening?")).toEqual({ kind: "none" });
		expect(parseTelegramControlCommand("/context@OtherBot", "GajaeCodeBot")).toEqual({
			kind: "ignored",
			commandName: "context",
		});
		expect(parseTelegramControlCommand("/context@OtherBot")).toEqual({ kind: "ignored", commandName: "context" });
		expect(parseTelegramControlCommand("plain text")).toEqual({ kind: "none" });
	});
});

describe("notify Discord and Slack setup", () => {
	test("parses provider-specific setup flags while bare setup remains Telegram", () => {
		expect(parseNotifyArgs(["notify", "setup"])?.provider).toBeUndefined();
		expect(
			parseNotifyArgs([
				"notify",
				"setup",
				"discord",
				"--discord-bot-token",
				"discord-secret",
				"--discord-application-id",
				"app",
				"--discord-guild-id",
				"guild",
				"--discord-parent-channel-id",
				"parent",
			]),
		).toMatchObject({ provider: "discord", discordBotToken: "discord-secret", discordApplicationId: "app" });
	});

	test("parses an existing Slack thread binding and prints only safe identifiers", async () => {
		expect(
			parseNotifyArgs(["notify", "bind-thread", "--session-id", "session-1", "--thread-ts", "1785573662.132329"]),
		).toMatchObject({
			action: "bind-thread",
			sessionId: "session-1",
			threadTs: "1785573662.132329",
		});
		const calls: Array<{ sessionId: string; threadTs: string }> = [];
		const settings = Settings.isolated({});
		const written: string[] = [];
		const write = spyOn(process.stdout, "write").mockImplementation(chunk => {
			written.push(String(chunk));
			return true;
		});
		try {
			await runNotifyCommand(
				{
					action: "bind-thread",
					rawArgs: [],
					sessionId: "session-1",
					threadTs: "1785573662.132329",
				},
				{
					settings,
					bindSlackThread: async input => {
						calls.push({ sessionId: input.sessionId, threadTs: input.threadTs });
						return {
							sessionId: input.sessionId,
							endpointGeneration: 7,
							teamId: "T1",
							channelId: "C1",
							rootTs: input.threadTs,
							ownerId: "1234-owner",
							daemonGeneration: 20,
						};
					},
				},
			);
		} finally {
			write.mockRestore();
		}
		expect(calls).toEqual([{ sessionId: "session-1", threadTs: "1785573662.132329" }]);
		const output = written.join("");
		expect(output).toContain("session-1");
		expect(output).toContain("T1/C1");
		expect(output).toContain("1785573662.132329");
		expect(output).toContain("1234-owner");
		expect(output).toMatch(/generation/);
		expect(output).not.toMatch(/xoxb|xapp|token/i);
	});

	test("refuses bind-thread invocations that carry a Slack target or credential", async () => {
		expect(
			parseNotifyArgs([
				"notify",
				"bind-thread",
				"--session-id",
				"session-1",
				"--thread-ts",
				"1785573662.132329",
				"--slack-workspace-id",
				"T9",
			]),
		).toBeUndefined();
		expect(
			parseNotifyArgs([
				"notify",
				"bind-thread",
				"--session-id",
				"session-1",
				"--thread-ts",
				"1785573662.132329",
				"--slack-bot-token",
				"xoxb-leak",
			]),
		).toBeUndefined();
		const calls: Array<{ sessionId: string; threadTs: string }> = [];
		const bindSlackThread = async (input: { sessionId: string; threadTs: string }) => {
			calls.push({ sessionId: input.sessionId, threadTs: input.threadTs });
			throw new Error("binding must not be reached");
		};
		await expect(
			runNotifyCommand(
				{
					action: "bind-thread",
					rawArgs: [],
					sessionId: "session-1",
					threadTs: "1785573662.132329",
					slackChannelId: "C9",
				},
				{ bindSlackThread },
			),
		).rejects.toThrow(/only --session-id and --thread-ts/);
		await expect(
			runNotifyCommand(
				{ action: "bind-thread", rawArgs: ["extra"], sessionId: "session-1", threadTs: "1785573662.132329" },
				{ bindSlackThread },
			),
		).rejects.toThrow(/does not accept additional arguments/);
		await expect(
			runNotifyCommand({ action: "bind-thread", rawArgs: [], sessionId: "session-1" }, { bindSlackThread }),
		).rejects.toThrow(/requires --session-id and --thread-ts/);
		await expect(
			runNotifyCommand(
				{ action: "bind-thread", rawArgs: [], sessionId: "session-1", threadTs: "1785573662" },
				{ bindSlackThread },
			),
		).rejects.toMatchObject({ name: "SlackThreadBindingError", code: "invalid_root" });
		expect(calls).toEqual([]);
	});

	test("rejects every bind-thread invocation the real notify command should not accept", async () => {
		const rejected: string[][] = [
			["bind-thread"],
			["bind-thread", "--session-id", "session-1"],
			["bind-thread", "--thread-ts", "1785573662.132329"],
			["bind-thread", "--session-id", "session-1", "--thread-ts", "1785573662.132329", "slack"],
			["bind-thread", "--session-id", "session-1", "--thread-ts", "1785573662.132329", "positional"],
			["bind-thread", "--session-id", "session-1", "--thread-ts", "1785573662.132329", "--message", "hi"],
			["bind-thread", "--session-id", "session-1", "--thread-ts", "1785573662.132329", "--redact"],
			["bind-thread", "--session-id", "session-1", "--thread-ts", "1785573662.132329", "--probe"],
			["bind-thread", "--session-id", "session-1", "--thread-ts", "1785573662.132329", "--smoke"],
			["bind-thread", "--session-id", "session-1", "--thread-ts", "1785573662.132329", "--token", "leak"],
			[
				"bind-thread",
				"--session-id",
				"session-1",
				"--thread-ts",
				"1785573662.132329",
				"--slack-bot-token",
				"xoxb-leak",
			],
			["bind-thread", "--session-id", "session-1", "--thread-ts", "1785573662.132329", "--owner-id", "owner"],
			["bind-thread", "--session-id", "session-1", "--thread-ts", "1785573662.132329", "--agent-dir", "/tmp"],
			["bind-thread", "--session-id", "session-1", "--thread-ts", "1785573662"],
			["bind-thread", "--session-id", "session-1", "--thread-ts", "17855736621323299999999999.1"],
		];
		for (const argv of rejected) {
			await expect(new Notify(argv, NOTIFY_TEST_CONFIG).run()).rejects.toThrow(
				/notify bind-thread|Slack root timestamp/,
			);
		}
		// Positive control: the gate the command calls accepts the exact grammar, so
		// the rejections above are attributable to the invocation shape alone.
		expect(
			assertStrictBindThreadInvocation({
				action: "bind-thread",
				rawArgs: ["--session-id", "session-1", "--thread-ts", "1785573662.132329"],
				sessionId: "session-1",
				threadTs: "1785573662.132329",
				smoke: false,
				redact: false,
				probe: false,
			}),
		).toEqual({ sessionId: "session-1", threadTs: "1785573662.132329" });
	});

	test("parses a prepared-session activation and prints only safe identifiers", async () => {
		expect(parseNotifyArgs(["notify", "activate-thread", "--session-id", "session-1"])).toMatchObject({
			action: "activate-thread",
			sessionId: "session-1",
		});
		const calls: string[] = [];
		const written: string[] = [];
		const write = spyOn(process.stdout, "write").mockImplementation(chunk => {
			written.push(String(chunk));
			return true;
		});
		try {
			await runNotifyCommand(
				{ action: "activate-thread", rawArgs: [], sessionId: "session-1" },
				{
					settings: Settings.isolated({}),
					activatePreparedSession: async input => {
						calls.push(input.sessionId);
						return { sessionId: input.sessionId, endpointGeneration: 7, status: "activated" };
					},
				},
			);
		} finally {
			write.mockRestore();
		}
		expect(calls).toEqual(["session-1"]);
		const output = written.join("");
		expect(output).toContain("session-1");
		expect(output).toContain("activated");
		expect(output).toMatch(/generation/);
		expect(output).not.toMatch(/xoxb|xapp|token/i);
	});

	test("refuses activate-thread invocations that carry anything but a session", async () => {
		expect(
			parseNotifyArgs(["notify", "activate-thread", "--session-id", "session-1", "--thread-ts", "1.2"]),
		).toBeUndefined();
		const activatePreparedSession = async () => {
			throw new Error("activation must not be reached");
		};
		await expect(
			runNotifyCommand(
				{ action: "activate-thread", rawArgs: [], sessionId: "session-1", slackChannelId: "C9" },
				{ activatePreparedSession },
			),
		).rejects.toThrow(/only --session-id/);
		await expect(
			runNotifyCommand(
				{ action: "activate-thread", rawArgs: ["extra"], sessionId: "session-1" },
				{ activatePreparedSession },
			),
		).rejects.toThrow(/does not accept additional arguments/);
		await expect(
			runNotifyCommand({ action: "activate-thread", rawArgs: [] }, { activatePreparedSession }),
		).rejects.toThrow(/requires --session-id/);
	});

	test("rejects every activate-thread invocation the real notify command should not accept", async () => {
		const rejected: string[][] = [
			["activate-thread"],
			["activate-thread", "--thread-ts", "1785573662.132329"],
			["activate-thread", "--session-id", "session-1", "positional"],
			["activate-thread", "--session-id", "session-1", "--thread-ts", "1785573662.132329"],
			["activate-thread", "--session-id", "session-1", "--message", "hi"],
			["activate-thread", "--session-id", "session-1", "--slack-bot-token", "xoxb-leak"],
			["activate-thread", "--session-id", "session-1", "--owner-id", "owner"],
			["activate-thread", "--session-id", "session-1", "--agent-dir", "/tmp"],
		];
		for (const argv of rejected) {
			await expect(new Notify(argv, NOTIFY_TEST_CONFIG).run()).rejects.toThrow(/notify activate-thread/);
		}
		// Positive control: the exact grammar is accepted by the same gate.
		expect(
			assertStrictActivateThreadInvocation({
				action: "activate-thread",
				rawArgs: ["--session-id", "session-1"],
				sessionId: "session-1",
				smoke: false,
				redact: false,
				probe: false,
			}),
		).toEqual({ sessionId: "session-1" });
	});

	test("saves complete providers, preserves unrelated settings, rejects partial config, and masks status tokens", async () => {
		const settings = Settings.isolated({ "modelProfile.default": "preserve" });
		const discordToken = "discord-secret-token";
		await runNotifyCommand(
			{
				action: "setup",
				rawArgs: ["discord"],
				provider: "discord",
				discordBotToken: discordToken,
				discordApplicationId: "app",
				discordGuildId: "guild",
				discordParentChannelId: "parent",
			},
			{
				settings,
				ensureProviderDaemon: async provider => {
					expect(provider).toBe("discord");
					return "owner_spawned";
				},
			},
		);
		expect(settings.get("notifications.discord.botToken")).toBe(discordToken);
		expect(settings.get("notifications.enabled")).toBe(true);
		expect(settings.get("modelProfile.default")).toBe("preserve");

		const slackBotToken = "xoxb-slack-secret-token";
		const slackAppToken = "xapp-slack-app-secret-token";
		const setupWrites: string[] = [];
		const originalSetupWrite = process.stdout.write;
		process.stdout.write = ((chunk: string | Uint8Array) => {
			setupWrites.push(String(chunk));
			return true;
		}) as typeof process.stdout.write;
		try {
			await runNotifyCommand(
				{
					action: "setup",
					rawArgs: ["slack"],
					provider: "slack",
					slackBotToken,
					slackAppToken,
					slackWorkspaceId: "workspace",
					slackChannelId: "channel",
				},
				{
					settings,
					ensureProviderDaemon: async provider => {
						expect(provider).toBe("slack");
						return "owner_spawned";
					},
				},
			);
		} finally {
			process.stdout.write = originalSetupWrite;
		}
		expect(settings.get("notifications.slack.botToken")).toBe(slackBotToken);
		expect(settings.get("notifications.slack.authorizedUserId")).toBeUndefined();
		expect(setupWrites.join("")).toContain("authorizedUserId=(unset; inbound denied)");
		expect(setupWrites.join("")).toContain("daemon=owner_spawned");
		expect(setupWrites.join("")).not.toContain(slackBotToken);
		expect(setupWrites.join("")).not.toContain(slackAppToken);

		const partialSettings = Settings.isolated({ "modelProfile.default": "preserve" });
		await expect(
			runNotifyCommand(
				{ action: "setup", rawArgs: ["slack"], provider: "slack", slackBotToken: "bot" },
				{ settings: partialSettings },
			),
		).rejects.toThrow("--slack-app-token is required");
		expect(partialSettings.get("notifications.slack.botToken")).toBeUndefined();

		const writes: string[] = [];
		const originalWrite = process.stdout.write;
		process.stdout.write = ((chunk: string | Uint8Array) => {
			writes.push(String(chunk));
			return true;
		}) as typeof process.stdout.write;
		try {
			await runNotifyCommand({ action: "status", rawArgs: [] }, { settings });
		} finally {
			process.stdout.write = originalWrite;
		}
		expect(writes.join("")).toContain("discord.botToken: disc…(len 20)");
		expect(writes.join("")).not.toContain(discordToken);
	});
});
