#!/usr/bin/env node
import { c as __exportAll, n as getTextErrorRemediationHints, o as toTimerMilliseconds, t as createOutputFormatter } from "./output-Bwnb3Txu.js";
import { A as findSession, C as textPrompt, Ct as exitCodeForOutputErrorCode, Dt as normalizeOutputError, F as normalizeName, G as normalizeAgentSessionId, Lt as AcpxOperationalError, N as listSessions, Nt as OUTPUT_FORMATS, O as absolutePath, St as isProcessAlive, U as serializeSessionRecordForDisk, V as parseSessionRecord, _t as sessionEventLockPath, b as mergePromptSourceWithText, c as probeQueueOwnerHealth, dt as boundedEventMaxSegments, ft as defaultSessionEventLog, gt as sessionEventActivePath, j as findSessionByDirectoryWalk, jt as EXIT_CODES, k as findGitRepositoryRoot, v as PromptInputValidationError, x as parsePromptSource, xt as _usingCtx, yt as writePrivateFile, z as writeSessionRecord, zt as AgentSpawnError } from "./ipc-D0dEwzXe.js";
import { i as DEFAULT_AGENT_NAME, k as renderArgvIdentity, l as normalizeAgentName$1, s as listBuiltInAgents } from "./agent-registry-Gtwn7nMp.js";
import { ct as acquireSessionScope, d as flushPerfMetricsCapture, dt as InterruptedError, f as installPerfMetricsCapture, ft as TimeoutError, i as runOnce, lt as listSessionEvents, n as runSessionQueueOwner, nt as getAcpxVersion, s as DISCARD_OUTPUT_FORMATTER, st as acquireSessionImport, u as buildQueueOwnerArgOverride, ut as isLegacyZedCodexAcpInvocation } from "./queue-owner-runtime-B_R4H7vy.js";
import { C as resolvePermissionMode, E as resolveSessionsListFlags, S as resolveOutputPolicy, T as resolveSessionNameFromFlags, _ as parseSessionName, a as addGlobalFlags, b as resolveAgentInvocation, c as addSessionOption, d as parseDaysOlderThan, f as parseHistoryLimit, g as parsePruneBeforeDate, h as parseOutputFormat$1, i as addExecConfigOption, m as parseNonEmptyValue, n as sessionConnectionOptions, o as addPromptInputOption, p as parseMaxTurns, r as sessionOptionsFromGlobalFlags, s as addSessionNameOption, t as resolvePermissionPolicyFromFlags, u as parseAllowedTools, v as parseTimeoutSeconds, w as resolvePromptFlags, x as resolveGlobalFlags, y as parseTtlSeconds } from "./invocation-options-9AeiiKDX.js";
import { realpathSync } from "node:fs";
import { fileURLToPath, pathToFileURL } from "node:url";
import path from "node:path";
import { Command, CommanderError, InvalidArgumentError, Option } from "commander";
import fs$1 from "node:fs/promises";
import os from "node:os";
import { ZodError, z } from "zod";
import { createHash, randomUUID } from "node:crypto";
import { text } from "node:stream/consumers";
import { performance } from "node:perf_hooks";
//#region src/cli-public.ts
function configurePublicCli(options) {
	const builtInAgents = options.listBuiltInAgents(options.config.agents);
	for (const agentName of builtInAgents) options.registerAgentCommand(options.program, agentName, options.config);
	options.registerDefaultCommands(options.program, options.config);
	const scan = options.detectAgentToken(options.argv);
	if (!scan.hasAgentOverride && scan.token && !options.topLevelVerbs.has(scan.token) && !builtInAgents.includes(scan.token)) options.registerAgentCommand(options.program, scan.token, options.config);
	options.program.helpCommand(true);
	options.program.argument("[prompt...]", "Prompt text").action(async function(promptParts) {
		if (promptParts.length === 0 && process.stdin.isTTY) {
			if (options.requestedJsonStrict) throw new InvalidArgumentError("Prompt is required (pass as argument, --file, or pipe via stdin)");
			this.outputHelp();
			return;
		}
		await options.handlePromptAction(this, promptParts);
	});
	options.program.addHelpText("after", `
Examples:
  acpx pi "review recent changes"
  acpx openclaw exec "summarize active session state"
  acpx codex sessions new
  acpx codex "fix the tests"
  acpx codex prompt "fix the tests"
  acpx codex --no-wait "queue follow-up task"
  acpx codex exec "what does this repo do"
  acpx codex cancel
  acpx codex set-mode plan
  acpx codex set model 'gpt-5.2[high]'
  acpx codex -s backend "fix the API"
  acpx codex sessions
  acpx codex sessions new --name backend
  acpx codex sessions ensure --name backend
  acpx codex sessions close backend
  acpx codex status
  acpx config show
  acpx config init
  acpx --ttl 30 codex "investigate flaky tests"
  acpx claude "refactor auth"
  acpx --agent ./my-custom-server "do something"`);
}
//#endregion
//#region src/session/export.ts
var SessionExportError = class extends AcpxOperationalError {
	code;
	exitCode = 2;
	constructor(message, code) {
		super(message, {
			outputCode: "USAGE",
			detailCode: code,
			origin: "cli"
		});
		this.code = code;
	}
};
function sessionLookupError(message, code) {
	return new SessionExportError(message, code);
}
async function loadSessionRecord(sessionLookup) {
	const cwd = path.resolve(sessionLookup.cwd ?? process.cwd());
	const name = normalizeName(sessionLookup.name);
	if (sessionLookup.agentCommand) {
		const active = await findSession({
			agentCommand: sessionLookup.agentCommand,
			cwd,
			name
		});
		if (active) return active;
		return (await listSessions()).find((session) => {
			if (session.agentCommand !== sessionLookup.agentCommand || session.cwd !== cwd) return false;
			if (name == null) return session.name == null;
			return session.name === name;
		});
	}
	const matches = (await listSessions()).filter((session) => {
		if (session.cwd !== cwd) return false;
		if (name == null) return session.name == null;
		return session.name === name;
	});
	if (matches.length > 1) throw sessionLookupError("multiple sessions match export lookup", "ambiguous-session");
	return matches[0];
}
function parseEventLockPayload(raw) {
	try {
		const parsed = JSON.parse(raw);
		if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return {};
		const record = parsed;
		return { pid: typeof record.pid === "number" ? record.pid : void 0 };
	} catch {
		return {};
	}
}
async function hasLiveEventLock(recordId) {
	try {
		const payload = await fs$1.readFile(sessionEventLockPath(recordId), "utf8");
		return isProcessAlive(parseEventLockPayload(payload).pid);
	} catch (error) {
		if (error.code === "ENOENT") return false;
		throw error;
	}
}
async function isSessionActive(record) {
	if (record.closed) return false;
	return isProcessAlive(record.pid) || await hasLiveEventLock(record.acpxRecordId);
}
function cwdRelativeToHome(cwd, home) {
	const relative = path.relative(home, cwd);
	if (relative.length === 0) return ".";
	if (relative.length > 0 && !relative.startsWith("..") && !path.isAbsolute(relative)) return relative;
	return cwd;
}
function serializeSessionRecordForArchive(record, cwdRelative) {
	const state = serializeSessionRecordForDisk(record);
	state.cwd = cwdRelative;
	if (state.event_log && typeof state.event_log === "object" && !Array.isArray(state.event_log)) state.event_log = {
		...state.event_log,
		active_path: ".stream.ndjson"
	};
	return state;
}
async function exportSession(sessionLookup, outputPath) {
	const record = await loadSessionRecord(sessionLookup);
	if (!record) throw sessionLookupError("session not found", "not-found");
	if (await isSessionActive(record)) throw sessionLookupError("session is currently locked by a running queue owner; close it first with `acpx sessions close`", "session-locked");
	const home = os.homedir();
	const cwdRelative = cwdRelativeToHome(record.cwd, home);
	const exported = {
		format_version: 1,
		exported_at: (/* @__PURE__ */ new Date()).toISOString(),
		exported_by: "acpx",
		session: {
			record_id: record.acpxRecordId,
			name: record.name ?? null,
			agent: record.agentCommand,
			agent_name: normalizeAgentName(sessionLookup.agentName),
			cwd_relative: cwdRelative,
			cwd_original: cwdRelative,
			created_at: record.createdAt,
			updated_at: record.lastUsedAt,
			state: serializeSessionRecordForArchive(record, cwdRelative)
		},
		history: await listSessionEvents(record.acpxRecordId, record.eventLog.max_segments)
	};
	const target = await resolveExportTarget(outputPath);
	await writePrivateFile(target, `${JSON.stringify(exported, null, 2)}\n`, { privateDirectory: false });
}
async function resolveExportTarget(filePath) {
	for (let hops = 0; hops < 40; hops += 1) {
		const stat = await fs$1.lstat(filePath).catch((error) => {
			if (error.code === "ENOENT") return;
			throw error;
		});
		if (!stat || stat.isFile()) return filePath;
		if (!stat.isSymbolicLink()) throw new Error("Session export output must be a regular file");
		const target = await fs$1.readlink(filePath);
		filePath = path.isAbsolute(target) ? target : `${path.dirname(filePath)}${path.sep}${target}`;
	}
	throw new Error("Too many symbolic links in session export output");
}
function normalizeAgentName(agentName) {
	const normalized = agentName?.trim().toLowerCase();
	if (!normalized || normalized.length === 0) return;
	return normalized === "factory-droid" || normalized === "factorydroid" ? "droid" : normalized;
}
//#endregion
//#region src/session/import.ts
const SUPPORTED_FORMAT_VERSION = 1;
const exportedSessionSchema = z.object({
	format_version: z.literal(SUPPORTED_FORMAT_VERSION),
	exported_at: z.string(),
	exported_by: z.string(),
	session: z.object({
		record_id: z.string(),
		name: z.string().nullable(),
		agent: z.string(),
		agent_name: z.string().optional(),
		cwd_relative: z.string(),
		cwd_original: z.string().optional(),
		cwd_absolute_original: z.string().optional(),
		created_at: z.string(),
		updated_at: z.string(),
		state: z.unknown()
	}),
	history: z.array(z.unknown())
});
var SessionImportError = class extends AcpxOperationalError {
	code;
	exitCode = 2;
	constructor(message, code) {
		super(message, {
			outputCode: "USAGE",
			detailCode: code,
			origin: "cli"
		});
		this.code = code;
	}
};
function importError(message, code) {
	return new SessionImportError(message, code);
}
function parseArchiveJson(raw) {
	try {
		return JSON.parse(raw);
	} catch (error) {
		throw importError(`Invalid session export archive JSON: ${error instanceof Error ? error.message : String(error)}`, "invalid-archive");
	}
}
function assertSupportedFormatVersion(parsed) {
	const record = parsed && typeof parsed === "object" ? parsed : {};
	if (record.format_version !== SUPPORTED_FORMAT_VERSION) throw importError(`Unsupported session export format_version ${String(record.format_version)}; supported version is ${SUPPORTED_FORMAT_VERSION}`, "unsupported-format-version");
}
function parseArchive(raw) {
	const parsed = parseArchiveJson(raw);
	assertSupportedFormatVersion(parsed);
	try {
		return exportedSessionSchema.parse(parsed);
	} catch (error) {
		if (error instanceof ZodError) throw importError(`Invalid session export archive: ${error.issues[0]?.message}`, "invalid-archive");
		throw error;
	}
}
async function generateRecordId(sessionsDir) {
	for (;;) {
		const recordId = randomUUID();
		const filePath = path.join(sessionsDir, `${encodeURIComponent(recordId)}.json`);
		try {
			await fs$1.access(filePath);
		} catch (error) {
			if (error.code === "ENOENT") return recordId;
			throw error;
		}
	}
}
function resolveImportedCwd(cwdRelative, newCwd) {
	if (newCwd) return path.resolve(newCwd);
	if (path.isAbsolute(cwdRelative)) return cwdRelative;
	return path.join(os.homedir(), cwdRelative);
}
function resolveImportedName(parsed, requestedName) {
	return requestedName ?? parsed.session.name ?? void 0;
}
function assertExpectedAgentCommand(parsed, sourceRecord, options) {
	const expectedAgentCommand = options.expectedAgentCommand;
	if (!expectedAgentCommand) return;
	const expectedAgentName = normalizeAgentIdentity(options.expectedAgentName);
	const archiveAgentName = normalizeAgentIdentity(parsed.session.agent_name);
	const archiveCommandMatches = agentCommandMatchesExpected(parsed.session.agent, expectedAgentCommand, expectedAgentName);
	const stateCommandMatches = agentCommandMatchesExpected(sourceRecord.agentCommand, expectedAgentCommand, expectedAgentName);
	if (archiveCommandMatches && stateCommandMatches && archiveAgentNameMatches({
		archiveAgentName,
		expectedAgentName,
		archiveCommand: parsed.session.agent,
		stateCommand: sourceRecord.agentCommand,
		expectedAgentCommand
	})) {
		sourceRecord.agentCommand = expectedAgentCommand;
		return;
	}
	throw importError("Session export archive agent does not match the requested agent", "agent-mismatch");
}
function archiveAgentNameMatches(params) {
	if (params.archiveCommand === params.expectedAgentCommand && params.stateCommand === params.expectedAgentCommand) return true;
	return params.archiveAgentName == null || params.expectedAgentName == null || params.archiveAgentName === params.expectedAgentName;
}
function normalizeAgentIdentity(agentName) {
	const normalized = agentName?.trim().toLowerCase();
	if (!normalized || normalized.length === 0) return;
	return normalized === "factory-droid" || normalized === "factorydroid" ? "droid" : normalized;
}
function agentCommandMatchesExpected(archivedCommand, expectedAgentCommand, expectedAgentName) {
	if (archivedCommand === expectedAgentCommand) return true;
	return expectedAgentName ? commandLooksLikeBuiltInAgent(archivedCommand, expectedAgentName) : false;
}
function commandLooksLikeBuiltInAgent(command, agentName) {
	const normalized = command.trim();
	switch (agentName) {
		case "pi": return /(?:^|\s)pi-acp(?:@|\s|$)/.test(normalized);
		case "codex": return /(?:^|\s)@agentclientprotocol\/codex-acp(?:@|\s|$)/.test(normalized);
		case "claude": return /(?:^|\s)@agentclientprotocol\/claude-agent-acp(?:@|\s|$)/.test(normalized);
		default: return false;
	}
}
async function assertDestinationScopeAvailable(record) {
	if (!await findSession({
		agentCommand: record.agentCommand,
		cwd: record.cwd,
		name: record.name
	})) return;
	throw importError("A session already exists for the import destination scope; pass --name or --cwd to import a separate copy", "session-scope-exists");
}
async function assertProviderSessionAvailable(record) {
	if (!(await listSessions()).find((session) => session.acpSessionId === record.acpSessionId)) return;
	throw importError("A local session already uses this provider session id; prune or remove the existing record before importing this archive", "session-provider-exists");
}
function buildImportedRecord(parsed, sourceRecord, options) {
	const eventLog = {
		...defaultSessionEventLog(options.newRecordId),
		max_segment_bytes: sourceRecord.eventLog.max_segment_bytes,
		max_segments: boundedEventMaxSegments(sourceRecord.eventLog.max_segments),
		segment_count: parsed.history.length > 0 ? 1 : sourceRecord.eventLog.segment_count
	};
	return {
		...sourceRecord,
		acpxRecordId: options.newRecordId,
		cwd: options.cwd,
		name: resolveImportedName(parsed, options.name),
		closed: false,
		closedAt: void 0,
		pid: void 0,
		agentStartedAt: void 0,
		lastAgentExitCode: void 0,
		lastAgentExitSignal: void 0,
		lastAgentExitAt: void 0,
		lastAgentDisconnectReason: void 0,
		eventLog,
		importedFrom: {
			recordId: parsed.session.record_id,
			cwdOriginal: parsed.session.cwd_original ?? parsed.session.cwd_relative,
			exportedBy: parsed.exported_by,
			exportedAt: parsed.exported_at
		}
	};
}
async function importSession(archivePath, options = {}) {
	try {
		var _usingCtx$1 = _usingCtx();
		const parsed = parseArchive(await fs$1.readFile(archivePath, "utf8"));
		const sourceRecord = parseSessionRecord(parsed.session.state);
		if (!sourceRecord) throw importError("Invalid session export archive: session.state is not a session record", "invalid-archive");
		assertExpectedAgentCommand(parsed, sourceRecord, options);
		const sessionsDir = path.join(os.homedir(), ".acpx", "sessions");
		await fs$1.mkdir(sessionsDir, { recursive: true });
		const cwd = resolveImportedCwd(parsed.session.cwd_relative, options.newCwd);
		const newRecordId = await generateRecordId(sessionsDir);
		const newRecord = buildImportedRecord(parsed, sourceRecord, {
			newRecordId,
			cwd,
			name: options.name
		});
		_usingCtx$1.a(await acquireSessionScope({
			agentCommand: newRecord.agentCommand,
			cwd: absolutePath(newRecord.cwd),
			name: normalizeName(newRecord.name)
		}));
		_usingCtx$1.a(await acquireSessionImport());
		await assertDestinationScopeAvailable(newRecord);
		await assertProviderSessionAvailable(newRecord);
		if (parsed.history.length > 0) {
			const history = parsed.history;
			await writePrivateFile(sessionEventActivePath(newRecordId), `${history.map((entry) => JSON.stringify(entry)).join("\n")}\n`);
		}
		await writeSessionRecord(newRecord);
		return {
			record_id: newRecordId,
			cwd
		};
	} catch (_) {
		_usingCtx$1.e = _;
	} finally {
		await _usingCtx$1.d();
	}
}
//#endregion
//#region src/cli/output/json-output.ts
function emitJsonResult(format, payload) {
	if (format !== "json") return false;
	process.stdout.write(`${JSON.stringify(payload)}\n`);
	return true;
}
//#endregion
//#region src/cli/prompt-input.ts
async function readPromptInput(filePath, promptText, cwd, positionalLabel = "argument") {
	try {
		if (filePath) return await readPromptFile(filePath, promptText, cwd);
		const joined = promptText.trim();
		if (joined.length > 0) return textPrompt(joined);
		if (process.stdin.isTTY) throw new InvalidArgumentError(`Prompt is required (pass as ${positionalLabel}, --file, or pipe via stdin)`);
		const prompt = parsePromptSource(await text(process.stdin));
		if (prompt.length === 0) throw new InvalidArgumentError("Prompt from stdin is empty");
		return prompt;
	} catch (error) {
		if (error instanceof PromptInputValidationError) throw new InvalidArgumentError(error.message);
		throw error;
	}
}
async function readPromptFile(filePath, promptText, cwd) {
	const source = filePath === "-" ? await text(process.stdin) : await fs$1.readFile(path.resolve(cwd, filePath), "utf8");
	const prompt = mergePromptSourceWithText(source, promptText);
	if (prompt.length === 0) throw new InvalidArgumentError("Prompt from --file is empty");
	return prompt;
}
//#endregion
//#region src/cli/command-handlers.ts
var NoSessionError = class extends Error {
	constructor(message) {
		super(message);
		this.name = "NoSessionError";
	}
};
let sessionModulePromise;
let outputModulePromise;
let outputRenderModulePromise;
function loadSessionModule() {
	sessionModulePromise ??= import("./output-Bwnb3Txu.js").then((n) => n.s);
	return sessionModulePromise;
}
function loadOutputModule() {
	outputModulePromise ??= import("./output-Bwnb3Txu.js").then((n) => n.r);
	return outputModulePromise;
}
function loadOutputRenderModule() {
	outputRenderModulePromise ??= Promise.resolve().then(() => render_exports);
	return outputRenderModulePromise;
}
function applyPermissionExitCode(result, quietOutput) {
	const stats = result.permissionStats;
	const deniedOrCancelled = stats.denied + stats.cancelled;
	if (stats.requested > 0 && stats.approved === 0 && deniedOrCancelled > 0) {
		process.exitCode = EXIT_CODES.PERMISSION_DENIED;
		quietOutput?.onError({
			code: "PERMISSION_DENIED",
			origin: "runtime",
			message: "Permission request denied or cancelled"
		});
		quietOutput?.flush();
	}
}
function resolveCompatibleConfigId(agent, configId) {
	if (isLegacyZedCodexAcpInvocation(agent.agentCommand) && configId === "thought_level") return "reasoning_effort";
	return configId;
}
function resolveRequestedOutputPolicy(globalFlags) {
	return {
		...resolveOutputPolicy(globalFlags.format, globalFlags.jsonStrict === true),
		suppressReads: globalFlags.suppressReads === true
	};
}
function buildSessionStartOptions(params) {
	return {
		...sessionConnectionOptions(params.globalFlags, params.config),
		agentCommand: params.agent.agentCommand,
		agentArgv: params.agent.agentArgv,
		cwd: params.agent.cwd,
		name: params.flags.name,
		resumeSessionId: params.flags.resumeSession,
		permissionMode: params.permissionMode,
		permissionPolicy: params.permissionPolicy,
		sessionOptions: sessionOptionsFromGlobalFlags(params.globalFlags),
		onModelWarning: params.globalFlags.jsonStrict ? void 0 : (message) => process.stderr.write(`[acpx] warning: ${message}\n`)
	};
}
function resolveSessionListFilterCwd(flags, agentCwd) {
	return flags.filterCwd ? path.resolve(agentCwd, flags.filterCwd) : void 0;
}
async function printLocalSessionsList(agentCommand, filterCwd, format) {
	const [{ listSessionsForAgent }, { printSessionsByFormat }] = await Promise.all([loadSessionModule(), loadOutputRenderModule()]);
	const sessions = await listSessionsForAgent(agentCommand);
	printSessionsByFormat(filterCwd ? sessions.filter((session) => session.cwd === filterCwd) : sessions, format);
}
function missingScopedSessionMessage(agent, sessionName) {
	return sessionName ? `No named session "${sessionName}" for cwd ${agent.cwd} and agent ${agent.agentName}` : `No cwd session for ${agent.cwd} and agent ${agent.agentName}`;
}
async function findScopedSessionOrThrow(agent, sessionName) {
	const record = await findSession({
		agentCommand: agent.agentCommand,
		cwd: agent.cwd,
		name: sessionName,
		includeClosed: true
	});
	if (!record) throw new Error(missingScopedSessionMessage(agent, sessionName));
	return record;
}
async function findRoutedSessionOrThrow(agentCommand, agentName, cwd, sessionName) {
	const walkBoundary = findGitRepositoryRoot(cwd) ?? cwd;
	const record = await findSessionByDirectoryWalk({
		agentCommand,
		cwd,
		name: sessionName,
		boundary: walkBoundary
	});
	if (record) return record;
	throw new NoSessionError(`⚠ No acpx session found (searched up to ${walkBoundary}).\nCreate one: ${sessionName ? `acpx ${agentName} sessions new --name ${sessionName}` : `acpx ${agentName} sessions new`}`);
}
async function handlePrompt(explicitAgentName, promptParts, flags, command, config) {
	const promptFlags = resolvePromptFlags(flags, command);
	const globalFlags = resolveGlobalFlags(command, config);
	const outputPolicy = resolveRequestedOutputPolicy(globalFlags);
	const permissionMode = resolvePermissionMode(globalFlags, config.defaultPermissions);
	const permissionPolicy = await resolvePermissionPolicyFromFlags(globalFlags);
	const prompt = await readPromptInput(promptFlags.file, promptParts.join(" "), globalFlags.cwd);
	const agent = resolveAgentInvocation(explicitAgentName, globalFlags, config);
	const [{ createOutputFormatter }, { printPromptSessionBanner, printQueuedPromptByFormat }, { sendSession }] = await Promise.all([
		loadOutputModule(),
		loadOutputRenderModule(),
		loadSessionModule()
	]);
	const record = await findRoutedSessionOrThrow(agent.agentCommand, agent.agentName, agent.cwd, resolveSessionNameFromFlags(flags, command));
	const outputFormatter = createOutputFormatter(outputPolicy.format, {
		jsonContext: { sessionId: record.acpxRecordId },
		suppressReads: outputPolicy.suppressReads
	});
	await printPromptSessionBanner(record, agent.cwd, outputPolicy.format, outputPolicy.jsonStrict);
	const result = await sendSession({
		...sessionConnectionOptions(globalFlags, config),
		sessionId: record.acpxRecordId,
		prompt,
		mcpConfigPath: config.mcpConfigPath,
		mcpConfigFingerprint: config.mcpConfigFingerprint,
		permissionMode,
		permissionPolicy,
		outputFormatter,
		errorEmissionPolicy: { queueErrorAlreadyEmitted: outputPolicy.queueErrorAlreadyEmitted },
		suppressSdkConsoleErrors: outputPolicy.suppressSdkConsoleErrors,
		ttlMs: globalFlags.ttl,
		maxQueueDepth: config.queueMaxDepth,
		promptRetries: globalFlags.promptRetries,
		waitForCompletion: promptFlags.wait,
		sessionOptions: sessionOptionsFromGlobalFlags(globalFlags)
	});
	if ("queued" in result) {
		printQueuedPromptByFormat(result, outputPolicy.format);
		return;
	}
	applyPermissionExitCode(result, outputPolicy.format === "quiet" ? outputFormatter : void 0);
	if (globalFlags.verbose && result.loadError) process.stderr.write(`[acpx] session reconnect failed, started fresh session: ${result.loadError}\n`);
}
async function handleExec(explicitAgentName, promptParts, flags, command, config) {
	const globalFlags = resolveGlobalFlags(command, config);
	if (config.disableExec) throw new AcpxOperationalError("exec subcommand is disabled by configuration (disableExec: true)", {
		outputCode: "EXEC_DISABLED",
		origin: "cli"
	});
	const outputPolicy = resolveRequestedOutputPolicy(globalFlags);
	const permissionMode = resolvePermissionMode(globalFlags, config.defaultPermissions);
	const permissionPolicy = await resolvePermissionPolicyFromFlags(globalFlags);
	const prompt = await readPromptInput(resolvePromptFlags(flags, command).file, promptParts.join(" "), globalFlags.cwd);
	const [{ createOutputFormatter }, { runOnce }] = await Promise.all([loadOutputModule(), loadSessionModule()]);
	const outputFormatter = createOutputFormatter(outputPolicy.format, { suppressReads: outputPolicy.suppressReads });
	const agent = resolveAgentInvocation(explicitAgentName, globalFlags, config);
	applyPermissionExitCode(await runOnce({
		...sessionConnectionOptions(globalFlags, config),
		agentCommand: agent.agentCommand,
		agentArgv: agent.agentArgv,
		cwd: agent.cwd,
		prompt,
		permissionMode,
		permissionPolicy,
		outputFormatter,
		errorEmissionPolicy: { queueErrorAlreadyEmitted: outputPolicy.queueErrorAlreadyEmitted },
		suppressSdkConsoleErrors: outputPolicy.suppressSdkConsoleErrors,
		promptRetries: globalFlags.promptRetries,
		sessionOptions: sessionOptionsFromGlobalFlags(globalFlags),
		configOptions: flags.configOption
	}), outputPolicy.format === "quiet" ? outputFormatter : void 0);
}
function printCancelResultByFormat(result, format) {
	if (emitJsonResult(format, {
		action: "cancel_result",
		acpxRecordId: result.sessionId || "unknown",
		cancelled: result.cancelled
	})) return;
	process.stdout.write(result.cancelled ? "cancel requested\n" : "nothing to cancel\n");
}
function printSetModeResultByFormat(modeId, result, format) {
	if (emitJsonResult(format, {
		action: "mode_set",
		modeId,
		resumed: result.resumed,
		acpxRecordId: result.record.acpxRecordId,
		acpxSessionId: result.record.acpSessionId,
		agentSessionId: result.record.agentSessionId
	})) return;
	process.stdout.write(format === "quiet" ? `${modeId}\n` : `mode set: ${modeId}\n`);
}
function printSetModelResultByFormat(modelId, result, format) {
	if (emitJsonResult(format, {
		action: "model_set",
		modelId,
		resumed: result.resumed,
		acpxRecordId: result.record.acpxRecordId,
		acpxSessionId: result.record.acpSessionId,
		agentSessionId: result.record.agentSessionId
	})) return;
	process.stdout.write(format === "quiet" ? `${modelId}\n` : `model set: ${modelId}\n`);
}
function printSetConfigOptionResultByFormat(configId, value, result, format) {
	const options = result.response?.configOptions;
	const configOptions = Array.isArray(options) ? options : result.record.acpx?.config_options ?? [];
	if (emitJsonResult(format, {
		action: "config_set",
		configId,
		value,
		resumed: result.resumed,
		configOptions,
		acpxRecordId: result.record.acpxRecordId,
		acpxSessionId: result.record.acpSessionId,
		agentSessionId: result.record.agentSessionId
	})) return;
	process.stdout.write(format === "quiet" ? `${value}\n` : `config set: ${configId}=${value} (${configOptions.length} options)\n`);
}
async function handleCancel(explicitAgentName, flags, command, config) {
	const globalFlags = resolveGlobalFlags(command, config);
	const agent = resolveAgentInvocation(explicitAgentName, globalFlags, config);
	const { cancelSessionPrompt } = await loadSessionModule();
	const walkBoundary = findGitRepositoryRoot(agent.cwd) ?? agent.cwd;
	const record = await findSessionByDirectoryWalk({
		agentCommand: agent.agentCommand,
		cwd: agent.cwd,
		name: resolveSessionNameFromFlags(flags, command),
		boundary: walkBoundary
	});
	if (!record) {
		printCancelResultByFormat({
			sessionId: "",
			cancelled: false
		}, globalFlags.format);
		return;
	}
	printCancelResultByFormat(await cancelSessionPrompt({
		sessionId: record.acpxRecordId,
		verbose: globalFlags.verbose
	}), globalFlags.format);
}
async function handleSetMode(explicitAgentName, modeId, flags, command, config) {
	const globalFlags = resolveGlobalFlags(command, config);
	const agent = resolveAgentInvocation(explicitAgentName, globalFlags, config);
	const { setSessionMode } = await loadSessionModule();
	const record = await findRoutedSessionOrThrow(agent.agentCommand, agent.agentName, agent.cwd, resolveSessionNameFromFlags(flags, command));
	const result = await setSessionMode({
		...sessionConnectionOptions(globalFlags, config),
		sessionId: record.acpxRecordId,
		modeId
	});
	if (globalFlags.verbose && result.loadError) process.stderr.write(`[acpx] session reconnect failed, started fresh session: ${result.loadError}\n`);
	printSetModeResultByFormat(modeId, result, globalFlags.format);
}
async function handleSetModel(explicitAgentName, modelId, flags, command, config) {
	const globalFlags = resolveGlobalFlags(command, config);
	const agent = resolveAgentInvocation(explicitAgentName, globalFlags, config);
	const { setSessionModel } = await loadSessionModule();
	const record = await findRoutedSessionOrThrow(agent.agentCommand, agent.agentName, agent.cwd, resolveSessionNameFromFlags(flags, command));
	const result = await setSessionModel({
		...sessionConnectionOptions(globalFlags, config),
		sessionId: record.acpxRecordId,
		modelId
	});
	if (globalFlags.verbose && result.loadError) process.stderr.write(`[acpx] session reconnect failed, started fresh session: ${result.loadError}\n`);
	printSetModelResultByFormat(modelId, result, globalFlags.format);
}
async function handleSetConfigOption(explicitAgentName, configId, value, flags, command, config) {
	const globalFlags = resolveGlobalFlags(command, config);
	const agent = resolveAgentInvocation(explicitAgentName, globalFlags, config);
	if (configId === "model") {
		await handleSetModel(explicitAgentName, value, flags, command, config);
		return;
	}
	const resolvedConfigId = resolveCompatibleConfigId(agent, configId);
	const { setSessionConfigOption } = await loadSessionModule();
	const record = await findRoutedSessionOrThrow(agent.agentCommand, agent.agentName, agent.cwd, resolveSessionNameFromFlags(flags, command));
	const result = await setSessionConfigOption({
		...sessionConnectionOptions(globalFlags, config),
		sessionId: record.acpxRecordId,
		configId: resolvedConfigId,
		value
	});
	if (globalFlags.verbose && result.loadError) process.stderr.write(`[acpx] session reconnect failed, started fresh session: ${result.loadError}\n`);
	printSetConfigOptionResultByFormat(configId, value, result, globalFlags.format);
}
async function tryListAgentSessions(agent, flags, globalFlags, config) {
	const permissionMode = resolvePermissionMode(globalFlags, config.defaultPermissions);
	const permissionPolicy = await resolvePermissionPolicyFromFlags(globalFlags);
	const { listAgentSessions } = await loadSessionModule();
	try {
		return await listAgentSessions({
			...sessionConnectionOptions(globalFlags, config),
			agentCommand: agent.agentCommand,
			agentArgv: agent.agentArgv,
			cwd: agent.cwd,
			cursor: flags.cursor,
			filterCwd: resolveSessionListFilterCwd(flags, agent.cwd),
			permissionMode,
			permissionPolicy
		});
	} catch (error) {
		if (error instanceof AgentSpawnError) return "spawn-failed";
		throw error;
	}
}
async function handleSessionsList(explicitAgentName, flags, command, config) {
	const listFlags = resolveSessionsListFlags(flags, command);
	const globalFlags = resolveGlobalFlags(command, config);
	const agent = resolveAgentInvocation(explicitAgentName, globalFlags, config);
	const filterCwd = resolveSessionListFilterCwd(listFlags, agent.cwd);
	if (listFlags.local) {
		if (listFlags.cursor) throw new InvalidArgumentError("--cursor cannot be combined with --local");
		await printLocalSessionsList(agent.agentCommand, filterCwd, globalFlags.format);
		return;
	}
	const [result, { printAgentSessionsByFormat }] = await Promise.all([tryListAgentSessions(agent, listFlags, globalFlags, config), loadOutputRenderModule()]);
	if (!result || result === "spawn-failed") {
		if (result !== "spawn-failed" && (listFlags.cursor || listFlags.filterCwd)) throw new Error(`Agent command "${agent.agentCommand}" does not advertise sessionCapabilities.list; cannot use agent-side session/list filters`);
		await printLocalSessionsList(agent.agentCommand, void 0, globalFlags.format);
		return;
	}
	printAgentSessionsByFormat(result, globalFlags.format);
}
async function handleSessionsClose(explicitAgentName, sessionName, command, config) {
	const globalFlags = resolveGlobalFlags(command, config);
	const agent = resolveAgentInvocation(explicitAgentName, globalFlags, config);
	const [{ closeSession }, { printClosedSessionByFormat }] = await Promise.all([loadSessionModule(), loadOutputRenderModule()]);
	const record = await findSession({
		agentCommand: agent.agentCommand,
		cwd: agent.cwd,
		name: sessionName
	});
	if (!record) throw new Error(missingScopedSessionMessage(agent, sessionName));
	printClosedSessionByFormat(await closeSession(record.acpxRecordId), globalFlags.format);
}
async function handleSessionsNew(explicitAgentName, flags, command, config) {
	const globalFlags = resolveGlobalFlags(command, config);
	const permissionMode = resolvePermissionMode(globalFlags, config.defaultPermissions);
	const permissionPolicy = await resolvePermissionPolicyFromFlags(globalFlags);
	const agent = resolveAgentInvocation(explicitAgentName, globalFlags, config);
	const [{ createSession, closeSession }, { printCreatedSessionBanner, printNewSessionByFormat }] = await Promise.all([loadSessionModule(), loadOutputRenderModule()]);
	const replaced = await findSession({
		agentCommand: agent.agentCommand,
		cwd: agent.cwd,
		name: flags.name
	});
	const resumesSameRecord = replaced != null && replaced.acpxRecordId === flags.resumeSession;
	const created = await createSession(buildSessionStartOptions({
		agent,
		flags,
		globalFlags,
		config,
		permissionMode,
		permissionPolicy
	}));
	if (replaced) {
		if (!resumesSameRecord) await closeSession(replaced.acpxRecordId);
		if (globalFlags.verbose) process.stderr.write(`[acpx] soft-closed prior session: ${replaced.acpxRecordId}\n`);
	}
	printCreatedSessionBanner(created, agent.agentName, globalFlags.format, globalFlags.jsonStrict);
	if (globalFlags.verbose) {
		const scope = flags.name ? `named session "${flags.name}"` : "cwd session";
		process.stderr.write(`[acpx] created ${scope}: ${created.acpxRecordId}\n`);
	}
	printNewSessionByFormat(created, replaced, globalFlags.format);
}
async function handleSessionsEnsure(explicitAgentName, flags, command, config) {
	const globalFlags = resolveGlobalFlags(command, config);
	const permissionMode = resolvePermissionMode(globalFlags, config.defaultPermissions);
	const permissionPolicy = await resolvePermissionPolicyFromFlags(globalFlags);
	const agent = resolveAgentInvocation(explicitAgentName, globalFlags, config);
	const [{ ensureSession }, { printCreatedSessionBanner, printEnsuredSessionByFormat }] = await Promise.all([loadSessionModule(), loadOutputRenderModule()]);
	const result = await ensureSession(buildSessionStartOptions({
		agent,
		flags,
		globalFlags,
		config,
		permissionMode,
		permissionPolicy
	}));
	if (result.created) printCreatedSessionBanner(result.record, agent.agentName, globalFlags.format, globalFlags.jsonStrict);
	printEnsuredSessionByFormat(result.record, result.created, globalFlags.format);
}
function userContentToText(content) {
	if ("Text" in content) return content.Text;
	if ("Mention" in content) return content.Mention.content;
	if ("Image" in content) return `[image] ${content.Image.mime_type || "image"}`;
	if ("Audio" in content) return `[audio] ${content.Audio.mime_type || "audio"}`;
	return "";
}
function agentContentToText(content) {
	if ("Text" in content) return content.Text;
	if ("Thinking" in content) return content.Thinking.text;
	if ("RedactedThinking" in content) return "[redacted_thinking]";
	if ("ToolUse" in content) return `[tool:${content.ToolUse.name}]`;
	return "";
}
function conversationHistoryEntries(record) {
	const entries = [];
	for (const message of record.messages) {
		if (message === "Resume") continue;
		if ("User" in message) {
			const text = message.User.content.map((entry) => userContentToText(entry)).join(" ").trim();
			if (!text) continue;
			entries.push({
				role: "user",
				timestamp: record.updated_at,
				textPreview: text
			});
			continue;
		}
		if ("Agent" in message) {
			const text = message.Agent.content.map((entry) => agentContentToText(entry)).join(" ").trim();
			if (!text) continue;
			entries.push({
				role: "assistant",
				timestamp: record.updated_at,
				textPreview: text
			});
		}
	}
	return entries;
}
function printSessionDetailsByFormat(record, format) {
	if (format === "json") {
		process.stdout.write(`${JSON.stringify(record)}\n`);
		return;
	}
	if (format === "quiet") {
		process.stdout.write(`${record.acpxRecordId}\n`);
		return;
	}
	for (const line of sessionDetailsLines(record)) process.stdout.write(`${line}\n`);
}
function sessionDetailsLines(record) {
	return [
		`id: ${record.acpxRecordId}`,
		`sessionId: ${record.acpSessionId}`,
		`agentSessionId: ${displayValue(record.agentSessionId)}`,
		`agent: ${record.agentCommand}`,
		`cwd: ${record.cwd}`,
		`name: ${displayValue(record.name)}`,
		`created: ${record.createdAt}`,
		`lastActivity: ${record.lastUsedAt}`,
		`lastPrompt: ${displayValue(record.lastPromptAt)}`,
		`closed: ${record.closed ? "yes" : "no"}`,
		`closedAt: ${displayValue(record.closedAt)}`,
		`pid: ${displayValue(record.pid)}`,
		`agentStartedAt: ${displayValue(record.agentStartedAt)}`,
		`lastExitCode: ${displayValue(record.lastAgentExitCode)}`,
		`lastExitSignal: ${displayValue(record.lastAgentExitSignal)}`,
		`lastExitAt: ${displayValue(record.lastAgentExitAt)}`,
		`disconnectReason: ${displayValue(record.lastAgentDisconnectReason)}`,
		`historyEntries: ${conversationHistoryEntries(record).length}`
	];
}
function displayValue(value) {
	return value == null ? "-" : String(value);
}
function printSessionHistoryByFormat(record, limit, format) {
	const history = conversationHistoryEntries(record);
	const visible = limit === 0 ? history : history.slice(Math.max(0, history.length - limit));
	if (format === "json") {
		process.stdout.write(`${JSON.stringify({
			id: record.acpxRecordId,
			sessionId: record.acpSessionId,
			limit,
			count: visible.length,
			entries: visible
		})}\n`);
		return;
	}
	if (format === "quiet") {
		for (const entry of visible) process.stdout.write(`${entry.textPreview}\n`);
		return;
	}
	process.stdout.write(`session: ${record.acpxRecordId} (${visible.length}/${history.length} shown)\n`);
	if (visible.length === 0) {
		process.stdout.write("No history\n");
		return;
	}
	for (const entry of visible) process.stdout.write(`${entry.timestamp}\t${entry.role}\t${entry.textPreview}\n`);
}
async function handleSessionsShow(explicitAgentName, sessionName, command, config) {
	const globalFlags = resolveGlobalFlags(command, config);
	printSessionDetailsByFormat(await findScopedSessionOrThrow(resolveAgentInvocation(explicitAgentName, globalFlags, config), sessionName), globalFlags.format);
}
async function handleSessionsHistory(explicitAgentName, sessionName, flags, command, config) {
	const globalFlags = resolveGlobalFlags(command, config);
	printSessionHistoryByFormat(await findScopedSessionOrThrow(resolveAgentInvocation(explicitAgentName, globalFlags, config), sessionName), flags.limit, globalFlags.format);
}
async function handleSessionsWatch(explicitAgentName, flags, command, config) {
	const globalFlags = resolveGlobalFlags(command, config);
	const agent = resolveAgentInvocation(explicitAgentName, globalFlags, config);
	const sessionName = resolveSessionNameFromFlags({ session: flags.name }, command);
	const scope = {
		agentCommand: agent.agentCommand,
		cwd: agent.cwd,
		name: sessionName,
		readOnly: true
	};
	const record = await findSession(scope) ?? await findSession({
		...scope,
		includeClosed: true
	});
	if (!record) throw new Error(missingScopedSessionMessage(agent, sessionName));
	const { runSessionWatch } = await import("./session-watch-Dyfy99RF.js");
	await runSessionWatch(record, {
		cursor: flags.cursor,
		policy: resolveRequestedOutputPolicy(globalFlags)
	});
}
async function handleSessionsExport(explicitAgentName, sessionName, flags, command, config) {
	const globalFlags = resolveGlobalFlags(command, config);
	const agent = resolveAgentInvocation(explicitAgentName, globalFlags, config);
	const cwd = flags.sourceCwd ? path.resolve(agent.cwd, flags.sourceCwd) : agent.cwd;
	await exportSession({
		agentName: globalFlags.agent ? void 0 : agent.agentName,
		agentCommand: agent.agentCommand,
		cwd,
		name: sessionName
	}, flags.output);
	if (emitJsonResult(globalFlags.format, {
		action: "session_exported",
		output: flags.output
	})) return;
	if (globalFlags.format === "quiet") {
		process.stdout.write(`${flags.output}\n`);
		return;
	}
	process.stdout.write(`exported session to ${flags.output}\n`);
}
async function handleSessionsImport(explicitAgentName, archivePath, flags, command, config) {
	const globalFlags = resolveGlobalFlags(command, config);
	const agent = resolveAgentInvocation(explicitAgentName, globalFlags, config);
	const result = await importSession(archivePath, {
		name: flags.name,
		newCwd: flags.destinationCwd ? path.resolve(globalFlags.cwd, flags.destinationCwd) : void 0,
		expectedAgentName: globalFlags.agent ? void 0 : agent.agentName,
		expectedAgentCommand: agent.agentCommand
	});
	if (emitJsonResult(globalFlags.format, {
		action: "session_imported",
		record_id: result.record_id,
		cwd: result.cwd
	})) return;
	if (globalFlags.format === "quiet") {
		process.stdout.write(`${result.record_id}\n`);
		return;
	}
	process.stdout.write(`imported session ${result.record_id} at ${result.cwd}\n`);
}
async function handleSessionsPrune(explicitAgentName, flags, command, config) {
	const globalFlags = resolveGlobalFlags(command, config);
	const agent = resolveAgentInvocation(explicitAgentName, globalFlags, config);
	const [{ pruneSessions }, { printPruneResultByFormat }] = await Promise.all([loadSessionModule(), loadOutputRenderModule()]);
	const olderThanMs = flags.olderThan != null ? flags.olderThan * 24 * 60 * 60 * 1e3 : void 0;
	printPruneResultByFormat(await pruneSessions({
		agentCommand: agent.agentCommand,
		before: flags.before,
		olderThanMs,
		includeHistory: flags.includeHistory,
		dryRun: flags.dryRun
	}), globalFlags.format);
}
//#endregion
//#region src/cli/compare-args.ts
function addCompareOptions(command) {
	return command.option("--cwd <dir>", "Target workspace").option("--approve-all", "Auto-approve all permission requests").option("--approve-reads", "Auto-approve read/search requests and prompt for writes").option("--deny-all", "Deny all permission requests").option("--timeout <seconds>", "Per-agent timeout in seconds", parseTimeoutSeconds).option("--format <fmt>", "Output format: text, json, quiet", parseOutputFormat$1).option("--json", "Alias for --format json").option("-f, --file <path>", "Read prompt text from file path (use - for stdin)", (value) => parseNonEmptyValue("Prompt file", value)).option("--prompt-file <path>", "Alias for --file", (value) => parseNonEmptyValue("Prompt file", value));
}
const VALUE_FLAGS = new Set(addCompareOptions(new Command()).options.filter((option) => option.required).flatMap((option) => [option.long, option.short].filter((flag) => flag !== void 0)));
function captureCompareOutputFlag(output, token, nextToken) {
	if (token === "--json") output.json = true;
	else if (token === "--format") output.format = nextToken;
	else if (token.startsWith("--format=")) output.format = token.slice(9);
}
function scanCompareArgs(argv) {
	let cwd;
	const output = {};
	for (let index = 0; index < argv.length; index += 1) {
		const token = argv[index];
		if (token === "--") return {
			cwd,
			...output,
			promptTokens: argv.slice(index + 1)
		};
		if (token === "--cwd") cwd = argv[index + 1];
		else if (token.startsWith("--cwd=")) cwd = token.slice(6);
		captureCompareOutputFlag(output, token, argv[index + 1]);
		if (VALUE_FLAGS.has(token)) index += 1;
	}
	return {
		cwd,
		...output
	};
}
//#endregion
//#region src/cli/compare-command.ts
const DEFAULT_COMPARE_TIMEOUT_MS = 3e5;
const FINAL_MESSAGE_PREVIEW_CHARS = 200;
function asRecord$2(value) {
	return Boolean(value) && typeof value === "object" && !Array.isArray(value) ? value : void 0;
}
function numberField(source, keys) {
	if (!source) return null;
	for (const key of keys) {
		const value = source[key];
		if (typeof value === "number" && Number.isFinite(value)) return value;
	}
	return null;
}
function collapseWhitespace(value) {
	return value.replace(/\s+/g, " ").trim();
}
function truncate(value, maxChars) {
	if (value.length <= maxChars) return value;
	return `${value.slice(0, Math.max(0, maxChars - 3))}...`;
}
function splitCompareArgs(args, filePath, promptTokens) {
	if (promptTokens !== void 0) {
		const agents = args.slice(0, args.length - promptTokens.length);
		if (agents.length === 0) throw new InvalidArgumentError("At least one agent is required");
		return {
			agents,
			promptText: promptTokens.join(" ")
		};
	}
	if (filePath) {
		if (args.length === 0) throw new InvalidArgumentError("At least one agent is required");
		return {
			agents: args,
			promptText: ""
		};
	}
	if (args.length < 2) throw new InvalidArgumentError("Usage: acpx compare <agent>... '<prompt>'");
	return {
		agents: args.slice(0, -1),
		promptText: args[args.length - 1] ?? ""
	};
}
function captureUsage(update, capture) {
	const source = asRecord$2(asRecord$2(update._meta)?.usage) ?? update;
	capture.usage = {
		input_tokens: numberField(source, ["input_tokens", "inputTokens"]) ?? void 0,
		output_tokens: numberField(source, ["output_tokens", "outputTokens"]) ?? void 0,
		total_tokens: numberField(source, ["total_tokens", "totalTokens"]) ?? void 0
	};
}
function captureSessionUpdate(notification, capture) {
	const update = asRecord$2(notification.update);
	if (!update) return;
	if (update.sessionUpdate === "agent_message_chunk") {
		const content = asRecord$2(update.content);
		if (content?.type === "text" && typeof content.text === "string") capture.finalMessage += content.text;
		return;
	}
	if (update.sessionUpdate === "usage_update") captureUsage(update, capture);
}
function rowStatusFromPermissionStats(stats) {
	return stats.denied + stats.cancelled > 0 ? "permission_denied" : "ok";
}
function buildSuccessRow(agentName, result, capture, startedAt) {
	const permissionStats = result.permissionStats;
	return {
		agent: agentName,
		status: result.stopReason === "cancelled" ? "cancelled" : rowStatusFromPermissionStats(permissionStats),
		stop_reason: result.stopReason,
		wall_ms: Math.round(performance.now() - startedAt),
		input_tokens: capture.usage.input_tokens ?? null,
		output_tokens: capture.usage.output_tokens ?? null,
		total_tokens: capture.usage.total_tokens ?? null,
		final_message: truncate(collapseWhitespace(capture.finalMessage), FINAL_MESSAGE_PREVIEW_CHARS),
		error: null,
		permission_requests: permissionStats.requested,
		permission_denied: permissionStats.denied + permissionStats.cancelled,
		...result._meta === void 0 ? {} : { _meta: result._meta }
	};
}
function rowStatusFromError(error) {
	if (error instanceof TimeoutError) return "cancelled";
	const { code } = normalizeOutputError(error);
	return code === "PERMISSION_PROMPT_UNAVAILABLE" || code === "PERMISSION_DENIED" ? "permission_denied" : "error";
}
function buildErrorRow(agentName, caught, capture, startedAt) {
	return {
		agent: agentName,
		status: rowStatusFromError(caught),
		stop_reason: null,
		wall_ms: Math.round(performance.now() - startedAt),
		input_tokens: capture.usage.input_tokens ?? null,
		output_tokens: capture.usage.output_tokens ?? null,
		total_tokens: capture.usage.total_tokens ?? null,
		final_message: truncate(collapseWhitespace(capture.finalMessage), FINAL_MESSAGE_PREVIEW_CHARS),
		error: truncate(collapseWhitespace(caught instanceof Error ? caught.message : String(caught)), FINAL_MESSAGE_PREVIEW_CHARS),
		permission_requests: capture.permissionStats.requested,
		permission_denied: capture.permissionStats.denied + capture.permissionStats.cancelled
	};
}
async function runAgentForCompare(params) {
	const capture = {
		finalMessage: "",
		usage: {},
		permissionStats: {
			requested: 0,
			approved: 0,
			denied: 0,
			cancelled: 0
		}
	};
	const t0 = performance.now();
	try {
		const agent = resolveAgentInvocation(params.agentName, params.globalFlags, params.config);
		const result = await runOnce({
			...sessionConnectionOptions(params.globalFlags, params.config),
			agentCommand: agent.agentCommand,
			agentArgv: agent.agentArgv,
			cwd: agent.cwd,
			prompt: params.prompt,
			permissionMode: params.permissionMode,
			permissionPolicy: params.permissionPolicy,
			outputFormatter: DISCARD_OUTPUT_FORMATTER,
			suppressSdkConsoleErrors: true,
			timeoutMs: params.globalFlags.timeout ?? DEFAULT_COMPARE_TIMEOUT_MS,
			promptRetries: params.globalFlags.promptRetries,
			sessionOptions: sessionOptionsFromGlobalFlags(params.globalFlags),
			onSessionUpdate: (notification) => captureSessionUpdate(notification, capture),
			onPermissionStats: (stats) => {
				capture.permissionStats = stats;
			}
		});
		return buildSuccessRow(params.agentName, result, capture, t0);
	} catch (caught) {
		return buildErrorRow(params.agentName, caught, capture, t0);
	}
}
function formatCell(value) {
	if (value == null || value === "") return "-";
	if (typeof value === "string") return collapseWhitespace(value);
	if (typeof value === "number" || typeof value === "boolean") return String(value);
	return collapseWhitespace(JSON.stringify(value));
}
function renderTable(rows) {
	const headers = [
		"agent",
		"status",
		"wall_ms",
		"input",
		"output",
		"total",
		"permissions",
		"stop_reason",
		"final_message",
		"error"
	];
	const body = rows.map((row) => [
		row.agent,
		row.status,
		row.wall_ms,
		row.input_tokens,
		row.output_tokens,
		row.total_tokens,
		`${row.permission_denied}/${row.permission_requests}`,
		row.stop_reason,
		row.final_message,
		row.error
	]);
	const widths = headers.map((header, index) => Math.max(header.length, ...body.map((cells) => formatCell(cells[index]).length)));
	const formatRow = (cells) => cells.map((cell, index) => truncate(formatCell(cell), widths[index] ?? 24).padEnd(widths[index] ?? 24)).join("  ").trimEnd();
	return [
		formatRow(headers),
		widths.map((width) => "-".repeat(width)).join("  "),
		...body.map(formatRow)
	].join("\n");
}
function printRows(rows, format) {
	if (format === "json") {
		process.stdout.write(`${JSON.stringify(rows)}\n`);
		return;
	}
	if (format === "quiet") {
		for (const row of rows) process.stdout.write(`${row.agent}\t${row.status}\n`);
		return;
	}
	process.stdout.write(`${renderTable(rows)}\n`);
}
function updateCompareExitCode(rows) {
	if (rows.some((row) => row.status === "error")) {
		process.exitCode = EXIT_CODES.ERROR;
		return;
	}
	if (rows.some((row) => row.status === "permission_denied")) {
		process.exitCode = EXIT_CODES.PERMISSION_DENIED;
		return;
	}
	if (rows.some((row) => row.status === "cancelled")) process.exitCode = EXIT_CODES.TIMEOUT;
}
async function runCompareAgents(agents, run) {
	const rows = [];
	let interrupted = false;
	const onInterrupt = () => {
		interrupted = true;
	};
	const signals = [
		"SIGINT",
		"SIGTERM",
		"SIGHUP"
	];
	for (const signal of signals) process.once(signal, onInterrupt);
	try {
		for (const agentName of agents) {
			if (interrupted) break;
			const row = await run(agentName);
			rows.push(interrupted ? {
				...row,
				status: "cancelled",
				error: "Interrupted"
			} : row);
		}
	} finally {
		for (const signal of signals) process.off(signal, onInterrupt);
	}
	return {
		rows,
		interrupted
	};
}
function resolvePromptFile(flags) {
	if (flags.file && flags.promptFile && flags.file !== flags.promptFile) throw new InvalidArgumentError("Use only one prompt file flag: --file or --prompt-file");
	return flags.file ?? flags.promptFile;
}
function registerCompareCommand(program, config) {
	addCompareOptions(program.command("compare").description("Run one prompt across multiple agents and summarize the results").argument("<args...>", "Agents followed by prompt text, or agents with --file")).action(async function(args, flags) {
		if (config.disableExec) throw new Error("compare subcommand is disabled by configuration (disableExec: true)");
		const globalFlags = resolveGlobalFlags(this, config);
		if (flags.cwd !== void 0) globalFlags.cwd = path.resolve(flags.cwd);
		if (globalFlags.agent) throw new InvalidArgumentError("Do not combine compare with --agent; pass agent names");
		const outputPolicy = resolveOutputPolicy(globalFlags.format, globalFlags.jsonStrict === true);
		const promptFile = resolvePromptFile(flags);
		const { promptTokens } = scanCompareArgs(program.args.slice(1));
		const { agents, promptText } = splitCompareArgs(args, promptFile, promptTokens);
		const permissionMode = resolvePermissionMode(globalFlags, config.defaultPermissions);
		const prompt = await readPromptInput(promptFile, promptText, globalFlags.cwd, "final argument");
		const permissionPolicy = await resolvePermissionPolicyFromFlags(globalFlags);
		const { rows, interrupted } = await runCompareAgents(agents, async (agentName) => await runAgentForCompare({
			agentName,
			prompt,
			config,
			globalFlags,
			permissionMode,
			permissionPolicy
		}));
		printRows(rows, outputPolicy.format);
		if (interrupted) process.exitCode = EXIT_CODES.INTERRUPTED;
		else updateCompareExitCode(rows);
	});
}
//#endregion
//#region src/mcp-servers.ts
function asRecord$1(value) {
	if (!value || typeof value !== "object" || Array.isArray(value)) return;
	return value;
}
function parseNonEmptyString(value, path) {
	if (typeof value !== "string" || value.trim().length === 0) throw new Error(`Invalid ${path}: expected non-empty string`);
	return value.trim();
}
function parseString(value, path) {
	if (typeof value !== "string") throw new Error(`Invalid ${path}: expected string`);
	return value;
}
function parseNameValuePairs(value, path) {
	if (value == null) return [];
	if (!Array.isArray(value)) throw new Error(`Invalid ${path}: expected array`);
	const entries = [];
	for (const [index, rawEntry] of value.entries()) {
		const entry = asRecord$1(rawEntry);
		if (!entry) throw new Error(`Invalid ${path}[${index}]: expected object`);
		entries.push({
			name: parseNonEmptyString(entry.name, `${path}[${index}].name`),
			value: parseString(entry.value, `${path}[${index}].value`)
		});
	}
	return entries;
}
function parseArgs(value, path) {
	if (value == null) return [];
	if (!Array.isArray(value)) throw new Error(`Invalid ${path}: expected array`);
	const args = [];
	for (const [index, rawArg] of value.entries()) args.push(parseString(rawArg, `${path}[${index}]`));
	return args;
}
function parseMeta(value, path) {
	if (value === void 0) return;
	if (value === null) return null;
	if (!asRecord$1(value)) throw new Error(`Invalid ${path}: expected object or null`);
	return value;
}
function parseServerType(rawType, path) {
	if (rawType === void 0) return "stdio";
	const parsedType = parseNonEmptyString(rawType, `${path}.type`);
	if (parsedType !== "http" && parsedType !== "sse" && parsedType !== "stdio") throw new Error(`Invalid ${path}.type: expected http, sse, or stdio`);
	return parsedType;
}
function parseHttpServer(serverRecord, path, type, name, _meta) {
	return {
		type,
		name,
		url: parseNonEmptyString(serverRecord.url, `${path}.url`),
		headers: parseNameValuePairs(serverRecord.headers, `${path}.headers`),
		_meta
	};
}
function parseStdioServer(serverRecord, path, name, _meta) {
	return {
		name,
		command: parseNonEmptyString(serverRecord.command, `${path}.command`),
		args: parseArgs(serverRecord.args, `${path}.args`),
		env: parseNameValuePairs(serverRecord.env, `${path}.env`),
		_meta
	};
}
function parseServer(rawServer, path) {
	const serverRecord = asRecord$1(rawServer);
	if (!serverRecord) throw new Error(`Invalid ${path}: expected object`);
	const name = parseNonEmptyString(serverRecord.name, `${path}.name`);
	const _meta = parseMeta(serverRecord._meta, `${path}._meta`);
	const typeValue = parseServerType(serverRecord.type, path);
	if (typeValue === "http" || typeValue === "sse") return parseHttpServer(serverRecord, path, typeValue, name, _meta);
	return parseStdioServer(serverRecord, path, name, _meta);
}
function parseMcpServers(value, sourcePath, fieldName = "mcpServers") {
	const fieldPath = `${fieldName} in ${sourcePath}`;
	if (!Array.isArray(value)) throw new Error(`Invalid ${fieldPath}: expected array`);
	const parsed = [];
	for (const [index, rawServer] of value.entries()) parsed.push(parseServer(rawServer, `${fieldName}[${index}] in ${sourcePath}`));
	return parsed;
}
function parseOptionalMcpServers(value, sourcePath, fieldName = "mcpServers") {
	if (value === void 0) return;
	return parseMcpServers(value, sourcePath, fieldName);
}
//#endregion
//#region src/cli/config.ts
const DEFAULT_TTL_MS = 3e5;
const DEFAULT_PERMISSION_MODE = "approve-reads";
const DEFAULT_NON_INTERACTIVE_PERMISSION_POLICY = "deny";
const DEFAULT_AUTH_POLICY = "skip";
const DEFAULT_OUTPUT_FORMAT = "text";
const DEFAULT_QUEUE_MAX_DEPTH = 16;
const DEFAULT_DISABLE_EXEC = false;
const VALID_PERMISSION_MODES = /* @__PURE__ */ new Set([
	"approve-all",
	"approve-reads",
	"deny-all"
]);
const VALID_NON_INTERACTIVE_PERMISSION_POLICIES = /* @__PURE__ */ new Set(["deny", "fail"]);
const VALID_AUTH_POLICIES = /* @__PURE__ */ new Set(["skip", "fail"]);
const VALID_OUTPUT_FORMATS = /* @__PURE__ */ new Set([
	"text",
	"json",
	"quiet"
]);
function defaultGlobalConfigPath() {
	return path.join(os.homedir(), ".acpx", "config.json");
}
function projectConfigPath(cwd) {
	return path.join(path.resolve(cwd), ".acpxrc.json");
}
function isObject(value) {
	return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}
function parseTtlMs(value, sourcePath) {
	if (value == null) return;
	if (typeof value !== "number" || !Number.isFinite(value) || value < 0) throw new Error(`Invalid config ttl in ${sourcePath}: expected non-negative seconds`);
	const milliseconds = toTimerMilliseconds(value, true);
	if (milliseconds === void 0) throw new Error(`Invalid config ttl in ${sourcePath}: exceeds maximum supported timer delay`);
	return milliseconds;
}
function parseTimeoutMs(value, sourcePath) {
	if (value == null) return;
	if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) throw new Error(`Invalid config timeout in ${sourcePath}: expected positive seconds or null`);
	const milliseconds = toTimerMilliseconds(value, false);
	if (milliseconds === void 0) throw new Error(`Invalid config timeout in ${sourcePath}: exceeds maximum supported timer delay`);
	return milliseconds;
}
function parseQueueMaxDepth(value, sourcePath) {
	if (value == null) return;
	if (!Number.isInteger(value) || value <= 0) throw new Error(`Invalid config queueMaxDepth in ${sourcePath}: expected positive integer`);
	return value;
}
function parsePermissionMode(value, sourcePath) {
	if (value == null) return;
	if (typeof value !== "string" || !VALID_PERMISSION_MODES.has(value)) throw new Error(`Invalid config defaultPermissions in ${sourcePath}: expected approve-all, approve-reads, or deny-all`);
	return value;
}
function parseNonInteractivePermissionPolicy(value, sourcePath) {
	if (value == null) return;
	if (typeof value !== "string" || !VALID_NON_INTERACTIVE_PERMISSION_POLICIES.has(value)) throw new Error(`Invalid config nonInteractivePermissions in ${sourcePath}: expected deny or fail`);
	return value;
}
function parseAuthPolicy(value, sourcePath) {
	if (value == null) return;
	if (typeof value !== "string" || !VALID_AUTH_POLICIES.has(value)) throw new Error(`Invalid config authPolicy in ${sourcePath}: expected skip or fail`);
	return value;
}
function parseOutputFormat(value, sourcePath) {
	if (value == null) return;
	if (typeof value !== "string" || !VALID_OUTPUT_FORMATS.has(value)) throw new Error(`Invalid config format in ${sourcePath}: expected text, json, or quiet`);
	return value;
}
function parseDefaultAgent(value, sourcePath) {
	if (value == null) return;
	if (typeof value !== "string" || value.trim().length === 0) throw new Error(`Invalid config defaultAgent in ${sourcePath}: expected non-empty string`);
	return normalizeAgentName$1(value);
}
function parseAgents(value, sourcePath) {
	if (value == null) return;
	if (!isObject(value)) throw new Error(`Invalid config agents in ${sourcePath}: expected object`);
	return Object.fromEntries(Object.entries(value).map(([name, raw]) => [normalizeAgentName$1(name), parseAgentEntry(raw, name, sourcePath)]));
}
function parseAgentEntry(raw, name, sourcePath) {
	if (!isObject(raw)) throw new Error(`Invalid config agents.${name} in ${sourcePath}: expected object with command`);
	return Object.prototype.hasOwnProperty.call(raw, "argv") ? parseArgvAgentEntry(raw, name, sourcePath) : parseLegacyAgentEntry(raw, name, sourcePath);
}
function parseArgvAgentEntry(raw, name, sourcePath) {
	if (Object.prototype.hasOwnProperty.call(raw, "command") || Object.prototype.hasOwnProperty.call(raw, "args")) throw new Error(`Invalid config agents.${name} in ${sourcePath}: use argv alone, not command or args`);
	const argv = parseAgentArgv(raw.argv, name, sourcePath);
	return {
		command: renderArgvIdentity(argv),
		argv
	};
}
function parseLegacyAgentEntry(raw, name, sourcePath) {
	const command = raw.command;
	if (typeof command !== "string" || command.trim().length === 0) throw new Error(`Invalid config agents.${name}.command in ${sourcePath}: expected non-empty string`);
	const trimmedCommand = command.trim();
	if (!Object.prototype.hasOwnProperty.call(raw, "args")) return { command: trimmedCommand };
	if (/[\s'"]/u.test(trimmedCommand)) throw new Error(`Invalid config agents.${name}.command in ${sourcePath}: command must be an unquoted executable with no whitespace when args is present; migrate the complete launch to argv`);
	const args = parseAgentArgs(raw.args, name, sourcePath);
	return {
		command: args.length > 0 ? `${trimmedCommand} ${args.map(quoteCommandArg).join(" ")}` : trimmedCommand,
		argv: [trimmedCommand, ...args]
	};
}
function parseAgentArgv(value, agentName, sourcePath) {
	if (!Array.isArray(value) || value.length === 0) throw new Error(`Invalid config agents.${agentName}.argv in ${sourcePath}: expected non-empty array of strings`);
	const argv = value.map((arg, index) => {
		if (typeof arg !== "string") throw new Error(`Invalid config agents.${agentName}.argv[${index}] in ${sourcePath}: expected string`);
		return arg;
	});
	if (argv[0]?.length === 0) throw new Error(`Invalid config agents.${agentName}.argv[0] in ${sourcePath}: expected non-empty executable`);
	return argv;
}
function parseAgentArgs(value, agentName, sourcePath) {
	if (value == null) return [];
	if (!Array.isArray(value)) throw new Error(`Invalid config agents.${agentName}.args in ${sourcePath}: expected array of strings`);
	return value.map((arg, index) => {
		if (typeof arg !== "string") throw new Error(`Invalid config agents.${agentName}.args[${index}] in ${sourcePath}: expected string`);
		return arg;
	});
}
function quoteCommandArg(value) {
	return JSON.stringify(value);
}
function parseAuth(value, sourcePath) {
	if (value == null) return;
	if (!isObject(value)) throw new Error(`Invalid config auth in ${sourcePath}: expected object`);
	const parsed = {};
	for (const [methodId, rawCredential] of Object.entries(value)) {
		if (typeof rawCredential !== "string" || rawCredential.trim().length === 0) throw new Error(`Invalid config auth.${methodId} in ${sourcePath}: expected non-empty string`);
		parsed[methodId] = rawCredential;
	}
	return parsed;
}
function parseDisableExec(value, sourcePath) {
	if (value == null) return;
	if (typeof value !== "boolean") throw new Error(`Invalid config disableExec in ${sourcePath}: expected boolean`);
	return value;
}
async function readConfigFile(filePath) {
	try {
		const payload = await fs$1.readFile(filePath, "utf8");
		let parsed;
		try {
			parsed = JSON.parse(payload);
		} catch (error) {
			const reason = error instanceof Error ? error.message : String(error);
			throw new Error(`Invalid JSON in ${filePath}: ${reason}`, { cause: error });
		}
		if (!isObject(parsed)) throw new Error(`Invalid config in ${filePath}: expected top-level JSON object`);
		return {
			config: parsed,
			exists: true
		};
	} catch (error) {
		if (error.code === "ENOENT") return { exists: false };
		throw error;
	}
}
async function loadExplicitMcpConfig(cwd, configuredPath) {
	if (!configuredPath) return {};
	const resolvedPath = path.resolve(cwd, configuredPath);
	const result = await readConfigFile(resolvedPath);
	if (!result.exists) throw new Error(`MCP config file not found: ${resolvedPath}`);
	return {
		path: resolvedPath,
		config: result.config
	};
}
async function loadResolvedConfig(cwd, options = {}) {
	const globalPath = defaultGlobalConfigPath();
	const projectPath = projectConfigPath(cwd);
	const [globalResult, projectResult, explicitMcp] = await Promise.all([
		readConfigFile(globalPath),
		readConfigFile(projectPath),
		loadExplicitMcpConfig(cwd, options.mcpConfigPath)
	]);
	const globalConfig = globalResult.config;
	const projectConfig = projectResult.config;
	function resolveScalar(key, parse, fallback) {
		return parse(projectConfig?.[key], projectPath) ?? parse(globalConfig?.[key], globalPath) ?? fallback;
	}
	const scalar = {
		defaultAgent: resolveScalar("defaultAgent", parseDefaultAgent, DEFAULT_AGENT_NAME),
		defaultPermissions: resolveScalar("defaultPermissions", parsePermissionMode, DEFAULT_PERMISSION_MODE),
		nonInteractivePermissions: resolveScalar("nonInteractivePermissions", parseNonInteractivePermissionPolicy, DEFAULT_NON_INTERACTIVE_PERMISSION_POLICY),
		authPolicy: resolveScalar("authPolicy", parseAuthPolicy, DEFAULT_AUTH_POLICY),
		ttlMs: resolveScalar("ttl", parseTtlMs, DEFAULT_TTL_MS),
		timeoutMs: resolveTimeoutMs(projectConfig, projectPath, globalConfig, globalPath),
		queueMaxDepth: resolveScalar("queueMaxDepth", parseQueueMaxDepth, DEFAULT_QUEUE_MAX_DEPTH),
		format: resolveScalar("format", parseOutputFormat, DEFAULT_OUTPUT_FORMAT)
	};
	const agents = {
		...parseAgents(globalConfig?.agents, globalPath),
		...parseAgents(projectConfig?.agents, projectPath)
	};
	const auth = {
		...parseAuth(globalConfig?.auth, globalPath),
		...parseAuth(projectConfig?.auth, projectPath)
	};
	const mcpServers = resolveMcpServers(projectConfig, projectPath, globalConfig, globalPath, explicitMcp.config, explicitMcp.path);
	const disableExec = resolveScalar("disableExec", parseDisableExec, DEFAULT_DISABLE_EXEC);
	return {
		...scalar,
		agents,
		auth,
		disableExec,
		mcpServers,
		globalPath,
		projectPath,
		mcpConfigPath: explicitMcp.path,
		mcpConfigFingerprint: explicitMcp.path ? createHash("sha256").update(JSON.stringify(mcpServers)).digest("hex") : void 0,
		hasGlobalConfig: globalResult.exists,
		hasProjectConfig: projectResult.exists
	};
}
function hasConfigKey(config, key) {
	return config != null && Object.prototype.hasOwnProperty.call(config, key);
}
function resolveTimeoutMs(projectConfig, projectPath, globalConfig, globalPath) {
	if (hasConfigKey(projectConfig, "timeout")) return parseTimeoutMs(projectConfig?.timeout, projectPath);
	if (hasConfigKey(globalConfig, "timeout")) return parseTimeoutMs(globalConfig?.timeout, globalPath);
}
function resolveMcpServers(projectConfig, projectPath, globalConfig, globalPath, mcpConfig, mcpConfigPath) {
	if (mcpConfigPath) return parseMcpServers(mcpConfig?.mcpServers, mcpConfigPath);
	if (hasConfigKey(projectConfig, "mcpServers")) return parseMcpServers(projectConfig?.mcpServers, projectPath);
	if (hasConfigKey(globalConfig, "mcpServers")) return parseMcpServers(globalConfig?.mcpServers, globalPath);
	return [];
}
function toConfigDisplay(config) {
	const agents = Object.fromEntries(Object.entries(config.agents).map(([name, agent]) => [name, agent.argv ? { argv: [...agent.argv] } : { command: agent.command }]));
	return {
		defaultAgent: config.defaultAgent,
		defaultPermissions: config.defaultPermissions,
		nonInteractivePermissions: config.nonInteractivePermissions,
		authPolicy: config.authPolicy,
		ttl: config.ttlMs / 1e3,
		timeout: config.timeoutMs == null ? null : config.timeoutMs / 1e3,
		queueMaxDepth: config.queueMaxDepth,
		format: config.format,
		agents,
		authMethods: Object.keys(config.auth).toSorted(),
		disableExec: config.disableExec
	};
}
async function initGlobalConfigFile() {
	const configPath = defaultGlobalConfigPath();
	await fs$1.mkdir(path.dirname(configPath), {
		recursive: true,
		mode: 448
	});
	try {
		await fs$1.access(configPath);
		return {
			path: configPath,
			created: false
		};
	} catch {}
	const payload = {
		defaultAgent: DEFAULT_AGENT_NAME,
		defaultPermissions: "approve-all",
		nonInteractivePermissions: "deny",
		authPolicy: "skip",
		ttl: 300,
		timeout: null,
		queueMaxDepth: DEFAULT_QUEUE_MAX_DEPTH,
		format: "text",
		agents: {},
		auth: {}
	};
	try {
		await fs$1.writeFile(configPath, `${JSON.stringify(payload, null, 2)}\n`, {
			encoding: "utf8",
			flag: "wx",
			mode: 384
		});
	} catch (error) {
		if (error.code === "EEXIST") return {
			path: configPath,
			created: false
		};
		throw error;
	}
	return {
		path: configPath,
		created: true
	};
}
//#endregion
//#region src/cli/config-command.ts
async function handleConfigShow(command, config) {
	const globalFlags = resolveGlobalFlags(command, config);
	const payload = {
		...toConfigDisplay(config),
		paths: {
			global: config.globalPath,
			project: config.projectPath,
			...config.mcpConfigPath ? { mcp: config.mcpConfigPath } : {}
		},
		loaded: {
			global: config.hasGlobalConfig,
			project: config.hasProjectConfig
		}
	};
	if (globalFlags.format === "json") {
		process.stdout.write(`${JSON.stringify(payload)}\n`);
		return;
	}
	process.stdout.write(`${JSON.stringify(payload, null, 2)}\n`);
}
async function handleConfigInit(command, config) {
	const globalFlags = resolveGlobalFlags(command, config);
	const result = await initGlobalConfigFile();
	if (globalFlags.format === "json") {
		process.stdout.write(`${JSON.stringify({
			path: result.path,
			created: result.created
		})}\n`);
		return;
	}
	if (globalFlags.format === "quiet") {
		process.stdout.write(`${result.path}\n`);
		return;
	}
	if (result.created) {
		process.stdout.write(`Created ${result.path}\n`);
		return;
	}
	process.stdout.write(`Config already exists: ${result.path}\n`);
}
function registerConfigCommand(program, config) {
	const configCommand = program.command("config").description("Inspect and initialize acpx configuration");
	configCommand.command("show").description("Show resolved config").option("--format <fmt>", "Output format: text, json, quiet", parseOutputFormat$1).action(async function() {
		await handleConfigShow(this, config);
	});
	configCommand.command("init").description("Create global config template").option("--format <fmt>", "Output format: text, json, quiet", parseOutputFormat$1).action(async function() {
		await handleConfigInit(this, config);
	});
	configCommand.action(async function() {
		await handleConfigShow(this, config);
	});
}
//#endregion
//#region src/cli/output/render.ts
var render_exports = /* @__PURE__ */ __exportAll({
	agentSessionIdPayload: () => agentSessionIdPayload,
	classifySessionConnectionStatus: () => classifySessionConnectionStatus,
	formatPromptSessionBannerLine: () => formatPromptSessionBannerLine,
	printAgentSessionsByFormat: () => printAgentSessionsByFormat,
	printClosedSessionByFormat: () => printClosedSessionByFormat,
	printCreatedSessionBanner: () => printCreatedSessionBanner,
	printEnsuredSessionByFormat: () => printEnsuredSessionByFormat,
	printNewSessionByFormat: () => printNewSessionByFormat,
	printPromptSessionBanner: () => printPromptSessionBanner,
	printPruneResultByFormat: () => printPruneResultByFormat,
	printQueuedPromptByFormat: () => printQueuedPromptByFormat,
	printSessionsByFormat: () => printSessionsByFormat
});
function formatSessionLabel(record) {
	return record.name ?? "cwd";
}
function formatRoutedFrom(sessionCwd, currentCwd) {
	const relative = path.relative(sessionCwd, currentCwd);
	if (!relative || relative === ".") return;
	return relative.startsWith(".") ? relative : `.${path.sep}${relative}`;
}
function classifySessionConnectionStatus(health) {
	if (health.healthy) return "connected";
	return health.hasLease ? "needs reconnect" : "starting";
}
async function resolveSessionConnectionStatus(record) {
	return classifySessionConnectionStatus(await probeQueueOwnerHealth(record.acpxRecordId));
}
function printSessionsByFormat(sessions, format) {
	if (format === "json") {
		process.stdout.write(`${JSON.stringify(sessions)}\n`);
		return;
	}
	if (format === "quiet") {
		printQuietSessions(sessions);
		return;
	}
	if (sessions.length === 0) {
		process.stdout.write("No sessions\n");
		return;
	}
	for (const session of sessions) {
		const closedMarker = session.closed ? " [closed]" : "";
		process.stdout.write(`${session.acpxRecordId}${closedMarker}\t${session.name ?? "-"}\t${session.cwd}\t${session.lastUsedAt}\n`);
	}
}
function printQuietSessions(sessions) {
	for (const session of sessions) {
		const closedMarker = session.closed ? " [closed]" : "";
		process.stdout.write(`${session.acpxRecordId}${closedMarker}\n`);
	}
}
function printAgentSessionsByFormat(result, format) {
	if (format === "json") {
		process.stdout.write(`${JSON.stringify(result)}\n`);
		return;
	}
	if (format === "quiet") {
		printQuietAgentSessions(result);
		return;
	}
	printTextAgentSessions(result);
}
function printQuietAgentSessions(result) {
	for (const session of result.sessions) process.stdout.write(`${session.sessionId}\n`);
}
function printTextAgentSessions(result) {
	if (result.sessions.length === 0) process.stdout.write("No sessions\n");
	else for (const session of result.sessions) {
		const title = session.title ?? "-";
		const updatedAt = session.updatedAt ?? "-";
		const meta = session._meta ? JSON.stringify(session._meta) : "-";
		process.stdout.write(`${session.sessionId}\t${title}\t${session.cwd}\t${updatedAt}\t${meta}\n`);
	}
	if (result.nextCursor) process.stdout.write(`Next cursor: ${result.nextCursor}\n`);
}
function printClosedSessionByFormat(record, format) {
	if (emitJsonResult(format, {
		action: "session_closed",
		acpxRecordId: record.acpxRecordId,
		acpxSessionId: record.acpSessionId,
		agentSessionId: record.agentSessionId
	})) return;
	if (format === "quiet") return;
	process.stdout.write(`${record.acpxRecordId}\n`);
}
function printNewSessionByFormat(record, replaced, format) {
	if (emitJsonResult(format, {
		action: "session_ensured",
		created: true,
		acpxRecordId: record.acpxRecordId,
		acpxSessionId: record.acpSessionId,
		agentSessionId: record.agentSessionId,
		name: record.name,
		replacedSessionId: replaced?.acpxRecordId
	})) return;
	if (format === "quiet") {
		process.stdout.write(`${record.acpxRecordId}\n`);
		return;
	}
	if (replaced) {
		process.stdout.write(`${record.acpxRecordId}\t(replaced ${replaced.acpxRecordId})\n`);
		return;
	}
	process.stdout.write(`${record.acpxRecordId}\n`);
}
function printEnsuredSessionByFormat(record, created, format) {
	if (emitJsonResult(format, {
		action: "session_ensured",
		created,
		acpxRecordId: record.acpxRecordId,
		acpxSessionId: record.acpSessionId,
		agentSessionId: record.agentSessionId,
		name: record.name
	})) return;
	if (format === "quiet") {
		process.stdout.write(`${record.acpxRecordId}\n`);
		return;
	}
	const action = created ? "created" : "existing";
	process.stdout.write(`${record.acpxRecordId}\t(${action})\n`);
}
function printQueuedPromptByFormat(result, format) {
	if (emitJsonResult(format, {
		action: "prompt_queued",
		acpxRecordId: result.sessionId,
		requestId: result.requestId
	})) return;
	if (format === "quiet") return;
	process.stdout.write(`[queued] ${result.requestId}\n`);
}
function formatPromptSessionBannerLine(record, currentCwd, connectionStatus = "starting") {
	const label = formatSessionLabel(record);
	const normalizedSessionCwd = path.resolve(record.cwd);
	const normalizedCurrentCwd = path.resolve(currentCwd);
	const routedFrom = normalizedSessionCwd === normalizedCurrentCwd ? void 0 : formatRoutedFrom(normalizedSessionCwd, normalizedCurrentCwd);
	const status = connectionStatus;
	if (routedFrom) return `[acpx] session ${label} (${record.acpxRecordId}) · ${normalizedSessionCwd} (routed from ${routedFrom}) · agent ${status}`;
	return `[acpx] session ${label} (${record.acpxRecordId}) · ${normalizedSessionCwd} · agent ${status}`;
}
async function printPromptSessionBanner(record, currentCwd, format, jsonStrict = false) {
	if (format === "quiet" || jsonStrict && format === "json") return;
	const status = await resolveSessionConnectionStatus(record);
	process.stderr.write(`${formatPromptSessionBannerLine(record, currentCwd, status)}\n`);
}
function printCreatedSessionBanner(record, agentName, format, jsonStrict = false) {
	if (format === "quiet" || jsonStrict && format === "json") return;
	const label = formatSessionLabel(record);
	process.stderr.write(`[acpx] created session ${label} (${record.acpxRecordId})\n`);
	process.stderr.write(`[acpx] agent: ${agentName}\n`);
	process.stderr.write(`[acpx] cwd: ${record.cwd}\n`);
}
function formatBytes(bytes) {
	if (bytes >= 1073741824) return `${(bytes / 1073741824).toFixed(1)} GB`;
	if (bytes >= 1048576) return `${(bytes / 1048576).toFixed(1)} MB`;
	if (bytes >= 1024) return `${(bytes / 1024).toFixed(1)} KB`;
	return `${bytes} B`;
}
function printPruneResultByFormat(result, format) {
	const count = result.pruned.length;
	if (emitPruneJsonResult(result, format, count)) return;
	if (format === "quiet") {
		printQuietPruneResult(result.pruned);
		return;
	}
	if (count === 0) {
		process.stdout.write(result.dryRun ? "[DRY RUN] No sessions to prune\n" : "No sessions pruned\n");
		return;
	}
	process.stdout.write(`${formatPruneSummaryLine(result, count)}\n`);
	for (const record of result.pruned) {
		const label = record.name ? ` (${record.name})` : "";
		process.stdout.write(`  ${record.acpxRecordId}${label}\t${record.closedAt ?? record.lastUsedAt}\n`);
	}
}
function emitPruneJsonResult(result, format, count) {
	return emitJsonResult(format, {
		action: result.dryRun ? "sessions_prune_dry_run" : "sessions_pruned",
		dryRun: result.dryRun,
		count,
		bytesFreed: result.bytesFreed,
		pruned: result.pruned.map((r) => r.acpxRecordId)
	});
}
function printQuietPruneResult(pruned) {
	for (const record of pruned) process.stdout.write(`${record.acpxRecordId}\n`);
}
function formatPruneSummaryLine(result, count) {
	const prefix = result.dryRun ? "[DRY RUN] Would prune" : "Pruned";
	const bytesSuffix = !result.dryRun && result.bytesFreed > 0 ? `, freed ${formatBytes(result.bytesFreed)}` : "";
	return `${prefix} ${count} session${count === 1 ? "" : "s"}${bytesSuffix}`;
}
function agentSessionIdPayload(agentSessionId) {
	const normalized = normalizeAgentSessionId(agentSessionId);
	if (!normalized) return {};
	return { agentSessionId: normalized };
}
//#endregion
//#region src/cli/status-command.ts
function formatUptime(startedAt) {
	if (!startedAt) return;
	const startedMs = Date.parse(startedAt);
	if (!Number.isFinite(startedMs)) return;
	const elapsedMs = Math.max(0, Date.now() - startedMs);
	const seconds = Math.floor(elapsedMs / 1e3);
	const hours = Math.floor(seconds / 3600);
	const minutes = Math.floor(seconds % 3600 / 60);
	const remSeconds = seconds % 60;
	return `${hours.toString().padStart(2, "0")}:${minutes.toString().padStart(2, "0")}:${remSeconds.toString().padStart(2, "0")}`;
}
function resolveStatusState(record, health) {
	if (health.healthy) return "running";
	if (health.hasLease) return "dead";
	if (record.lastAgentExitSignal || (record.lastAgentExitCode ?? 0) !== 0) return "dead";
	return "idle";
}
function statusSummary(state) {
	switch (state) {
		case "running": return "queue owner healthy";
		case "idle": return "session idle; queue owner will start on next prompt";
		case "dead": return "queue owner unavailable";
	}
	return "queue owner unavailable";
}
async function handleStatus(explicitAgentName, flags, command, config) {
	const globalFlags = resolveGlobalFlags(command, config);
	const agent = resolveAgentInvocation(explicitAgentName, globalFlags, config);
	const record = await findSession({
		agentCommand: agent.agentCommand,
		cwd: agent.cwd,
		name: resolveSessionNameFromFlags(flags, command)
	});
	if (!record) {
		printMissingStatus(globalFlags.format, agent.agentCommand);
		return;
	}
	await printSessionStatus(record, globalFlags.format);
}
function printMissingStatus(format, agentCommand) {
	if (emitJsonResult(format, {
		action: "status_snapshot",
		status: "no-session",
		summary: "no active session"
	})) return;
	if (format === "quiet") {
		process.stdout.write("no-session\n");
		return;
	}
	process.stdout.write("session: -\n");
	process.stdout.write(`agent: ${agentCommand}\n`);
	process.stdout.write("pid: -\n");
	process.stdout.write("status: no-session\n");
	process.stdout.write("model: -\n");
	process.stdout.write("mode: -\n");
	process.stdout.write("uptime: -\n");
	process.stdout.write("lastPromptTime: -\n");
}
async function printSessionStatus(record, format) {
	const health = await probeQueueOwnerHealth(record.acpxRecordId);
	const statusState = resolveStatusState(record, health);
	const payload = createStatusPayload(record, health, statusState);
	const running = isRunningStatus(statusState);
	const dead = isDeadStatus(statusState);
	if (emitStatusJson(format, record, payload, statusState, running, dead)) return;
	if (format === "quiet") {
		process.stdout.write(`${payload.status}\n`);
		return;
	}
	printTextStatus(payload, dead);
}
function createStatusPayload(record, health, statusState) {
	const running = isRunningStatus(statusState);
	const acpx = statusAcpxFields(record);
	return {
		sessionId: record.acpxRecordId,
		agentCommand: record.agentCommand,
		pid: statusPid(health),
		status: statusState,
		model: acpx.model,
		mode: acpx.mode,
		availableModels: acpx.availableModels,
		uptime: running ? optionalStatusString(formatUptime(record.agentStartedAt)) : null,
		lastPromptTime: optionalStatusString(record.lastPromptAt),
		exitCode: running ? null : optionalStatusNumber(record.lastAgentExitCode),
		signal: running ? null : optionalStatusSignal(record.lastAgentExitSignal),
		...agentSessionIdPayload(record.agentSessionId)
	};
}
function statusAcpxFields(record) {
	return {
		model: record.acpx?.current_model_id ?? null,
		mode: record.acpx?.current_mode_id ?? null,
		availableModels: record.acpx?.available_models ?? null
	};
}
function statusPid(health) {
	if (health.pidAlive) return health.pid ?? null;
	return null;
}
function optionalStatusString(value) {
	return value ?? null;
}
function optionalStatusNumber(value) {
	return value ?? null;
}
function optionalStatusSignal(value) {
	return value ?? null;
}
function isRunningStatus(status) {
	return status === "running";
}
function isDeadStatus(status) {
	return status === "dead";
}
function emitStatusJson(format, record, payload, statusState, running, dead) {
	return emitJsonResult(format, statusJsonPayload(record, payload, statusState, running, dead));
}
function statusJsonPayload(record, payload, statusState, running, dead) {
	const result = {
		action: "status_snapshot",
		status: running ? "alive" : statusState,
		summary: statusSummary(statusState),
		acpxRecordId: record.acpxRecordId,
		acpxSessionId: record.acpSessionId,
		agentSessionId: record.agentSessionId
	};
	assignDefinedJsonField(result, "pid", payload.pid);
	assignDefinedJsonField(result, "model", payload.model);
	assignDefinedJsonField(result, "mode", payload.mode);
	assignDefinedJsonField(result, "availableModels", payload.availableModels);
	assignDefinedJsonField(result, "uptime", payload.uptime);
	assignDefinedJsonField(result, "lastPromptTime", payload.lastPromptTime);
	if (dead) {
		assignDefinedJsonField(result, "exitCode", payload.exitCode);
		assignDefinedJsonField(result, "signal", payload.signal);
	}
	return result;
}
function assignDefinedJsonField(target, key, value) {
	if (value !== null && value !== void 0) target[key] = value;
}
function printTextStatus(payload, dead) {
	process.stdout.write(`session: ${payload.sessionId}\n`);
	if ("agentSessionId" in payload) process.stdout.write(`agentSessionId: ${payload.agentSessionId}\n`);
	process.stdout.write(`agent: ${payload.agentCommand}\n`);
	process.stdout.write(`pid: ${payload.pid ?? "-"}\n`);
	process.stdout.write(`status: ${payload.status}\n`);
	process.stdout.write(`model: ${payload.model ?? "-"}\n`);
	process.stdout.write(`mode: ${payload.mode ?? "-"}\n`);
	process.stdout.write(`uptime: ${payload.uptime ?? "-"}\n`);
	process.stdout.write(`lastPromptTime: ${payload.lastPromptTime ?? "-"}\n`);
	if (dead) printDeadStatusDetails(payload);
}
function printDeadStatusDetails(payload) {
	process.stdout.write(`exitCode: ${payload.exitCode ?? "-"}\n`);
	process.stdout.write(`signal: ${payload.signal ?? "-"}\n`);
}
function registerStatusCommand(parent, explicitAgentName, config, description) {
	const statusCommand = parent.command("status").description(description);
	addSessionNameOption(statusCommand);
	statusCommand.action(async function(flags) {
		await handleStatus(explicitAgentName, flags, this, config);
	});
}
//#endregion
//#region src/cli/command-registration.ts
var LocalAttributeOption = class extends Option {
	localAttributeName;
	constructor(flags, description, localAttributeName) {
		super(flags, description);
		this.localAttributeName = localAttributeName;
	}
	attributeName() {
		return this.localAttributeName;
	}
};
function addSessionsListOptions(command) {
	return command.option("--local", "List local acpx session records instead of agent protocol sessions").option("--cursor <cursor>", "Opaque ACP session/list cursor", (value) => parseNonEmptyValue("Cursor", value)).option("--filter-cwd <dir>", "Filter agent sessions by working directory", (value) => parseNonEmptyValue("Filter cwd", value));
}
function registerSessionsCommand(parent, explicitAgentName, config) {
	const sessionsCommand = parent.command("sessions").description("List, ensure, create, or close sessions for this agent");
	addSessionsListOptions(sessionsCommand);
	sessionsCommand.action(async function(flags) {
		await handleSessionsList(explicitAgentName, flags, this, config);
	});
	addSessionsListOptions(sessionsCommand.command("list")).description("List sessions").action(async function(flags) {
		await handleSessionsList(explicitAgentName, flags, this, config);
	});
	sessionsCommand.command("new").description("Create a fresh session for current cwd").option("-s, --name <name>", "Session name", parseSessionName).option("--resume-session <id>", "Resume existing ACP session id", (value) => parseNonEmptyValue("Resume session id", value)).action(async function(flags) {
		await handleSessionsNew(explicitAgentName, flags, this, config);
	});
	sessionsCommand.command("ensure").description("Ensure a session exists for current cwd or ancestor").option("-s, --name <name>", "Session name", parseSessionName).option("--resume-session <id>", "Resume existing ACP session id", (value) => parseNonEmptyValue("Resume session id", value)).action(async function(flags) {
		await handleSessionsEnsure(explicitAgentName, flags, this, config);
	});
	sessionsCommand.command("close").description("Close session for current cwd").argument("[name]", "Session name", parseSessionName).action(async function(name) {
		await handleSessionsClose(explicitAgentName, name, this, config);
	});
	sessionsCommand.command("show").description("Show session metadata for current cwd").argument("[name]", "Session name", parseSessionName).action(async function(name) {
		await handleSessionsShow(explicitAgentName, name, this, config);
	});
	sessionsCommand.command("history").description("Show recent session history entries").argument("[name]", "Session name", parseSessionName).option("--limit <count>", `Maximum number of entries to show (default: 20)`, parseHistoryLimit, 20).action(async function(name, flags) {
		await handleSessionsHistory(explicitAgentName, name, flags, this, config);
	});
	sessionsCommand.command("read").description("Read full session history").argument("[name]", "Session name", parseSessionName).option("--tail <count>", "Show only the last N entries instead of all history", parseHistoryLimit).action(async function(name, flags) {
		await handleSessionsHistory(explicitAgentName, name, { limit: flags.tail ?? 0 }, this, config);
	});
	sessionsCommand.command("watch").description("Replay and follow session events without affecting the active turn").option("-s, --name <name>", "Session name", parseSessionName).option("--cursor <cursor>", "Resume after a session watch cursor", (value) => parseNonEmptyValue("Cursor", value)).action(async function(flags) {
		await handleSessionsWatch(explicitAgentName, flags, this, config);
	});
	sessionsCommand.command("export").description("Export a portable session archive").argument("[name]", "Session name", parseSessionName).requiredOption("--output <path>", "Output archive path", (value) => parseNonEmptyValue("Output path", value)).addOption(new LocalAttributeOption("--cwd <cwd>", "Session cwd to export", "sourceCwd").argParser((value) => parseNonEmptyValue("Session cwd", value))).action(async function(name, flags) {
		await handleSessionsExport(explicitAgentName, name, flags, this, config);
	});
	sessionsCommand.command("import").description("Import a portable session archive").argument("<archive-path>", "Archive path", (value) => parseNonEmptyValue("Archive path", value)).option("--name <name>", "Imported session name", parseSessionName).addOption(new LocalAttributeOption("--cwd <cwd>", "Imported session cwd", "destinationCwd").argParser((value) => parseNonEmptyValue("Imported session cwd", value))).action(async function(archivePath, flags) {
		await handleSessionsImport(explicitAgentName, archivePath, flags, this, config);
	});
	sessionsCommand.command("prune").description("Delete closed sessions and free disk space").option("--dry-run", "Preview what would be pruned without deleting anything").option("--before <date>", "Prune sessions closed before this date", parsePruneBeforeDate).option("--older-than <days>", "Prune sessions closed more than N days ago", parseDaysOlderThan).option("--include-history", "Also delete event stream files (.stream.ndjson)").action(async function(flags) {
		await handleSessionsPrune(explicitAgentName, flags, this, config);
	});
}
function registerSharedAgentSubcommands(parent, explicitAgentName, config, descriptions) {
	const promptCommand = parent.command("prompt").description(descriptions.prompt).argument("[prompt...]", "Prompt text").showHelpAfterError();
	addSessionOption(promptCommand);
	addPromptInputOption(promptCommand);
	promptCommand.action(async function(promptParts, flags) {
		await handlePrompt(explicitAgentName, promptParts, flags, this, config);
	});
	const execCommand = parent.command("exec").description(descriptions.exec).argument("[prompt...]", "Prompt text").showHelpAfterError();
	addPromptInputOption(execCommand);
	addExecConfigOption(execCommand);
	execCommand.action(async function(promptParts, flags) {
		await handleExec(explicitAgentName, promptParts, flags, this, config);
	});
	const cancelCommand = parent.command("cancel").description(descriptions.cancel);
	addSessionNameOption(cancelCommand);
	cancelCommand.action(async function(flags) {
		await handleCancel(explicitAgentName, flags, this, config);
	});
	const setModeCommand = parent.command("set-mode").description(descriptions.setMode).argument("<mode>", "Mode id", (value) => parseNonEmptyValue("Mode", value));
	addSessionNameOption(setModeCommand);
	setModeCommand.action(async function(modeId, flags) {
		await handleSetMode(explicitAgentName, modeId, flags, this, config);
	});
	const setConfigCommand = parent.command("set").description(descriptions.setConfig).argument("<key>", "Config option id", (value) => parseNonEmptyValue("Config option key", value)).argument("<value>", "Config option value", (value) => parseNonEmptyValue("Config option value", value));
	addSessionNameOption(setConfigCommand);
	setConfigCommand.action(async function(key, value, flags) {
		await handleSetConfigOption(explicitAgentName, key, value, flags, this, config);
	});
	registerStatusCommand(parent, explicitAgentName, config, descriptions.status);
}
function registerAgentCommand(program, agentName, config) {
	const agentCommand = program.command(agentName).description(`Use ${agentName} agent`).argument("[prompt...]", "Prompt text").enablePositionalOptions().passThroughOptions().showHelpAfterError();
	addSessionOption(agentCommand);
	addPromptInputOption(agentCommand);
	agentCommand.action(async function(promptParts, flags) {
		await handlePrompt(agentName, promptParts, flags, this, config);
	});
	registerSharedAgentSubcommands(agentCommand, agentName, config, {
		prompt: "Prompt using persistent session",
		exec: "One-shot prompt without saved session",
		cancel: "Cooperatively cancel current in-flight prompt",
		setMode: "Set session mode",
		setConfig: "Set session config option",
		status: "Show local status of current session agent process"
	});
	registerSessionsCommand(agentCommand, agentName, config);
}
function registerFlowCommand(program, config) {
	program.command("flow").description("Run multi-step ACP workflows from flow files").command("run").description("Run a flow file").argument("<file>", "Flow module path").option("--input-json <json>", "Flow input as JSON").option("--input-file <path>", "Read flow input JSON from file").option("--default-agent <name>", "Default agent profile for ACP nodes without profile", (value) => parseNonEmptyValue("Default agent", value)).action(async function(file, flags) {
		const { handleFlowRun } = await import("./cli-DYp8Dmwm.js");
		await handleFlowRun(file, flags, this, config);
	});
}
function registerDefaultCommands(program, config) {
	registerSharedAgentSubcommands(program, void 0, config, {
		prompt: `Prompt using ${config.defaultAgent} by default`,
		exec: `One-shot prompt using ${config.defaultAgent} by default`,
		cancel: `Cancel active prompt for ${config.defaultAgent} by default`,
		setMode: `Set session mode for ${config.defaultAgent} by default`,
		setConfig: `Set session config option for ${config.defaultAgent} by default`,
		status: `Show local status for ${config.defaultAgent} by default`
	});
	registerSessionsCommand(program, void 0, config);
	registerConfigCommand(program, config);
	registerCompareCommand(program, config);
	registerFlowCommand(program, config);
}
//#endregion
//#region src/session/queue/owner-input.ts
function asRecord(value) {
	if (!value || typeof value !== "object" || Array.isArray(value)) return;
	return value;
}
function parseQueueOwnerPayload(raw) {
	let parsed;
	try {
		parsed = JSON.parse(raw);
	} catch {
		throw new Error("queue owner payload must be valid JSON");
	}
	const record = asRecord(parsed);
	if (!record) throw new Error("queue owner payload must be an object");
	if (typeof record.sessionId !== "string" || record.sessionId.trim().length === 0) throw new Error("queue owner payload missing sessionId");
	if (record.permissionMode !== "approve-all" && record.permissionMode !== "approve-reads" && record.permissionMode !== "deny-all") throw new Error("queue owner payload has invalid permissionMode");
	const options = {
		sessionId: record.sessionId,
		permissionMode: record.permissionMode
	};
	assignQueueOwnerTransportOptions(options, record);
	assignQueueOwnerScalarOptions(options, record);
	assignQueueOwnerSessionOptions(options, record.sessionOptions);
	return options;
}
function assignQueueOwnerTransportOptions(options, record) {
	const parsedMcpServers = parseOptionalMcpServers(record.mcpServers, "queue owner payload");
	if (parsedMcpServers) options.mcpServers = parsedMcpServers;
	if (typeof record.mcpConfigPath === "string" && record.mcpConfigPath.length > 0) options.mcpConfigPath = record.mcpConfigPath;
	if (typeof record.mcpConfigFingerprint === "string" && record.mcpConfigFingerprint.length > 0) options.mcpConfigFingerprint = record.mcpConfigFingerprint;
	if (record.authCredentials && typeof record.authCredentials === "object") {
		const entries = Object.entries(record.authCredentials).filter(([, value]) => typeof value === "string");
		options.authCredentials = Object.fromEntries(entries);
	}
}
function assignQueueOwnerScalarOptions(options, record) {
	if (record.nonInteractivePermissions === "deny" || record.nonInteractivePermissions === "fail") options.nonInteractivePermissions = record.nonInteractivePermissions;
	if (record.authPolicy === "skip" || record.authPolicy === "fail") options.authPolicy = record.authPolicy;
	assignBooleanOption(options, "fs", record.fs);
	assignBooleanOption(options, "terminal", record.terminal);
	assignBooleanOption(options, "suppressSdkConsoleErrors", record.suppressSdkConsoleErrors);
	assignBooleanOption(options, "verbose", record.verbose);
	assignFiniteNumberOption(options, "ttlMs", record.ttlMs);
	assignRoundedNumberOption(options, "maxQueueDepth", record.maxQueueDepth, 1);
	assignRoundedNumberOption(options, "promptRetries", record.promptRetries, 0);
}
function assignBooleanOption(options, key, value) {
	if (typeof value === "boolean") options[key] = value;
}
function assignFiniteNumberOption(options, key, value) {
	if (typeof value === "number" && Number.isFinite(value)) options[key] = value;
}
function assignRoundedNumberOption(options, key, value, min) {
	if (typeof value === "number" && Number.isFinite(value)) options[key] = Math.max(min, Math.round(value));
}
function assignQueueOwnerSessionOptions(options, rawSessionOptions) {
	const sessionOpts = asRecord(rawSessionOptions);
	if (!sessionOpts) return;
	options.sessionOptions = {};
	assignSessionModel(options.sessionOptions, sessionOpts.model);
	assignSessionAllowedTools(options.sessionOptions, sessionOpts.allowedTools);
	assignSessionTools(options.sessionOptions, sessionOpts.tools);
	assignSessionMaxTurns(options.sessionOptions, sessionOpts.maxTurns);
	assignSessionSystemPrompt(options.sessionOptions, sessionOpts.systemPrompt);
	assignSessionEnv(options.sessionOptions, sessionOpts.env);
}
function assignSessionModel(options, value) {
	if (typeof value === "string" && value.trim().length > 0) options.model = value;
}
function assignSessionAllowedTools(options, value) {
	if (Array.isArray(value)) options.allowedTools = value.filter((tool) => typeof tool === "string");
}
function assignSessionTools(options, value) {
	if (Array.isArray(value) && value.every((tool) => typeof tool === "string")) options.tools = [...value];
}
function assignSessionMaxTurns(options, value) {
	if (typeof value === "number" && Number.isFinite(value)) options.maxTurns = Math.max(1, Math.round(value));
}
function assignSessionSystemPrompt(options, value) {
	if (typeof value === "string") {
		options.systemPrompt = value;
		return;
	}
	const systemPrompt = asRecord(value);
	if (typeof systemPrompt?.append === "string") options.systemPrompt = { append: systemPrompt.append };
}
function assignSessionEnv(options, value) {
	const env = asRecord(value);
	if (!env) return;
	const entries = Object.entries(env).filter((entry) => typeof entry[1] === "string");
	if (entries.length > 0) options.env = Object.fromEntries(entries);
}
async function runQueueOwnerFromStdin() {
	const payload = await text(process.stdin);
	if (!payload) throw new Error("queue owner startup payload is empty");
	await runSessionQueueOwner(parseQueueOwnerPayload(payload));
}
//#endregion
//#region src/cli-core.ts
const TOP_LEVEL_VERBS = /* @__PURE__ */ new Set([
	"prompt",
	"exec",
	"cancel",
	"compare",
	"flow",
	"set-mode",
	"set",
	"sessions",
	"status",
	"config",
	"help"
]);
const TOP_LEVEL_VERSION_VALUE_FLAG_VALUES = [
	"--agent",
	"--cwd",
	"--auth-policy",
	"--non-interactive-permissions",
	"--permission-policy",
	"--policy",
	"--format",
	"--model",
	"--allowed-tools",
	"--max-turns",
	"--system-prompt",
	"--append-system-prompt",
	"--prompt-retries",
	"--timeout",
	"--ttl",
	"--mcp-config"
];
const TOP_LEVEL_VERSION_VALUE_FLAGS = new Set(TOP_LEVEL_VERSION_VALUE_FLAG_VALUES);
const TOP_LEVEL_VERSION_BOOLEAN_FLAGS = /* @__PURE__ */ new Set([
	"--approve-all",
	"--approve-reads",
	"--deny-all",
	"--suppress-reads",
	"--json-strict",
	"--no-fs",
	"--no-terminal",
	"--verbose"
]);
const AGENT_SCAN_VALUE_FLAG_VALUES = [...TOP_LEVEL_VERSION_VALUE_FLAG_VALUES, "--file"];
const AGENT_SCAN_VALUE_FLAGS = new Set(AGENT_SCAN_VALUE_FLAG_VALUES);
const AGENT_SCAN_BOOLEAN_FLAGS = new Set(TOP_LEVEL_VERSION_BOOLEAN_FLAGS);
let skillflagModulePromise;
function loadSkillflagModule() {
	skillflagModulePromise ??= import("skillflag");
	return skillflagModulePromise;
}
function shouldMaybeHandleSkillflag(argv) {
	return argv.some((token) => token === "--skill" || token.startsWith("--skill="));
}
function matchesLongFlagValue(token, flags) {
	for (const flag of flags) if (token.startsWith(`${flag}=`)) return true;
	return false;
}
function classifyAgentTokenFlag(token) {
	if (token === "--agent" || token.startsWith("--agent=")) return "agent-value";
	if (AGENT_SCAN_VALUE_FLAGS.has(token)) return "skip-next";
	if (AGENT_SCAN_BOOLEAN_FLAGS.has(token) || matchesLongFlagValue(token, AGENT_SCAN_VALUE_FLAGS) || token.startsWith("--json-strict=")) return "skip";
	return "unknown";
}
function scanAgentTokenStep(token, hasAgentOverride) {
	if (token === "--") return {
		result: { hasAgentOverride },
		indexDelta: 0
	};
	if (!token.startsWith("-") || token === "-") return {
		result: {
			token,
			hasAgentOverride
		},
		indexDelta: 0
	};
	const flagScan = classifyAgentTokenFlag(token);
	if (flagScan === "agent-value") return {
		indexDelta: token === "--agent" ? 1 : 0,
		hasAgentOverride: true
	};
	if (flagScan === "skip-next") return { indexDelta: 1 };
	if (flagScan === "skip") return { indexDelta: 0 };
	return {
		result: { hasAgentOverride },
		indexDelta: 0
	};
}
function detectAgentToken(argv) {
	let hasAgentOverride = false;
	for (let index = 0; index < argv.length; index += 1) {
		const token = argv[index];
		const step = scanAgentTokenStep(token, hasAgentOverride);
		if (step.result) return {
			...step.result,
			index
		};
		if (step.hasAgentOverride) hasAgentOverride = true;
		index += step.indexDelta;
	}
	return { hasAgentOverride };
}
function lastTopLevelFlagValue(argv, flag) {
	let value;
	for (let index = 0; index < argv.length; index += 1) {
		const token = argv[index];
		const scan = classifyTopLevelFlagScan(token);
		if (scan.stop) break;
		if (token === flag) value = argv[index + 1];
		else if (token.startsWith(`${flag}=`)) value = token.slice(flag.length + 1);
		if (scan.skipNext) index += 1;
	}
	return value;
}
function detectInitialCwd(argv) {
	const command = detectAgentToken(argv);
	if (command.token === "compare" && command.index !== void 0) {
		const { cwd } = scanCompareArgs(argv.slice(command.index + 1));
		if (cwd !== void 0) return path.resolve(cwd);
	}
	return path.resolve(lastTopLevelFlagValue(argv, "--cwd") ?? process.cwd());
}
function detectMcpConfigPath(argv, cwd) {
	const value = lastTopLevelFlagValue(argv, "--mcp-config");
	return value ? path.resolve(cwd, value) : void 0;
}
function detectRequestedOutputFormat(argv, fallback) {
	let detectedFormat;
	for (let index = 0; index < argv.length; index += 1) {
		const token = argv[index];
		const scan = classifyTopLevelFlagScan(token);
		if (scan.stop) break;
		if (isJsonStrictToken(token)) return "json";
		const format = readFormatFlagValue(token, argv[index + 1]);
		if (format) detectedFormat = format;
		if (scan.skipNext) index += 1;
	}
	return detectCompareOutputFormat(argv, detectedFormat, fallback);
}
function detectCompareOutputFormat(argv, rootFormat, fallback) {
	const command = detectAgentToken(argv);
	if (command.token !== "compare" || command.index === void 0) return rootFormat ?? fallback;
	const local = scanCompareArgs(argv.slice(command.index + 1));
	const localFormat = isOutputFormat(local.format) ? local.format : void 0;
	return local.json ? "json" : rootFormat ?? localFormat ?? fallback;
}
function classifyTopLevelFlagScan(token) {
	if (token === "--" || !token.startsWith("-") || token === "-") return {
		stop: true,
		skipNext: false
	};
	return {
		stop: false,
		skipNext: TOP_LEVEL_VERSION_VALUE_FLAGS.has(token)
	};
}
function readFormatFlagValue(token, nextToken) {
	const raw = token === "--format" ? nextToken : readInlineFlagValue(token, "--format");
	return isOutputFormat(raw) ? raw : void 0;
}
function readInlineFlagValue(token, flag) {
	if (!token.startsWith(`${flag}=`)) return;
	return token.slice(flag.length + 1).trim();
}
function isOutputFormat(value) {
	return typeof value === "string" && OUTPUT_FORMATS.includes(value);
}
function isJsonStrictToken(token) {
	return token === "--json-strict" || token.startsWith("--json-strict=");
}
function detectJsonStrict(argv) {
	for (let index = 0; index < argv.length; index += 1) {
		const token = argv[index];
		const scan = classifyTopLevelFlagScan(token);
		if (scan.stop) break;
		if (isJsonStrictToken(token)) return true;
		if (scan.skipNext) index += 1;
	}
	return false;
}
function shouldSkipTopLevelVersionToken(token) {
	return matchesLongFlagValue(token, TOP_LEVEL_VERSION_VALUE_FLAG_VALUES) || TOP_LEVEL_VERSION_BOOLEAN_FLAGS.has(token);
}
function topLevelVersionTokenDecision(token) {
	if (token === "--version" || token === "-V") return "version";
	if (!token.startsWith("-") || token === "-") return "stop";
	if (TOP_LEVEL_VERSION_VALUE_FLAGS.has(token)) return "skip-next";
	if (shouldSkipTopLevelVersionToken(token)) return "skip";
	return "stop";
}
function isTopLevelVersionRequest(argv) {
	for (let index = 0; index < argv.length; index += 1) {
		const token = argv[index];
		if (token === "--") return false;
		const decision = topLevelVersionTokenDecision(token);
		if (decision === "version") return true;
		if (decision === "stop") return false;
		if (decision === "skip-next") index += 1;
	}
	return false;
}
async function emitJsonErrorEvent(error) {
	const formatter = createOutputFormatter("json", {
		jsonContext: { sessionId: "unknown" },
		suppressReads: false
	});
	formatter.onError(error);
	formatter.flush();
}
function isOutputAlreadyEmitted(error) {
	if (!error || typeof error !== "object") return false;
	return error.outputAlreadyEmitted === true;
}
async function emitRequestedError(error, normalized, outputPolicy) {
	if (isOutputAlreadyEmitted(error)) return;
	if (outputPolicy.format === "json") {
		await emitJsonErrorEvent(normalized);
		return;
	}
	if (outputPolicy.format === "quiet") {
		const formatter = createOutputFormatter("quiet");
		formatter.onError(normalized);
		formatter.flush();
		return;
	}
	if (!outputPolicy.suppressNonJsonStderr) {
		process.stderr.write(`${normalized.message}\n`);
		if (outputPolicy.format === "text") for (const hint of getTextErrorRemediationHints(normalized)) process.stderr.write(`${hint}\n`);
	}
}
async function handleQueueOwnerCommand(argv) {
	installPerfMetricsCapture({
		argv: argv.slice(2),
		role: argv[2] === "__queue-owner" ? "queue_owner" : "cli"
	});
	if (argv[2] !== "__queue-owner") return false;
	try {
		await runQueueOwnerFromStdin();
		return true;
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error);
		process.stderr.write(`[acpx] queue owner failed: ${message}\n`);
		process.exit(EXIT_CODES.ERROR);
		return true;
	}
}
async function maybeHandleSkillflag(argv) {
	if (!shouldMaybeHandleSkillflag(argv)) return;
	const { findSkillsRoot, maybeHandleSkillflag } = await loadSkillflagModule();
	await maybeHandleSkillflag(argv, {
		skillsRoot: findSkillsRoot(import.meta.url),
		includeBundledSkill: false
	});
}
function createProgram(requestedJsonStrict) {
	const program = new Command();
	program.name("acpx").description("Headless CLI client for the Agent Client Protocol").version(getAcpxVersion()).enablePositionalOptions().showHelpAfterError();
	if (requestedJsonStrict) program.configureOutput({
		writeOut: () => {},
		writeErr: () => {}
	});
	return program;
}
async function handleProgramParseError(error, requestedOutputPolicy) {
	if (error instanceof CommanderError) {
		if (error.code === "commander.helpDisplayed" || error.code === "commander.version" || error.code === "commander.help" && error.exitCode === 0) process.exit(EXIT_CODES.SUCCESS);
		const normalized = normalizeOutputError(error, {
			defaultCode: "USAGE",
			origin: "cli"
		});
		await emitRequestedError(error, normalized, requestedOutputPolicy);
		process.exit(exitCodeForOutputErrorCode(normalized.code));
	}
	if (error instanceof InterruptedError) process.exit(EXIT_CODES.INTERRUPTED);
	const normalized = normalizeOutputError(error, { origin: "cli" });
	await emitRequestedError(error, normalized, requestedOutputPolicy);
	process.exit(exitCodeForOutputErrorCode(normalized.code));
}
async function main(argv = process.argv) {
	const rawArgs = normalizeLifecycleScriptArgs(argv.slice(2));
	const normalizedArgv = [
		argv[0] ?? "node",
		argv[1] ?? "acpx",
		...rawArgs
	];
	if (await handleQueueOwnerCommand(argv)) return;
	if (isTopLevelVersionRequest(rawArgs)) {
		process.stdout.write(`${getAcpxVersion()}\n`);
		return;
	}
	await maybeHandleSkillflag(normalizedArgv);
	const requestedJsonStrict = detectJsonStrict(rawArgs);
	let requestedOutputPolicy = {
		...resolveOutputPolicy(detectRequestedOutputFormat(rawArgs, "text"), requestedJsonStrict),
		suppressReads: rawArgs.some((token) => token === "--suppress-reads")
	};
	try {
		const initialCwd = detectInitialCwd(rawArgs);
		const config = await loadResolvedConfig(initialCwd, { mcpConfigPath: detectMcpConfigPath(rawArgs, initialCwd) });
		requestedOutputPolicy = {
			...resolveOutputPolicy(detectRequestedOutputFormat(rawArgs, config.format), requestedJsonStrict),
			suppressReads: requestedOutputPolicy.suppressReads
		};
		const program = createProgram(requestedJsonStrict);
		addGlobalFlags(program);
		configurePublicCli({
			program,
			argv: rawArgs,
			config,
			requestedJsonStrict,
			topLevelVerbs: TOP_LEVEL_VERBS,
			listBuiltInAgents,
			detectAgentToken,
			registerAgentCommand,
			registerDefaultCommands,
			handlePromptAction: async (command, promptParts) => {
				await handlePrompt(void 0, promptParts, {}, command, config);
			}
		});
		program.exitOverride((error) => {
			throw error;
		});
		await program.parseAsync(normalizedArgv);
	} catch (error) {
		await handleProgramParseError(error, requestedOutputPolicy);
	} finally {
		flushPerfMetricsCapture();
	}
}
function normalizeLifecycleScriptArgs(rawArgs) {
	if (rawArgs[0] === "--" && (process.env.npm_lifecycle_event || process.env.npm_lifecycle_script)) return rawArgs.slice(1);
	return rawArgs;
}
//#endregion
//#region src/cli/broken-pipe.ts
function handleBrokenPipeError(error, mode, exit = (code) => process.exit(code)) {
	if (error.code !== "EPIPE") throw error;
	if (mode === "exit") exit(0);
}
function installBrokenPipeHandler(stream, mode) {
	stream.on("error", (error) => {
		handleBrokenPipeError(error, mode);
	});
}
//#endregion
//#region src/cli.ts
function isCliEntrypoint(argv) {
	const entry = argv[1];
	if (!entry) return false;
	try {
		const resolved = pathToFileURL(realpathSync(entry)).href;
		return import.meta.url === resolved;
	} catch {
		return false;
	}
}
if (isCliEntrypoint(process.argv)) {
	const isQueueOwner = process.argv[2] === "__queue-owner";
	installBrokenPipeHandler(process.stdout, "exit");
	installBrokenPipeHandler(process.stderr, isQueueOwner ? "ignore" : "exit");
	const queueOwnerArgOverride = buildQueueOwnerArgOverride(fileURLToPath(import.meta.url));
	if (queueOwnerArgOverride) process.env.ACPX_QUEUE_OWNER_ARGS ??= queueOwnerArgOverride;
	main(process.argv);
}
//#endregion
export { formatPromptSessionBannerLine, parseAllowedTools, parseMaxTurns, parseTtlSeconds };

//# sourceMappingURL=cli.js.map