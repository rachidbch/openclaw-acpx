import { o as toTimerMilliseconds } from "./output-Bwnb3Txu.js";
import { At as AUTH_POLICIES, Ft as PERMISSION_POLICY_ACTIONS, Mt as NON_INTERACTIVE_PERMISSION_POLICIES, Nt as OUTPUT_FORMATS } from "./ipc-D0dEwzXe.js";
import { d as resolveAgentCommand, l as normalizeAgentName, p as resolveCanonicalAgentName, u as resolveAgentArgv } from "./agent-registry-Gtwn7nMp.js";
import "./queue-owner-runtime-B_R4H7vy.js";
import path from "node:path";
import { InvalidArgumentError } from "commander";
import fs from "node:fs/promises";
//#region src/cli/flags.ts
function hasExplicitPermissionModeFlag(flags) {
	return flags.approveAll === true || flags.approveReads === true || flags.denyAll === true;
}
function stringOption(value) {
	return typeof value === "string" ? value : void 0;
}
function numberOption(value) {
	return typeof value === "number" ? value : void 0;
}
function stringArrayOption(value) {
	return Array.isArray(value) && value.every((entry) => typeof entry === "string") ? value : void 0;
}
function nonEmptyStringOption(value) {
	return typeof value === "string" && value.length > 0 ? value : void 0;
}
function parseOutputFormat(value) {
	if (!OUTPUT_FORMATS.includes(value)) throw new InvalidArgumentError(`Invalid format "${value}". Expected one of: ${OUTPUT_FORMATS.join(", ")}`);
	return value;
}
function parseAuthPolicy(value) {
	if (!AUTH_POLICIES.includes(value)) throw new InvalidArgumentError(`Invalid auth policy "${value}". Expected one of: ${AUTH_POLICIES.join(", ")}`);
	return value;
}
function parseNonInteractivePermissionPolicy(value) {
	if (!NON_INTERACTIVE_PERMISSION_POLICIES.includes(value)) throw new InvalidArgumentError(`Invalid non-interactive permission policy "${value}". Expected one of: ${NON_INTERACTIVE_PERMISSION_POLICIES.join(", ")}`);
	return value;
}
function parseTimeoutSeconds(value) {
	const parsed = Number(value);
	if (!Number.isFinite(parsed) || parsed <= 0) throw new InvalidArgumentError("Timeout must be a positive number of seconds");
	const milliseconds = toTimerMilliseconds(parsed, false);
	if (milliseconds === void 0) throw new InvalidArgumentError("Timeout exceeds the maximum supported timer delay");
	return milliseconds;
}
function parseTtlSeconds(value) {
	const parsed = Number(value);
	if (!Number.isFinite(parsed) || parsed < 0) throw new InvalidArgumentError("TTL must be a non-negative number of seconds");
	const milliseconds = toTimerMilliseconds(parsed, true);
	if (milliseconds === void 0) throw new InvalidArgumentError("TTL exceeds the maximum supported timer delay");
	return milliseconds;
}
function parseSessionName(value) {
	const trimmed = value.trim();
	if (trimmed.length === 0) throw new InvalidArgumentError("Session name must not be empty");
	return trimmed;
}
function parseNonEmptyValue(label, value) {
	const trimmed = value.trim();
	if (trimmed.length === 0) throw new InvalidArgumentError(`${label} must not be empty`);
	return trimmed;
}
function parseSessionConfigOptionAssignment(value) {
	const separator = value.indexOf("=");
	if (separator <= 0 || separator === value.length - 1) throw new InvalidArgumentError("Session config option must use \"<key>=<value>\" with non-empty parts");
	const configId = value.slice(0, separator).trim();
	const configValue = value.slice(separator + 1).trim();
	if (configId.length === 0 || configValue.length === 0) throw new InvalidArgumentError("Session config option must use \"<key>=<value>\" with non-empty parts");
	return {
		configId,
		value: configValue
	};
}
function collectSessionConfigOptionAssignment(value, previous = []) {
	return [...previous, parseSessionConfigOptionAssignment(value)];
}
function parseHistoryLimit(value) {
	const parsed = Number(value);
	if (!Number.isInteger(parsed) || parsed <= 0) throw new InvalidArgumentError("Limit must be a positive integer");
	return parsed;
}
function parseDaysOlderThan(value) {
	const parsed = Number(value);
	if (!Number.isInteger(parsed) || parsed <= 0) throw new InvalidArgumentError("--older-than must be a positive integer number of days");
	return parsed;
}
function parsePruneBeforeDate(value) {
	const date = new Date(value);
	if (Number.isNaN(date.getTime())) throw new InvalidArgumentError(`--before must be a valid date (e.g. 2026-01-01 or 2026-01-01T00:00:00Z)`);
	return date;
}
function parseAllowedTools(value) {
	const trimmed = value.trim();
	if (trimmed.length === 0) return [];
	const items = trimmed.split(",").map((item) => item.trim());
	if (items.some((item) => item.length === 0)) throw new InvalidArgumentError("Allowed tools must be a comma-separated list without empty entries");
	return items;
}
function parseMaxTurns(value) {
	const parsed = Number(value);
	if (!Number.isInteger(parsed) || parsed <= 0) throw new InvalidArgumentError("Max turns must be a positive integer");
	return parsed;
}
function resolveSystemPromptFlag(opts) {
	const replace = nonEmptyStringOption(opts.systemPrompt);
	const append = nonEmptyStringOption(opts.appendSystemPrompt);
	if (replace !== void 0 && append !== void 0) throw new InvalidArgumentError("Use only one of --system-prompt or --append-system-prompt");
	if (replace !== void 0) return replace;
	if (append !== void 0) return { append };
}
function parsePromptRetries(value) {
	const parsed = Number(value);
	if (!Number.isInteger(parsed) || parsed < 0) throw new InvalidArgumentError("Prompt retries must be a non-negative integer");
	return parsed;
}
function resolvePermissionMode(flags, defaultMode) {
	if ([
		flags.approveAll,
		flags.approveReads,
		flags.denyAll
	].filter(Boolean).length > 1) throw new InvalidArgumentError("Use only one permission mode: --approve-all, --approve-reads, or --deny-all");
	if (flags.approveAll) return "approve-all";
	if (flags.approveReads) return "approve-reads";
	if (flags.denyAll) return "deny-all";
	return defaultMode;
}
function addGlobalFlags(command) {
	return command.option("--agent <command>", "Raw ACP agent command (escape hatch)").option("--cwd <dir>", "Working directory", process.cwd()).option("--auth-policy <policy>", "Authentication policy: skip or fail when auth is required", parseAuthPolicy).option("--approve-all", "Auto-approve all permission requests").option("--approve-reads", "Auto-approve read/search requests and prompt for writes").option("--deny-all", "Deny all permission requests").option("--non-interactive-permissions <policy>", "When prompting is unavailable: deny or fail", parseNonInteractivePermissionPolicy).option("--permission-policy <json-or-file>", "Permission policy JSON or path (autoApprove, autoDeny, escalate, defaultAction)").option("--policy <json-or-file>", "Alias for --permission-policy").option("--format <fmt>", "Output format: text, json, quiet", parseOutputFormat).option("--suppress-reads", "Suppress raw read-file contents in output").option("--model <id>", "Agent model id").option("--allowed-tools <list>", "Allowed tool names as a comma-separated list (use \"\" for no tools)", parseAllowedTools).option("--max-turns <count>", "Maximum turns for the session", parseMaxTurns).option("--system-prompt <text>", "Replace the agent system prompt (claude-agent-acp via ACP _meta.systemPrompt)", (value) => parseNonEmptyValue("System prompt", value)).option("--append-system-prompt <text>", "Append text to the agent system prompt (claude-agent-acp via ACP _meta.systemPrompt.append)", (value) => parseNonEmptyValue("Append system prompt", value)).option("--prompt-retries <count>", "Retry failed prompt turns on transient errors (default: 0)", parsePromptRetries).option("--json-strict", "Strict JSON mode: requires --format json and suppresses non-JSON stderr output").option("--no-fs", "Do not advertise ACP filesystem capabilities").option("--no-terminal", "Do not advertise ACP terminal capability").option("--timeout <seconds>", "Maximum time to wait for agent response", parseTimeoutSeconds).option("--ttl <seconds>", "Queue owner idle TTL before shutdown (0 = keep alive forever) (default: 300)", parseTtlSeconds).option("--mcp-config <path>", "Load MCP servers from a JSON config file instead of project/global mcpServers").option("--verbose", "Enable verbose debug logs");
}
function addSessionOption(command) {
	return command.option("-s, --session <name>", "Use named session instead of cwd default", parseSessionName).option("--no-wait", "Queue prompt and return immediately when another prompt is already running");
}
function addSessionNameOption(command) {
	return command.option("-s, --session <name>", "Use named session instead of cwd default", parseSessionName);
}
function resolveSessionNameFromFlags(flags, command) {
	const directSession = parseOptionalSessionName(flags.session);
	if (directSession !== void 0) return directSession;
	return parseOptionalSessionName(command.optsWithGlobals().session);
}
function parseOptionalSessionName(value) {
	const session = stringOption(value);
	return session === void 0 ? void 0 : parseSessionName(session);
}
function resolveCommandOption(command, key, fallback) {
	for (let current = command; current; current = current.parent) if (current.getOptionValueSource(key) === "cli") return current.opts()[key];
	return fallback;
}
function resolvePromptFlags(flags, command) {
	return {
		...flags,
		file: stringOption(resolveCommandOption(command, "file", flags.file)),
		wait: resolveCommandOption(command, "wait", flags.wait) !== false
	};
}
function resolveSessionsListFlags(flags, command) {
	return {
		cursor: stringOption(resolveCommandOption(command, "cursor", flags.cursor)),
		filterCwd: stringOption(resolveCommandOption(command, "filterCwd", flags.filterCwd)),
		local: resolveCommandOption(command, "local", flags.local) === true
	};
}
function addPromptInputOption(command) {
	return command.option("-f, --file <path>", "Read prompt text from file path (use - for stdin)");
}
function addExecConfigOption(command) {
	return command.option("--config-option <key=value>", "Set an ACP session config option before the one-shot prompt (repeatable)", collectSessionConfigOptionAssignment);
}
function resolveGlobalFlags(command, config) {
	const opts = command.optsWithGlobals();
	const format = parseOutputFormat(opts.json === true ? "json" : stringOption(opts.format) ?? config.format ?? "text");
	const jsonStrict = opts.jsonStrict === true;
	const verbose = opts.verbose === true;
	assertOutputFlagCompatibility(format, jsonStrict, verbose);
	return {
		agent: resolveAgentOverride(stringOption(opts.agent)),
		cwd: resolveCwdOption(opts.cwd),
		authPolicy: resolveAuthPolicy(opts.authPolicy, config),
		nonInteractivePermissions: resolveNonInteractivePermissions(opts.nonInteractivePermissions, config),
		permissionPolicy: resolvePermissionPolicyOption(opts),
		jsonStrict,
		suppressReads: opts.suppressReads === true,
		fs: resolveCapabilityOption(opts.fs),
		terminal: resolveCapabilityOption(opts.terminal),
		timeout: resolveTimeoutOption(opts.timeout, config),
		ttl: resolveTtlOption(opts.ttl, config),
		verbose,
		format,
		model: resolveModelOption(opts.model),
		allowedTools: stringArrayOption(opts.allowedTools),
		maxTurns: numberOption(opts.maxTurns),
		systemPrompt: resolveSystemPromptFlag(opts),
		promptRetries: numberOption(opts.promptRetries),
		approveAll: opts.approveAll ? true : void 0,
		approveReads: opts.approveReads ? true : void 0,
		denyAll: opts.denyAll ? true : void 0
	};
}
function resolveCwdOption(value) {
	return stringOption(value) ?? process.cwd();
}
function resolveAuthPolicy(optsValue, config) {
	const value = stringOption(optsValue);
	return value === void 0 ? config.authPolicy : parseAuthPolicy(value);
}
function resolveNonInteractivePermissions(optsValue, config) {
	const value = stringOption(optsValue);
	return value === void 0 ? config.nonInteractivePermissions : parseNonInteractivePermissionPolicy(value);
}
function resolvePermissionPolicyOption(opts) {
	const primary = stringOption(opts.permissionPolicy);
	const alias = stringOption(opts.policy);
	if (primary !== void 0 && alias !== void 0 && primary !== alias) throw new InvalidArgumentError("Use only one permission policy flag: --permission-policy or --policy");
	return primary ?? alias;
}
function resolveCapabilityOption(value) {
	return value === false ? false : void 0;
}
function resolveTimeoutOption(value, config) {
	return numberOption(value) ?? config.timeoutMs;
}
function resolveTtlOption(value, config) {
	return numberOption(value) ?? config.ttlMs ?? 3e5;
}
function assertOutputFlagCompatibility(format, jsonStrict, verbose) {
	if (jsonStrict && format !== "json") throw new InvalidArgumentError("--json-strict requires --format json");
	if (jsonStrict && verbose) throw new InvalidArgumentError("--json-strict cannot be combined with --verbose");
}
function resolveModelOption(value) {
	const model = stringOption(value);
	return model === void 0 ? void 0 : parseNonEmptyValue("Model", model);
}
function resolveAgentOverride(value) {
	return value === void 0 ? void 0 : parseNonEmptyValue("Agent command", value);
}
function resolveOutputPolicy(format, jsonStrict) {
	return {
		format,
		jsonStrict,
		suppressReads: false,
		suppressNonJsonStderr: jsonStrict,
		queueErrorAlreadyEmitted: format !== "quiet",
		suppressSdkConsoleErrors: jsonStrict || format === "quiet"
	};
}
function resolveAgentInvocation(explicitAgentName, globalFlags, config) {
	const override = resolveAgentOverride(globalFlags.agent);
	if (override !== void 0 && explicitAgentName !== void 0) throw new InvalidArgumentError("Do not combine positional agent with --agent override");
	const agentName = explicitAgentName ?? config.defaultAgent ?? "codex";
	return {
		agentName,
		...resolveInvocationCommand(agentName, override, config),
		cwd: path.resolve(globalFlags.cwd)
	};
}
function resolveInvocationCommand(agentName, override, config) {
	if (override !== void 0) return { agentCommand: override };
	const normalizedAgentName = normalizeAgentName(agentName);
	const canonicalAgentName = resolveCanonicalAgentName(agentName);
	const configuredAgent = Object.hasOwn(config.agents, normalizedAgentName) ? config.agents[normalizedAgentName] : Object.hasOwn(config.agents, canonicalAgentName) ? config.agents[canonicalAgentName] : void 0;
	if (configuredAgent) return {
		agentCommand: configuredAgent.command,
		...configuredAgent.argv ? { agentArgv: [...configuredAgent.argv] } : {}
	};
	const agentArgv = resolveAgentArgv(agentName);
	return {
		agentCommand: resolveAgentCommand(agentName),
		...agentArgv ? { agentArgv } : {}
	};
}
//#endregion
//#region src/permission-policy.ts
function asRecord(value) {
	if (!value || typeof value !== "object" || Array.isArray(value)) return;
	return value;
}
function parseRuleList(value, key, source) {
	if (value == null) return;
	if (!Array.isArray(value)) throw new Error(`${source}: permission policy ${key} must be an array of strings`);
	return value.map((entry) => {
		if (typeof entry !== "string" || entry.trim().length === 0) throw new Error(`${source}: permission policy ${key} must contain only non-empty strings`);
		return entry.trim();
	});
}
function isPermissionPolicyAction(value) {
	return typeof value === "string" && PERMISSION_POLICY_ACTIONS.includes(value);
}
function parseDefaultAction(value, source) {
	if (value == null) return;
	if (!isPermissionPolicyAction(value)) throw new Error(`${source}: permission policy defaultAction must be one of ${PERMISSION_POLICY_ACTIONS.join(", ")}`);
	return value;
}
function assignRuleList(policy, key, value) {
	if (value) policy[key] = value;
}
function parsePermissionPolicy(value, source = "permission policy") {
	const record = asRecord(value);
	if (!record) throw new Error(`${source}: permission policy must be a JSON object`);
	const policy = {};
	assignRuleList(policy, "autoApprove", parseRuleList(record.autoApprove, "autoApprove", source));
	assignRuleList(policy, "autoDeny", parseRuleList(record.autoDeny, "autoDeny", source));
	assignRuleList(policy, "escalate", parseRuleList(record.escalate, "escalate", source));
	const defaultAction = parseDefaultAction(record.defaultAction, source);
	if (defaultAction) policy.defaultAction = defaultAction;
	return policy;
}
async function loadPermissionPolicySpec(spec, cwd) {
	const trimmed = spec?.trim();
	if (!trimmed) return;
	if (trimmed.startsWith("{")) return parsePermissionPolicy(JSON.parse(trimmed), "--permission-policy");
	const policyPath = path.resolve(cwd, trimmed);
	const raw = await fs.readFile(policyPath, "utf8");
	return parsePermissionPolicy(JSON.parse(raw), policyPath);
}
//#endregion
//#region src/cli/invocation-options.ts
function sessionOptionsFromGlobalFlags(globalFlags) {
	return {
		model: globalFlags.model,
		allowedTools: globalFlags.allowedTools,
		maxTurns: globalFlags.maxTurns,
		systemPrompt: globalFlags.systemPrompt
	};
}
function sessionConnectionOptions(globalFlags, config) {
	return {
		mcpServers: config.mcpServers,
		nonInteractivePermissions: globalFlags.nonInteractivePermissions,
		authCredentials: config.auth,
		authPolicy: globalFlags.authPolicy,
		fs: globalFlags.fs,
		terminal: globalFlags.terminal,
		timeoutMs: globalFlags.timeout,
		verbose: globalFlags.verbose
	};
}
async function resolvePermissionPolicyFromFlags(globalFlags) {
	try {
		return await loadPermissionPolicySpec(globalFlags.permissionPolicy, globalFlags.cwd);
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error);
		throw new InvalidArgumentError(`Invalid permission policy: ${message}`);
	}
}
//#endregion
export { resolvePermissionMode as C, resolveSessionsListFlags as E, resolveOutputPolicy as S, resolveSessionNameFromFlags as T, parseSessionName as _, addGlobalFlags as a, resolveAgentInvocation as b, addSessionOption as c, parseDaysOlderThan as d, parseHistoryLimit as f, parsePruneBeforeDate as g, parseOutputFormat as h, addExecConfigOption as i, hasExplicitPermissionModeFlag as l, parseNonEmptyValue as m, sessionConnectionOptions as n, addPromptInputOption as o, parseMaxTurns as p, sessionOptionsFromGlobalFlags as r, addSessionNameOption as s, resolvePermissionPolicyFromFlags as t, parseAllowedTools as u, parseTimeoutSeconds as v, resolvePromptFlags as w, resolveGlobalFlags as x, parseTtlSeconds as y };

//# sourceMappingURL=invocation-options-9AeiiKDX.js.map