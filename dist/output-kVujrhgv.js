import { A as findSession, I as pruneSessions, N as listSessions, P as listSessionsForAgent, St as isProcessAlive, ct as parsePermissionNotice, j as findSessionByDirectoryWalk, k as findGitRepositoryRoot, lt as parsePromptStopReason, rt as extractSessionUpdateNotification, st as parseJsonRpcErrorMessage } from "./ipc-BIMIZYBg.js";
import { C as setSessionModeOnOwner, S as setSessionMode, T as setSessionModelOnOwner, _ as cancelSessionPrompt, a as runQueuedTask, at as DEFAULT_QUEUE_OWNER_TTL_MS, b as setSessionConfigOption, dt as InterruptedError, ft as TimeoutError, g as listAgentSessions, h as ensureSession, i as runOnce, m as createSessionWithClient, n as runSessionQueueOwner, o as sendSessionDirect, ot as normalizeQueueOwnerTtlMs, p as createSession, r as sendSession, t as queueOwnerRuntimeTestInternals, v as closeSession, w as setSessionModel, x as setSessionConfigOptionOnOwner, y as sessionControlTestInternals } from "./queue-owner-runtime-ZiKw3KHx.js";
//#region \0rolldown/runtime.js
var __defProp = Object.defineProperty;
var __exportAll = (all, no_symbols) => {
	let target = {};
	for (var name in all) __defProp(target, name, {
		get: all[name],
		enumerable: true
	});
	if (!no_symbols) __defProp(target, Symbol.toStringTag, { value: "Module" });
	return target;
};
//#endregion
//#region src/session/session.ts
var session_exports = /* @__PURE__ */ __exportAll({
	DEFAULT_HISTORY_LIMIT: () => 20,
	DEFAULT_QUEUE_OWNER_TTL_MS: () => DEFAULT_QUEUE_OWNER_TTL_MS,
	InterruptedError: () => InterruptedError,
	TimeoutError: () => TimeoutError,
	cancelSessionPrompt: () => cancelSessionPrompt,
	closeSession: () => closeSession,
	createSession: () => createSession,
	createSessionWithClient: () => createSessionWithClient,
	ensureSession: () => ensureSession,
	findGitRepositoryRoot: () => findGitRepositoryRoot,
	findSession: () => findSession,
	findSessionByDirectoryWalk: () => findSessionByDirectoryWalk,
	isProcessAlive: () => isProcessAlive,
	listAgentSessions: () => listAgentSessions,
	listSessions: () => listSessions,
	listSessionsForAgent: () => listSessionsForAgent,
	normalizeQueueOwnerTtlMs: () => normalizeQueueOwnerTtlMs,
	pruneSessions: () => pruneSessions,
	queueOwnerRuntimeTestInternals: () => queueOwnerRuntimeTestInternals,
	runOnce: () => runOnce,
	runQueuedTask: () => runQueuedTask,
	runSessionQueueOwner: () => runSessionQueueOwner,
	sendSession: () => sendSession,
	sendSessionDirect: () => sendSessionDirect,
	sessionControlTestInternals: () => sessionControlTestInternals,
	setSessionConfigOption: () => setSessionConfigOption,
	setSessionConfigOptionOnOwner: () => setSessionConfigOptionOnOwner,
	setSessionMode: () => setSessionMode,
	setSessionModeOnOwner: () => setSessionModeOnOwner,
	setSessionModel: () => setSessionModel,
	setSessionModelOnOwner: () => setSessionModelOnOwner
});
//#endregion
//#region src/cli/timer-duration.ts
const MAX_TIMER_DELAY_MS = 2147483647;
function toTimerMilliseconds(seconds, allowZero) {
	if (allowZero && seconds === 0) return 0;
	const milliseconds = Math.max(1, Math.round(seconds * 1e3));
	return milliseconds <= 2147483647 ? milliseconds : void 0;
}
//#endregion
//#region src/acp/jsonrpc-error.ts
const OUTPUT_ERROR_JSONRPC_CODES = {
	NO_SESSION: -32002,
	TIMEOUT: -32070,
	PERMISSION_DENIED: -32071,
	PERMISSION_PROMPT_UNAVAILABLE: -32072,
	EXEC_DISABLED: -32603,
	RUNTIME: -32603,
	USAGE: -32602
};
function hasValidAcpError(acp) {
	return Boolean(acp && Number.isFinite(acp.code) && typeof acp.message === "string" && acp.message.trim().length > 0);
}
function buildFallbackData(params) {
	const data = {
		acpxCode: params.outputCode,
		detailCode: params.detailCode,
		origin: params.origin,
		retryable: params.retryable,
		timestamp: params.timestamp,
		sessionId: params.sessionId
	};
	for (const [key, value] of Object.entries(data)) if (value === void 0) delete data[key];
	return data;
}
function mergeAcpErrorData(acpData, fallbackData) {
	if (Object.keys(fallbackData).length === 0) return acpData;
	if (acpData === void 0) return fallbackData;
	if (acpData && typeof acpData === "object" && !Array.isArray(acpData)) return {
		...fallbackData,
		...acpData
	};
	return acpData;
}
function buildErrorObject(params) {
	const fallbackData = buildFallbackData(params);
	if (hasValidAcpError(params.acp)) {
		const data = mergeAcpErrorData(params.acp.data, fallbackData);
		return {
			code: params.acp.code,
			message: params.acp.message,
			...data !== void 0 ? { data } : {}
		};
	}
	const data = fallbackData;
	return {
		code: OUTPUT_ERROR_JSONRPC_CODES[params.outputCode] ?? -32603,
		message: params.message,
		...Object.keys(data).length > 0 ? { data } : {}
	};
}
function buildJsonRpcErrorResponse(params) {
	return {
		jsonrpc: "2.0",
		id: params.id ?? null,
		error: buildErrorObject(params)
	};
}
//#endregion
//#region src/cli/output/read-suppression.ts
const SUPPRESSED_READ_OUTPUT = "[read output suppressed]";
function inferToolKindFromTitle(title) {
	const normalized = title?.trim().toLowerCase();
	if (!normalized) return;
	const head = normalized.split(/[:\s]/, 1)[0];
	if (!head) return;
	if ([
		"read",
		"cat",
		"open",
		"view"
	].includes(head)) return "read";
}
function isReadLikeTool(tool) {
	return tool.kind?.trim().toLowerCase() === "read" || inferToolKindFromTitle(tool.title) === "read";
}
//#endregion
//#region src/cli/output/json-formatter.ts
const DEFAULT_JSON_SESSION_ID = "unknown";
function asRecord$1(value) {
	if (!value || typeof value !== "object" || Array.isArray(value)) return;
	return value;
}
function jsonRpcIdKey(value) {
	if (typeof value === "string") return `s:${value}`;
	if (typeof value === "number" && Number.isFinite(value)) return `n:${value}`;
}
function requestCorrelationKey(id, direction) {
	const idKey = jsonRpcIdKey(id);
	return idKey === void 0 ? void 0 : `${direction ?? "unknown"}:${idKey}`;
}
function reverseDirection(direction) {
	if (direction === void 0) return;
	return direction === "inbound" ? "outbound" : "inbound";
}
function sanitizeReadResult(result) {
	const record = asRecord$1(result);
	if (!record || typeof record.content !== "string") return result;
	return {
		...record,
		content: SUPPRESSED_READ_OUTPUT
	};
}
function sanitizeToolContent(content) {
	if (!Array.isArray(content)) return content;
	return [{
		type: "content",
		content: {
			type: "text",
			text: SUPPRESSED_READ_OUTPUT
		}
	}];
}
function sanitizeToolMessage(message) {
	const root = asRecord$1(message);
	const params = asRecord$1(root?.params);
	const update = asRecord$1(params?.update);
	if (!root || !params || !update) return message;
	return {
		...root,
		params: {
			...params,
			update: sanitizeToolUpdate(update)
		}
	};
}
function sanitizeToolUpdate(update) {
	return {
		...update,
		rawOutput: sanitizedRawOutput(update),
		content: sanitizedContent(update)
	};
}
function sanitizedRawOutput(update) {
	if (Object.prototype.hasOwnProperty.call(update, "rawOutput") && update.rawOutput !== void 0) return { content: SUPPRESSED_READ_OUTPUT };
	return update.rawOutput;
}
function sanitizedContent(update) {
	if (Object.prototype.hasOwnProperty.call(update, "content") && update.content !== void 0) return sanitizeToolContent(update.content);
	return update.content;
}
var JsonMessageSanitizer = class {
	suppressReads;
	partialHistory;
	requestMethodById = /* @__PURE__ */ new Map();
	toolStateById = /* @__PURE__ */ new Map();
	constructor(suppressReads, partialHistory = false) {
		this.suppressReads = suppressReads;
		this.partialHistory = partialHistory;
	}
	sanitize(message, direction) {
		if (!this.suppressReads) return message;
		const sanitizedResponse = this.sanitizeReadResponse(message, direction);
		if (sanitizedResponse !== message) return sanitizedResponse;
		const sanitizedToolMessage = this.sanitizeReadToolMessage(message);
		if (sanitizedToolMessage !== message) return sanitizedToolMessage;
		this.trackRequestMethod(message, direction);
		return message;
	}
	trackRequestMethod(message, direction) {
		const candidate = message;
		if (typeof candidate.method !== "string") return;
		const idKey = requestCorrelationKey(candidate.id, direction);
		if (!idKey) return;
		this.requestMethodById.set(idKey, candidate.method);
	}
	sanitizeReadResponse(message, direction) {
		const candidate = message;
		const idKey = requestCorrelationKey(candidate.id, reverseDirection(direction));
		if (!idKey) return message;
		const hasResult = Object.hasOwn(candidate, "result");
		const hasError = Object.hasOwn(candidate, "error");
		if (!hasResult && !hasError) return message;
		const method = this.requestMethodById.get(idKey);
		this.requestMethodById.delete(idKey);
		if (!hasResult || !this.suppressReadResponse(method)) return message;
		const root = asRecord$1(message);
		if (!root) return message;
		return {
			...root,
			result: sanitizeReadResult(candidate.result)
		};
	}
	suppressReadResponse(method) {
		return method === "fs/read_text_file" || this.partialHistory && method === void 0;
	}
	sanitizeReadToolMessage(message) {
		const update = this.readToolUpdate(message);
		if (!update) return message;
		const toolCallId = typeof update.toolCallId === "string" ? update.toolCallId : void 0;
		if (!toolCallId) return message;
		const current = this.mergeToolState(toolCallId, update);
		this.toolStateById.set(toolCallId, current);
		const unclassified = this.partialHistory && !current.kind && !current.title;
		return isReadLikeTool(current) || unclassified ? sanitizeToolMessage(message) : message;
	}
	readToolUpdate(message) {
		const root = asRecord$1(message);
		if (root?.method !== "session/update") return;
		const params = asRecord$1(root.params);
		const update = asRecord$1(params?.update);
		if (!params || !update) return;
		const sessionUpdate = update.sessionUpdate;
		if (sessionUpdate !== "tool_call" && sessionUpdate !== "tool_call_update") return;
		return update;
	}
	mergeToolState(toolCallId, update) {
		const previous = this.toolStateById.get(toolCallId) ?? {};
		return {
			title: typeof update.title === "string" ? update.title : previous.title,
			kind: typeof update.kind === "string" || update.kind === null ? update.kind : previous.kind
		};
	}
};
var JsonOutputFormatter = class {
	stdout;
	sanitizer;
	sessionId;
	constructor(stdout, suppressReads, context) {
		this.stdout = stdout;
		this.sanitizer = new JsonMessageSanitizer(suppressReads);
		this.sessionId = context?.sessionId?.trim() || DEFAULT_JSON_SESSION_ID;
	}
	setContext(context) {
		this.sessionId = context.sessionId?.trim() || this.sessionId || DEFAULT_JSON_SESSION_ID;
	}
	onAcpMessage(message, direction) {
		this.stdout.write(`${JSON.stringify(this.sanitizer.sanitize(message, direction))}\n`);
	}
	onError(params) {
		this.stdout.write(`${JSON.stringify(buildJsonRpcErrorResponse({
			outputCode: params.code,
			detailCode: params.detailCode,
			origin: params.origin,
			message: params.message,
			retryable: params.retryable,
			timestamp: params.timestamp,
			sessionId: this.sessionId,
			acp: params.acp
		}))}\n`);
	}
	onPermissionEscalation() {}
	flush() {}
};
function createJsonOutputFormatter(stdout, suppressReads = false, context) {
	return new JsonOutputFormatter(stdout, suppressReads, context);
}
//#endregion
//#region src/cli/output/output.ts
var output_exports = /* @__PURE__ */ __exportAll({
	createOutputFormatter: () => createOutputFormatter,
	getTextErrorRemediationHints: () => getTextErrorRemediationHints
});
const MAX_THOUGHT_CHARS = 900;
const MAX_INLINE_CHARS = 220;
const MAX_OUTPUT_CHARS = 2e3;
const MAX_OUTPUT_LINES = 28;
const MAX_LOCATION_ITEMS = 5;
const OUTPUT_PRIORITY_KEYS = [
	"stdout",
	"stderr",
	"output",
	"content",
	"text",
	"message",
	"result",
	"response",
	"value"
];
function asStatus(status) {
	return status ?? "unknown";
}
function isFinalStatus(status) {
	return status === "completed" || status === "failed";
}
function toStatusLabel(status) {
	return isFinalStatus(status) || status === "pending" ? status : "running";
}
function asRecord(value) {
	if (!value || typeof value !== "object" || Array.isArray(value)) return;
	return value;
}
function parseJsonRpcErrorSummary(message) {
	const fallback = parseJsonRpcErrorMessage(message);
	if (!fallback) return;
	return preferredAcpErrorDetails(asRecord(message.error)?.data) ?? fallback;
}
function extractJsonRpcMethod(message) {
	return Object.hasOwn(message, "method") ? message.method?.toString() : void 0;
}
function collapseWhitespace(value) {
	return value.replace(/\s+/g, " ").trim();
}
function normalizeLineEndings(value) {
	return value.replace(/\r\n?/g, "\n");
}
function truncate(value, maxChars) {
	if (value.length <= maxChars) return value;
	if (maxChars <= 3) return value.slice(0, maxChars);
	return `${value.slice(0, maxChars - 3)}...`;
}
function toInline(value, maxChars = MAX_INLINE_CHARS) {
	return truncate(collapseWhitespace(value), maxChars);
}
function indentBlock(value, prefix) {
	return value.split("\n").map((line) => `${prefix}${line}`).join("\n");
}
function dedupeStrings(values) {
	return [...new Set(values)];
}
function safeJson(value, spacing) {
	const seen = /* @__PURE__ */ new WeakSet();
	try {
		return JSON.stringify(value, (_key, entry) => {
			if (typeof entry === "bigint") return `${entry}n`;
			if (typeof entry === "function") return `[Function ${entry.name || "anonymous"}]`;
			if (typeof entry === "symbol") return entry.toString();
			if (entry && typeof entry === "object") {
				if (seen.has(entry)) return "[Circular]";
				seen.add(entry);
			}
			return entry;
		}, spacing);
	} catch {
		return;
	}
}
function readFirstString(source, keys) {
	for (const key of keys) {
		const value = source[key];
		if (typeof value === "string" && value.trim()) return value.trim();
	}
}
function readFirstFiniteNumber(source, keys) {
	for (const key of keys) {
		const value = source[key];
		if (typeof value === "number" && Number.isFinite(value)) return value;
	}
}
function formatMetadataNumber(value) {
	return Number.isInteger(value) ? String(value) : String(Number(value.toFixed(8)));
}
function readFirstStringArray(source, keys) {
	for (const key of keys) {
		const value = source[key];
		if (!Array.isArray(value)) continue;
		const entries = value.map((entry) => typeof entry === "string" ? entry.trim() : "").filter((entry) => entry.length > 0);
		if (entries.length > 0) return entries;
	}
}
function formatDisjunction(values) {
	if (values.length <= 1) return values[0] ?? "";
	if (values.length === 2) return `${values[0]} or ${values[1]}`;
	return `${values.slice(0, -1).join(", ")}, or ${values.at(-1)}`;
}
function parseAuthMethodIdsFromMessage(message) {
	const methods = [];
	const methodListMatch = message.match(/auth methods \[([^\]]+)\]/iu);
	if (methodListMatch) methods.push(...methodListMatch[1].split(",").map((entry) => entry.trim()).filter((entry) => entry.length > 0));
	const singleMethodMatch = message.match(/auth method ([\w.-]+)/iu);
	if (singleMethodMatch) methods.push(singleMethodMatch[1]);
	return dedupeStrings(methods);
}
function parseAuthMethodIdsFromAcpData(data) {
	const record = asRecord(data);
	if (!record) return [];
	const methodIds = [];
	if (typeof record.methodId === "string" && record.methodId.trim().length > 0) methodIds.push(record.methodId.trim());
	if (Array.isArray(record.methods)) for (const entry of record.methods) {
		const id = parseAuthMethodIdEntry(entry);
		if (id) methodIds.push(id);
	}
	return dedupeStrings(methodIds);
}
function parseAuthMethodIdEntry(entry) {
	if (typeof entry === "string" && entry.trim().length > 0) return entry.trim();
	const id = asRecord(entry)?.id;
	return typeof id === "string" && id.trim().length > 0 ? id.trim() : void 0;
}
function renderAuthRequiredHint(params) {
	const methodIds = dedupeStrings([...parseAuthMethodIdsFromAcpData(params.acp?.data), ...parseAuthMethodIdsFromMessage(params.message)]);
	if (methodIds.length === 0) return "hint: run `acpx config show` to locate the active config, then add the required credential under `auth` and retry.";
	return `hint: run \`acpx config show\` to locate the active config, then add ${formatDisjunction(methodIds.map((methodId) => `\`auth.${methodId}\``))} and retry.`;
}
function getTextErrorRemediationHints(params) {
	const lowerMessage = (preferredAcpErrorDetails(params.acp?.data) ?? params.message).toLowerCase();
	if (params.detailCode === "AUTH_REQUIRED") return [renderAuthRequiredHint(params)];
	if (params.code === "TIMEOUT") return ["hint: increase `--timeout <seconds>` for long-running prompts, or check whether the agent/provider is stalled."];
	if (params.code === "NO_SESSION") return noSessionHints(lowerMessage);
	return matchingTextErrorRule(params, lowerMessage)?.hints ?? [];
}
const TEXT_ERROR_HINT_RULES = [
	{
		matches: (_params, lowerMessage) => isUnsupportedSessionLoadError(lowerMessage),
		hints: ["hint: this adapter cannot resume saved ACP sessions; create a fresh one with `acpx <agent> sessions new` instead of reusing `--resume-session`."]
	},
	{
		matches: (_params, lowerMessage) => isSessionLoadError(lowerMessage),
		hints: ["hint: rerun with `--verbose` to capture the ACP load failure details.", "hint: if you do not need the old backend session, start a fresh one with `acpx <agent> sessions new` and retry."]
	},
	{
		matches: (_params, lowerMessage) => isRateLimitError(lowerMessage),
		hints: ["hint: the provider appears rate-limited; retry later, switch model, or check provider quota/billing."]
	},
	{
		matches: (_params, lowerMessage) => isModelLookupError(lowerMessage),
		hints: ["hint: check the configured model name for this agent, then retry with `--model <model>` or `sessions set-model <model>`."]
	},
	{
		matches: (_params, lowerMessage) => isSessionConfigMethodError(lowerMessage),
		hints: ["hint: rerun with `--verbose` to capture the ACP method/error details before retrying."]
	},
	{
		matches: isRuntimeAcpProtocolError,
		hints: ["hint: rerun with `--verbose` to capture the underlying ACP error details."]
	}
];
function matchingTextErrorRule(params, lowerMessage) {
	return TEXT_ERROR_HINT_RULES.find((rule) => rule.matches(params, lowerMessage));
}
function noSessionHints(lowerMessage) {
	if (lowerMessage.includes("create one:")) return [];
	return ["hint: the saved ACP session is missing or stale; start a fresh session with `acpx <agent> sessions new`, then retry."];
}
function isUnsupportedSessionLoadError(lowerMessage) {
	return lowerMessage.includes("does not support session/resume") || lowerMessage.includes("does not support session/load");
}
function isSessionLoadError(lowerMessage) {
	return lowerMessage.includes("failed to resume acp session") || lowerMessage.includes("session/resume") || lowerMessage.includes("session/load");
}
function isRateLimitError(lowerMessage) {
	return /\b429\b/u.test(lowerMessage) || [
		"rate limit",
		"quota exceeded",
		"usage limit"
	].some((text) => lowerMessage.includes(text));
}
function isModelLookupError(lowerMessage) {
	return [
		"model not found",
		"unknown model",
		"invalid model"
	].some((text) => lowerMessage.includes(text));
}
function isSessionConfigMethodError(lowerMessage) {
	return [
		"session/set_mode",
		"session/set_model",
		"session/set_config_option"
	].some((text) => lowerMessage.includes(text));
}
function isRuntimeAcpProtocolError(params, lowerMessage) {
	return params.origin === "acp" && params.code === "RUNTIME" && (params.acp?.code === -32602 || params.acp?.code === -32603 || lowerMessage.includes("internal error"));
}
function summarizeToolInput(rawInput) {
	if (rawInput == null) return;
	if (isScalarToolInput(rawInput)) return toInline(String(rawInput));
	const record = asRecord(rawInput);
	if (record) return summarizeToolInputRecord(record) ?? summarizeToolInputJson(rawInput);
	return summarizeToolInputJson(rawInput);
}
function isScalarToolInput(value) {
	return typeof value === "string" || typeof value === "number" || typeof value === "boolean";
}
function summarizeToolInputRecord(record) {
	const command = readFirstString(record, [
		"command",
		"cmd",
		"program"
	]);
	const args = readFirstStringArray(record, ["args", "arguments"]);
	if (command) return toInline([command, ...args ?? []].join(" "));
	const location = readFirstString(record, [
		"path",
		"file",
		"filePath",
		"filepath",
		"target",
		"uri",
		"url"
	]);
	const query = readFirstString(record, [
		"query",
		"pattern",
		"text",
		"search"
	]);
	return location || query ? toInline(location ?? query ?? "") : void 0;
}
function summarizeToolInputJson(rawInput) {
	const json = safeJson(rawInput, 0);
	return json ? toInline(json) : void 0;
}
function malformedToolUpdateMessage(update) {
	if (update.locations != null && !validToolLocations(update.locations)) return "Malformed tool call locations";
	return malformedToolContentMessage(update.content);
}
function validToolLocations(locations) {
	return Array.isArray(locations) && locations.every(validToolLocation);
}
function validToolLocation(location) {
	const record = asRecord(location);
	if (!record) return false;
	if (record.path != null && typeof record.path !== "string") return false;
	return record.line == null || typeof record.line === "number" && Number.isFinite(record.line);
}
function malformedToolContentMessage(content) {
	if (content == null) return;
	if (!Array.isArray(content)) return "Malformed tool call content";
	for (const entry of content) {
		const error = malformedToolContentEntry(entry);
		if (error) return error;
	}
}
function malformedToolContentEntry(entry) {
	const record = asRecord(entry);
	if (!record) return "Malformed tool call content";
	if (record.type === "diff") return malformedDiffMessage(record);
	if (record.type === "content" && !validToolContentBlock(record.content)) return "Malformed tool call content";
	if (record.type === "terminal" && typeof record.terminalId !== "string") return "Malformed tool call content";
}
function malformedDiffMessage(record) {
	if (typeof record.path === "string" && typeof record.newText === "string" && (record.oldText == null || typeof record.oldText === "string")) return;
	return "Malformed tool call diff";
}
function validToolContentBlock(content) {
	const block = asRecord(content);
	if (!block || typeof block.type !== "string") return false;
	if (block.type === "text") return typeof block.text === "string";
	if (block.type === "resource") return asRecord(block.resource) !== void 0;
	return true;
}
function formatLocations(locations) {
	if (!Array.isArray(locations) || locations.length === 0) return;
	const unique = /* @__PURE__ */ new Set();
	for (const location of locations) {
		const formatted = formatLocation(location);
		if (formatted) unique.add(formatted);
	}
	const items = [...unique];
	if (items.length === 0) return;
	const visible = items.slice(0, MAX_LOCATION_ITEMS);
	const hidden = items.length - visible.length;
	if (hidden <= 0) return visible.join(", ");
	return `${visible.join(", ")}, +${hidden} more`;
}
function formatLocation(location) {
	if (!location || typeof location !== "object") return;
	const rawPath = location.path;
	if (typeof rawPath !== "string") return;
	const path = rawPath.trim();
	if (!path) return;
	return `${path}${typeof location.line === "number" && Number.isFinite(location.line) ? `:${Math.max(1, Math.trunc(location.line))}` : ""}`;
}
function summarizeDiff(path, oldText, newText) {
	const oldLines = oldText ? oldText.split("\n").length : 0;
	const delta = newText.split("\n").length - oldLines;
	if (delta === 0) return `diff ${path} (line count unchanged)`;
	return `diff ${path} (${`${delta > 0 ? "+" : ""}${delta}`} lines)`;
}
function textFromContentBlock(content) {
	switch (content.type) {
		case "text": return content.text;
		case "resource_link": return content.title ?? content.name ?? content.uri;
		case "resource": return textFromResourceBlock(content);
		case "image": return `[image] ${content.mimeType}`;
		case "audio": return `[audio] ${content.mimeType}`;
		default: return;
	}
}
function textFromResourceBlock(content) {
	if (!content.resource || typeof content.resource !== "object") return "[resource]";
	if ("text" in content.resource && typeof content.resource.text === "string") return content.resource.text;
	const uri = content.resource.uri;
	const mimeType = content.resource.mimeType;
	return `[resource] ${uri}${mimeType ? ` (${mimeType})` : ""}`;
}
function summarizeToolContent(content) {
	if (!Array.isArray(content) || content.length === 0) return;
	const fragments = [];
	for (const entry of content) {
		const fragment = summarizeToolContentEntry(entry);
		if (fragment) fragments.push(fragment);
	}
	const unique = dedupeStrings(fragments.map((fragment) => fragment.trim()).filter((fragment) => fragment.length > 0));
	if (unique.length === 0) return;
	return unique.join("\n\n");
}
function summarizeToolContentEntry(entry) {
	if (!entry || typeof entry !== "object") return;
	if (entry.type === "content") return summarizeContentEntry(entry);
	if (entry.type === "diff") return summarizeDiffEntry(entry);
	if (entry.type === "terminal") return `[terminal] ${entry.terminalId}`;
}
function summarizeContentEntry(entry) {
	if (!entry.content || typeof entry.content !== "object") return;
	const text = textFromContentBlock(entry.content);
	return typeof text === "string" && text.trim() ? text.trimEnd() : void 0;
}
function summarizeDiffEntry(entry) {
	if (typeof entry.path !== "string" || typeof entry.newText !== "string") return;
	const oldText = typeof entry.oldText === "string" ? entry.oldText : void 0;
	return summarizeDiff(entry.path, oldText, entry.newText);
}
function extractOutputText(value, depth = 0, seen = /* @__PURE__ */ new Set()) {
	if (value == null) return;
	const scalar = extractScalarOutputText(value);
	if (scalar !== null) return scalar;
	if (depth >= 4) return;
	if (Array.isArray(value)) return extractOutputTextArray(value, depth, seen);
	return extractOutputTextRecord(value, depth, seen);
}
function extractOutputTextRecord(value, depth, seen) {
	const record = asRecord(value);
	if (!record || seen.has(record)) return;
	seen.add(record);
	return extractPreferredOutputText(record, depth, seen) ?? extractJsonOutputText(record);
}
function extractScalarOutputText(value) {
	if (typeof value === "string") {
		const trimmed = value.trimEnd();
		return trimmed.length > 0 ? trimmed : void 0;
	}
	if (typeof value === "number" || typeof value === "boolean") return String(value);
	return null;
}
function extractOutputTextArray(value, depth, seen) {
	const parts = value.map((entry) => extractOutputText(entry, depth + 1, seen)).filter((entry) => Boolean(entry));
	return parts.length > 0 ? dedupeStrings(parts).join("\n") : void 0;
}
function extractPreferredOutputText(record, depth, seen) {
	const uniquePreferred = dedupeStrings(OUTPUT_PRIORITY_KEYS.flatMap((key) => {
		if (!(key in record)) return [];
		const extracted = extractOutputText(record[key], depth + 1, seen);
		return extracted ? [extracted] : [];
	}));
	return uniquePreferred.length > 0 ? uniquePreferred.join("\n") : void 0;
}
function extractJsonOutputText(record) {
	const json = safeJson(record, 2);
	return !json || json === "{}" ? void 0 : json;
}
function summarizeToolOutput(rawOutput, content) {
	const fragments = dedupeStrings([extractOutputText(rawOutput), summarizeToolContent(content)].map((fragment) => fragment?.trim()).filter((fragment) => Boolean(fragment)));
	if (fragments.length === 0) return;
	return fragments.join("\n\n");
}
function renderToolOutput(state, suppressReads) {
	if (suppressReads && isReadLikeTool(state)) return SUPPRESSED_READ_OUTPUT;
	return summarizeToolOutput(state.rawOutput, state.content);
}
function limitOutputBlock(value) {
	const normalized = value.replace(/\r\n/g, "\n").trim();
	if (!normalized) return "";
	const lines = normalized.split("\n");
	const visible = lines.slice(0, MAX_OUTPUT_LINES);
	let result = visible.join("\n");
	if (lines.length > visible.length) {
		const hidden = lines.length - visible.length;
		result += `\n... (${hidden} more lines)`;
	}
	if (result.length > MAX_OUTPUT_CHARS) result = `${result.slice(0, 1997)}...`;
	return result;
}
var TextOutputFormatter = class {
	stdout;
	useColor;
	suppressReads;
	toolStates = /* @__PURE__ */ new Map();
	thoughtBuffer = "";
	wroteAny = false;
	atLineStart = true;
	constructor(stdout, suppressReads) {
		this.stdout = stdout;
		this.useColor = Boolean(stdout.isTTY);
		this.suppressReads = suppressReads;
	}
	setContext(_context) {}
	onAcpMessage(message) {
		const notice = parsePermissionNotice(message);
		if (notice) {
			this.beginSection();
			this.writeLine(`${this.bold("[permission]")} ${notice}`);
			return;
		}
		const notification = extractSessionUpdateNotification(message);
		if (notification) {
			this.renderSessionUpdate(notification);
			return;
		}
		const method = extractJsonRpcMethod(message);
		if (method && ![
			"session/prompt",
			"session/cancel",
			"session/update"
		].includes(method)) {
			this.onClientOperation({
				method,
				status: "running",
				summary: method,
				timestamp: (/* @__PURE__ */ new Date()).toISOString()
			});
			return;
		}
		const stopReason = parsePromptStopReason(message);
		if (stopReason) {
			this.renderDone(stopReason);
			return;
		}
		const errorMessage = parseJsonRpcErrorSummary(message);
		if (errorMessage) this.onError({
			code: "RUNTIME",
			origin: "acp",
			message: errorMessage
		});
	}
	renderSessionUpdate(notification) {
		const update = notification.update;
		if (update.sessionUpdate !== "agent_thought_chunk") this.flushThoughtBuffer();
		this.renderSessionUpdateBody(update);
	}
	renderSessionUpdateBody(update) {
		switch (update.sessionUpdate) {
			case "agent_message_chunk":
				if (update.content.type === "text") this.write(update.content.text);
				return;
			case "agent_thought_chunk":
				if (update.content.type === "text") this.thoughtBuffer += update.content.text;
				return;
			case "tool_call":
				this.renderToolUpdate(update);
				return;
			case "tool_call_update":
				this.renderToolUpdate(update);
				return;
			case "plan":
				this.renderPlanUpdate(update.entries);
				return;
			default: return;
		}
	}
	renderPlanUpdate(entries) {
		this.beginSection();
		this.writeLine(this.bold("[plan]"));
		for (const entry of entries) this.writeLine(`  - [${entry.status}] ${entry.content}`);
	}
	renderDone(stopReason) {
		this.flushThoughtBuffer();
		this.beginSection();
		this.writeLine(this.dim(`[done] ${stopReason}`));
	}
	onError(params) {
		this.flushThoughtBuffer();
		this.beginSection();
		const message = preferredAcpErrorDetails(params.acp?.data) ?? params.message;
		this.writeLine(this.formatAnsi(`[error] ${params.code}: ${message}`, "31"));
		for (const hint of getTextErrorRemediationHints(params)) this.writeLine(this.dim(hint));
	}
	onClientOperation(operation) {
		this.flushThoughtBuffer();
		this.beginSection();
		const normalizedStatus = operation.status === "completed" ? "completed" : operation.status === "failed" ? "failed" : "in_progress";
		const statusText = this.colorStatus(operation.status, normalizedStatus);
		this.writeLine(`${this.bold("[client]")} ${operation.summary} (${statusText})`);
		if (operation.details && operation.details.trim().length > 0) {
			this.writeLine("  details:");
			this.writeLine(indentBlock(operation.details, "    "));
		}
	}
	onPermissionEscalation(event) {
		this.flushThoughtBuffer();
		this.beginSection();
		this.writeLine(`${this.bold("[permission]")} ${event.message}`);
		const details = [
			`sessionId: ${event.sessionId}`,
			`toolCallId: ${event.toolCallId}`,
			event.toolName ? `toolName: ${event.toolName}` : void 0,
			`toolTitle: ${event.toolTitle}`,
			event.toolInput !== void 0 ? `toolInput: ${summarizeToolInput(event.toolInput) ?? "(structured input)"}` : void 0,
			event.toolKind ? `toolKind: ${event.toolKind}` : void 0,
			event.matchedRule ? `matchedRule: ${event.matchedRule}` : void 0
		].filter((line) => Boolean(line));
		this.writeLine(indentBlock(details.join("\n"), "  "));
	}
	flush() {
		this.flushThoughtBuffer();
		if (!this.atLineStart) this.write("\n");
	}
	write(chunk) {
		if (!chunk) return;
		this.stdout.write(chunk);
		this.wroteAny = true;
		this.atLineStart = chunk.endsWith("\n");
	}
	writeLine(line) {
		this.write(`${line}\n`);
	}
	beginSection() {
		if (!this.atLineStart) this.write("\n");
		if (this.wroteAny) this.write("\n");
	}
	flushThoughtBuffer() {
		const thought = truncate(normalizeLineEndings(this.thoughtBuffer).trim(), MAX_THOUGHT_CHARS);
		this.thoughtBuffer = "";
		if (!thought) return;
		this.beginSection();
		const [firstLine, ...restLines] = thought.split("\n");
		this.writeLine(this.dim(`[thinking] ${firstLine}`));
		for (const line of restLines) this.writeLine(this.dim(`           ${line}`));
	}
	renderToolUpdate(update) {
		const shapeError = malformedToolUpdateMessage(update);
		if (shapeError) {
			this.onError({
				code: "RUNTIME",
				origin: "acp",
				message: shapeError
			});
			return;
		}
		const state = this.getOrCreateToolState(update.toolCallId);
		this.mergeToolState(state, update);
		const status = asStatus(state.status);
		const final = isFinalStatus(status);
		if (!final && state.startedPrinted) return;
		const display = {
			input: summarizeToolInput(state.rawInput),
			files: formatLocations(state.locations),
			output: final ? renderToolOutput(state, this.suppressReads) : void 0
		};
		if (final) {
			const payload = {
				title: state.title,
				status: state.status,
				kind: state.kind,
				...display
			};
			const signature = safeJson(payload, 0) ?? JSON.stringify(payload);
			if (signature === state.finalSignature) return;
			state.finalSignature = signature;
		} else state.startedPrinted = true;
		this.renderToolState(state, status, display);
	}
	getOrCreateToolState(toolCallId) {
		const existing = this.toolStates.get(toolCallId);
		if (existing) return existing;
		const created = {
			id: toolCallId,
			startedPrinted: false
		};
		this.toolStates.set(toolCallId, created);
		return created;
	}
	mergeToolState(state, update) {
		this.mergeToolTitle(state, update.title);
		this.mergeToolPayloadState(state, update);
	}
	mergeToolTitle(state, title) {
		if (typeof title === "string" && title.trim().length > 0) state.title = title;
	}
	mergeToolPayloadState(state, update) {
		if (update.status !== void 0) state.status = update.status;
		if (update.kind !== void 0) state.kind = update.kind;
		if (update.locations !== void 0) state.locations = update.locations;
		if (update.rawInput !== void 0) state.rawInput = update.rawInput;
		if (update.rawOutput !== void 0) state.rawOutput = update.rawOutput;
		if (update.content !== void 0) state.content = update.content;
	}
	renderToolState(state, status, display) {
		this.beginSection();
		const title = state.title ?? state.id;
		const statusText = this.colorStatus(toStatusLabel(status), status);
		this.writeLine(`${this.bold("[tool]")} ${title} (${statusText})`);
		if (isFinalStatus(status) && state.kind) this.writeLine(`  kind: ${state.kind}`);
		if (display.input) this.writeLine(`  input: ${display.input}`);
		if (display.files) this.writeLine(`  files: ${display.files}`);
		if (display.output) {
			this.writeLine("  output:");
			this.writeLine(indentBlock(limitOutputBlock(display.output), "    "));
		}
	}
	formatAnsi(text, code) {
		if (!this.useColor) return text;
		return `\u001b[${code}m${text}\u001b[0m`;
	}
	bold(text) {
		return this.formatAnsi(text, "1");
	}
	dim(text) {
		return this.formatAnsi(text, "2");
	}
	colorStatus(text, status) {
		if (!this.useColor) return text;
		switch (status) {
			case "completed": return this.formatAnsi(text, "32");
			case "failed": return this.formatAnsi(text, "31");
			default: return this.formatAnsi(text, "33");
		}
	}
};
var QuietOutputFormatter = class {
	stdout;
	stderr;
	chunks = [];
	finished = false;
	outputWritten = false;
	metadataFlushed = false;
	constructor(stdout, stderr) {
		this.stdout = stdout;
		this.stderr = stderr;
	}
	setContext(_context) {}
	onAcpMessage(message) {
		const notice = parsePermissionNotice(message);
		if (notice) {
			this.stderr.write(`[acpx] permission: ${notice.replace(/\r\n?|\n/g, " ")}\n`);
			return;
		}
		const update = extractSessionUpdateNotification(message);
		if (update?.update.sessionUpdate === "agent_message_chunk" && update.update.content.type === "text") {
			if (!this.finished) this.chunks.push(update.update.content.text);
			return;
		}
		if (parsePromptStopReason(message)) {
			if (!this.finished) {
				this.finished = true;
				this.flushBufferedOutput(true);
			}
			this.flushMetadata(message);
		}
	}
	onError(params) {
		this.flush();
		const qualifier = params.detailCode ? `${params.code} ${params.detailCode}` : params.code;
		const message = preferredAcpErrorDetails(params.acp?.data) ?? params.message;
		this.stderr.write(`[acpx] error: ${qualifier} ${message.replace(/\r\n?|\n/g, " ")}\n`);
	}
	onPermissionEscalation(_event) {}
	flush() {
		this.flushBufferedOutput();
	}
	flushBufferedOutput(allowEmpty = false) {
		const text = this.chunks.join("");
		this.chunks = [];
		if (text.length === 0 && (!allowEmpty || this.outputWritten)) return;
		this.outputWritten = true;
		this.stdout.write(text.endsWith("\n") ? text : `${text}\n`);
	}
	flushMetadata(message) {
		if (this.metadataFlushed) return;
		this.metadataFlushed = true;
		const result = asRecord(message.result);
		if (!result) return;
		const usageLine = this.formatUsageLine(asRecord(result.usage));
		if (usageLine) this.stderr.write(`${usageLine}\n`);
		const costLine = this.formatCostLine(result.cost);
		if (costLine) this.stderr.write(`${costLine}\n`);
	}
	formatUsageLine(usage) {
		if (!usage) return;
		const parts = [];
		for (const [label, keys] of [
			["input", ["inputTokens", "input_tokens"]],
			["output", ["outputTokens", "output_tokens"]],
			["cache_read", [
				"cachedReadTokens",
				"cacheReadInputTokens",
				"cache_read_input_tokens"
			]],
			["cache_write", [
				"cachedWriteTokens",
				"cacheCreationInputTokens",
				"cache_creation_input_tokens"
			]],
			["total", ["totalTokens", "total_tokens"]]
		]) {
			const value = readFirstFiniteNumber(usage, keys);
			if (value !== void 0) parts.push(`${label}=${formatMetadataNumber(value)}`);
		}
		return parts.length > 0 ? `[acpx] tokens: ${parts.join(" ")}` : void 0;
	}
	formatCostLine(cost) {
		const scalar = this.formatScalarCostLine(cost);
		if (scalar) return scalar;
		const record = asRecord(cost);
		if (!record) return;
		const amount = readFirstFiniteNumber(record, [
			"amount",
			"value",
			"total"
		]);
		if (amount === void 0) return;
		const currency = typeof record.currency === "string" && record.currency.trim() ? ` ${record.currency.trim()}` : "";
		return `[acpx] cost: ${formatMetadataNumber(amount)}${currency}`;
	}
	formatScalarCostLine(cost) {
		if (typeof cost === "number" && Number.isFinite(cost)) return `[acpx] cost: ${formatMetadataNumber(cost)}`;
		if (typeof cost === "string" && cost.trim()) return `[acpx] cost: ${cost.trim()}`;
	}
};
function preferredAcpErrorDetails(data) {
	const record = asRecord(data);
	for (const value of [record?.details, record?.message]) if (typeof value === "string" && value.trim().length > 0) return value.trim();
}
function createOutputFormatter(format, options = {}) {
	const stdout = options.stdout ?? process.stdout;
	const stderr = options.stderr ?? process.stderr;
	const suppressReads = options.suppressReads === true;
	switch (format) {
		case "text": return new TextOutputFormatter(stdout, suppressReads);
		case "json": return createJsonOutputFormatter(stdout, suppressReads, options.jsonContext);
		case "quiet": return new QuietOutputFormatter(stdout, stderr);
		default: throw new Error("Unsupported output format");
	}
}
//#endregion
export { MAX_TIMER_DELAY_MS as a, __exportAll as c, JsonMessageSanitizer as i, getTextErrorRemediationHints as n, toTimerMilliseconds as o, output_exports as r, session_exports as s, createOutputFormatter as t };

//# sourceMappingURL=output-kVujrhgv.js.map