import { a as MAX_TIMER_DELAY_MS, t as createOutputFormatter } from "./output-4y55_cBu.js";
import { C as textPrompt, It as SESSION_RECORD_SCHEMA, Pt as PERMISSION_MODES, R as resolveSessionRecord, S as promptToDisplayText, bt as writePrivateJsonFile, ft as defaultSessionEventLog, yt as writePrivateFile } from "./ipc-BIMIZYBg.js";
import { N as runTimedExecFile } from "./agent-registry-Gtwn7nMp.js";
import { G as recordSessionUpdate, H as createSessionConversation, U as recordClientOperation, W as recordPromptSubmission, dt as InterruptedError, ft as TimeoutError, ht as withTimeout, i as runOnce, m as createSessionWithClient, mt as withInterrupt, o as sendSessionDirect, q as cloneSessionAcpxState } from "./queue-owner-runtime-BtV9pAT7.js";
import path from "node:path";
import { spawn } from "node:child_process";
import fs from "node:fs/promises";
import os from "node:os";
import { appendRegularFile } from "@openclaw/fs-safe/advanced";
import { setTimeout as setTimeout$1 } from "node:timers/promises";
import { ZodError, z } from "zod";
import { createHash, randomUUID } from "node:crypto";
import { performance } from "node:perf_hooks";
//#region src/flows/timeout.ts
function resolveFlowTimeoutMs(timeoutMs) {
	if (timeoutMs === void 0) return;
	if (!Number.isFinite(timeoutMs) || timeoutMs > 2147483647) throw new TypeError(`timeoutMs must be a finite number no greater than ${MAX_TIMER_DELAY_MS}`);
	return timeoutMs > 0 ? timeoutMs : void 0;
}
//#endregion
//#region src/flows/attempt.ts
/** One node attempt owns admission and completion of its managed operations. */
var FlowAttempt = class {
	signal;
	nodeId;
	attemptId;
	startedAt;
	terminationSignal = "SIGTERM";
	controller = new AbortController();
	pending = /* @__PURE__ */ new Set();
	cancellations = /* @__PURE__ */ new Set();
	cleanupFailures = [];
	deadlineAt;
	timeoutMs;
	timer;
	accepting = true;
	finished = false;
	constructor(options) {
		this.nodeId = options.nodeId;
		this.attemptId = options.attemptId;
		this.startedAt = options.startedAt;
		this.signal = this.controller.signal;
		const timeoutMs = resolveFlowTimeoutMs(options.timeoutMs);
		if (timeoutMs !== void 0) {
			this.timeoutMs = timeoutMs;
			this.deadlineAt = performance.now() + timeoutMs;
			this.timer = setTimeout(() => this.cancel(new TimeoutError(timeoutMs)), timeoutMs);
		}
	}
	get active() {
		return this.accepting && !this.signal.aborted;
	}
	assertActive() {
		this.checkDeadline();
		this.signal.throwIfAborted();
		if (!this.accepting) throw new Error("Flow attempt has finished accepting work");
	}
	remainingTimeoutMs() {
		this.assertActive();
		return this.deadlineAt === void 0 ? void 0 : Math.max(1, Math.ceil(this.deadlineAt - performance.now()));
	}
	cancel(reason, signal = "SIGTERM") {
		if (this.finished || this.signal.aborted) return;
		this.accepting = false;
		this.terminationSignal = signal;
		this.controller.abort(reason);
		for (const cancel of this.cancellations) this.trackCancellation(cancel);
	}
	registerCancellation(cancel) {
		if (!this.active) {
			this.trackCancellation(cancel);
			return () => {};
		}
		this.cancellations.add(cancel);
		return () => {
			this.cancellations.delete(cancel);
		};
	}
	/** Track runtime work, never an arbitrary user callback that may ignore cancellation. */
	own(run, options = {}) {
		this.assertActive();
		const operation = run().then((value) => {
			this.checkDeadline();
			if (this.signal.aborted) this.signal.throwIfAborted();
			return value;
		});
		const outcome = operation.then(() => ({ ok: true }), (error) => options.bestEffort ? { ok: true } : {
			ok: false,
			error
		});
		this.pending.add(outcome);
		outcome.then(() => this.pending.delete(outcome));
		return operation;
	}
	async run(run) {
		let rejectAbort = () => {};
		const aborted = new Promise((_resolve, reject) => {
			rejectAbort = reject;
		});
		const onAbort = () => rejectAbort(this.signal.reason);
		this.signal.addEventListener("abort", onAbort, { once: true });
		try {
			this.assertActive();
			const value = await Promise.race([run(), aborted]);
			this.accepting = false;
			const errors = await this.drain();
			if (errors.length > 0) throw this.cleanupError(errors);
			this.checkDeadline();
			this.signal.throwIfAborted();
			return value;
		} catch (error) {
			this.cancel(error);
			const errors = await this.drain();
			if (errors.length > 0) throw this.cleanupError([error, ...errors]);
			throw error;
		} finally {
			this.accepting = false;
			this.finished = true;
			clearTimeout(this.timer);
			this.signal.removeEventListener("abort", onAbort);
		}
	}
	checkDeadline() {
		if (this.deadlineAt !== void 0 && performance.now() >= this.deadlineAt) this.cancel(new TimeoutError(this.timeoutMs));
	}
	async drain() {
		const errors = [];
		while (this.pending.size > 0) for (const outcome of await Promise.all(this.pending)) if (!outcome.ok && outcome.error !== this.signal.reason) errors.push(outcome.error);
		return [.../* @__PURE__ */ new Set([...errors, ...this.cleanupFailures.splice(0)])];
	}
	trackCancellation(cancel) {
		const outcome = Promise.resolve().then(() => cancel(this.terminationSignal)).then(() => ({ ok: true }), (error) => {
			this.cleanupFailures.push(error);
			return { ok: true };
		});
		this.pending.add(outcome);
		outcome.then(() => this.pending.delete(outcome));
	}
	cleanupError(errors) {
		const reason = this.signal.reason;
		const failures = [.../* @__PURE__ */ new Set([...this.signal.aborted ? [reason] : [], ...errors])];
		return new AggregateError(failures, "Flow attempt cleanup failed", { cause: failures[0] });
	}
};
//#endregion
//#region src/flows/authoring.ts
const FLOW_DEFINITION_BRAND = Symbol.for("acpx.flow.definition");
function markDefinedFlow(definition) {
	if (isDefinedFlow(definition)) return definition;
	Object.defineProperty(definition, FLOW_DEFINITION_BRAND, {
		value: true,
		enumerable: false,
		configurable: false,
		writable: false
	});
	return definition;
}
function isDefinedFlow(value) {
	return value != null && typeof value === "object" && value[FLOW_DEFINITION_BRAND] === true;
}
//#endregion
//#region src/flows/schema.ts
const FLOW_NODE_TYPES = [
	"acp",
	"compute",
	"action",
	"checkpoint"
];
const finiteNonNegativeNumberSchema = z.number().finite().nonnegative();
const nonEmptyTrimmedStringSchema = z.string().refine((value) => value.trim().length > 0, { message: "must not be empty" });
function extensibleObject(shape) {
	return z.object(shape).passthrough();
}
function functionSchema(label) {
	return z.custom((value) => typeof value === "function", { message: `${label} must be a function` });
}
const flowNodeCommonShape = {
	timeoutMs: finiteNonNegativeNumberSchema.max(MAX_TIMER_DELAY_MS).optional(),
	heartbeatMs: finiteNonNegativeNumberSchema.optional(),
	statusDetail: z.string().optional()
};
const flowPermissionRequirementsSchema = extensibleObject({
	requiredMode: z.enum(PERMISSION_MODES),
	requireExplicitGrant: z.boolean().optional(),
	reason: nonEmptyTrimmedStringSchema.optional()
});
const flowRunDefinitionSchema = extensibleObject({ title: z.union([z.string(), functionSchema("run.title")]).optional() });
const acpSessionSchema = extensibleObject({
	handle: z.string().optional(),
	isolated: z.boolean().optional()
});
const acpNodeSchema = extensibleObject({
	...flowNodeCommonShape,
	nodeType: z.literal("acp"),
	profile: z.string().optional(),
	cwd: z.union([z.string(), functionSchema("cwd")]).optional(),
	session: acpSessionSchema.optional(),
	prompt: functionSchema("prompt"),
	parse: functionSchema("parse").optional()
});
const computeNodeSchema = extensibleObject({
	...flowNodeCommonShape,
	nodeType: z.literal("compute"),
	run: functionSchema("run")
});
const functionActionNodeSchema = extensibleObject({
	...flowNodeCommonShape,
	nodeType: z.literal("action"),
	run: functionSchema("run")
});
const shellActionNodeSchema = extensibleObject({
	...flowNodeCommonShape,
	nodeType: z.literal("action"),
	exec: functionSchema("exec"),
	parse: functionSchema("parse").optional()
}).refine((node) => !hasOwn(node, "run"), { message: "shell action nodes must not define run" });
const checkpointNodeSchema = extensibleObject({
	...flowNodeCommonShape,
	nodeType: z.literal("checkpoint"),
	summary: z.string().optional(),
	run: functionSchema("run").optional()
});
const directFlowEdgeSchema = extensibleObject({
	from: z.string(),
	to: z.string()
});
const switchFlowEdgeSchema = extensibleObject({
	from: z.string(),
	switch: extensibleObject({
		on: z.string(),
		cases: z.record(z.string(), z.string())
	})
});
const flowDefinitionSchema = extensibleObject({
	name: nonEmptyTrimmedStringSchema,
	run: flowRunDefinitionSchema.optional(),
	permissions: flowPermissionRequirementsSchema.optional(),
	startAt: z.string(),
	nodes: z.record(z.string(), z.unknown()),
	edges: z.array(z.unknown())
});
const flowNodeTypeSchema = z.object({ nodeType: z.enum(FLOW_NODE_TYPES) });
function assertValidFlowDefinitionShape(flow) {
	const parsed = parseWithSchema("flow definition", flowDefinitionSchema, flow);
	for (const [nodeId, node] of Object.entries(flow.nodes)) assertValidFlowNodeDefinitionShape(node, `flow node "${nodeId}"`);
	parsed.edges.forEach((edge, index) => {
		assertValidFlowEdgeShape(edge, `flow definition: edges.${index}`);
	});
}
function assertValidAcpNodeDefinition(node) {
	parseWithSchema("acp node definition", acpNodeSchema, node);
}
function assertValidComputeNodeDefinition(node) {
	parseWithSchema("compute node definition", computeNodeSchema, node);
}
function assertValidActionNodeDefinition(node) {
	assertValidActionNodeDefinitionShape(node, "action node definition");
}
function assertValidShellActionNodeDefinition(node) {
	parseWithSchema("shell action node definition", shellActionNodeSchema, node);
}
function assertValidCheckpointNodeDefinition(node) {
	parseWithSchema("checkpoint node definition", checkpointNodeSchema, node);
}
function assertValidFlowNodeDefinitionShape(node, label) {
	const { nodeType } = parseWithSchema(label, flowNodeTypeSchema, node);
	switch (nodeType) {
		case "acp":
			parseWithSchema(label, acpNodeSchema, node);
			return;
		case "compute":
			parseWithSchema(label, computeNodeSchema, node);
			return;
		case "action":
			assertValidActionNodeDefinitionShape(node, label);
			return;
		case "checkpoint":
			parseWithSchema(label, checkpointNodeSchema, node);
			return;
	}
}
function assertValidActionNodeDefinitionShape(node, label) {
	const hasRun = hasOwn(node, "run");
	const hasExec = hasOwn(node, "exec");
	if (hasRun === hasExec) throw new Error(`Invalid ${label}: action nodes must define exactly one of run or exec`);
	if (hasExec) {
		parseWithSchema(label, shellActionNodeSchema, node);
		return;
	}
	parseWithSchema(label, functionActionNodeSchema, node);
}
function assertValidFlowEdgeShape(edge, label) {
	const hasTo = hasOwn(edge, "to");
	if (hasTo === hasOwn(edge, "switch")) throw new Error(`Invalid ${label}: edge must define exactly one of to or switch`);
	if (hasTo) {
		parseWithSchema(label, directFlowEdgeSchema, edge);
		return;
	}
	parseWithSchema(label, switchFlowEdgeSchema, edge);
	const cases = edge.switch.cases;
	for (const [key, target] of Object.entries(cases)) parseWithSchema(`${label}: switch.cases.${key}`, z.string(), target);
}
function parseWithSchema(label, schema, value) {
	try {
		return schema.parse(value);
	} catch (error) {
		if (error instanceof ZodError) throw new Error(formatValidationError(label, error), { cause: error });
		throw error;
	}
}
function formatValidationError(label, error) {
	return `Invalid ${label}: ${Array.from(new Set(error.issues.flatMap((issue) => formatIssue(issue)))).join("; ")}`;
}
function formatIssue(issue, parentPath = []) {
	const path = [...parentPath, ...issue.path.map(String)];
	if (issue.code === "invalid_union") return issue.errors.flatMap((branch) => branch.flatMap((nestedIssue) => formatIssue(nestedIssue, path)));
	const renderedPath = path.join(".");
	return [renderedPath ? `${renderedPath}: ${issue.message}` : issue.message];
}
function hasOwn(value, key) {
	return value != null && typeof value === "object" && Object.prototype.hasOwnProperty.call(value, key);
}
//#endregion
//#region src/flows/definition.ts
function defineFlow(definition) {
	assertValidFlowDefinitionShape(definition);
	return markDefinedFlow(definition);
}
function acp(definition) {
	const node = {
		nodeType: "acp",
		...definition
	};
	assertValidAcpNodeDefinition(node);
	return node;
}
function compute(definition) {
	const node = {
		nodeType: "compute",
		...definition
	};
	assertValidComputeNodeDefinition(node);
	return node;
}
function action(definition) {
	const node = {
		nodeType: "action",
		...definition
	};
	assertValidActionNodeDefinition(node);
	return node;
}
function shell(definition) {
	const node = {
		nodeType: "action",
		...definition
	};
	assertValidShellActionNodeDefinition(node);
	return node;
}
function checkpoint(definition = {}) {
	const node = {
		nodeType: "checkpoint",
		...definition
	};
	assertValidCheckpointNodeDefinition(node);
	return node;
}
//#endregion
//#region src/flows/executors/shell-output.ts
function validateShellActionMaxBufferBytes(value) {
	if (value === void 0) return;
	if (!Number.isSafeInteger(value) || value < 0) throw new Error("Shell action maxBufferBytes must be a non-negative safe integer");
}
function createShellOutputCapture(maxBufferBytes, inactive, overflow) {
	const output = {
		stdout: "",
		stderr: ""
	};
	const bytes = {
		stdout: 0,
		stderr: 0
	};
	return {
		output,
		append(stream, chunk) {
			if (inactive()) return;
			if (maxBufferBytes !== void 0) {
				bytes[stream] += Buffer.byteLength(chunk, "utf8");
				if (bytes[stream] > maxBufferBytes) {
					overflow(/* @__PURE__ */ new Error(`Shell action exceeded maxBuffer (${maxBufferBytes} bytes) on ${stream}`));
					return;
				}
			}
			output[stream] += chunk;
		}
	};
}
//#endregion
//#region src/flows/executors/shell-process.ts
const KILL_GRACE_MS = 1e3;
async function readPosixProcesses() {
	return (await runTimedExecFile("ps", ["-eo", "pid=,ppid=,pgid=,stat="])).trim().split("\n").map((line) => {
		const [pid, parent, group, state = ""] = line.trim().split(/\s+/u);
		return {
			pid: Number(pid),
			parent: Number(parent),
			group: Number(group),
			state
		};
	}).filter((entry) => Number.isInteger(entry.pid) && entry.pid > 0);
}
function isLive(entry) {
	return entry.state.length > 0 && !entry.state.startsWith("Z");
}
function rememberOwnedProcesses(rows, root, owned) {
	for (const entry of rows) if (entry.group === root) owned.add(entry.pid);
	for (const parent of owned) for (const entry of rows) if (entry.parent === parent) owned.add(entry.pid);
}
function signalPid(pid, signal) {
	try {
		process.kill(pid, signal);
	} catch (error) {
		if (error.code !== "ESRCH") throw error;
	}
}
async function signalGroup(root, signal) {
	try {
		process.kill(-root, signal);
	} catch (error) {
		const code = error.code;
		if (code === "ESRCH") return;
		if (code === "EPERM" && !(await readPosixProcesses()).some((entry) => entry.group === root && isLive(entry))) return;
		throw error;
	}
}
async function signalOwned(root, owned, signal) {
	const rows = await readPosixProcesses();
	rememberOwnedProcesses(rows, root, owned);
	await signalGroup(root, signal);
	for (const entry of rows) if (owned.has(entry.pid) && entry.group !== root && isLive(entry)) signalPid(entry.pid, signal);
}
async function waitForOwned(root, owned) {
	const deadline = Date.now() + KILL_GRACE_MS;
	do {
		const rows = await readPosixProcesses();
		rememberOwnedProcesses(rows, root, owned);
		if (!rows.some((entry) => owned.has(entry.pid) && isLive(entry))) return true;
		await setTimeout$1(25);
	} while (Date.now() < deadline);
	return false;
}
async function forceKnownProcesses(root, owned) {
	const results = await Promise.allSettled([signalGroup(root, "SIGKILL"), ...[...owned].map(async (pid) => signalPid(pid, "SIGKILL"))]);
	const errors = [];
	for (const result of results) if (result.status === "rejected") errors.push(result.reason);
	if (errors.length > 0) throw new AggregateError(errors, "Known shell processes could not be stopped", { cause: errors[0] });
}
async function stopPosixTree(pid, signal) {
	const owned = /* @__PURE__ */ new Set([pid]);
	try {
		await signalOwned(pid, owned, signal);
		if (await waitForOwned(pid, owned)) return;
		await signalOwned(pid, owned, "SIGKILL");
		if (!await waitForOwned(pid, owned)) throw new Error("Shell process tree did not terminate after SIGKILL");
	} catch (error) {
		try {
			await forceKnownProcesses(pid, owned);
		} catch (cleanupError) {
			throw new AggregateError([error, cleanupError], "Shell process cleanup failed", { cause: cleanupError });
		}
		throw error;
	}
}
async function stopWindowsTree(child) {
	if (child.pid == null || child.exitCode != null || child.signalCode != null) return;
	try {
		await runTimedExecFile("taskkill", [
			"/pid",
			String(child.pid),
			"/t",
			"/f"
		], { windowsHide: true });
	} catch (error) {
		child.kill("SIGKILL");
		throw error;
	}
}
async function waitForShellClose(closed) {
	try {
		await withTimeout(closed, KILL_GRACE_MS);
	} catch (cause) {
		throw new Error("Shell process streams did not close after termination", { cause });
	}
}
async function stopShellProcess(child, closed, signal) {
	try {
		if (child.pid != null) {
			if (process.platform === "win32") await stopWindowsTree(child);
			else await stopPosixTree(child.pid, signal);
		}
	} catch (error) {
		try {
			await waitForShellClose(closed);
		} catch (closeError) {
			throw new AggregateError([error, closeError], "Shell process cleanup failed", { cause: closeError });
		}
		throw error;
	}
	await waitForShellClose(closed);
}
function hasShellProcesses(child) {
	if (child.pid == null) return false;
	if (process.platform === "win32") return child.exitCode == null && child.signalCode == null;
	try {
		process.kill(-child.pid, 0);
		return true;
	} catch (error) {
		return error.code !== "ESRCH";
	}
}
//#endregion
//#region src/flows/executors/shell.ts
function writeShellStdin(child, stdin) {
	const stream = child.stdin;
	if (!stream) return;
	stream.on("error", () => {});
	if (stdin != null && stream.writable && !stream.writableEnded) stream.write(stdin);
	if (stream.writable && !stream.writableEnded) stream.end();
}
function formatShellActionSummary(spec) {
	return `shell: ${renderShellCommand(spec.command, spec.args ?? [])}`;
}
function renderShellCommand(command, args) {
	const renderedArgs = args.map((arg) => JSON.stringify(arg)).join(" ");
	return renderedArgs.length > 0 ? `${command} ${renderedArgs}` : command;
}
function createShellFailureError(spec, args, exitCode, signal, stderr) {
	const status = signal ? `signal ${signal}` : `exit ${String(exitCode)}`;
	const details = stderr.length > 0 ? `\n${stderr.trim()}` : "";
	return /* @__PURE__ */ new Error(`Shell action failed (${renderShellCommand(spec.command, args)}): ${status}${details}`);
}
/**
* Resolve a shell-action timeout.
* Non-positive values match withTimeout: no deadline (undefined).
* Positive values arm SIGTERM/SIGKILL after that many ms.
*/
function resolveShellActionTimeoutMs(timeoutMs) {
	return resolveFlowTimeoutMs(timeoutMs);
}
function rejectIfShellFailed(spec, args, result, timedOut, timeoutMs) {
	if (timedOut) return new TimeoutError(timeoutMs ?? spec.timeoutMs ?? 0);
	if (((result.exitCode ?? 0) !== 0 || result.signal != null) && spec.allowNonZeroExit !== true) return createShellFailureError(spec, args, result.exitCode, result.signal, result.stderr);
}
function waitForShellResult(child, spec, args, cwd, startMs, termination, mode) {
	const stdoutStream = child.stdout;
	const stderrStream = child.stderr;
	if (!stdoutStream || !stderrStream) throw new Error("Shell action child is missing stdio pipes");
	return new Promise((resolve, reject) => {
		let settled = false;
		let drainDeadline;
		const fail = (error) => {
			clearTimeout(drainDeadline);
			settled = true;
			reject(error);
		};
		const capture = createShellOutputCapture(spec.maxBufferBytes, () => settled || mode === "node" && termination.cancelled(), (error) => {
			if (termination.cancelled()) return;
			fail(error);
			termination.cancel("SIGTERM").catch(fail);
		});
		stdoutStream.setEncoding("utf8");
		stderrStream.setEncoding("utf8");
		stdoutStream.on("data", (chunk) => {
			capture.append("stdout", chunk);
		});
		stderrStream.on("data", (chunk) => {
			capture.append("stderr", chunk);
		});
		child.once("error", fail);
		const finish = (exitCode, signal) => {
			clearTimeout(drainDeadline);
			if (settled) return;
			settled = true;
			if (mode === "node" && !termination.cancelled()) {
				stdoutStream.unref();
				stderrStream.unref();
			}
			const { stdout, stderr } = capture.output;
			resolve({
				command: spec.command,
				args,
				cwd,
				stdout,
				stderr,
				combinedOutput: `${stdout}${stderr}`,
				exitCode,
				signal,
				durationMs: Date.now() - startMs,
				timedOut: mode === "node" ? termination.cancelled() : termination.timedOut()
			});
		};
		child.once("close", finish);
		if (mode === "node") child.once("exit", (exitCode, signal) => {
			if (settled) return;
			drainDeadline = setTimeout(() => finish(exitCode, signal), 100);
		});
	});
}
function createShellTermination(child, timeoutMs, options) {
	const closed = new Promise((resolve) => child.once("close", () => resolve()));
	let rejectCleanup = () => {};
	const cleanupFailure = new Promise((_resolve, reject) => {
		rejectCleanup = reject;
	});
	let termination;
	let timedOut = false;
	let cancelled = false;
	let released = false;
	let deadline;
	let monitor;
	let unregister;
	const clearDeadline = () => {
		if (deadline) clearTimeout(deadline);
	};
	const release = () => {
		if (released) return;
		released = true;
		clearDeadline();
		if (monitor) clearInterval(monitor);
		options.signal?.removeEventListener("abort", onAbort);
		unregister?.();
	};
	const cancel = (signal) => {
		if (termination) return termination;
		if (released) return Promise.resolve();
		clearDeadline();
		cancelled = true;
		termination = stopShellProcess(child, closed, signal).finally(release);
		termination.catch(rejectCleanup);
		return termination;
	};
	const onAbort = () => {
		cancel(options.terminationSignal ?? "SIGTERM");
	};
	options.signal?.addEventListener("abort", onAbort, { once: true });
	if (timeoutMs != null) deadline = setTimeout(() => {
		timedOut = true;
		cancel("SIGTERM");
	}, timeoutMs);
	unregister = options.registerOwner?.({
		cancel,
		release
	});
	if (unregister) child.once("close", () => {
		if (released || termination) return;
		const prune = () => {
			if (!termination && !hasShellProcesses(child)) release();
		};
		monitor = setInterval(prune, 100);
		monitor.unref();
		prune();
	});
	return {
		cancel,
		timedOut: () => timedOut,
		cancelled: () => cancelled,
		cleanupFailure,
		async dispose() {
			clearDeadline();
			try {
				await termination;
			} finally {
				if (!unregister) release();
			}
		}
	};
}
async function runShellAction(spec, options = {}) {
	const { timedOut, ...result } = await runShellProcess(spec, options, "node");
	const failure = rejectIfShellFailed(spec, result.args, result, timedOut, resolveShellActionTimeoutMs(spec.timeoutMs));
	if (failure) throw failure;
	return result;
}
async function runShellCommand(spec, options) {
	return await runShellProcess(spec, options, "command");
}
async function runShellProcess(spec, options, mode) {
	options.signal?.throwIfAborted();
	const cwd = spec.cwd ?? process.cwd();
	const args = spec.args ?? [];
	const startMs = Date.now();
	const timeoutMs = resolveShellActionTimeoutMs(spec.timeoutMs);
	validateShellActionMaxBufferBytes(spec.maxBufferBytes);
	if (spec.stdin !== void 0 && typeof spec.stdin !== "string") throw new TypeError("stdin must be a string");
	const child = spawn(spec.command, args, {
		cwd,
		env: {
			...process.env,
			...spec.env
		},
		shell: spec.shell,
		stdio: [
			"pipe",
			"pipe",
			"pipe"
		],
		windowsHide: true,
		detached: process.platform !== "win32"
	});
	const termination = createShellTermination(child, timeoutMs, options);
	const finish = waitForShellResult(child, spec, args, cwd, startMs, termination, mode);
	try {
		writeShellStdin(child, spec.stdin);
		const result = await Promise.race([finish, termination.cleanupFailure]);
		throwIfShellCancelled(options.signal, mode);
		return result;
	} catch (error) {
		await termination.cancel("SIGTERM");
		throw error;
	} finally {
		await termination.dispose();
	}
}
function throwIfShellCancelled(signal, mode) {
	if (!signal?.aborted) return;
	const reason = signal.reason;
	if (mode === "command" || reason instanceof TimeoutError || reason instanceof InterruptedError) throw reason;
}
//#endregion
//#region src/flows/graph.ts
function validateFlowDefinition(flow) {
	assertValidFlowDefinitionShape(flow);
	assertKnownFlowNode(flow, flow.startAt, "Flow start node is missing");
	const outgoingEdges = /* @__PURE__ */ new Set();
	for (const edge of flow.edges) validateFlowEdge(flow, edge, outgoingEdges);
}
function assertKnownFlowNode(flow, nodeId, description) {
	if (!Object.hasOwn(flow.nodes, nodeId)) throw new Error(`${description}: ${nodeId}`);
}
function isDirectEdge(edge) {
	return Object.hasOwn(edge, "to");
}
function validateFlowEdge(flow, edge, outgoingEdges) {
	assertKnownFlowNode(flow, edge.from, "Flow edge references unknown from-node");
	if (outgoingEdges.has(edge.from)) throw new Error(`Flow node must not declare multiple outgoing edges: ${edge.from}`);
	outgoingEdges.add(edge.from);
	if (isDirectEdge(edge)) {
		assertKnownFlowNode(flow, edge.to, "Flow edge references unknown to-node");
		return;
	}
	for (const target of Object.values(edge.switch.cases)) assertKnownFlowNode(flow, target, "Flow switch references unknown to-node");
}
function canFollowEdge(edge, result) {
	return !result || result.outcome === "ok" || !isDirectEdge(edge) && edge.switch.on.startsWith("$result.");
}
function resolveNext(edges, from, output, result) {
	const edge = edges.find((candidate) => candidate.from === from);
	if (!edge || !canFollowEdge(edge, result)) return null;
	if (isDirectEdge(edge)) return edge.to;
	const value = getBySwitchPath(output, result, edge.switch.on);
	if (typeof value !== "string" && typeof value !== "number" && typeof value !== "boolean") throw new Error(`Flow switch value must be scalar for ${edge.switch.on}`);
	const key = String(value);
	if (!Object.hasOwn(edge.switch.cases, key)) throw new Error(`No flow switch case for ${edge.switch.on}=${JSON.stringify(value)}`);
	return edge.switch.cases[key];
}
function getBySwitchPath(output, result, jsonPath) {
	if (jsonPath.startsWith("$result.")) return getByPath(result, `$.${jsonPath.slice(8)}`);
	if (jsonPath.startsWith("$output.")) return getByPath(output, `$.${jsonPath.slice(8)}`);
	return getByPath(output, jsonPath);
}
function getByPath(value, jsonPath) {
	if (!jsonPath.startsWith("$.")) throw new Error(`Unsupported JSON path: ${jsonPath}`);
	return jsonPath.slice(2).split(".").reduce((current, key) => {
		if (current == null || typeof current !== "object") return;
		return current[key];
	}, value);
}
//#endregion
//#region src/flows/runtime-support.ts
function isoNow$1() {
	return (/* @__PURE__ */ new Date()).toISOString();
}
function persistRunFailure(store, runDir, state, error) {
	if (state.finishedAt !== void 0 && (state.status === "failed" || state.status === "timed_out")) return Promise.resolve();
	state.status = error instanceof TimeoutError ? "timed_out" : "failed";
	state.updatedAt = isoNow$1();
	state.finishedAt = state.updatedAt;
	state.error = error instanceof Error ? error.message : String(error);
	state.statusDetail = state.currentNode ? `Failed in ${state.currentNode}: ${state.error}` : state.error;
	return store.writeSnapshot(runDir, state, {
		scope: "run",
		type: "run_failed",
		payload: {
			status: state.status,
			error: state.error
		}
	});
}
function makeFlowNodeContext(state, input, services) {
	return {
		input,
		outputs: state.outputs,
		results: state.results,
		state,
		services
	};
}
function markNodeStarted(state, nodeId, attemptId, nodeType, startedAt, detail) {
	state.status = "running";
	state.waitingOn = void 0;
	state.currentNode = nodeId;
	state.currentAttemptId = attemptId;
	state.currentNodeType = nodeType;
	state.currentNodeStartedAt = startedAt;
	state.lastHeartbeatAt = startedAt;
	state.statusDetail = detail ?? `Running ${nodeType} node ${nodeId}`;
}
function clearActiveNode(state, detail) {
	state.currentNode = void 0;
	state.currentAttemptId = void 0;
	state.currentNodeType = void 0;
	state.currentNodeStartedAt = void 0;
	state.lastHeartbeatAt = void 0;
	state.statusDetail = detail;
}
function updateStatusDetail(state, detail) {
	if (!detail) return;
	state.statusDetail = detail;
}
async function finalizeStepTrace(store, runDir, state, nodeId, attemptId, output, baseTrace) {
	const trace = baseTrace ? structuredClone(baseTrace) : {};
	if (output !== void 0) {
		const inlineOutput = toInlineOutput(output);
		if (inlineOutput !== void 0) trace.outputInline = inlineOutput;
		else trace.outputArtifact = await store.writeArtifact(runDir, state, output, {
			mediaType: outputArtifactMediaType(output),
			extension: outputArtifactExtension(output),
			nodeId,
			attemptId
		});
	}
	return Object.keys(trace).length > 0 ? trace : null;
}
function normalizePromptInput(prompt) {
	return typeof prompt === "string" ? textPrompt(prompt) : prompt;
}
async function resolveNodeCwd(defaultCwd, cwd, context) {
	if (typeof cwd === "function") {
		const resolved = await cwd(context) ?? defaultCwd;
		return path.resolve(defaultCwd, resolved);
	}
	return path.resolve(defaultCwd, cwd ?? defaultCwd);
}
function resolveShellActionCwd(defaultCwd, cwd) {
	return path.resolve(defaultCwd, cwd ?? defaultCwd);
}
function summarizePrompt(promptText, explicitDetail) {
	if (explicitDetail) return explicitDetail;
	const line = promptText.split("\n").map((candidate) => candidate.trim()).find((candidate) => candidate.length > 0);
	if (!line) return "Running ACP prompt";
	return `ACP: ${line.length > 120 ? `${line.slice(0, 117)}...` : line}`;
}
function createQuietCaptureOutput() {
	const chunks = [];
	let sessionId;
	const formatter = createOutputFormatter("quiet", { stdout: { write(chunk) {
		chunks.push(chunk);
	} } });
	const setContext = formatter.setContext.bind(formatter);
	formatter.setContext = (context) => {
		sessionId = context.sessionId ?? sessionId;
		setContext(context);
	};
	return {
		formatter,
		read: () => chunks.join("").trim(),
		sessionId: () => sessionId
	};
}
async function resolveFlowRunTitle(flow, input, flowPath) {
	const titleDefinition = flow.run?.title;
	if (titleDefinition === void 0) return;
	return normalizeFlowRunTitle(typeof titleDefinition === "function" ? await Promise.resolve(titleDefinition({
		input,
		flowName: flow.name,
		flowPath
	})) : titleDefinition);
}
function normalizeFlowRunTitle(value) {
	const trimmed = value?.trim();
	return trimmed ? trimmed : void 0;
}
function createRunId(flowName) {
	return `${isoNow$1().replaceAll(":", "").replaceAll(".", "")}-${slugifyAsciiIdPart(flowName)}-${randomUUID().slice(0, 8)}`;
}
function createSessionBindingKey(agentCommand, cwd, handle, agentArgv) {
	return JSON.stringify([
		agentCommand,
		agentArgv ?? null,
		cwd,
		handle
	]);
}
function createSessionName(flowName, handle, cwd, runId) {
	return `${flowName}-${handle}-${stableShortHash(cwd)}-${runId.slice(-8)}`;
}
function createSessionBundleId(handle, key) {
	return `${slugifyAsciiIdPart(handle) || "session"}-${stableShortHash(key)}`;
}
function slugifyAsciiIdPart(value) {
	let slug = "";
	let lastWasSeparator = false;
	for (const char of value) {
		const safeChar = toLowerAsciiAlphaNumeric(char);
		if (safeChar) {
			slug += safeChar;
			lastWasSeparator = false;
			continue;
		}
		if (slug.length > 0 && !lastWasSeparator) {
			slug += "-";
			lastWasSeparator = true;
		}
	}
	return lastWasSeparator ? slug.slice(0, -1) : slug;
}
function toLowerAsciiAlphaNumeric(char) {
	const code = char.charCodeAt(0);
	if (code >= 48 && code <= 57) return char;
	if (code >= 65 && code <= 90) return String.fromCharCode(code + 32);
	if (code >= 97 && code <= 122) return char;
	return null;
}
function createIsolatedSessionBinding(flowName, runId, attemptId, profile, agent) {
	const key = `isolated::${attemptId}`;
	const handle = "isolated";
	return {
		key,
		handle,
		bundleId: createSessionBundleId(`${handle}-${attemptId}`, `${key}::${agent.cwd}`),
		name: `${flowName}-${attemptId}-${runId.slice(-8)}`,
		profile,
		agentName: agent.agentName,
		agentCommand: agent.agentCommand,
		agentArgv: agent.agentArgv,
		cwd: agent.cwd,
		acpxRecordId: key,
		acpSessionId: key
	};
}
function createSyntheticSessionRecord(options) {
	return {
		schema: SESSION_RECORD_SCHEMA,
		acpxRecordId: options.binding.acpxRecordId,
		acpSessionId: options.binding.acpSessionId,
		agentSessionId: options.binding.agentSessionId,
		agentCommand: options.binding.agentCommand,
		agentArgv: options.binding.agentArgv,
		cwd: options.binding.cwd,
		name: options.binding.name,
		createdAt: options.createdAt,
		lastUsedAt: options.updatedAt,
		lastSeq: options.lastSeq,
		lastRequestId: void 0,
		eventLog: defaultSessionEventLog(options.binding.acpxRecordId),
		closed: true,
		closedAt: options.updatedAt,
		title: options.conversation.title,
		messages: options.conversation.messages,
		updated_at: options.conversation.updated_at,
		cumulative_token_usage: options.conversation.cumulative_token_usage,
		cumulative_cost: options.conversation.cumulative_cost,
		request_token_usage: options.conversation.request_token_usage,
		acpx: options.acpxState
	};
}
function createNodeResult(options) {
	return {
		attemptId: options.attemptId,
		nodeId: options.nodeId,
		nodeType: options.nodeType,
		outcome: options.outcome,
		startedAt: options.startedAt,
		finishedAt: options.finishedAt,
		durationMs: new Date(options.finishedAt).getTime() - new Date(options.startedAt).getTime(),
		output: options.output,
		error: options.error
	};
}
function outcomeForError(error) {
	if (error instanceof TimeoutError) return "timed_out";
	if (error instanceof InterruptedError) return "cancelled";
	return "failed";
}
function stableShortHash(value) {
	return createHash("sha1").update(value).digest("hex").slice(0, 8);
}
function nextAttemptId(attemptCounts, nodeId) {
	const next = (attemptCounts.get(nodeId) ?? 0) + 1;
	attemptCounts.set(nodeId, next);
	return `${nodeId}#${next}`;
}
function createNodeOutcomePayload(result, trace) {
	return {
		nodeType: result.nodeType,
		outcome: result.outcome,
		durationMs: result.durationMs,
		error: result.error ?? null,
		...trace
	};
}
function attachStepTrace(error, trace) {
	const attached = error instanceof Error ? error : new Error(typeof error === "string" ? error : String(error));
	attached.flowStepTrace = trace;
	return attached;
}
function extractAttachedStepTrace(error) {
	if (!(error instanceof Error)) return;
	return error.flowStepTrace;
}
function toInlineOutput(value) {
	if (value == null || typeof value === "number" || typeof value === "boolean") return value;
	if (typeof value === "string") return isInlineSerializableText(value) ? value : void 0;
	try {
		if (isInlineSerializableText(JSON.stringify(value))) return value;
	} catch {
		return;
	}
}
function isInlineSerializableText(value) {
	return value.length <= 200 && !value.includes("\n");
}
function outputArtifactMediaType(value) {
	return typeof value === "string" ? "text/plain" : "application/json";
}
function outputArtifactExtension(value) {
	return typeof value === "string" ? "txt" : "json";
}
function findConversationDeltaStart(before, after) {
	const maxOverlap = Math.min(before.length, after.length);
	for (let overlap = maxOverlap; overlap >= 0; overlap -= 1) {
		let matches = true;
		for (let index = 0; index < overlap; index += 1) {
			const beforeMessage = before[before.length - overlap + index];
			const afterMessage = after[index];
			if (!deepEqualJson(beforeMessage, afterMessage)) {
				matches = false;
				break;
			}
		}
		if (matches) return overlap;
	}
	return 0;
}
function deepEqualJson(left, right) {
	return JSON.stringify(left) === JSON.stringify(right);
}
//#endregion
//#region src/flows/store.ts
const FLOW_BUNDLE_SCHEMA = "acpx.flow-run-bundle.v1";
const FLOW_TRACE_SCHEMA = "acpx.flow-trace-event.v1";
const FLOW_SNAPSHOT_SCHEMA = "acpx.flow-definition-snapshot.v1";
const MANIFEST_PATH = "manifest.json";
const FLOW_SNAPSHOT_PATH = "flow.json";
const TRACE_PATH = "trace.ndjson";
const PROJECTIONS_DIR = "projections";
const RUN_PROJECTION_PATH = "projections/run.json";
const LIVE_PROJECTION_PATH = "projections/live.json";
const STEPS_PROJECTION_PATH = "projections/steps.json";
const SESSIONS_DIR = "sessions";
const ARTIFACTS_DIR = "artifacts";
function flowRunsBaseDir(homeDir = os.homedir()) {
	return path.join(homeDir, ".acpx", "flows", "runs");
}
var FlowRunStore = class {
	outputRoot;
	traceSeqByRun = /* @__PURE__ */ new Map();
	sessionSeqByBundle = /* @__PURE__ */ new Map();
	manifestByRun = /* @__PURE__ */ new Map();
	writeChainByPath = /* @__PURE__ */ new Map();
	constructor(outputRoot = flowRunsBaseDir()) {
		this.outputRoot = outputRoot;
	}
	releaseRun(runDir) {
		this.traceSeqByRun.delete(runDir);
		this.manifestByRun.delete(runDir);
		for (const key of this.sessionSeqByBundle.keys()) if (key.startsWith(`${runDir}::`)) this.sessionSeqByBundle.delete(key);
	}
	async createRunDir(runId) {
		const runDir = path.join(this.outputRoot, runId);
		await fs.mkdir(path.join(runDir, PROJECTIONS_DIR), {
			recursive: true,
			mode: 448
		});
		await fs.mkdir(path.join(runDir, SESSIONS_DIR), {
			recursive: true,
			mode: 448
		});
		await fs.mkdir(path.join(runDir, ARTIFACTS_DIR), {
			recursive: true,
			mode: 448
		});
		this.traceSeqByRun.set(runDir, 0);
		return runDir;
	}
	async initializeRunBundle(runDir, options) {
		const snapshot = createFlowDefinitionSnapshot(options.flow);
		const manifest = createRunManifest(options.state);
		this.manifestByRun.set(runDir, manifest);
		await writePrivateJsonFile(this.resolveRunPath(runDir, FLOW_SNAPSHOT_PATH), snapshot);
		await writePrivateJsonFile(this.resolveRunPath(runDir, MANIFEST_PATH), manifest);
		await writePrivateJsonFile(this.resolveRunPath(runDir, RUN_PROJECTION_PATH), options.state);
		await writePrivateJsonFile(this.resolveRunPath(runDir, LIVE_PROJECTION_PATH), createLiveState(options.state));
		await writePrivateJsonFile(this.resolveRunPath(runDir, STEPS_PROJECTION_PATH), options.state.steps);
		await ensureFile(this.resolveRunPath(runDir, TRACE_PATH));
		await this.appendTrace(runDir, options.state, {
			scope: "run",
			type: "run_started",
			payload: {
				flowName: options.state.flowName,
				...options.state.runTitle ? { runTitle: options.state.runTitle } : {},
				...options.state.flowPath ? { flowPath: options.state.flowPath } : {},
				...options.inputArtifact ? { inputArtifact: options.inputArtifact } : {}
			}
		});
	}
	async writeSnapshot(runDir, state, event) {
		state.updatedAt = isoNow();
		await writePrivateJsonFile(this.resolveRunPath(runDir, RUN_PROJECTION_PATH), state);
		await writePrivateJsonFile(this.resolveRunPath(runDir, LIVE_PROJECTION_PATH), createLiveState(state));
		await writePrivateJsonFile(this.resolveRunPath(runDir, STEPS_PROJECTION_PATH), state.steps);
		await this.writeManifest(runDir, state);
		await this.appendTrace(runDir, state, event);
	}
	async writeLive(runDir, state, event) {
		state.updatedAt = isoNow();
		await writePrivateJsonFile(this.resolveRunPath(runDir, LIVE_PROJECTION_PATH), createLiveState(state));
		await this.writeManifest(runDir, state);
		await this.appendTrace(runDir, state, event);
	}
	async appendTrace(runDir, state, event) {
		const traceEvent = {
			seq: this.nextTraceSeq(runDir),
			at: isoNow(),
			runId: state.runId,
			...event
		};
		await this.appendJsonLine(this.resolveRunPath(runDir, TRACE_PATH), traceEvent);
		return traceEvent;
	}
	async writeArtifact(runDir, state, content, options) {
		const buffer = toArtifactBuffer(content, options.mediaType);
		const sha256 = createHash("sha256").update(buffer).digest("hex");
		const relativePath = path.posix.join(ARTIFACTS_DIR, `sha256-${sha256}${normalizeArtifactExtension(options.extension)}`);
		const filePath = this.resolveRunPath(runDir, relativePath);
		try {
			await fs.access(filePath);
		} catch {
			await writePrivateFile(filePath, buffer);
		}
		const artifact = {
			path: relativePath,
			mediaType: options.mediaType,
			bytes: buffer.byteLength,
			sha256
		};
		if (options.emitTrace !== false) await this.appendTrace(runDir, state, {
			scope: "artifact",
			type: "artifact_written",
			nodeId: options.nodeId,
			attemptId: options.attemptId,
			sessionId: options.sessionId,
			artifact,
			payload: { artifact }
		});
		return artifact;
	}
	async ensureSessionBundle(runDir, state, inputBinding, record) {
		const binding = structuredClone(inputBinding);
		const sessionDir = this.resolveRunPath(runDir, sessionDirPath(binding.bundleId));
		await this.serializeWrite(path.join(sessionDir, "binding.json"), async () => {
			await fs.mkdir(sessionDir, {
				recursive: true,
				mode: 448
			});
			await writePrivateJsonFile(path.join(sessionDir, "binding.json"), binding);
			await ensureFile(path.join(sessionDir, "events.ndjson"));
			if (record) await this.writeSessionRecord(runDir, state, binding, record);
			const manifest = this.getManifest(runDir, state);
			if (!manifest.sessions.find((entry) => entry.id === binding.bundleId)) {
				const bindingArtifact = await this.writeArtifact(runDir, state, binding, {
					mediaType: "application/json",
					extension: "json",
					emitTrace: false
				});
				const entry = {
					id: binding.bundleId,
					handle: binding.handle,
					bindingPath: path.posix.join(sessionDirPath(binding.bundleId), "binding.json"),
					recordPath: path.posix.join(sessionDirPath(binding.bundleId), "record.json"),
					eventsPath: path.posix.join(sessionDirPath(binding.bundleId), "events.ndjson")
				};
				manifest.sessions.push(entry);
				await writePrivateJsonFile(this.resolveRunPath(runDir, MANIFEST_PATH), manifest);
				await this.appendTrace(runDir, state, {
					scope: "session",
					type: "session_bound",
					sessionId: binding.bundleId,
					payload: {
						sessionId: binding.bundleId,
						handle: binding.handle,
						bindingArtifact
					}
				});
			}
		});
	}
	async writeSessionRecord(runDir, _state, binding, record) {
		const bundledRecord = createBundledSessionRecord(binding, record, this.sessionSeqByBundle.get(`${runDir}::${binding.bundleId}`)?.persisted ?? 0);
		await writePrivateJsonFile(this.resolveRunPath(runDir, path.posix.join(sessionDirPath(binding.bundleId), "record.json")), bundledRecord);
	}
	async appendSessionEvent(runDir, binding, direction, message) {
		const sessionKey = `${runDir}::${binding.bundleId}`;
		const sequence = this.sessionSeqByBundle.get(sessionKey) ?? {
			allocated: 0,
			persisted: 0
		};
		this.sessionSeqByBundle.set(sessionKey, sequence);
		const seq = ++sequence.allocated;
		await this.appendJsonLine(this.resolveRunPath(runDir, path.posix.join(sessionDirPath(binding.bundleId), "events.ndjson")), {
			seq,
			at: isoNow(),
			direction,
			message
		});
		sequence.persisted = Math.max(sequence.persisted, seq);
		return seq;
	}
	getManifest(runDir, state) {
		const existing = this.manifestByRun.get(runDir);
		if (existing) {
			existing.startedAt = state.startedAt;
			existing.finishedAt = state.finishedAt;
			existing.status = state.status;
			existing.flowPath = state.flowPath;
			existing.flowName = state.flowName;
			existing.runTitle = state.runTitle;
			return existing;
		}
		const created = createRunManifest(state);
		this.manifestByRun.set(runDir, created);
		return created;
	}
	async writeManifest(runDir, state) {
		const manifest = this.getManifest(runDir, state);
		await writePrivateJsonFile(this.resolveRunPath(runDir, MANIFEST_PATH), manifest);
	}
	nextTraceSeq(runDir) {
		const next = (this.traceSeqByRun.get(runDir) ?? 0) + 1;
		this.traceSeqByRun.set(runDir, next);
		return next;
	}
	resolveRunPath(runDir, relativePath) {
		return path.join(runDir, ...relativePath.split("/"));
	}
	async appendJsonLine(filePath, value) {
		await this.serializeWrite(filePath, async () => {
			await fs.mkdir(path.dirname(filePath), {
				recursive: true,
				mode: 448
			});
			await appendRegularFile({
				filePath,
				content: `${JSON.stringify(value)}\n`,
				mode: 384
			});
		});
	}
	async serializeWrite(filePath, write) {
		const tracked = (this.writeChainByPath.get(filePath) ?? Promise.resolve()).then(write).finally(() => {
			if (this.writeChainByPath.get(filePath) === tracked) this.writeChainByPath.delete(filePath);
		});
		this.writeChainByPath.set(filePath, tracked);
		await tracked;
	}
};
function createRunManifest(state) {
	return {
		schema: FLOW_BUNDLE_SCHEMA,
		runId: state.runId,
		flowName: state.flowName,
		runTitle: state.runTitle,
		flowPath: state.flowPath,
		startedAt: state.startedAt,
		finishedAt: state.finishedAt,
		status: state.status,
		traceSchema: FLOW_TRACE_SCHEMA,
		paths: {
			flow: FLOW_SNAPSHOT_PATH,
			trace: TRACE_PATH,
			runProjection: RUN_PROJECTION_PATH,
			liveProjection: LIVE_PROJECTION_PATH,
			stepsProjection: STEPS_PROJECTION_PATH,
			sessionsDir: SESSIONS_DIR,
			artifactsDir: ARTIFACTS_DIR
		},
		sessions: []
	};
}
function createLiveState(state) {
	return {
		runId: state.runId,
		flowName: state.flowName,
		runTitle: state.runTitle,
		flowPath: state.flowPath,
		startedAt: state.startedAt,
		finishedAt: state.finishedAt,
		updatedAt: state.updatedAt,
		status: state.status,
		currentNode: state.currentNode,
		currentAttemptId: state.currentAttemptId,
		currentNodeType: state.currentNodeType,
		currentNodeStartedAt: state.currentNodeStartedAt,
		lastHeartbeatAt: state.lastHeartbeatAt,
		statusDetail: state.statusDetail,
		waitingOn: state.waitingOn,
		error: state.error,
		sessionBindings: state.sessionBindings
	};
}
function createFlowDefinitionSnapshot(flow) {
	return {
		schema: FLOW_SNAPSHOT_SCHEMA,
		name: flow.name,
		...flow.run?.title !== void 0 ? { run: { hasTitle: true } } : {},
		...flow.permissions ? { permissions: structuredClone(flow.permissions) } : {},
		startAt: flow.startAt,
		nodes: Object.fromEntries(Object.entries(flow.nodes).map(([nodeId, node]) => [nodeId, snapshotNode(node)])),
		edges: structuredClone(flow.edges)
	};
}
function snapshotNode(node) {
	const common = snapshotCommonNodeFields(node);
	switch (node.nodeType) {
		case "acp": return snapshotAcpNode(node, common);
		case "compute": return {
			...common,
			hasRun: true
		};
		case "action": {
			const actionExecution = "exec" in node ? "shell" : "function";
			return {
				...common,
				actionExecution,
				hasRun: "run" in node,
				hasExec: "exec" in node,
				hasParse: "parse" in node && typeof node.parse === "function"
			};
		}
		case "checkpoint": return {
			...common,
			...node.summary ? { summary: node.summary } : {},
			hasRun: typeof node.run === "function"
		};
	}
	throw new Error(`Unsupported flow node type: ${String(node)}`);
}
function snapshotCommonNodeFields(node) {
	return {
		nodeType: node.nodeType,
		...node.timeoutMs !== void 0 ? { timeoutMs: node.timeoutMs } : {},
		...node.heartbeatMs !== void 0 ? { heartbeatMs: node.heartbeatMs } : {},
		...node.statusDetail ? { statusDetail: node.statusDetail } : {}
	};
}
function snapshotAcpNode(node, common) {
	return {
		...common,
		...node.profile ? { profile: node.profile } : {},
		session: {
			...node.session?.handle ? { handle: node.session.handle } : {},
			...node.session?.isolated ? { isolated: true } : {}
		},
		cwd: snapshotCwd(node.cwd),
		hasPrompt: true,
		hasParse: typeof node.parse === "function"
	};
}
function snapshotCwd(cwd) {
	if (typeof cwd === "function") return { mode: "dynamic" };
	if (typeof cwd === "string") return {
		mode: "static",
		value: cwd
	};
	return { mode: "default" };
}
function createBundledSessionRecord(binding, record, bundleLastSeq) {
	return {
		...structuredClone(record),
		lastSeq: bundleLastSeq,
		eventLog: {
			...structuredClone(record.eventLog),
			active_path: path.posix.join(sessionDirPath(binding.bundleId), "events.ndjson"),
			segment_count: 1,
			max_segments: 1
		}
	};
}
async function ensureFile(filePath) {
	await appendRegularFile({
		filePath,
		content: "",
		mode: 384
	});
}
function toArtifactBuffer(content, mediaType) {
	if (typeof content === "string") return Buffer.from(content, "utf8");
	if (Buffer.isBuffer(content)) return content;
	if (content instanceof Uint8Array) return Buffer.from(content);
	if (mediaType === "application/json") return Buffer.from(`${JSON.stringify(content, null, 2)}\n`, "utf8");
	return Buffer.from(String(content), "utf8");
}
function normalizeArtifactExtension(extension) {
	if (!extension) return "";
	return extension.startsWith(".") ? extension : `.${extension}`;
}
function sessionDirPath(bundleId) {
	return path.posix.join(SESSIONS_DIR, bundleId);
}
function isoNow() {
	return (/* @__PURE__ */ new Date()).toISOString();
}
//#endregion
//#region src/flows/runtime.ts
const DEFAULT_FLOW_HEARTBEAT_MS = 5e3;
const DEFAULT_FLOW_STEP_TIMEOUT_MS = 9e5;
function setNodeValue(values, nodeId, value) {
	Object.defineProperty(values, nodeId, {
		value,
		enumerable: true,
		configurable: true,
		writable: true
	});
}
var FlowRunner = class {
	resolveAgent;
	defaultCwd;
	connectionOptions;
	defaultNodeTimeoutMs;
	suppressSdkConsoleErrors;
	sessionOptions;
	services;
	store;
	pendingPersistentSessionClients = /* @__PURE__ */ new Map();
	attempts = /* @__PURE__ */ new Map();
	pendingClientReleases = /* @__PURE__ */ new WeakMap();
	runInterruptions = /* @__PURE__ */ new Map();
	shellOwners = /* @__PURE__ */ new Map();
	constructor(options) {
		this.resolveAgent = options.resolveAgent;
		this.defaultCwd = options.resolveAgent(void 0).cwd;
		this.connectionOptions = {
			permissionMode: options.permissionMode,
			mcpServers: options.mcpServers,
			nonInteractivePermissions: options.nonInteractivePermissions,
			permissionPolicy: options.permissionPolicy,
			authCredentials: options.authCredentials,
			authPolicy: options.authPolicy,
			fs: options.fs,
			verbose: options.verbose
		};
		this.defaultNodeTimeoutMs = options.defaultNodeTimeoutMs ?? options.timeoutMs ?? DEFAULT_FLOW_STEP_TIMEOUT_MS;
		this.suppressSdkConsoleErrors = options.suppressSdkConsoleErrors;
		this.sessionOptions = options.sessionOptions;
		this.services = options.services ?? {};
		this.store = new FlowRunStore(options.outputRoot);
	}
	async run(flow, input, options = {}) {
		validateFlowDefinition(flow);
		const runId = createRunId(flow.name);
		const runTitle = await resolveFlowRunTitle(flow, input, options.flowPath);
		const runDir = await this.store.createRunDir(runId);
		const state = {
			runId,
			flowName: flow.name,
			runTitle,
			flowPath: options.flowPath,
			startedAt: isoNow$1(),
			updatedAt: isoNow$1(),
			status: "running",
			input,
			outputs: {},
			results: {},
			steps: [],
			sessionBindings: {}
		};
		try {
			const inputArtifact = await this.store.writeArtifact(runDir, state, input, {
				mediaType: "application/json",
				extension: "json",
				emitTrace: false
			});
			await this.store.initializeRunBundle(runDir, {
				flow,
				state,
				inputArtifact
			});
			return await this.runWithOwnership(flow, input, runDir, state);
		} finally {
			try {
				await this.closePendingPersistentSessionClients(runDir);
			} finally {
				this.store.releaseRun(runDir);
			}
		}
	}
	async runWithOwnership(flow, input, runDir, state) {
		let execution;
		let cancellation;
		let interruption;
		const result = await withInterrupt(() => {
			execution = this.executeFlowRun(flow, input, runDir, state);
			return execution;
		}, async (signal) => {
			const reason = interruption ?? new InterruptedError();
			interruption = reason;
			this.runInterruptions.set(runDir, reason);
			cancellation ??= this.cancelShellOwners(runDir, signal);
			cancellation.catch(() => {});
			this.attempts.get(runDir)?.cancel(reason, signal);
			await execution?.catch(() => {});
		}).then((value) => ({
			ok: true,
			value
		}), (error) => ({
			ok: false,
			error
		}));
		try {
			try {
				await cancellation;
			} catch (cleanupError) {
				const failure = result.ok || result.error === cleanupError ? cleanupError : new AggregateError([result.error, cleanupError], "Shell cleanup failed during interruption", { cause: cleanupError });
				await persistRunFailure(this.store, runDir, state, failure);
				throw failure;
			}
			if (result.ok && !interruption) return result.value;
			const failure = result.ok ? interruption : result.error;
			if (interruption) await persistRunFailure(this.store, runDir, state, failure);
			throw failure;
		} finally {
			this.releaseShellOwners(runDir);
		}
	}
	registerShellOwner(runDir, owner) {
		let owners = this.shellOwners.get(runDir);
		if (!owners) {
			owners = /* @__PURE__ */ new Set();
			this.shellOwners.set(runDir, owners);
		}
		owners.add(owner);
		const registered = owners;
		return () => {
			registered.delete(owner);
			if (registered.size === 0) this.shellOwners.delete(runDir);
		};
	}
	async cancelShellOwners(runDir, signal) {
		const owners = [...this.shellOwners.get(runDir) ?? []];
		const results = await Promise.allSettled(owners.map((owner) => owner.cancel(signal)));
		const errors = [];
		for (const result of results) if (result.status === "rejected") errors.push(result.reason);
		if (errors.length > 0) throw new AggregateError(errors, "Shell process cleanup failed", { cause: errors[0] });
	}
	releaseShellOwners(runDir) {
		for (const owner of this.shellOwners.get(runDir) ?? []) owner.release();
		this.shellOwners.delete(runDir);
	}
	async executeFlowRun(flow, input, runDir, state) {
		let current = flow.startAt;
		const attemptCounts = /* @__PURE__ */ new Map();
		try {
			while (current !== null) {
				this.throwIfRunInterrupted(runDir);
				const step = await this.executeFlowStep(flow, input, runDir, state, current, attemptCounts);
				this.throwIfRunInterrupted(runDir, step.executionError);
				const waiting = await this.maybeCompleteCheckpointStep(runDir, state, step);
				if (waiting) return waiting;
				await this.recordFlowStepOutcome(runDir, state, step);
				current = this.resolveNextNode(flow, step);
			}
			return await this.completeFlowRun(runDir, state);
		} catch (error) {
			if (!this.runInterruptions.has(runDir)) await persistRunFailure(this.store, runDir, state, error);
			throw error;
		} finally {
			this.runInterruptions.delete(runDir);
		}
	}
	throwIfRunInterrupted(runDir, error) {
		const interrupted = this.runInterruptions.get(runDir);
		if (interrupted) throw error ?? interrupted;
	}
	async executeFlowStep(flow, input, runDir, state, nodeId, attemptCounts) {
		if (!Object.hasOwn(flow.nodes, nodeId)) throw new Error(`Unknown flow node: ${nodeId}`);
		const node = flow.nodes[nodeId];
		const attemptId = nextAttemptId(attemptCounts, nodeId);
		const startedAt = isoNow$1();
		markNodeStarted(state, nodeId, attemptId, node.nodeType, startedAt, node.statusDetail);
		const attempt = new FlowAttempt({
			nodeId,
			attemptId,
			startedAt,
			timeoutMs: node.timeoutMs ?? this.defaultNodeTimeoutMs
		});
		this.attempts.set(runDir, attempt);
		const context = this.makeAttemptContext(runDir, state, input, node, attempt);
		let stopHeartbeat = () => {};
		let executed;
		let outcome = "ok";
		let executionError;
		try {
			this.throwIfRunInterrupted(runDir);
			executed = await attempt.run(async () => {
				await attempt.own(() => this.writeNodeStartedSnapshot(runDir, state, nodeId, attemptId, node));
				stopHeartbeat = this.startHeartbeat(runDir, state, node, attempt);
				const result = await this.executeNode(runDir, state, flow, nodeId, node, context);
				result.trace = await attempt.own(() => finalizeStepTrace(this.store, runDir, state, nodeId, attemptId, result.output, result.trace));
				return result;
			});
			this.throwIfRunInterrupted(runDir);
		} catch (error) {
			outcome = outcomeForError(error);
			executionError = error;
			executed = {
				output: void 0,
				promptText: null,
				rawText: null,
				sessionInfo: null,
				agentInfo: null,
				...context.acpResult,
				trace: await finalizeStepTrace(this.store, runDir, state, nodeId, attemptId, void 0, context.acpResult ? context.acpResult.trace : extractAttachedStepTrace(error) ?? null)
			};
		} finally {
			stopHeartbeat();
			this.attempts.delete(runDir);
		}
		const nodeResult = createNodeResult({
			attemptId,
			nodeId,
			nodeType: node.nodeType,
			outcome,
			startedAt,
			finishedAt: isoNow$1(),
			...outcome === "ok" ? { output: executed.output } : { error: executionError instanceof Error ? executionError.message : String(executionError) }
		});
		setNodeValue(state.results, nodeId, nodeResult);
		return {
			...executed,
			nodeResult,
			executionError,
			attemptId,
			nodeId,
			node,
			startedAt,
			state
		};
	}
	makeAttemptContext(runDir, state, input, node, attempt) {
		const context = {
			nodeContext: {
				...makeFlowNodeContext(state, input, this.services),
				signal: attempt.signal
			},
			attempt
		};
		if (node.nodeType === "action" && "run" in node) context.nodeContext.runShell = (execution) => this.runCallbackShell(runDir, attempt, execution);
		return context;
	}
	async writeNodeStartedSnapshot(runDir, state, nodeId, attemptId, node) {
		await this.store.writeSnapshot(runDir, state, {
			scope: "node",
			type: "node_started",
			nodeId,
			attemptId,
			payload: {
				nodeType: node.nodeType,
				timeoutMs: node.timeoutMs ?? this.defaultNodeTimeoutMs,
				...state.statusDetail ? { statusDetail: state.statusDetail } : {}
			}
		});
	}
	async maybeCompleteCheckpointStep(runDir, state, step) {
		if (step.nodeResult.outcome !== "ok" || step.node.nodeType !== "checkpoint") return;
		setNodeValue(state.outputs, step.nodeId, step.output);
		state.waitingOn = step.nodeId;
		state.updatedAt = isoNow$1();
		state.status = "waiting";
		await this.recordFlowStepOutcome(runDir, state, step, { statusDetail: step.output?.summary ?? step.nodeId });
		return {
			runDir,
			state
		};
	}
	resolveNextNode(flow, step) {
		if (step.nodeResult.outcome === "ok") {
			setNodeValue(step.state.outputs, step.nodeId, step.output);
			return resolveNext(flow.edges, step.nodeId, step.output, step.nodeResult);
		}
		const next = resolveNext(flow.edges, step.nodeId, void 0, step.nodeResult);
		if (next !== null) return next;
		throw step.executionError;
	}
	async completeFlowRun(runDir, state) {
		state.status = "completed";
		state.finishedAt = isoNow$1();
		state.updatedAt = state.finishedAt;
		clearActiveNode(state);
		await this.store.writeSnapshot(runDir, state, {
			scope: "run",
			type: "run_completed",
			payload: { status: state.status }
		});
		return {
			runDir,
			state
		};
	}
	async recordFlowStepOutcome(runDir, state, step, overrides = {}) {
		state.updatedAt = isoNow$1();
		clearActiveNode(state, overrides.statusDetail);
		state.steps.push({
			attemptId: step.attemptId,
			nodeId: step.nodeId,
			nodeType: step.node.nodeType,
			outcome: step.nodeResult.outcome,
			startedAt: step.startedAt,
			finishedAt: step.nodeResult.finishedAt,
			promptText: step.promptText,
			rawText: step.rawText,
			output: step.output,
			error: step.nodeResult.error,
			session: step.sessionInfo,
			agent: step.agentInfo,
			...step.trace ? { trace: step.trace } : {}
		});
		await this.store.writeSnapshot(runDir, state, {
			scope: "node",
			type: "node_outcome",
			nodeId: step.nodeId,
			attemptId: step.attemptId,
			payload: createNodeOutcomePayload(step.nodeResult, step.trace)
		});
	}
	async executeNode(runDir, state, flow, nodeId, node, context) {
		switch (node.nodeType) {
			case "compute": return await this.executeCallbackNode(nodeId, node, context);
			case "action": return "run" in node ? await this.executeCallbackNode(nodeId, node, context) : await this.executeShellNode(runDir, state, node, context);
			case "checkpoint": return await this.executeCallbackNode(nodeId, node, context);
			case "acp": return await this.executeAcpNode(runDir, state, flow, node, context);
			default: throw new Error(`Unsupported flow node: ${String(node)}`);
		}
	}
	async executeCallbackNode(nodeId, node, context) {
		context.attempt.assertActive();
		const output = node.nodeType === "checkpoint" && typeof node.run !== "function" ? {
			checkpoint: nodeId,
			summary: node.summary ?? nodeId
		} : await node.run?.(context.nodeContext);
		context.attempt.assertActive();
		return {
			output,
			promptText: null,
			rawText: null,
			sessionInfo: null,
			agentInfo: null,
			trace: node.nodeType === "action" ? { action: { actionType: "function" } } : null
		};
	}
	async executeShellNode(runDir, state, node, context) {
		const attempt = context.attempt;
		const execution = await node.exec(context.nodeContext);
		attempt.assertActive();
		const effectiveExecution = {
			...execution,
			cwd: resolveShellActionCwd(this.defaultCwd, execution.cwd),
			timeoutMs: resolveShellActionTimeoutMs(execution.timeoutMs ?? attempt.remainingTimeoutMs())
		};
		updateStatusDetail(state, formatShellActionSummary(effectiveExecution));
		await attempt.own(() => this.store.writeLive(runDir, state, {
			scope: "node",
			type: "node_heartbeat",
			nodeId: attempt.nodeId,
			attemptId: attempt.attemptId,
			payload: { statusDetail: state.statusDetail }
		}));
		await attempt.own(() => this.store.appendTrace(runDir, state, {
			scope: "action",
			type: "action_prepared",
			nodeId: attempt.nodeId,
			attemptId: attempt.attemptId,
			payload: { action: {
				actionType: "shell",
				command: effectiveExecution.command,
				args: effectiveExecution.args ?? [],
				cwd: effectiveExecution.cwd
			} }
		}));
		const result = await attempt.own(() => runShellAction(effectiveExecution, this.shellControl(runDir, attempt)));
		const stdoutArtifact = await attempt.own(() => this.store.writeArtifact(runDir, state, result.stdout, {
			mediaType: "text/plain",
			extension: "txt",
			nodeId: attempt.nodeId,
			attemptId: attempt.attemptId
		}));
		const stderrArtifact = await attempt.own(() => this.store.writeArtifact(runDir, state, result.stderr, {
			mediaType: "text/plain",
			extension: "txt",
			nodeId: attempt.nodeId,
			attemptId: attempt.attemptId
		}));
		const actionTrace = {
			actionType: "shell",
			command: result.command,
			args: result.args,
			cwd: result.cwd,
			exitCode: result.exitCode,
			signal: result.signal,
			durationMs: result.durationMs
		};
		await attempt.own(() => this.store.appendTrace(runDir, state, {
			scope: "action",
			type: "action_completed",
			nodeId: attempt.nodeId,
			attemptId: attempt.attemptId,
			payload: {
				action: actionTrace,
				stdoutArtifact,
				stderrArtifact
			}
		}));
		const trace = {
			action: actionTrace,
			stdoutArtifact,
			stderrArtifact
		};
		let output;
		try {
			output = node.parse ? await node.parse(result, context.nodeContext) : result;
			attempt.assertActive();
		} catch (error) {
			throw attachStepTrace(error, trace);
		}
		return {
			output,
			promptText: null,
			rawText: result.combinedOutput,
			sessionInfo: null,
			agentInfo: null,
			trace
		};
	}
	shellControl(runDir, attempt) {
		return {
			signal: attempt.signal,
			get terminationSignal() {
				return attempt.terminationSignal;
			},
			registerOwner: (owner) => {
				const releaseAttempt = attempt.registerCancellation((signal) => owner.cancel(signal));
				const releaseRun = this.registerShellOwner(runDir, owner);
				return () => {
					releaseAttempt();
					releaseRun();
				};
			}
		};
	}
	runCallbackShell(runDir, attempt, execution) {
		return attempt.own(() => runShellCommand({
			...execution,
			cwd: resolveShellActionCwd(this.defaultCwd, execution.cwd)
		}, this.shellControl(runDir, attempt)));
	}
	async executeAcpNode(runDir, state, flow, node, context) {
		const attempt = context.attempt;
		const prepared = await this.prepareAcpPrompt(runDir, state, node, context);
		if (node.session?.isolated) return await this.executeIsolatedAcpPrompt(runDir, state, flow, node, context, prepared);
		const boundSession = await this.ensureSessionBinding(runDir, state, flow, node, prepared.agentInfo, attempt);
		return await this.executePersistentAcpPrompt(runDir, state, node, context, prepared, boundSession);
	}
	async prepareAcpPrompt(runDir, state, node, context) {
		const resolvedAgent = this.resolveAgent(node.profile);
		const agentInfo = {
			...resolvedAgent,
			agentArgv: resolvedAgent.agentArgv?.slice(),
			cwd: await resolveNodeCwd(resolvedAgent.cwd, node.cwd, context.nodeContext)
		};
		context.attempt.assertActive();
		const prompt = normalizePromptInput(await Promise.resolve(node.prompt(context.nodeContext)));
		context.attempt.assertActive();
		const promptText = promptToDisplayText(prompt);
		const result = {
			output: void 0,
			promptText,
			rawText: null,
			sessionInfo: null,
			agentInfo,
			trace: null
		};
		context.acpResult = result;
		updateStatusDetail(state, summarizePrompt(promptText, node.statusDetail));
		await context.attempt.own(() => this.writeAcpPromptHeartbeat(runDir, state, context.attempt));
		const promptArtifact = await context.attempt.own(() => this.store.writeArtifact(runDir, state, promptText, {
			mediaType: "text/plain",
			extension: "txt",
			nodeId: context.attempt.nodeId,
			attemptId: context.attempt.attemptId
		}));
		result.trace = { promptArtifact };
		return {
			agentInfo,
			prompt,
			promptArtifact,
			attempt: context.attempt,
			result
		};
	}
	async writeAcpPromptHeartbeat(runDir, state, attempt) {
		await this.store.writeLive(runDir, state, {
			scope: "node",
			type: "node_heartbeat",
			nodeId: attempt.nodeId,
			attemptId: attempt.attemptId,
			payload: { statusDetail: state.statusDetail }
		});
	}
	async executeIsolatedAcpPrompt(runDir, state, flow, node, context, prepared) {
		const binding = createIsolatedSessionBinding(flow.name, state.runId, prepared.attempt.attemptId, node.profile, prepared.agentInfo);
		prepared.result.sessionInfo = binding;
		await prepared.attempt.own(() => this.initializeIsolatedSessionBundle(runDir, state, binding, prepared.attempt));
		await prepared.attempt.own(() => this.appendAcpPromptPreparedTrace(runDir, state, binding, prepared.promptArtifact, prepared.attempt));
		const prompt = await prepared.attempt.own(() => this.runIsolatedPrompt(runDir, state, binding, prepared));
		return await this.finishAcpPrompt(runDir, state, node, context, prepared, prompt);
	}
	async initializeIsolatedSessionBundle(runDir, state, binding, attempt) {
		const timestamp = attempt.startedAt;
		const initialRecord = createSyntheticSessionRecord({
			binding,
			createdAt: timestamp,
			updatedAt: timestamp,
			conversation: createSessionConversation(timestamp),
			acpxState: void 0,
			lastSeq: 0
		});
		await this.store.ensureSessionBundle(runDir, state, binding, initialRecord);
	}
	async executePersistentAcpPrompt(runDir, state, node, context, prepared, binding) {
		prepared.result.sessionInfo = binding;
		await prepared.attempt.own(() => this.appendAcpPromptPreparedTrace(runDir, state, binding, prepared.promptArtifact, prepared.attempt));
		const prompt = await prepared.attempt.own(() => this.runPersistentPrompt(runDir, state, binding, prepared));
		return await this.finishAcpPrompt(runDir, state, node, context, prepared, prompt);
	}
	async appendAcpPromptPreparedTrace(runDir, state, binding, promptArtifact, attempt) {
		await this.store.appendTrace(runDir, state, {
			scope: "acp",
			type: "acp_prompt_prepared",
			nodeId: attempt.nodeId,
			attemptId: attempt.attemptId,
			sessionId: binding.bundleId,
			payload: {
				sessionId: binding.bundleId,
				promptArtifact
			}
		});
	}
	async finishAcpPrompt(runDir, state, node, context, prepared, prompt) {
		await prepared.attempt.own(() => this.appendAcpResponseParsedTrace(runDir, state, prompt, prepared.attempt));
		const output = await this.parseAcpOutput(node, context, prompt.rawText);
		return {
			...prepared.result,
			output
		};
	}
	async publishAcpCapture(runDir, state, prepared, sessionInfo, record, messageStart, rawText, events) {
		const result = prepared.result;
		result.sessionInfo = sessionInfo;
		const trace = {
			sessionId: sessionInfo.bundleId,
			promptArtifact: prepared.promptArtifact
		};
		result.trace = trace;
		await this.store.ensureSessionBundle(runDir, state, sessionInfo);
		await this.store.writeSessionRecord(runDir, state, sessionInfo, record);
		if (events) trace.conversation = {
			sessionId: sessionInfo.bundleId,
			messageStart,
			messageEnd: Math.max(messageStart, record.messages.length - 1),
			...events
		};
		const rawResponseArtifact = await this.store.writeArtifact(runDir, state, rawText, {
			mediaType: "text/plain",
			extension: "txt",
			nodeId: prepared.attempt.nodeId,
			attemptId: prepared.attempt.attemptId,
			sessionId: sessionInfo.bundleId
		});
		trace.rawResponseArtifact = rawResponseArtifact;
		return {
			rawText,
			sessionInfo,
			conversation: trace.conversation,
			rawResponseArtifact
		};
	}
	async appendAcpResponseParsedTrace(runDir, state, prompt, attempt) {
		await this.store.appendTrace(runDir, state, {
			scope: "acp",
			type: "acp_response_parsed",
			nodeId: attempt.nodeId,
			attemptId: attempt.attemptId,
			sessionId: prompt.sessionInfo.bundleId,
			payload: {
				sessionId: prompt.sessionInfo.bundleId,
				conversation: prompt.conversation,
				rawResponseArtifact: prompt.rawResponseArtifact
			}
		});
	}
	async parseAcpOutput(node, context, rawText) {
		context.attempt.assertActive();
		const output = node.parse ? await node.parse(rawText, context.nodeContext) : rawText;
		context.attempt.assertActive();
		return output;
	}
	startHeartbeat(runDir, state, node, attempt) {
		const heartbeatMs = Math.max(0, Math.round(node.heartbeatMs ?? DEFAULT_FLOW_HEARTBEAT_MS));
		if (heartbeatMs === 0) return () => {};
		let pending = false;
		const heartbeat = async () => {
			if (!attempt.active || pending) return;
			pending = true;
			try {
				await attempt.own(async () => {
					state.lastHeartbeatAt = isoNow$1();
					state.updatedAt = state.lastHeartbeatAt;
					await this.store.writeLive(runDir, state, {
						scope: "node",
						type: "node_heartbeat",
						nodeId: attempt.nodeId,
						attemptId: attempt.attemptId,
						payload: { statusDetail: state.statusDetail }
					});
				}, { bestEffort: true });
			} catch {} finally {
				pending = false;
			}
		};
		const timer = setInterval(() => {
			heartbeat();
		}, heartbeatMs);
		return () => clearInterval(timer);
	}
	async ensureSessionBinding(runDir, state, flow, node, agent, attempt) {
		const handle = node.session?.handle ?? "main";
		const key = createSessionBindingKey(agent.agentCommand, agent.cwd, handle, agent.agentArgv);
		const existing = state.sessionBindings[key];
		if (existing) {
			await attempt.own(() => this.store.ensureSessionBundle(runDir, state, existing));
			return existing;
		}
		const name = createSessionName(flow.name, handle, agent.cwd, state.runId);
		const created = await attempt.own(async () => {
			const acquired = await createSessionWithClient({
				agentCommand: agent.agentCommand,
				agentArgv: agent.agentArgv,
				cwd: agent.cwd,
				name,
				...this.connectionOptions,
				signal: attempt.signal,
				handleProcessInterrupts: false,
				sessionOptions: this.sessionOptions
			});
			this.pendingClientReleases.set(acquired.client, attempt.registerCancellation(async () => {
				const clients = this.pendingPersistentSessionClients.get(runDir);
				if (clients?.get(key) === acquired.client) clients.delete(key);
				await acquired.client.close();
			}));
			return acquired;
		});
		attempt.assertActive();
		const binding = {
			key,
			handle,
			bundleId: createSessionBundleId(handle, key),
			name,
			profile: node.profile,
			agentName: agent.agentName,
			agentCommand: agent.agentCommand,
			agentArgv: agent.agentArgv,
			cwd: agent.cwd,
			acpxRecordId: created.record.acpxRecordId,
			acpSessionId: created.record.acpSessionId,
			agentSessionId: created.record.agentSessionId
		};
		state.sessionBindings[key] = binding;
		let clients = this.pendingPersistentSessionClients.get(runDir);
		if (!clients) {
			clients = /* @__PURE__ */ new Map();
			this.pendingPersistentSessionClients.set(runDir, clients);
		}
		clients.set(binding.key, created.client);
		await attempt.own(() => this.store.ensureSessionBundle(runDir, state, binding, created.record));
		return binding;
	}
	createPromptEventCapture(runDir, binding) {
		const pending = /* @__PURE__ */ new Set();
		let ordinal = 0;
		let failure;
		let eventStartSeq;
		let eventEndSeq = 0;
		const snapshot = (outcome) => ({
			outcome,
			events: !failure && eventStartSeq !== void 0 ? {
				eventStartSeq,
				eventEndSeq
			} : void 0,
			lastSeq: eventEndSeq
		});
		return {
			onAcpMessage: (direction, message) => {
				const index = ordinal++;
				const write = this.store.appendSessionEvent(runDir, binding, direction, message).then((seq) => {
					eventStartSeq = Math.min(eventStartSeq ?? seq, seq);
					eventEndSeq = Math.max(eventEndSeq, seq);
				}, (reason) => {
					if (!failure || index < failure.ordinal) failure = {
						ordinal: index,
						reason
					};
				}).finally(() => pending.delete(write));
				pending.add(write);
			},
			async run(operation, finalize) {
				let result;
				try {
					result = {
						status: "fulfilled",
						value: await operation()
					};
				} catch (reason) {
					result = {
						status: "rejected",
						reason
					};
				}
				await Promise.all(pending);
				let finalized;
				try {
					finalized = {
						status: "fulfilled",
						value: await finalize(snapshot(result))
					};
				} catch (reason) {
					finalized = {
						status: "rejected",
						reason
					};
				}
				if (result.status === "rejected") throw result.reason;
				if (failure) throw failure.reason;
				if (eventStartSeq === void 0) throw new Error(`Missing ACP event capture for session ${binding.bundleId}`);
				if (finalized.status === "rejected") throw finalized.reason;
				return finalized.value;
			}
		};
	}
	async runPersistentPrompt(runDir, state, binding, prepared) {
		const { attempt, prompt } = prepared;
		const capture = createQuietCaptureOutput();
		const beforeRecord = await resolveSessionRecord(binding.acpxRecordId);
		attempt.assertActive();
		const events = this.createPromptEventCapture(runDir, binding);
		const clients = this.pendingPersistentSessionClients.get(runDir);
		const initialClient = clients?.get(binding.key);
		if (initialClient) {
			clients?.delete(binding.key);
			this.pendingClientReleases.get(initialClient)?.();
			this.pendingClientReleases.delete(initialClient);
		}
		return await events.run(() => sendSessionDirect({
			sessionId: binding.acpxRecordId,
			prompt,
			resumePolicy: "same-session-only",
			...this.connectionOptions,
			outputFormatter: capture.formatter,
			errorEmissionPolicy: { queueErrorAlreadyEmitted: false },
			onAcpMessage: events.onAcpMessage,
			suppressSdkConsoleErrors: this.suppressSdkConsoleErrors,
			client: initialClient
		}, {
			signal: attempt.signal,
			handleProcessInterrupts: false
		}), async (receipt) => {
			const rawText = capture.read();
			prepared.result.rawText = rawText;
			const afterRecord = await resolveSessionRecord(binding.acpxRecordId);
			const sessionInfo = {
				...binding,
				acpSessionId: afterRecord.acpSessionId,
				agentSessionId: afterRecord.agentSessionId
			};
			state.sessionBindings[sessionInfo.key] = sessionInfo;
			return await this.publishAcpCapture(runDir, state, prepared, sessionInfo, afterRecord, findConversationDeltaStart(beforeRecord.messages, afterRecord.messages), rawText, receipt.events);
		});
	}
	async closePendingPersistentSessionClients(runDir) {
		const pendingClients = [...this.pendingPersistentSessionClients.get(runDir)?.values() ?? []];
		this.pendingPersistentSessionClients.delete(runDir);
		const failure = (await Promise.allSettled(pendingClients.map(async (client) => {
			this.pendingClientReleases.get(client)?.();
			this.pendingClientReleases.delete(client);
			await client.close();
		}))).find((result) => result.status === "rejected");
		if (failure?.status === "rejected") throw failure.reason;
	}
	async runIsolatedPrompt(runDir, state, binding, prepared) {
		const { agentInfo: agent, prompt, attempt } = prepared;
		const capture = createQuietCaptureOutput();
		const conversation = createSessionConversation(attempt.startedAt);
		let acpxState;
		recordPromptSubmission(conversation, prompt, attempt.startedAt);
		const events = this.createPromptEventCapture(runDir, binding);
		return await events.run(() => runOnce({
			agentCommand: agent.agentCommand,
			agentArgv: agent.agentArgv,
			cwd: agent.cwd,
			prompt,
			...this.connectionOptions,
			outputFormatter: capture.formatter,
			errorEmissionPolicy: { queueErrorAlreadyEmitted: false },
			onAcpMessage: events.onAcpMessage,
			onSessionUpdate: (notification) => {
				acpxState = recordSessionUpdate(conversation, acpxState, notification);
			},
			onClientOperation: (operation) => {
				acpxState = recordClientOperation(conversation, acpxState, operation);
			},
			suppressSdkConsoleErrors: this.suppressSdkConsoleErrors,
			sessionOptions: this.sessionOptions
		}, {
			signal: attempt.signal,
			handleProcessInterrupts: false
		}), async (receipt) => {
			const rawText = capture.read();
			prepared.result.rawText = rawText;
			const sessionId = receipt.outcome.status === "fulfilled" ? receipt.outcome.value.sessionId : capture.sessionId();
			const sessionInfo = sessionId === void 0 ? binding : {
				...binding,
				acpxRecordId: sessionId,
				acpSessionId: sessionId
			};
			const record = createSyntheticSessionRecord({
				binding: sessionInfo,
				createdAt: attempt.startedAt,
				updatedAt: conversation.updated_at,
				conversation,
				acpxState: cloneSessionAcpxState(acpxState),
				lastSeq: receipt.lastSeq
			});
			return await this.publishAcpCapture(runDir, state, prepared, sessionInfo, record, 0, rawText, receipt.events);
		});
	}
};
//#endregion
//#region src/flows/json.ts
function normalizeJsonText(text) {
	if (typeof text === "string") return text.trim();
	if (text == null) return "";
	if (typeof text === "number" || typeof text === "boolean" || typeof text === "bigint" || typeof text === "symbol") return String(text).trim();
	return "";
}
function parseJsonObject(text, options = {}) {
	const trimmed = normalizeJsonText(text);
	if (!trimmed) throw new Error("Expected JSON output, got empty text");
	const mode = options.mode ?? "compat";
	const direct = tryParse(trimmed);
	if (direct.ok) return direct.value;
	const fenced = parseFencedJsonIfAllowed(trimmed, mode);
	if (fenced.ok) return fenced.value;
	if (mode === "compat") {
		const balanced = parseBalancedJsonCandidate(trimmed);
		if (balanced.ok) return balanced.value;
	}
	throw new Error(`Could not parse JSON from assistant output:\n${trimmed}`);
}
function parseFencedJsonIfAllowed(text, mode) {
	if (mode !== "fenced" && mode !== "compat") return { ok: false };
	const fencedText = extractFencedJsonText(text);
	return fencedText === null ? { ok: false } : tryParse(fencedText);
}
const COMPAT_JSON_WORK_MULTIPLIER = 8;
function consumeRecoveryWork(budget, units) {
	if (units > budget.remaining) {
		budget.remaining = 0;
		return false;
	}
	budget.remaining -= units;
	return true;
}
function parseBalancedJsonCandidate(text) {
	const budget = { remaining: text.length * COMPAT_JSON_WORK_MULTIPLIER };
	for (let index = 0; index < text.length && budget.remaining > 0; index += 1) {
		if (text[index] !== "{" && text[index] !== "[") continue;
		const end = scanBalanced(text, index, budget);
		if (end === null) continue;
		if (!consumeRecoveryWork(budget, end - index)) return { ok: false };
		const parsed = tryParse(text.slice(index, end));
		if (parsed.ok) return parsed;
	}
	return { ok: false };
}
function parseStrictJsonObject(text) {
	return parseJsonObject(text, { mode: "strict" });
}
function extractJsonObject(text) {
	return parseJsonObject(text, { mode: "compat" });
}
function tryParse(text) {
	try {
		return {
			ok: true,
			value: JSON.parse(text)
		};
	} catch {
		return { ok: false };
	}
}
function extractFencedJsonText(text) {
	const openingFenceIndex = text.indexOf("```");
	if (openingFenceIndex === -1) return null;
	let contentStart = openingFenceIndex + 3;
	if (text.slice(contentStart, contentStart + 4).toLowerCase() === "json" && isFenceWhitespace(text[contentStart + 4])) contentStart += 4;
	while (isFenceWhitespace(text[contentStart])) contentStart += 1;
	const closingFenceIndex = text.indexOf("```", contentStart);
	if (closingFenceIndex === -1) return null;
	return text.slice(contentStart, closingFenceIndex).trim();
}
function isFenceWhitespace(char) {
	return char === " " || char === "\n" || char === "\r" || char === "	";
}
function scanBalanced(text, startIndex, budget) {
	const stack = [];
	let inString = false;
	let escaped = false;
	for (let index = startIndex; index < text.length; index += 1) {
		if (!consumeRecoveryWork(budget, 1)) return null;
		const char = text[index];
		if (inString) {
			const next = scanStringChar(char, escaped);
			escaped = next.escaped;
			inString = next.inString;
			continue;
		}
		if (char === "\"") {
			inString = true;
			continue;
		}
		const result = scanBalancedToken(index, char, stack);
		if (result !== SCAN_CONTINUE) return result;
	}
	return null;
}
const SCAN_CONTINUE = Symbol("scan-continue");
function scanBalancedToken(index, char, stack) {
	if (char === "{" || char === "[") {
		stack.push(char);
		return SCAN_CONTINUE;
	}
	if (char !== "}" && char !== "]") return SCAN_CONTINUE;
	if (!balancedClosingTokenMatches(stack.at(-1), char)) return null;
	stack.pop();
	return stack.length === 0 ? index + 1 : SCAN_CONTINUE;
}
function balancedClosingTokenMatches(open, close) {
	if (open === "{") return close === "}";
	if (open === "[") return close === "]";
	return false;
}
function scanStringChar(char, escaped) {
	if (escaped) return {
		escaped: false,
		inString: true
	};
	if (char === "\\") return {
		escaped: true,
		inString: true
	};
	return {
		escaped: false,
		inString: char !== "\""
	};
}
//#endregion
//#region src/flows/decision.ts
const DEFAULT_FIELD = "route";
const SIMPLE_FIELD_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;
function decision(definition) {
	const { question, choices, field: fieldOverride, ...acpOptions } = definition;
	const field = normalizeField(fieldOverride);
	assertValidChoices(choices);
	const allowed = new Set(choices);
	return acp({
		...acpOptions,
		async prompt(context) {
			return formatDecisionPrompt(typeof question === "function" ? await question(context) : question, choices, field);
		},
		parse(text) {
			const raw = extractJsonObject(text);
			if (raw === null || typeof raw !== "object" || Array.isArray(raw)) throw new Error(`Decision response must be a JSON object, got ${typeof raw}`);
			const value = raw[field];
			if (typeof value !== "string" || !allowed.has(value)) {
				const allowedLabels = choices.map((choice) => JSON.stringify(choice)).join(", ");
				throw new Error(`Decision returned invalid ${field}=${JSON.stringify(value)}; expected one of ${allowedLabels}`);
			}
			return raw;
		}
	});
}
function decisionEdge(args) {
	const field = normalizeField(args.field);
	assertValidChoices(args.choices);
	for (const choice of args.choices) if (!Object.hasOwn(args.cases, choice)) throw new Error(`Decision edge is missing case for choice ${JSON.stringify(choice)}`);
	return {
		from: args.from,
		switch: {
			on: `$.${field}`,
			cases: args.cases
		}
	};
}
function assertValidChoices(choices) {
	if (choices.length === 0) throw new Error("Decision choices must include at least one value");
	const seen = /* @__PURE__ */ new Set();
	for (const choice of choices) {
		if (typeof choice !== "string" || choice.length === 0) throw new Error("Decision choices must be non-empty strings");
		if (seen.has(choice)) throw new Error(`Decision choices must be unique; duplicate ${JSON.stringify(choice)}`);
		seen.add(choice);
	}
}
function normalizeField(fieldOverride) {
	const field = fieldOverride ?? DEFAULT_FIELD;
	if (!SIMPLE_FIELD_PATTERN.test(field)) throw new Error(`Decision field must be a simple JSON key matching ${SIMPLE_FIELD_PATTERN.source}`);
	return field;
}
function formatDecisionPrompt(question, choices, field) {
	const allowed = choices.map((choice) => JSON.stringify(choice)).join(" | ");
	return [
		question,
		"",
		"Return exactly one JSON object with this shape:",
		"{",
		`  ${JSON.stringify(field)}: ${allowed},`,
		"  \"reason\": \"short justification\"",
		"}",
		"",
		"Do not include any other text outside the JSON object."
	].join("\n");
}
//#endregion
export { parseStrictJsonObject as a, validateFlowDefinition as c, checkpoint as d, compute as f, isDefinedFlow as h, parseJsonObject as i, acp as l, shell as m, decisionEdge as n, FlowRunner as o, defineFlow as p, extractJsonObject as r, flowRunsBaseDir as s, decision as t, action as u };

//# sourceMappingURL=flows-Bw7cw3Rn.js.map