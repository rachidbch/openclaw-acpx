import { it as permissionModeSatisfies } from "./queue-owner-runtime-ZiKw3KHx.js";
import { C as resolvePermissionMode, S as resolveOutputPolicy, b as resolveAgentInvocation, l as hasExplicitPermissionModeFlag, r as sessionOptionsFromGlobalFlags, t as resolvePermissionPolicyFromFlags, x as resolveGlobalFlags } from "./invocation-options-PTVfiZrm.js";
import { c as validateFlowDefinition, h as isDefinedFlow, o as FlowRunner } from "./flows-BtNRRLBI.js";
import Module, { register } from "node:module";
import { fileURLToPath, pathToFileURL } from "node:url";
import path from "node:path";
import { InvalidArgumentError } from "commander";
import fs from "node:fs/promises";
import { randomUUID } from "node:crypto";
//#region src/flows/module-resolution.ts
const moduleResolver = Module;
const resolveFilenameKey = "_resolveFilename";
let installed = false;
/** Resolve the public flow entrypoint in both ESM and CommonJS dependency graphs. */
function installFlowRuntimeResolution(runtimeUrl) {
	if (installed) return;
	register(`data:text/javascript,${encodeURIComponent(`
    let runtimeUrl;
    export function initialize(data) { runtimeUrl = data.runtimeUrl; }
    export function resolve(specifier, context, nextResolve) {
      return nextResolve(specifier === "acpx/flows" ? runtimeUrl : specifier, context);
    }
  `)}`, { data: { runtimeUrl } });
	const resolve = moduleResolver[resolveFilenameKey];
	const runtimePath = fileURLToPath(runtimeUrl);
	moduleResolver[resolveFilenameKey] = function(specifier, parent, isMain, options) {
		return resolve.call(this, specifier === "acpx/flows" ? runtimePath : specifier, parent, isMain, options);
	};
	installed = true;
}
//#endregion
//#region src/flows/cli.ts
async function handleFlowRun(flowFile, flags, command, config) {
	const globalFlags = resolveGlobalFlags(command, config);
	const permissionMode = resolvePermissionMode(globalFlags, config.defaultPermissions);
	const permissionPolicy = await resolvePermissionPolicyFromFlags(globalFlags);
	const outputPolicy = resolveOutputPolicy(globalFlags.format, globalFlags.jsonStrict === true);
	const input = await readFlowInput(flags);
	const flowPath = path.resolve(flowFile);
	const flow = await loadFlowModule(flowPath);
	assertFlowPermissionRequirements(flow, permissionMode, globalFlags);
	printFlowRunResult(await new FlowRunner({
		resolveAgent: (profile) => {
			return resolveAgentInvocation(profile ?? flags.defaultAgent, globalFlags, config);
		},
		permissionMode,
		mcpServers: config.mcpServers,
		nonInteractivePermissions: globalFlags.nonInteractivePermissions,
		permissionPolicy,
		authCredentials: config.auth,
		authPolicy: globalFlags.authPolicy,
		fs: globalFlags.fs,
		timeoutMs: globalFlags.timeout,
		ttlMs: globalFlags.ttl,
		verbose: globalFlags.verbose,
		suppressSdkConsoleErrors: outputPolicy.suppressSdkConsoleErrors,
		sessionOptions: sessionOptionsFromGlobalFlags(globalFlags)
	}).run(flow, input, { flowPath }), globalFlags);
}
function assertFlowPermissionRequirements(flow, permissionMode, globalFlags) {
	const permissions = flow.permissions;
	if (!permissions) return;
	if (permissions.requireExplicitGrant && !hasExplicitPermissionModeFlag(globalFlags)) throw new InvalidArgumentError(buildFlowPermissionFailureMessage(flow, permissions.requiredMode, permissions.reason, true));
	if (!permissionModeSatisfies(permissionMode, permissions.requiredMode)) throw new InvalidArgumentError(buildFlowPermissionFailureMessage(flow, permissions.requiredMode, permissions.reason, false));
}
function buildFlowPermissionFailureMessage(flow, requiredMode, reason, explicit = false) {
	return [
		explicit ? `Flow "${flow.name}" requires an explicit ${requiredMode} grant.` : `Flow "${flow.name}" requires permission mode ${requiredMode}.`,
		`Rerun with --${requiredMode}.`,
		...reason ? [`Reason: ${reason}`] : []
	].join(" ");
}
async function readFlowInput(flags) {
	if (flags.inputJson && flags.inputFile) throw new InvalidArgumentError("Use only one of --input-json or --input-file");
	if (flags.inputJson) return parseJsonInput(flags.inputJson, "--input-json");
	if (flags.inputFile) {
		const inputPath = path.resolve(flags.inputFile);
		return parseJsonInput(await fs.readFile(inputPath, "utf8"), "--input-file");
	}
	return {};
}
async function loadFlowModule(flowPath) {
	const extension = path.extname(flowPath).toLowerCase();
	installFlowRuntimeResolution(pathToFileURL(resolveFlowRuntimeImportSpecifier()).href);
	const candidate = findFlowDefinition(await loadFlowRuntimeModule(pathToFileURL(flowPath).href, extension));
	if (!candidate) throw new Error(`Flow module must export default defineFlow({...}) from "acpx/flows": ${flowPath}`);
	validateFlowDefinition(candidate);
	return candidate;
}
function resolveFlowRuntimeImportSpecifier() {
	const selfPath = fileURLToPath(import.meta.url);
	let runtimePath;
	if (selfPath.endsWith(`${path.sep}src${path.sep}flows${path.sep}cli.ts`)) runtimePath = fileURLToPath(new URL("../flows.ts", import.meta.url));
	else if (selfPath.endsWith(`${path.sep}src${path.sep}flows${path.sep}cli.js`)) runtimePath = fileURLToPath(new URL("../flows.js", import.meta.url));
	else runtimePath = fileURLToPath(new URL("./flows.js", import.meta.url));
	return runtimePath.replaceAll(path.sep, "/");
}
async function loadFlowRuntimeModule(flowUrl, extension) {
	if (extension === ".ts" || extension === ".tsx" || extension === ".cts") {
		const { register } = await import("tsx/cjs/api");
		const loader = register({ namespace: randomUUID() });
		try {
			return loader.require(flowUrl, import.meta.url);
		} finally {
			loader.unregister();
		}
	}
	if (extension === ".mts") {
		const { register } = await import("tsx/esm/api");
		const loader = register({ namespace: randomUUID() });
		try {
			return await loader.import(flowUrl, import.meta.url);
		} finally {
			await loader.unregister();
		}
	}
	return await import(flowUrl);
}
function findFlowDefinition(module) {
	const candidates = [
		module,
		module.default,
		module["module.exports"],
		getNestedDefault(module.default),
		getNestedDefault(module["module.exports"])
	];
	for (const candidate of candidates) if (isDefinedFlow(candidate)) return candidate;
	return null;
}
function getNestedDefault(value) {
	if (!value || typeof value !== "object" || !("default" in value)) return null;
	return value.default ?? null;
}
function parseJsonInput(raw, label) {
	try {
		return JSON.parse(raw);
	} catch (error) {
		throw new InvalidArgumentError(`${label} must contain valid JSON: ${error instanceof Error ? error.message : String(error)}`);
	}
}
function printFlowRunResult(result, globalFlags) {
	const payload = {
		action: "flow_run_result",
		runId: result.state.runId,
		flowName: result.state.flowName,
		runTitle: result.state.runTitle,
		flowPath: result.state.flowPath,
		status: result.state.status,
		currentNode: result.state.currentNode,
		currentNodeType: result.state.currentNodeType,
		currentNodeStartedAt: result.state.currentNodeStartedAt,
		lastHeartbeatAt: result.state.lastHeartbeatAt,
		statusDetail: result.state.statusDetail,
		waitingOn: result.state.waitingOn,
		runDir: result.runDir,
		outputs: result.state.outputs,
		sessionBindings: result.state.sessionBindings
	};
	if (globalFlags.format === "json") {
		process.stdout.write(`${JSON.stringify(payload)}\n`);
		return;
	}
	if (globalFlags.format === "quiet") {
		process.stdout.write(`${result.state.runId}\n`);
		return;
	}
	process.stdout.write(`runId: ${payload.runId}\n`);
	process.stdout.write(`flow: ${payload.flowName}\n`);
	if (payload.runTitle) process.stdout.write(`title: ${payload.runTitle}\n`);
	process.stdout.write(`status: ${payload.status}\n`);
	process.stdout.write(`runDir: ${payload.runDir}\n`);
	if (payload.currentNode) process.stdout.write(`currentNode: ${payload.currentNode}\n`);
	if (payload.statusDetail) process.stdout.write(`statusDetail: ${payload.statusDetail}\n`);
	if (payload.waitingOn) process.stdout.write(`waitingOn: ${payload.waitingOn}\n`);
	process.stdout.write(`${JSON.stringify(payload.outputs, null, 2)}\n`);
}
//#endregion
export { handleFlowRun };

//# sourceMappingURL=cli-JujOf1El.js.map