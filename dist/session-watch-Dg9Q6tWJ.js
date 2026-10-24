import { i as JsonMessageSanitizer, t as createOutputFormatter } from "./output-kVujrhgv.js";
import { it as isAcpJsonRpcMessage, lt as parsePromptStopReason } from "./ipc-BIMIZYBg.js";
import { t as watchSession } from "./watch-CeXApGbZ.js";
import { once } from "node:events";
//#region src/cli/session-watch.ts
function watchRenderer(policy) {
	const chunks = [];
	const stdout = { write: (chunk) => {
		chunks.push(chunk);
	} };
	let sanitizer = new JsonMessageSanitizer(policy.suppressReads, true);
	let formatter = createOutputFormatter(policy.format, { stdout });
	return (event) => {
		chunks.length = 0;
		if (event.type === "turn_started") {
			sanitizer = new JsonMessageSanitizer(policy.suppressReads, true);
			formatter = createOutputFormatter(policy.format, { stdout });
		}
		if (policy.format === "json") chunks.push(JSON.stringify(event.type === "message" ? {
			...event,
			message: sanitizer.sanitize(event.message)
		} : event) + "\n");
		else if (event.type === "message") renderWatchMessage(event, sanitizer, formatter, policy.format === "quiet");
		else {
			if (event.type === "turn_result") formatter.flush();
			if (policy.format !== "quiet") chunks.push(watchLifecycleLine(event));
		}
		return chunks;
	};
}
function renderWatchMessage(event, sanitizer, formatter, quiet) {
	const message = sanitizer.sanitize(event.message);
	if (!isAcpJsonRpcMessage(message)) throw new Error("Invalid sanitized session message");
	if (quiet || parsePromptStopReason(message) === void 0) formatter.onAcpMessage(message);
}
function watchLifecycleLine(event) {
	if (event.type === "turn_started") return `\n[${event.requestId}] started cursor=${event.cursor}\n`;
	const detail = event.result.status === "failed" ? event.result.error.message : event.result.stopReason;
	return `\n[${event.requestId}] ${event.result.status}${detail ? `: ${detail}` : ""} cursor=${event.cursor}\n`;
}
async function runSessionWatch(record, options) {
	const stopped = new AbortController();
	let outputError;
	const stop = () => stopped.abort();
	const onOutputError = (error) => {
		if (error.code !== "EPIPE") outputError = error;
		stop();
	};
	const render = watchRenderer(options.policy);
	process.on("SIGINT", stop);
	process.on("SIGTERM", stop);
	process.on("SIGHUP", stop);
	process.stdout.on("error", onOutputError);
	try {
		for await (const event of watchSession({
			record,
			cursor: options.cursor,
			signal: stopped.signal
		})) for (const chunk of render(event)) {
			if (stopped.signal.aborted) break;
			if (!process.stdout.write(chunk)) await once(process.stdout, "drain", { signal: stopped.signal });
		}
	} catch (error) {
		if (!stopped.signal.aborted) throw error;
	} finally {
		process.off("SIGINT", stop);
		process.off("SIGTERM", stop);
		process.off("SIGHUP", stop);
		process.stdout.off("error", onOutputError);
	}
	if (outputError) throw outputError;
}
//#endregion
export { runSessionWatch };

//# sourceMappingURL=session-watch-Dg9Q6tWJ.js.map