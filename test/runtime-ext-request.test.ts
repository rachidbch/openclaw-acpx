import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { createAcpRuntime, createAgentRegistry, createRuntimeStore } from "../src/runtime.js";

test("custom requests use active and retained connections; agent info preserves metadata", async (t) => {
  const cwd = await fs.mkdtemp(path.join(process.env.HOME!, "tmp/acpx-ext-"));
  const runtime = createAcpRuntime({
    cwd,
    permissionMode: "deny-all",
    sessionStore: createRuntimeStore({ stateDir: path.join(cwd, "state") }),
    agentRegistry: createAgentRegistry({ overrides: { fixture: [process.execPath, fileURLToPath(new URL("./mock-agent.js", import.meta.url))] } }),
  });
  t.after(async () => { await runtime.shutdown(); await fs.rm(cwd, { recursive: true, force: true }); });
  const handle = await runtime.ensureSession({ sessionKey: "custom", agent: "fixture", mode: "persistent" });
  const params = { sessionId: handle.backendSessionId, prompt: [{ type: "text", text: "steer" }] };
  assert.deepEqual(await runtime.extRequest({ handle, method: "_test/echo", params }), params);
  assert.deepEqual((await runtime.getAgentInfo({ handle }))._meta, { steering: { supported: true } });
  const turn = runtime.startTurn({ handle, text: "sleep 10000", mode: "prompt", requestId: "custom-active" });
  const events = (async () => { for await (const event of turn.events) void event; })();
  await turn.promptStarted;
  assert.deepEqual(await runtime.extRequest({ handle, method: "_test/echo", params }), params);
  await turn.cancel();
  await events;
  await turn.result;
  await runtime.close({ handle, reason: "test" });
  await assert.rejects(runtime.extRequest({ handle, method: "_test/echo", params }), { code: "ACP_BACKEND_UNAVAILABLE" });
  await assert.rejects(runtime.getAgentInfo({ handle }), { code: "ACP_BACKEND_UNAVAILABLE" });
});
