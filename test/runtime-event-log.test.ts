import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { createAcpRuntime, createAgentRegistry, createRuntimeStore } from "../src/runtime.js";
import { withTempHome } from "./runtime-test-helpers.js";

const MOCK_AGENT_PATH = fileURLToPath(new URL("./mock-agent.js", import.meta.url));

async function runTurn(
  runtime: ReturnType<typeof createAcpRuntime>,
  handle: Awaited<ReturnType<ReturnType<typeof createAcpRuntime>["ensureSession"]>>,
  text: string,
): Promise<string> {
  const turn = runtime.startTurn({ handle, text, mode: "prompt", requestId: `r-${text}` });
  for await (const event of turn.events) {
    void event;
  }
  return (await turn.result).status;
}

test("runtime records keep their event log under the store's stateDir, never ~/.acpx", async () => {
  await withTempHome("acpx-event-log-home-", async (homeDir) => {
    const cwd = await fs.mkdtemp(path.join(os.tmpdir(), "acpx-event-log-"));
    const stateDir = path.join(cwd, "state");
    const store = createRuntimeStore({ stateDir });
    const runtime = createAcpRuntime({
      cwd,
      sessionStore: store,
      agentRegistry: createAgentRegistry({
        overrides: { fixture: [process.execPath, MOCK_AGENT_PATH] },
      }),
      permissionMode: "approve-reads",
    });
    try {
      const handle = await runtime.ensureSession({
        sessionKey: "log",
        agent: "fixture",
        mode: "persistent",
      });
      assert.equal(await runTurn(runtime, handle, "echo hi"), "completed");
      const stored = await store.load(handle.acpxRecordId ?? handle.sessionKey);
      const activePath = stored?.eventLog.active_path ?? "";
      assert.ok(
        activePath.startsWith(path.join(stateDir, "sessions") + path.sep),
        `event log path ${activePath} is outside ${stateDir}`,
      );
      await assert.rejects(fs.stat(path.join(homeDir, ".acpx")), { code: "ENOENT" });
    } finally {
      await runtime.shutdown();
      await fs.rm(cwd, { recursive: true, force: true });
    }
  });
});
