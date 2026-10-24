import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { createAcpRuntime, createAgentRegistry, createRuntimeStore } from "../src/runtime.js";

const MOCK_AGENT_PATH = fileURLToPath(new URL("./mock-agent.js", import.meta.url));

function isRunning(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

test("runtime.close ends an open turn whose agent ignores session/cancel", async (t) => {
  const cwd = await fs.mkdtemp(path.join(os.tmpdir(), "acpx-close-active-"));
  const pidFile = path.join(cwd, "agent.pid");
  const runtime = createAcpRuntime({
    cwd,
    sessionStore: createRuntimeStore({ stateDir: path.join(cwd, "state") }),
    agentRegistry: createAgentRegistry({
      overrides: {
        fixture: [
          process.execPath,
          MOCK_AGENT_PATH,
          "--cancel-delay-ms",
          "600000",
          "--pid-file",
          pidFile,
        ],
      },
    }),
    permissionMode: "approve-reads",
  });
  t.after(async () => {
    await runtime.shutdown();
    await fs.rm(cwd, { recursive: true, force: true });
  });

  const handle = await runtime.ensureSession({
    sessionKey: "silent",
    agent: "fixture",
    mode: "persistent",
  });
  const turn = runtime.startTurn({
    handle,
    text: "sleep 600000",
    mode: "prompt",
    requestId: "silent-1",
  });
  const drained = (async () => {
    for await (const event of turn.events) {
      void event;
    }
  })();
  await turn.promptStarted;
  const agentPid = Number((await fs.readFile(pidFile, "utf8")).trim());
  assert.ok(isRunning(agentPid));

  await turn.cancel({ reason: "test" });
  await runtime.close({ handle, reason: "test" });

  const settled = await Promise.race([
    turn.result.then(() => "resolved"),
    new Promise<string>((resolve) => setTimeout(() => resolve("pending"), 5000)),
  ]);
  assert.equal(settled, "resolved");
  await drained;
  assert.equal(isRunning(agentPid), false);
});
