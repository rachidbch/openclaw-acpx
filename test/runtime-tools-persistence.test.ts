import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { createAcpRuntime, createAgentRegistry, createRuntimeStore } from "../src/runtime.js";

const mockAgent = fileURLToPath(new URL("./mock-agent.js", import.meta.url));

for (const reconnect of [
  { method: "session/load", flag: "--supports-load-session" },
  { method: "session/resume", flag: "--supports-resume-session" },
]) {
  for (const tools of [["Read"], []] as const) {
    test(`Claude tools ${JSON.stringify(tools)} survive disk write and ${reconnect.method}`, async (t) => {
      const cwd = await fs.mkdtemp(path.join(os.tmpdir(), "acpx-tools-persistence-"));
      const stateDir = path.join(cwd, "state");
      const requestFile = path.join(cwd, "requests.ndjson");
      const options = {
        cwd,
        sessionStore: createRuntimeStore({ stateDir }),
        agentRegistry: createAgentRegistry({
          overrides: {
            fixture: [
              process.execPath,
              mockAgent,
              reconnect.flag,
              "--record-requests",
              requestFile,
            ],
          },
        }),
        permissionMode: "approve-reads" as const,
      };
      let runtime = createAcpRuntime(options);
      t.after(async () => {
        await runtime.shutdown();
        await fs.rm(cwd, { recursive: true, force: true });
      });

      const handle = await runtime.ensureSession({
        sessionKey: "claude-tools",
        agent: "fixture",
        mode: "persistent",
        sessionOptions: { tools: [...tools], allowedTools: ["Read"] },
      });
      async function prompt(text: string): Promise<void> {
        const turn = runtime.startTurn({ handle, text, mode: "prompt", requestId: text });
        for await (const event of turn.events) {
          void event;
        }
        assert.equal((await turn.result).status, "completed");
      }
      await prompt("first");

      const files = await fs.readdir(path.join(stateDir, "sessions"));
      assert.equal(files.length, 1);
      const raw = JSON.parse(await fs.readFile(path.join(stateDir, "sessions", files[0]), "utf8"));
      assert.deepEqual(raw.acpx.session_options.tools, tools);
      assert.deepEqual(raw.acpx.session_options.allowed_tools, ["Read"]);

      await runtime.shutdown();
      runtime = createAcpRuntime(options);
      await prompt("second");

      const requests = (await fs.readFile(requestFile, "utf8"))
        .trim()
        .split("\n")
        .map((line) => JSON.parse(line) as { method: string; meta?: Record<string, unknown> });
      for (const method of ["session/new", reconnect.method]) {
        const request = requests.find((item) => item.method === method);
        assert.deepEqual(request?.meta?.claudeCode, {
          options: { allowedTools: ["Read"], tools },
        });
      }
    });
  }
}
