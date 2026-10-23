import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { createAcpRuntime, createAgentRegistry, createRuntimeStore } from "../src/runtime.js";

const MOCK_AGENT_PATH = fileURLToPath(new URL("./mock-agent.js", import.meta.url));

type Recorded = { method: string; meta: Record<string, unknown> | null };

async function readRecorded(file: string): Promise<Recorded[]> {
  const text = await fs.readFile(file, "utf8").catch(() => "");
  return text
    .split("\n")
    .filter(Boolean)
    .map((line) => JSON.parse(line) as Recorded);
}

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

const AGENT_META = {
  piAcp: { launch: { args: ["-ne", "--model", "fake/model"] }, headless: true },
};

for (const reconnect of [
  { method: "session/load", flag: "--supports-load-session" },
  { method: "session/resume", flag: "--supports-resume-session" },
]) {
  test(`sessionOptions.agentMeta reaches session/new and ${reconnect.method} _meta and is persisted`, async (t) => {
    const cwd = await fs.mkdtemp(path.join(os.tmpdir(), "acpx-agent-meta-"));
    const recordFile = path.join(cwd, "requests.ndjson");
    const store = createRuntimeStore({ stateDir: path.join(cwd, "state") });
    const options = {
      cwd,
      sessionStore: store,
      agentRegistry: createAgentRegistry({
        overrides: {
          fixture: [
            process.execPath,
            MOCK_AGENT_PATH,
            reconnect.flag,
            "--record-requests",
            recordFile,
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
      sessionKey: "meta",
      agent: "fixture",
      mode: "persistent",
      sessionOptions: { agentMeta: AGENT_META, systemPrompt: { append: "Be terse." } },
    });
    assert.equal(await runTurn(runtime, handle, "echo one"), "completed");

    const stored = await store.load(handle.acpxRecordId ?? handle.sessionKey);
    assert.deepEqual(stored?.acpx?.session_options?.agent_meta, AGENT_META);

    // A fresh runtime reconnects from the stored record only.
    await runtime.shutdown();
    runtime = createAcpRuntime(options);
    assert.equal(await runTurn(runtime, handle, "echo two"), "completed");

    const recorded = await readRecorded(recordFile);
    const created = recorded.find((entry) => entry.method === "session/new");
    const reconnected = recorded.find((entry) => entry.method === reconnect.method);
    assert.deepEqual(created?.meta, { ...AGENT_META, systemPrompt: { append: "Be terse." } });
    assert.deepEqual(reconnected?.meta, { ...AGENT_META, systemPrompt: { append: "Be terse." } });
  });
}

test("agentMeta never overrides the acpx-owned systemPrompt key", async (t) => {
  const cwd = await fs.mkdtemp(path.join(os.tmpdir(), "acpx-agent-meta-owned-"));
  const recordFile = path.join(cwd, "requests.ndjson");
  const runtime = createAcpRuntime({
    cwd,
    sessionStore: createRuntimeStore({ stateDir: path.join(cwd, "state") }),
    agentRegistry: createAgentRegistry({
      overrides: { fixture: [process.execPath, MOCK_AGENT_PATH, "--record-requests", recordFile] },
    }),
    permissionMode: "approve-reads",
  });
  t.after(async () => {
    await runtime.shutdown();
    await fs.rm(cwd, { recursive: true, force: true });
  });
  await runtime.ensureSession({
    sessionKey: "owned",
    agent: "fixture",
    mode: "oneshot",
    sessionOptions: { agentMeta: { systemPrompt: "agent", extra: 1 }, systemPrompt: "acpx" },
  });
  const created = (await readRecorded(recordFile)).find((entry) => entry.method === "session/new");
  assert.deepEqual(created?.meta, { systemPrompt: "acpx", extra: 1 });
});
