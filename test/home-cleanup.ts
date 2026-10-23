import { execFile } from "node:child_process";
import fs from "node:fs/promises";
import path from "node:path";
import { queueSocketBaseDir } from "../src/session/queue/paths.js";

// Lock files also name test processes and fixtures; only a real queue owner may be stopped.
function isQueueOwnerProcess(pid: number): Promise<boolean> {
  return new Promise((resolve) => {
    execFile("ps", ["-o", "args=", "-p", String(pid)], (error, stdout) => {
      resolve(!error && stdout.includes("__queue-owner"));
    });
  });
}

function isPidAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

async function waitForExit(pid: number, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (!isPidAlive(pid)) {
      return true;
    }
    await new Promise<void>((resolve) => setTimeout(resolve, 50));
  }
  return !isPidAlive(pid);
}

/**
 * Retire what a test home left outside its own tree: queue owners that its lock files still
 * name (a prompt without a finite --ttl keeps its detached owner idling for the default
 * 300 s) and the unix socket directory acpx derives from the home path under /tmp.
 */
export async function cleanupTestHome(homeDir: string): Promise<void> {
  const queuesDir = path.join(homeDir, ".acpx", "queues");
  let names: string[] = [];
  try {
    names = (await fs.readdir(queuesDir)).filter((entry) => entry.endsWith(".lock"));
  } catch {
    // no queue state
  }
  for (const name of names) {
    let pid = 0;
    try {
      const lock = JSON.parse(await fs.readFile(path.join(queuesDir, name), "utf8")) as {
        pid?: unknown;
      };
      pid = Number(lock.pid);
    } catch {
      continue;
    }
    if (
      !Number.isInteger(pid) ||
      pid <= 0 ||
      pid === process.pid ||
      !isPidAlive(pid) ||
      !(await isQueueOwnerProcess(pid))
    ) {
      continue;
    }
    process.kill(pid, "SIGTERM");
    if (!(await waitForExit(pid, 10_000))) {
      process.kill(pid, "SIGKILL");
    }
  }
  const socketDir = queueSocketBaseDir(homeDir);
  if (socketDir) {
    await fs.rm(socketDir, { recursive: true, force: true });
  }
}
