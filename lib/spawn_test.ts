// spawn_test.ts
import { assert, assertEquals } from "@std/assert";
import { basename, join, resolve } from "@std/path";

import type { ExposableService, SpawnHost } from "./exposable.ts";
import { spawn, type SpawnedContext, type SpawnSummary } from "./spawn.ts";

function fixturesDir(): string {
  return resolve(join(import.meta.dirname ?? ".", "../support/fixtures"));
}

function fixturePath(name: string): string {
  return resolve(join(fixturesDir(), name));
}

async function commandExists(cmd: string): Promise<boolean> {
  try {
    const p = new Deno.Command(cmd, {
      args: ["--version"],
      stdout: "null",
      stderr: "null",
    });
    const { code } = await p.output();
    return code === 0;
  } catch {
    return false;
  }
}

function reserveFreePort(): number {
  const listener = Deno.listen({ hostname: "127.0.0.1", port: 0 });
  const port = (listener.addr as Deno.NetAddr).port;
  listener.close();
  return port;
}

async function waitForHttp200(url: string, timeoutMs = 15_000): Promise<void> {
  const started = Date.now();
  let lastErr: unknown;

  while (Date.now() - started < timeoutMs) {
    let res: Response | undefined;

    try {
      res = await fetch(url, { redirect: "manual" });

      if (res.status === 200) {
        await res.body?.cancel();
        return;
      }

      await res.body?.cancel();
      lastErr = new Error(`HTTP ${res.status}`);
    } catch (e) {
      try {
        await res?.body?.cancel();
      } catch {
        // ignore
      }
      lastErr = e;
    }

    await new Promise((r) => setTimeout(r, 250));
  }

  throw new Error(
    `Timed out waiting for HTTP 200 at ${url}. Last error: ${String(lastErr)}`,
  );
}

async function killPid(pid: number): Promise<void> {
  try {
    Deno.kill(pid, "SIGTERM");
  } catch {
    return;
  }

  const started = Date.now();
  while (Date.now() - started < 1500) {
    try {
      Deno.kill(pid, 0);
      await new Promise((r) => setTimeout(r, 100));
    } catch {
      return;
    }
  }

  try {
    Deno.kill(pid, "SIGKILL");
  } catch {
    // ignore
  }
}

function mkStatePaths(tag: string): {
  dir: string;
  spawnStatePath: (
    entry: ExposableService,
    nature: "context" | "stdout" | "stderr",
  ) => string | undefined;
} {
  const enabled = Deno.env.get("TEST_SPAWN_LOGS") === "1";
  const dir = Deno.makeTempDirSync({ prefix: `truth-yard-spawn-test-${tag}-` });

  const spawnStatePath = (
    entry: ExposableService,
    nature: "context" | "stdout" | "stderr",
  ) => {
    if (!enabled && nature !== "context") return undefined;

    const base = `${entry.kind}-${entry.id}`;
    if (nature === "context") return join(dir, `${base}.context.json`);
    if (nature === "stdout") return join(dir, `${base}.stdout.log`);
    return join(dir, `${base}.stderr.log`);
  };

  return { dir, spawnStatePath };
}

// Wrapper avoids TS2554/typing quirks around AsyncGenerator.next()
async function drainSpawn(
  gen: AsyncGenerator<SpawnedContext, SpawnSummary>,
): Promise<{ contexts: SpawnedContext[]; summary: SpawnSummary }> {
  const contexts: SpawnedContext[] = [];
  while (true) {
    const { value, done } = await gen.next(undefined as unknown as never);
    if (done) return { contexts, summary: value };
    contexts.push(value);
  }
}

function findCtx(
  contexts: SpawnedContext[],
  kind: SpawnedContext["service"]["kind"],
  expectedBasename: string,
): SpawnedContext | undefined {
  return contexts.find((c) =>
    c.service.kind === kind &&
    basename(c.supplier.location) === expectedBasename
  );
}

Deno.test("spawn: smoke spawn and HTTP 200 (surveilr + sqlpage)", async (t) => {
  const hasSurveilr = await commandExists("surveilr");
  const hasSqlpage = await commandExists("sqlpage");

  if (!hasSurveilr) {
    console.log("surveilr not found on PATH; will skip surveilr subtest");
  }
  if (!hasSqlpage) {
    console.log("sqlpage not found on PATH; will skip sqlpage subtest");
  }
  if (!hasSurveilr && !hasSqlpage) return;

  const host: SpawnHost = { identity: "spawn_test", pid: Deno.pid };

  const rssdDb = fixturePath("empty-rssd.sqlite.db");
  const sqlpageDb = fixturePath("scf-2025.3.sqlite.db");

  // One spawn run, one discovery set.
  const { spawnStatePath } = mkStatePaths("combined");
  const portStart = reserveFreePort();

  const gen = spawn(
    [{ path: fixturesDir() }],
    (entry, _candidate) => {
      const base = basename(entry.supplier.location);

      if (
        hasSurveilr &&
        entry.kind === "surveilr" &&
        base === basename(rssdDb)
      ) {
        return { proxyEndpointPrefix: "/" };
      }

      if (
        hasSqlpage &&
        entry.kind === "sqlpage" &&
        base === basename(sqlpageDb)
      ) {
        return { proxyEndpointPrefix: "/" };
      }

      return false;
    },
    spawnStatePath,
    {
      host,
      listenHost: "127.0.0.1",
      portStart,
      surveilrBin: "surveilr",
      sqlpageBin: "sqlpage",
      sqlpageEnv: "development",
      probe: { enabled: false },
    },
  );

  const { contexts, summary } = await drainSpawn(gen);

  // Basic assertions about the spawn run.
  assertEquals(summary.errored.length, 0);
  assert(summary.spawned.length >= 1);

  await t.step("surveilr spawns and serves HTTP 200 on /", async () => {
    if (!hasSurveilr) return;

    const ctx = findCtx(contexts, "surveilr", basename(rssdDb));
    assert(ctx, "Expected a surveilr SpawnedContext for empty-rssd.sqlite.db");

    try {
      await waitForHttp200(`http://127.0.0.1:${ctx.listen.port}/`);
    } finally {
      await killPid(ctx.spawned.pid);
    }
  });

  await t.step("sqlpage spawns and serves HTTP 200 on /", async () => {
    if (!hasSqlpage) return;

    const ctx = findCtx(contexts, "sqlpage", basename(sqlpageDb));
    assert(ctx, "Expected a sqlpage SpawnedContext for scf-2025.3.sqlite.db");

    try {
      await waitForHttp200(`http://127.0.0.1:${ctx.listen.port}/`);
    } finally {
      await killPid(ctx.spawned.pid);
    }
  });

  // Safety net: if a subtest returned early before killing, kill anything left.
  // (best-effort; most cases already killed above)
  for (const c of contexts) {
    await killPid(c.spawned.pid);
  }
});
