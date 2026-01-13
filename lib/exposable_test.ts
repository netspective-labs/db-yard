// exposable_test.ts
import { assert, assertEquals } from "@std/assert";
import { basename, join, resolve } from "@std/path";

import { tabular, type TabularDataSupplier } from "./tabular.ts";
import {
  exposable,
  type ExposableService,
  type SpawnHost,
  type SpawnLogTarget,
  type SqlPageExposableService,
  type SurveilrExposableService,
} from "./exposable.ts";

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

      // Important for Deno leak checks: always consume or cancel the body.
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

function logTargetsForTest(tag: string): {
  stdout?: SpawnLogTarget;
  stderr?: SpawnLogTarget;
} {
  const enabled = Deno.env.get("TEST_SPAWN_LOGS") === "1";
  if (!enabled) return {};

  const dir = Deno.makeTempDirSync({ prefix: "truth-yard-exposable-test-" });
  return {
    stdout: join(dir, `${tag}.stdout.log`),
    stderr: join(dir, `${tag}.stderr.log`),
  };
}

async function collectSuppliers(): Promise<TabularDataSupplier[]> {
  const out: TabularDataSupplier[] = [];
  for await (
    const s of tabular(
      [{ path: fixturesDir() }],
      // If your tabular() signature differs, adjust this call to match.
      { dedupeBy: "location" } as const,
    )
  ) out.push(s);
  return out;
}

async function findExposable(
  services: AsyncIterable<ExposableService>,
  predicate: (s: ExposableService) => boolean,
): Promise<ExposableService | undefined> {
  for await (const s of services) {
    if (predicate(s)) return s;
  }
  return undefined;
}

Deno.test("exposable: smoke spawn and HTTP 200", async (t) => {
  const host: SpawnHost = { identity: "exposable_test", pid: Deno.pid };

  const rssdDb = fixturePath("empty-rssd.sqlite.db");
  const sqlpageDb = fixturePath("scf-2025.3.sqlite.db");

  await t.step("surveilr RSSD spawns and serves HTTP 200 on /", async () => {
    const hasSurveilr = await commandExists("surveilr");
    if (!hasSurveilr) {
      console.log("surveilr not found on PATH; skipping surveilr smoke test");
      return;
    }

    const suppliers = await collectSuppliers();
    const services = exposable(suppliers);

    const svc = await findExposable(
      services,
      (e) =>
        e.kind === "surveilr" &&
        basename(e.supplier.location) === basename(rssdDb),
    );

    assert(svc, "Expected a SurveilrExposableService for empty-rssd.sqlite.db");
    assertEquals(svc.kind, "surveilr");

    const port = reserveFreePort();
    const logs = logTargetsForTest("surveilr");

    const proc = await (svc as SurveilrExposableService).spawn({
      host,
      init: {
        listenHost: "127.0.0.1",
        port,
        proxyEndpointPrefix: "/",
        surveilrBin: "surveilr",
        stdoutLogPath: logs.stdout,
        stderrLogPath: logs.stderr,
      },
    });

    try {
      await waitForHttp200(`http://127.0.0.1:${port}/`);
    } finally {
      await proc.kill();
    }
  });

  await t.step("sqlpage spawns and serves HTTP 200 on /", async () => {
    const hasSqlpage = await commandExists("sqlpage");
    if (!hasSqlpage) {
      console.log("sqlpage not found on PATH; skipping sqlpage smoke test");
      return;
    }

    const suppliers = await collectSuppliers();
    const services = exposable(suppliers);

    const svc = await findExposable(
      services,
      (e) =>
        e.kind === "sqlpage" &&
        basename(e.supplier.location) === basename(sqlpageDb),
    );

    assert(svc, "Expected a SqlPageExposableService for scf-2025.3.sqlite.db");
    assertEquals(svc.kind, "sqlpage");

    const port = reserveFreePort();
    const logs = logTargetsForTest("sqlpage");

    const proc = await (svc as SqlPageExposableService).spawn({
      host,
      init: {
        listenHost: "127.0.0.1",
        port,
        proxyEndpointPrefix: "/",
        sqlpageBin: "sqlpage",
        sqlpageEnv: "development",
        stdoutLogPath: logs.stdout,
        stderrLogPath: logs.stderr,
      },
    });

    try {
      await waitForHttp200(`http://127.0.0.1:${port}/`);
    } finally {
      await proc.kill();
    }
  });
});
