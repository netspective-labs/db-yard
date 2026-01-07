// lib/orchestrate.ts
import type {
  OrchestratorConfig,
  OwnerIdentity,
  Running,
  SpawnedCtxSnapshot,
  SpawnedProcess,
  SpawnKind,
} from "./governance.ts";

import {
  cleanupSpawnedDir,
  computeRelPath,
  defaultRelativeInstanceId,
  ensureDir,
  fileStatSafe,
  isPidAlive,
  loadOrCreateOwnerToken,
  normalizeSlash,
  nowMs,
  pickFreePort,
  readSpawnedRecord,
  removeFileIfExists,
  safeBaseName,
  spawnedJsonPath,
  spawnedStderrPath,
  spawnedStdoutPath,
  writeSpawnedPidsFile,
  writeSpawnedRecord,
} from "./fs.ts";

import { makeDefaultDrivers } from "./spawnable.ts";
import {
  readDbYardConfig,
  runSqliteQueryViaCli,
  tableExists,
} from "./sqlite.ts";
import { vlog } from "./text-ui.ts";
import {
  compileGlobMatchers,
  deriveWatchDirsFromGlobs,
  expandAll,
  matchesAny,
} from "./watch.ts";

function stripDbSuffix(name: string): string {
  const s = name.trim();
  if (!s) return "db";
  const lo = s.toLowerCase();
  if (lo.endsWith(".sqlite.db")) return s.slice(0, -".sqlite.db".length);
  if (lo.endsWith(".db")) return s.slice(0, -".db".length);
  return s;
}

function normalizePrefix(p: string): string {
  const s = String(p ?? "").trim();
  if (!s) return "/";
  const withSlash = s.startsWith("/") ? s : `/${s}`;
  const withTrail = withSlash.endsWith("/") ? withSlash : `${withSlash}/`;
  return withTrail.replaceAll(/\/+/g, "/");
}

function computeProxyEndpointPrefix(args: {
  dbRelPath?: string;
  dbBasename: string;
  dbYardConfig?: Record<string, unknown>;
}): string {
  const cfg = args.dbYardConfig ?? {};
  const override = cfg["proxy-conf.location-prefix"];
  if (typeof override === "string" && override.trim()) {
    return normalizePrefix(override);
  }

  const rel = (typeof args.dbRelPath === "string" && args.dbRelPath.trim())
    ? args.dbRelPath.trim()
    : args.dbBasename;

  const p = normalizeSlash(rel).replace(/^\.\/+/, "").replace(/^\/+/, "");
  const parts = p.split("/").filter((x) => x.length);

  const last = parts.pop() ?? "db";
  const dir = parts.join("/");
  const base = stripDbSuffix(last) || "db";

  const prefix = dir ? `/${dir}/${base}/` : `/${base}/`;
  return prefix.replaceAll(/\/+/g, "/");
}

export type Orchestrator = {
  runningByDb: Map<string, Running>;
  close(): void;
};

function compileEnvRegex(list: readonly string[] | undefined): RegExp[] {
  const out: RegExp[] = [];
  for (const s of list ?? []) {
    const t = String(s ?? "").trim();
    if (!t) continue;
    try {
      out.push(new RegExp(t));
    } catch {
      // ignore invalid regex
    }
  }
  return out;
}

function filterEnv(
  env: Record<string, string>,
  allowRegex: readonly string[] | undefined,
): Record<string, string> {
  const matchers = compileEnvRegex(allowRegex);
  if (!matchers.length) return env; // default: inherit everything (current behavior)
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(env)) {
    if (matchers.some((rx) => rx.test(k))) out[k] = v;
  }
  return out;
}

export async function stopByPid(pid: number) {
  // Platform-specific: on POSIX we try process-group kill first (negative pid).
  const killGroup = () => {
    try {
      Deno.kill(-pid, "SIGTERM");
      return true;
    } catch {
      return false;
    }
  };

  const killSingle = (sig: Deno.Signal) => {
    try {
      Deno.kill(pid, sig);
      return true;
    } catch {
      return false;
    }
  };

  const triedGroup = Deno.build.os !== "windows" ? killGroup() : false;
  if (!triedGroup) {
    if (!killSingle("SIGTERM")) return;
  }

  for (let i = 0; i < 20; i++) {
    if (!isPidAlive(pid)) return;
    await new Promise((r) => setTimeout(r, 100));
  }

  if (Deno.build.os !== "windows") {
    try {
      Deno.kill(-pid, "SIGKILL");
      return;
    } catch {
      // fall back
    }
  }
  killSingle("SIGKILL");
}

function buildOwnerIdentity(args: { ownerToken: string }): OwnerIdentity {
  return {
    ownerToken: args.ownerToken,
    watcherPid: Deno.pid,
    host: (() => {
      try {
        return Deno.hostname();
      } catch {
        return "unknown-host";
      }
    })(),
    startedAtMs: nowMs(),
  };
}

function computeInstanceId(args: {
  watchRootsAbs: readonly string[];
  dbAbsPath: string;
  dbYardConfig: Record<string, unknown>;
}): { id: string; rel?: string } {
  const rel = computeRelPath(args.watchRootsAbs, args.dbAbsPath);
  const override = args.dbYardConfig["instance.id"];
  if (typeof override === "string" && override.trim()) {
    return { id: override.trim(), rel };
  }
  return { id: defaultRelativeInstanceId(rel ?? args.dbAbsPath), rel };
}

async function runSpawnedCtxBundle(args: {
  sqliteExec: string;
  dbPath: string;
  sqls: string[];
}): Promise<Record<string, SpawnedCtxSnapshot | undefined>> {
  const out: Record<string, SpawnedCtxSnapshot | undefined> = {};
  for (const sql of args.sqls) {
    const t = sql.trim();
    if (!t) continue;
    const snap = await runSqliteQueryViaCli({
      exec: args.sqliteExec,
      dbPath: args.dbPath,
      sql: t,
    });
    out[t] = snap;
  }
  return out;
}

type DriverChoice = SpawnKind | undefined;

async function chooseDriver(args: {
  sqliteExec: string;
  dbPath: string;
  dbYardConfig: Record<string, unknown>;
}): Promise<DriverChoice> {
  const explicit = args.dbYardConfig["spawn-driver"];
  if (typeof explicit === "string" && explicit.trim()) {
    const v = explicit.trim().toLowerCase();
    if (v === "surveilr" || v === "rssd" || v === "web-ui" || v === "webui") {
      return "rssd";
    }
    if (v === "sqlpage") return "sqlpage";
    return undefined;
  }

  if (
    await tableExists({
      sqliteExec: args.sqliteExec,
      dbPath: args.dbPath,
      name: "uniform_resource",
    })
  ) return "rssd";

  if (
    await tableExists({
      sqliteExec: args.sqliteExec,
      dbPath: args.dbPath,
      name: "sqlpage_files",
    })
  ) return "sqlpage";

  return undefined;
}

function dbChanged(rec: SpawnedProcess, st: Deno.FileInfo): boolean {
  const size = st.size;
  const mtime = st.mtime?.getTime() ?? 0;
  return size !== rec.fileSize || mtime !== rec.fileMtimeMs;
}

const RESPAWN_BACKOFF_MS = 15_000;
const FAST_EXIT_MS = 750;

type SpawnFailure = { lastFailAtMs: number; failCount: number };
type ChangeCounters = {
  spawned: number;
  stopped: number;
  refreshed: number;
  skipped: number;
};

function sumChanged(c: ChangeCounters): number {
  return c.spawned + c.stopped + c.refreshed + c.skipped;
}

async function updatePidsFileFromRunning(args: {
  spawnedDir: string;
  runningByDb: Map<string, Running>;
  ownerToken: string;
  adoptForeignState: boolean;
}) {
  const pids: number[] = [];
  for (const r of args.runningByDb.values()) {
    const owned = r.record.owner?.ownerToken === args.ownerToken;
    if (owned || args.adoptForeignState) {
      pids.push(r.record.pid);
    }
  }
  await writeSpawnedPidsFile(args.spawnedDir, pids);
}

function shellQuote(s: string): string {
  // Platform-specific: POSIX quoting for the "sh -lc" detach script.
  return `'${s.replaceAll("'", `'\\''`)}'`;
}

async function spawnDetachedAndGetPid(args: {
  command: string;
  argv: string[];
  // plan env (per-db)
  env: Record<string, string>;
  // inherited env (from yard process, possibly filtered)
  inheritedEnv: Record<string, string>;
  cwd?: string;
  stdoutPath: string;
  stderrPath: string;
}): Promise<number> {
  if (Deno.build.os === "windows") {
    const child = new Deno.Command(args.command, {
      args: args.argv,
      env: { ...args.inheritedEnv, ...args.env },
      cwd: args.cwd,
      stdin: "null",
      stdout: "null",
      stderr: "null",
    }).spawn();
    return child.pid;
  }

  const envPrefix = Object.entries(args.env)
    .map(([k, v]) => `${k}=${shellQuote(String(v))}`)
    .join(" ");

  const cmdPart = [
    envPrefix,
    shellQuote(args.command),
    ...args.argv.map(shellQuote),
  ].filter(Boolean).join(" ");

  const script = [
    args.cwd ? `cd ${shellQuote(args.cwd)};` : "",
    `setsid nohup sh -lc ${shellQuote(cmdPart)} >> ${
      shellQuote(args.stdoutPath)
    } 2>> ${shellQuote(args.stderrPath)} < /dev/null & echo $!`,
  ].filter(Boolean).join(" ");

  const out = await new Deno.Command("sh", {
    args: ["-lc", script],
    stdin: "null",
    stdout: "piped",
    stderr: "piped",
    env: args.inheritedEnv,
  }).output();

  const stdout = new TextDecoder().decode(out.stdout).trim();
  const stderr = new TextDecoder().decode(out.stderr).trim();

  if (!out.success) {
    throw new Error(`detach spawn failed: code=${out.code} stderr=${stderr}`);
  }

  const pid = Number(stdout.split(/\s+/)[0]);
  if (!Number.isFinite(pid) || pid <= 0) {
    throw new Error(
      `detach spawn did not return pid; stdout=${stdout} stderr=${stderr}`,
    );
  }

  return pid;
}

export async function startOrchestrator(
  cfg: OrchestratorConfig,
): Promise<Orchestrator> {
  const drivers = cfg.drivers ?? makeDefaultDrivers();
  const matchers = compileGlobMatchers(cfg.watchGlobs);
  const inheritedEnv = filterEnv(Deno.env.toObject(), cfg.inheritEnvRegex);

  await ensureDir(cfg.spawnedDir);

  const ownerToken = await loadOrCreateOwnerToken(cfg.spawnedDir);
  const owner = buildOwnerIdentity({ ownerToken });

  const runningByDb = new Map<string, Running>();
  const pending = new Set<string>();
  const failuresByDb = new Map<string, SpawnFailure>();

  let closed = false;

  // Adopt state from JSON ledger.
  for await (const e of Deno.readDir(cfg.spawnedDir)) {
    if (!e.isFile || !e.name.endsWith(".json")) continue;
    const p = `${cfg.spawnedDir}/${e.name}`;
    const rec = await readSpawnedRecord(p);
    if (!rec?.dbPath) continue;

    const st = await fileStatSafe(rec.dbPath);
    if (!st?.isFile) {
      await removeFileIfExists(p);
      continue;
    }

    const ownedByToken = rec.owner?.ownerToken === owner.ownerToken;
    if (!ownedByToken && !cfg.adoptForeignState) continue;

    runningByDb.set(rec.dbPath, { record: rec });
  }

  await updatePidsFileFromRunning({
    spawnedDir: cfg.spawnedDir,
    runningByDb,
    ownerToken: owner.ownerToken,
    adoptForeignState: cfg.adoptForeignState,
  });

  async function refreshRecordIfChanged(
    rec: SpawnedProcess,
    counters: ChangeCounters,
  ) {
    const st = await fileStatSafe(rec.dbPath);
    if (!st?.isFile) return;

    if (!dbChanged(rec, st)) {
      rec.lastSeenAtMs = nowMs();
      return;
    }

    rec.lastSeenAtMs = nowMs();
    rec.fileSize = st.size;
    rec.fileMtimeMs = st.mtime?.getTime() ?? 0;

    const dbYardConfig = await readDbYardConfig({
      sqliteExec: cfg.spawnedCtxExec,
      dbPath: rec.dbPath,
    });
    rec.dbYardConfig = dbYardConfig;

    rec.spawnedCtx = await runSpawnedCtxBundle({
      sqliteExec: cfg.spawnedCtxExec,
      dbPath: rec.dbPath,
      sqls: cfg.spawnedCtxSqls,
    });

    rec.proxyEndpointPrefix = computeProxyEndpointPrefix({
      dbRelPath: rec.dbRelPath,
      dbBasename: rec.dbBasename,
    });

    const jsonPath = spawnedJsonPath(cfg.spawnedDir, rec.dbBasename, rec.id);
    await writeSpawnedRecord(jsonPath, rec);

    counters.refreshed++;

    vlog(cfg.verbose, "refresh", "db changed; updated record", {
      kind: rec.kind,
      id: rec.id,
      pid: rec.pid,
      port: rec.port,
      db: rec.dbPath,
      json: jsonPath,
    });
  }

  function shouldThrottle(dbPath: string): boolean {
    const f = failuresByDb.get(dbPath);
    if (!f) return false;
    return (nowMs() - f.lastFailAtMs) < RESPAWN_BACKOFF_MS;
  }

  function noteFailure(dbPath: string) {
    const f = failuresByDb.get(dbPath);
    if (!f) failuresByDb.set(dbPath, { lastFailAtMs: nowMs(), failCount: 1 });
    else {
      failuresByDb.set(dbPath, {
        lastFailAtMs: nowMs(),
        failCount: f.failCount + 1,
      });
    }
  }

  async function ensureSpawnedForDb(
    dbAbsPath: string,
    counters: ChangeCounters,
  ): Promise<void> {
    const st = await fileStatSafe(dbAbsPath);
    if (!st?.isFile) return;

    const existing = runningByDb.get(dbAbsPath);
    if (existing && isPidAlive(existing.record.pid)) {
      await refreshRecordIfChanged(existing.record, counters);
      return;
    }

    if (shouldThrottle(dbAbsPath)) {
      counters.skipped++;
      vlog(cfg.verbose, "skip", "respawn throttled (previous failure)", {
        db: dbAbsPath,
        backoffMs: RESPAWN_BACKOFF_MS,
      });
      return;
    }

    const dbYardConfig = await readDbYardConfig({
      sqliteExec: cfg.spawnedCtxExec,
      dbPath: dbAbsPath,
    });

    const chosen = await chooseDriver({
      sqliteExec: cfg.spawnedCtxExec,
      dbPath: dbAbsPath,
      dbYardConfig,
    });

    if (!chosen) return;

    const driver = drivers.find((d) => d.kind === chosen);
    if (!driver) {
      counters.skipped++;
      vlog(cfg.verbose, "skip", "no driver implementation", {
        chosen,
        db: dbAbsPath,
      });
      return;
    }

    const { id, rel } = computeInstanceId({
      watchRootsAbs: cfg.watchRoots,
      dbAbsPath,
      dbYardConfig,
    });

    const listenHost = typeof dbYardConfig["listen.host"] === "string"
      ? String(dbYardConfig["listen.host"])
      : cfg.listenHost;

    const portOverride = dbYardConfig["listen.port"];
    const port =
      typeof portOverride === "number" && Number.isFinite(portOverride) &&
        portOverride > 0
        ? Math.floor(portOverride)
        : pickFreePort(listenHost);
    const dbBasename = safeBaseName(dbAbsPath);

    const proxyEndpointPrefix = computeProxyEndpointPrefix({
      dbRelPath: rel,
      dbBasename,
    });

    const plan = driver.buildPlan({
      dbPath: dbAbsPath,
      listenHost,
      port,
      sqlpageEnv: cfg.sqlpageEnv,
      surveilrBin: cfg.surveilrBin,
      sqlpageBin: cfg.sqlpageBin,
      dbYardConfig,
      proxyEndpointPrefix,
    });

    const stdoutLogPath = spawnedStdoutPath(cfg.spawnedDir, dbBasename, id);
    const stderrLogPath = spawnedStderrPath(cfg.spawnedDir, dbBasename, id);

    let pid: number;
    try {
      pid = await spawnDetachedAndGetPid({
        command: plan.command,
        argv: plan.args,
        env: plan.env,
        inheritedEnv,
        cwd: plan.cwd,
        stdoutPath: stdoutLogPath,
        stderrPath: stderrLogPath,
      });
    } catch (e) {
      noteFailure(dbAbsPath);
      counters.skipped++;
      vlog(cfg.verbose, "stop", "detach spawn failed", {
        db: dbAbsPath,
        err: e instanceof Error ? e.message : String(e),
        stdout: stdoutLogPath,
        stderr: stderrLogPath,
      });
      return;
    }

    await new Promise((r) => setTimeout(r, FAST_EXIT_MS));
    if (!isPidAlive(pid)) {
      noteFailure(dbAbsPath);
      counters.skipped++;
      vlog(cfg.verbose, "stop", "spawned process exited quickly (throttling)", {
        db: dbAbsPath,
        pid,
        stdout: stdoutLogPath,
        stderr: stderrLogPath,
      });
      return;
    }

    const rec: SpawnedProcess = {
      version: 1,
      kind: plan.kind,
      id,
      watchRoots: cfg.watchRoots,
      dbPath: dbAbsPath,
      dbRelPath: rel,
      dbBasename,
      proxyEndpointPrefix: computeProxyEndpointPrefix({
        dbRelPath: rel,
        dbBasename,
      }),
      listenHost,
      port,
      spawnedAtMs: nowMs(),
      lastSeenAtMs: nowMs(),
      fileSize: st.size,
      fileMtimeMs: st.mtime?.getTime() ?? 0,
      pid,
      command: plan.command,
      args: plan.args,
      env: plan.env,
      cwd: plan.cwd,
      stdoutLogPath,
      stderrLogPath,
      owner,
      dbYardConfig,
      notes: ["spawned by db-yard (detached)"],
    };

    rec.spawnedCtx = await runSpawnedCtxBundle({
      sqliteExec: cfg.spawnedCtxExec,
      dbPath: dbAbsPath,
      sqls: cfg.spawnedCtxSqls,
    });

    runningByDb.set(dbAbsPath, { record: rec });

    const jsonPath = spawnedJsonPath(cfg.spawnedDir, rec.dbBasename, rec.id);
    await writeSpawnedRecord(jsonPath, rec);

    await updatePidsFileFromRunning({
      spawnedDir: cfg.spawnedDir,
      runningByDb,
      ownerToken: owner.ownerToken,
      adoptForeignState: cfg.adoptForeignState,
    });

    counters.spawned++;

    const extra: Record<string, unknown> = {
      id: rec.id,
      pid: rec.pid,
      host: `${listenHost}:${port}`,
      db: rec.dbPath,
      json: jsonPath,
    };
    if (cfg.verbose) {
      extra.stdout = stdoutLogPath;
      extra.stderr = stderrLogPath;
    }

    vlog(cfg.verbose, "spawn", "spawned", extra);
  }

  async function stopForDb(
    dbAbsPath: string,
    counters: ChangeCounters,
  ): Promise<void> {
    const running = runningByDb.get(dbAbsPath);
    if (!running) return;

    const rec = running.record;

    const ownedByToken = rec.owner?.ownerToken === owner.ownerToken;
    if (!ownedByToken && !cfg.adoptForeignState) {
      runningByDb.delete(dbAbsPath);
      counters.skipped++;
      await updatePidsFileFromRunning({
        spawnedDir: cfg.spawnedDir,
        runningByDb,
        ownerToken: owner.ownerToken,
        adoptForeignState: cfg.adoptForeignState,
      });
      vlog(cfg.verbose, "stop", "detached foreign record (not stopping pid)", {
        id: rec.id,
        pid: rec.pid,
        db: rec.dbPath,
      });
      return;
    }

    await stopByPid(rec.pid);
    runningByDb.delete(dbAbsPath);

    const jsonPath = spawnedJsonPath(cfg.spawnedDir, rec.dbBasename, rec.id);
    await removeFileIfExists(jsonPath);

    await updatePidsFileFromRunning({
      spawnedDir: cfg.spawnedDir,
      runningByDb,
      ownerToken: owner.ownerToken,
      adoptForeignState: cfg.adoptForeignState,
    });

    counters.stopped++;

    const extra: Record<string, unknown> = {
      id: rec.id,
      pid: rec.pid,
      db: rec.dbPath,
      removed: jsonPath,
    };
    if (cfg.verbose) {
      extra.stdout = rec.stdoutLogPath;
      extra.stderr = rec.stderrLogPath;
    }

    vlog(cfg.verbose, "stop", "stopped and removed json", extra);
  }

  async function reconcileFull(): Promise<void> {
    const counters: ChangeCounters = {
      spawned: 0,
      stopped: 0,
      refreshed: 0,
      skipped: 0,
    };

    const files = await expandAll(cfg.watchGlobs);
    const live = new Set(files);

    for (const f of files) await ensureSpawnedForDb(f, counters);

    for (const dbPath of [...runningByDb.keys()]) {
      if (!live.has(dbPath)) await stopForDb(dbPath, counters);
    }

    await cleanupSpawnedDir(cfg.spawnedDir, live);

    if (cfg.verbose && sumChanged(counters) > 0) {
      vlog(true, "reconcile", "reconciled", counters);
    }
  }

  async function reconcileDelta(paths: string[]): Promise<void> {
    const counters: ChangeCounters = {
      spawned: 0,
      stopped: 0,
      refreshed: 0,
      skipped: 0,
    };

    for (const p of paths) {
      const st = await fileStatSafe(p);
      if (st?.isFile) await ensureSpawnedForDb(p, counters);
      else await stopForDb(p, counters);
    }

    if (cfg.verbose && sumChanged(counters) > 0) {
      vlog(true, "reconcile", "reconciled (delta)", counters);
    }
  }

  function enqueue(path: string, hint?: string) {
    const p = normalizeSlash(path);
    if (!matchesAny(p, matchers)) return;
    pending.add(p);
    vlog(cfg.verbose, "detect", hint ? `event ${hint}` : "event", { path: p });
  }

  await reconcileFull();

  const watchDirs = await deriveWatchDirsFromGlobs(cfg.watchGlobs);

  for (const d of watchDirs) {
    (async () => {
      let watcher: Deno.FsWatcher | undefined;
      try {
        watcher = Deno.watchFs(d, { recursive: true });
      } catch (e) {
        console.warn(
          `[warn] cannot watch ${d} (will rely on reconcile loop):`,
          e,
        );
        return;
      }

      for await (const ev of watcher) {
        if (closed) break;
        for (const p0 of ev.paths) enqueue(p0, ev.kind);
      }
    })();
  }

  (async () => {
    while (!closed) {
      await new Promise((r) => setTimeout(r, cfg.reconcileMs));

      const batch = [...pending];
      pending.clear();

      if (batch.length) {
        await reconcileDelta(batch);
      } else {
        await reconcileFull();
      }
    }
  })();

  return {
    runningByDb,
    close() {
      closed = true;
    },
  };
}
