import { expandGlob } from "@std/fs";
import { globToRegExp } from "@std/path";
import { dirname } from "./governance.ts";

import {
  buildSqlpageDatabaseUrl,
  computeRelPath,
  defaultRelativeInstanceId,
  ensureDir,
  fileStatSafe,
  fnv1a32Hex,
  isPidAlive,
  loadOrCreateOwnerToken,
  normalizePath,
  nowMs,
  pickFreePort,
  readDbYardConfig,
  runSqliteQueryViaCli,
  safeBaseName,
  vlog,
  writeSpawnedPidsFile,
} from "./governance.ts";

import type {
  OwnerIdentity,
  Running,
  SpawnedCtxSnapshot,
  SpawnedRecord,
  SpawnKind,
  SqlpageEnv,
} from "./governance.ts";

export type SpawnPlan = {
  kind: SpawnKind;
  command: string;
  args: string[];
  env: Record<string, string>;
  cwd?: string;
  tag: string;
};

export type SpawnDriver = {
  kind: SpawnKind;
  buildPlan(args: {
    dbPath: string;
    listenHost: string;
    port: number;
    sqlpageEnv: SqlpageEnv;
    surveilrBin: string;
    sqlpageBin: string;
    dbYardConfig: Record<string, unknown>;
  }): SpawnPlan;
};

export function makeDefaultDrivers(): SpawnDriver[] {
  const rssd: SpawnDriver = {
    kind: "rssd",
    buildPlan: (a) => {
      const bin = typeof a.dbYardConfig["surveilr.bin"] === "string"
        ? String(a.dbYardConfig["surveilr.bin"])
        : a.surveilrBin;

      const extraArgs = Array.isArray(a.dbYardConfig["surveilr.args"])
        ? (a.dbYardConfig["surveilr.args"] as unknown[]).map(String)
        : [];

      return {
        kind: "rssd",
        command: bin,
        args: [
          "web-ui",
          "-d",
          a.dbPath,
          "--port",
          String(a.port),
          ...extraArgs,
        ],
        env: {},
        tag: `rssd:${safeBaseName(a.dbPath)}`,
      };
    },
  };

  const sqlpage: SpawnDriver = {
    kind: "sqlpage",
    buildPlan: (a) => {
      const bin = typeof a.dbYardConfig["sqlpage.bin"] === "string"
        ? String(a.dbYardConfig["sqlpage.bin"])
        : a.sqlpageBin;

      const env: Record<string, string> = {
        DATABASE_URL: buildSqlpageDatabaseUrl(a.dbPath),
        LISTEN_ON: `${a.listenHost}:${a.port}`,
        SQLPAGE_ENVIRONMENT: a.sqlpageEnv,
      };

      const extraEnv = a.dbYardConfig["sqlpage.env"];
      if (
        extraEnv && typeof extraEnv === "object" && !Array.isArray(extraEnv)
      ) {
        for (
          const [k, v] of Object.entries(extraEnv as Record<string, unknown>)
        ) {
          env[k] = String(v);
        }
      }

      const extraArgs = Array.isArray(a.dbYardConfig["sqlpage.args"])
        ? (a.dbYardConfig["sqlpage.args"] as unknown[]).map(String)
        : [];

      return {
        kind: "sqlpage",
        command: bin,
        args: [...extraArgs],
        env,
        tag: `sqlpage:${safeBaseName(a.dbPath)}`,
      };
    },
  };

  return [rssd, sqlpage];
}

export type OrchestratorConfig = {
  watchGlobs: string[];
  watchRoots: string[];

  spawnedDir: string;
  listenHost: string;
  reconcileMs: number;

  sqlpageEnv: SqlpageEnv;
  sqlpageBin: string;
  surveilrBin: string;

  spawnedCtxExec: string;
  spawnedCtxSqls: string[];

  adoptForeignState: boolean;
  verbose: boolean;

  drivers?: SpawnDriver[];
};

export type Orchestrator = {
  runningByDb: Map<string, Running>;
  close(): void;
};

async function readSpawnedRecord(
  path: string,
): Promise<SpawnedRecord | undefined> {
  try {
    const raw = await Deno.readTextFile(path);
    const obj = JSON.parse(raw);
    if (!obj || obj.version !== 1) return undefined;
    return obj as SpawnedRecord;
  } catch {
    return undefined;
  }
}

async function writeSpawnedRecord(
  path: string,
  rec: SpawnedRecord,
): Promise<void> {
  const tmp = `${path}.tmp`;
  await Deno.writeTextFile(tmp, JSON.stringify(rec, null, 2));
  await Deno.rename(tmp, path);
}

async function removeFileIfExists(path: string): Promise<void> {
  try {
    await Deno.remove(path);
  } catch {
    // ignore
  }
}

export async function stopByPid(pid: number) {
  // Prefer killing the process group when we detached with setsid.
  const killGroup = () => {
    try {
      // Negative pid targets the process group on POSIX.
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

  // Escalate
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

async function cleanupSpawnedDir(spawnedDir: string, liveDbPaths: Set<string>) {
  try {
    for await (const e of Deno.readDir(spawnedDir)) {
      if (!e.isFile || !e.name.endsWith(".json")) continue;
      const p = `${spawnedDir}/${e.name}`;
      const rec = await readSpawnedRecord(p);
      if (!rec?.dbPath) continue;
      if (!liveDbPaths.has(rec.dbPath)) {
        await removeFileIfExists(p);
      }
    }
  } catch {
    // ignore
  }
}

function spawnedJsonPath(
  spawnedDir: string,
  dbBasename: string,
  instanceId: string,
): string {
  const idHash = fnv1a32Hex(instanceId);
  return `${normalizePath(spawnedDir)}/${dbBasename}.${idHash}.json`;
}

function spawnedStdoutPath(
  spawnedDir: string,
  dbBasename: string,
  instanceId: string,
): string {
  const idHash = fnv1a32Hex(instanceId);
  return `${normalizePath(spawnedDir)}/${dbBasename}.${idHash}.stdout.log`;
}

function spawnedStderrPath(
  spawnedDir: string,
  dbBasename: string,
  instanceId: string,
): string {
  const idHash = fnv1a32Hex(instanceId);
  return `${normalizePath(spawnedDir)}/${dbBasename}.${idHash}.stderr.log`;
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

async function tableExists(args: {
  sqliteExec: string;
  dbPath: string;
  name: string;
}): Promise<boolean> {
  const nameEsc = args.name.replaceAll("'", "''");
  const sql =
    `select 1 as ok from sqlite_master where (type='table' or type='view') and name='${nameEsc}' limit 1`;
  const snap = await runSqliteQueryViaCli({
    exec: args.sqliteExec,
    dbPath: args.dbPath,
    sql,
  });
  if (!snap.ok) return false;
  if (Array.isArray(snap.output)) return snap.output.length > 0;
  const s = typeof snap.output === "string" ? snap.output.trim() : "";
  return s.length > 0;
}

type DriverChoice = "rssd" | "sqlpage" | undefined;

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

function compileGlobMatchers(globs: readonly string[]): RegExp[] {
  const out: RegExp[] = [];
  for (const g of globs) {
    try {
      out.push(globToRegExp(g, { extended: true, globstar: true }));
    } catch {
      // ignore invalid glob
    }
  }
  return out;
}

function matchesAny(path: string, matchers: readonly RegExp[]): boolean {
  const p = normalizePath(path);
  for (const rx of matchers) if (rx.test(p)) return true;
  return false;
}

async function expandAll(globs: readonly string[]): Promise<string[]> {
  const out: string[] = [];
  for (const g of globs) {
    for await (const e of expandGlob(g, { globstar: true })) {
      if (e.isFile) out.push(normalizePath(e.path));
    }
  }
  return out;
}

function dbChanged(rec: SpawnedRecord, st: Deno.FileInfo): boolean {
  const size = st.size;
  const mtime = st.mtime?.getTime() ?? 0;
  return size !== rec.fileSize || mtime !== rec.fileMtimeMs;
}

// Anti-storm throttle.
const RESPAWN_BACKOFF_MS = 15_000;
// “Fast exit” threshold.
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
  // Minimal POSIX-safe quoting
  return `'${s.replaceAll("'", `'\\''`)}'`;
}

/**
 * Detach a long-running service so it survives orchestrator crashes:
 * - setsid: new session/process group
 * - nohup: ignore SIGHUP
 * - redirects stdout/stderr to files (no pipes, no SIGPIPE risk)
 * - runs in background and prints PID
 */
async function spawnDetachedAndGetPid(args: {
  command: string;
  argv: string[];
  env: Record<string, string>;
  cwd?: string;
  stdoutPath: string;
  stderrPath: string;
}): Promise<number> {
  if (Deno.build.os === "windows") {
    // Windows: best-effort (no setsid/nohup). Still avoid pipes.
    const child = new Deno.Command(args.command, {
      args: args.argv,
      env: { ...Deno.env.toObject(), ...args.env },
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
    // setsid + nohup + redirect + background + echo pid
    `setsid nohup sh -lc ${shellQuote(cmdPart)} >> ${
      shellQuote(args.stdoutPath)
    } 2>> ${shellQuote(args.stderrPath)} < /dev/null & echo $!`,
  ].filter(Boolean).join(" ");

  const out = await new Deno.Command("sh", {
    args: ["-lc", script],
    stdin: "null",
    stdout: "piped",
    stderr: "piped",
    env: Deno.env.toObject(),
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

  await ensureDir(cfg.spawnedDir);

  const ownerToken = await loadOrCreateOwnerToken(cfg.spawnedDir);
  const owner = buildOwnerIdentity({ ownerToken });

  const runningByDb = new Map<string, Running>();
  const pending = new Set<string>();
  const failuresByDb = new Map<string, SpawnFailure>();

  let closed = false;

  // Adopt state
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
    rec: SpawnedRecord,
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
    else {failuresByDb.set(dbPath, {
        lastFailAtMs: nowMs(),
        failCount: f.failCount + 1,
      });}
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

    const plan = driver.buildPlan({
      dbPath: dbAbsPath,
      listenHost,
      port,
      sqlpageEnv: cfg.sqlpageEnv,
      surveilrBin: cfg.surveilrBin,
      sqlpageBin: cfg.sqlpageBin,
      dbYardConfig,
    });

    const dbBasename = safeBaseName(dbAbsPath);
    const stdoutLogPath = spawnedStdoutPath(cfg.spawnedDir, dbBasename, id);
    const stderrLogPath = spawnedStderrPath(cfg.spawnedDir, dbBasename, id);

    let pid: number;
    try {
      pid = await spawnDetachedAndGetPid({
        command: plan.command,
        argv: plan.args,
        env: plan.env,
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

    // Fast-exit check (we no longer have a ChildProcess handle, so just probe PID)
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

    const rec: SpawnedRecord = {
      version: 1,
      kind: plan.kind,
      id,
      watchRoots: cfg.watchRoots,
      dbPath: dbAbsPath,
      dbRelPath: rel,
      dbBasename,
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
    const p = normalizePath(path);
    if (!matchesAny(p, matchers)) return;
    pending.add(p);
    vlog(cfg.verbose, "detect", hint ? `event ${hint}` : "event", { path: p });
  }

  await reconcileFull();

  // Watch dirs derived from actual matches (no globParent)
  const watchDirs = new Set<string>();
  for (const g of cfg.watchGlobs) {
    for await (const e of expandGlob(g, { globstar: true })) {
      if (!e.isFile) continue;
      watchDirs.add(normalizePath(dirname(e.path)));
    }
  }
  if (watchDirs.size === 0) watchDirs.add(normalizePath(Deno.cwd()));

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
