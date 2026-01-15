// lib/materialize.ts
import { ensureDir } from "@std/fs";
import { basename, join, resolve } from "@std/path";

import type { Path } from "./discover.ts";
import { encounters, fileSystemSource } from "./discover.ts";
import type { ExposableService } from "./exposable.ts";
import { richTextUISpawnEvents } from "./spawn-event.ts";
import {
  isPidAlive,
  killPID,
  readProcCmdline,
  spawn,
  type SpawnedContext,
  type SpawnEventListener,
  type SpawnSummary,
  type TaggedProcess,
  taggedProcesses,
} from "./spawn.ts";
import { proxyPrefixFromRel, relDirFromRoots, relFromRoots } from "./path.ts";

export function sortableDateTimeText(d = new Date()): string {
  function fmt2(n: number): string {
    return String(n).padStart(2, "0");
  }
  const yyyy = d.getFullYear();
  const mm = fmt2(d.getMonth() + 1);
  const dd = fmt2(d.getDate());
  const hh = fmt2(d.getHours());
  const mi = fmt2(d.getMinutes());
  const ss = fmt2(d.getSeconds());
  return `${yyyy}-${mm}-${dd}-${hh}-${mi}-${ss}`;
}

export async function createSpawnSessionHome(spawnedLedgerHome: string) {
  const home = resolve(spawnedLedgerHome);
  await ensureDir(spawnedLedgerHome);

  const sessionName = sortableDateTimeText();
  const sessionHome = join(home, sessionName);
  await ensureDir(sessionHome);

  // Pointer file for “current session” (portable, no symlinks).
  await Deno.writeTextFile(join(home, ".current-session"), `${sessionName}\n`);

  return { spawnedLedgerHome: home, sessionHome, sessionName };
}

export type MaterializeVerbose = false | "essential" | "comprehensive";

export type MaterializeWatchOptions = Readonly<{
  enabled?: boolean;

  /**
   * Debounce window for watch events.
   */
  debounceMs?: number;

  /**
   * If true, only kill processes that match BOTH:
   * - TaggedProcess.provenance
   * - TaggedProcess.sessionId (the sessionId used by this materialize/watch loop)
   *
   * Default: false (kill by provenance only).
   */
  strictKillsOnly?: boolean;
}>;

export type MaterializeOptions = Readonly<{
  verbose: MaterializeVerbose;
  spawnedLedgerHome: string;

  /**
   * Service bind/listen host (default handled by spawn() when omitted).
   */
  listenHost?: string;

  /**
   * Starting port for upstream services (default 3000).
   */
  startPort?: number;

  /**
   * If true (default), materialize will use taggedProcesses() (Linux-only)
   * to avoid spawning services that are already running.
   */
  smartSpawn?: boolean;

  /**
   * Optional watch mode. If enabled, the function to call is materializeWatch().
   */
  watch?: MaterializeWatchOptions;
}>;

export type MaterializeResult = Readonly<{
  sessionHome: string;
  summary: SpawnSummary;
  spawned: SpawnedContext[];

  /**
   * Present when smartSpawn/watch are used to scope strict kills.
   */
  sessionId?: string;
}>;

export function spawnedLedgerPathForEntry(
  entry: ExposableService,
  nature: "context" | "stdout" | "stderr",
  args: Readonly<{ sessionHome: string; rootsAbs: readonly string[] }>,
): string | undefined {
  const fileAbs = Deno.realPathSync(resolve(entry.supplier.location));

  const relFromRoot = relFromRoots(fileAbs, args.rootsAbs);
  const relDir = relDirFromRoots(fileAbs, args.rootsAbs);

  const outDir = relDir ? join(args.sessionHome, relDir) : args.sessionHome;
  const fileName = basename(relFromRoot);

  if (nature === "context") return join(outDir, `${fileName}.context.json`);
  if (nature === "stdout") return join(outDir, `${fileName}.stdout.log`);
  if (nature === "stderr") return join(outDir, `${fileName}.stderr.log`);
  return undefined;
}

function onEventForVerbose(
  v: MaterializeVerbose,
): SpawnEventListener | undefined {
  if (v === "essential" || v === "comprehensive") {
    return richTextUISpawnEvents(v);
  }
  return undefined;
}

function normalizeProvenanceKey(p: string): string {
  // taggedProcesses() emits real paths in TRUTH_YARD_PROVENANCE; normalize to resolve() for stable compare.
  return resolve(String(p ?? ""));
}

async function buildRunningProvenanceIndex(): Promise<Set<string>> {
  const set = new Set<string>();
  if (Deno.build.os !== "linux") return set;
  for await (const tp of taggedProcesses()) {
    set.add(normalizeProvenanceKey(tp.provenance));
  }
  return set;
}

async function buildTaggedByProvenance(): Promise<
  Map<string, TaggedProcess[]>
> {
  const m = new Map<string, TaggedProcess[]>();
  if (Deno.build.os !== "linux") return m;

  for await (const tp of taggedProcesses()) {
    const key = normalizeProvenanceKey(tp.provenance);
    const arr = m.get(key);
    if (arr) arr.push(tp);
    else m.set(key, [tp]);
  }
  return m;
}

async function killByRemovedPath(
  removedPath: string,
  args: Readonly<{
    strictKillsOnly: boolean;
    sessionId: string;
    taggedByProv: Map<string, TaggedProcess[]>;
  }>,
): Promise<boolean> {
  const key = normalizeProvenanceKey(removedPath);
  const hits = args.taggedByProv.get(key);
  if (!hits || hits.length === 0) return false;

  let killedAny = false;

  for (const tp of hits) {
    if (args.strictKillsOnly && tp.sessionId !== args.sessionId) continue;
    await killPID(tp.pid);
    killedAny = true;
  }

  return killedAny;
}

async function materializeOnce(
  srcPaths: Iterable<Path>,
  opts: MaterializeOptions,
  args: Readonly<{
    sessionHome: string;
    rootsAbs: readonly string[];
    sessionId: string;
  }>,
): Promise<MaterializeResult> {
  const src = Array.from(srcPaths);
  const smartSpawn = opts.smartSpawn ?? true;

  const onEvent = onEventForVerbose(opts.verbose);

  const spawned: SpawnedContext[] = [];

  const runningProvenance = smartSpawn
    ? await buildRunningProvenanceIndex()
    : new Set<string>();

  const spawnedLedgerPath = (
    entry: ExposableService,
    nature: "context" | "stdout" | "stderr",
  ): string | undefined =>
    spawnedLedgerPathForEntry(entry, nature, {
      sessionHome: args.sessionHome,
      rootsAbs: args.rootsAbs,
    });

  const expose = (entry: ExposableService, _candidate: string) => {
    // Smart spawn gating (Linux-only taggedProcesses). If not Linux, runningProvenance is empty.
    if (smartSpawn) {
      let provKey: string;
      try {
        const prov = Deno.realPathSync(resolve(entry.supplier.location));
        provKey = normalizeProvenanceKey(prov);
      } catch {
        // If we can't resolve, don't block spawning; fallback to allowing.
        provKey = normalizeProvenanceKey(resolve(entry.supplier.location));
      }

      if (runningProvenance.has(provKey)) return false;
    }

    const fileAbs = Deno.realPathSync(resolve(entry.supplier.location));
    const relFromRoot = relFromRoots(fileAbs, args.rootsAbs);
    const proxyEndpointPrefix = proxyPrefixFromRel(relFromRoot);
    return { proxyEndpointPrefix, exposableServiceConf: {} } as const;
  };

  const gen = spawn(src, expose, spawnedLedgerPath, {
    onEvent,
    probe: { enabled: false },
    sessionId: args.sessionId,
    listenHost: opts.listenHost,
    portStart: opts.startPort,
  });

  while (true) {
    const next = await gen.next();
    if (next.done) {
      return {
        sessionHome: args.sessionHome,
        summary: next.value as SpawnSummary,
        spawned,
        sessionId: args.sessionId,
      };
    }
    spawned.push(next.value);
  }
}

/**
 * One-shot materialize.
 *
 * If you want watch mode, call materializeWatch().
 */
export async function materialize(
  srcPaths: Iterable<Path>,
  opts: MaterializeOptions,
): Promise<MaterializeResult> {
  const src = Array.from(srcPaths);

  const rootsAbs = src.map((p) => Deno.realPathSync(resolve(p.path)));

  const spawnedLedgerHome = resolve(opts.spawnedLedgerHome);
  await ensureDir(spawnedLedgerHome);

  const session = await createSpawnSessionHome(spawnedLedgerHome);

  // One-shot sessionId: used for process tags (and strict kill scoping if you reuse it elsewhere).
  const sessionId = crypto.randomUUID();

  return await materializeOnce(src, opts, {
    sessionHome: session.sessionHome,
    rootsAbs,
    sessionId,
  });
}

/**
 * Watch mode:
 * - remove => kill
 * - create/modify/other => rerun materializeOnce in smartSpawn mode (reconciles via expose gating)
 *
 * Yields a MaterializeResult for the initial run and then after each debounced event batch.
 */
export async function* materializeWatch(
  srcPaths: Iterable<Path>,
  opts: MaterializeOptions,
): AsyncGenerator<MaterializeResult> {
  const watch = opts.watch;
  const watchEnabled = watch?.enabled ?? true;
  if (!watchEnabled) {
    yield await materialize(srcPaths, opts);
    return;
  }

  const src = Array.from(srcPaths);
  const rootsAbs = src.map((p) => Deno.realPathSync(resolve(p.path)));

  const spawnedLedgerHome = resolve(opts.spawnedLedgerHome);
  await ensureDir(spawnedLedgerHome);

  // Watch loop uses a stable sessionId so strictKillsOnly can scope kills to what this loop spawned.
  const sessionId = crypto.randomUUID();

  const session = await createSpawnSessionHome(spawnedLedgerHome);

  const debounceMs = watch?.debounceMs ?? 750;
  const strictKillsOnly = watch?.strictKillsOnly ?? false;

  // Initial run
  yield await materializeOnce(src, {
    ...opts,
    smartSpawn: opts.smartSpawn ?? true,
  }, {
    sessionHome: session.sessionHome,
    rootsAbs,
    sessionId,
  });

  // Watch all roots (best effort). If a srcPath is a file, watchFs will still work.
  // Watch all roots (best effort). If a srcPath is a file, watchFs will still work.
  const rootsToWatch = src.map((p) => p.path);
  const watcher = Deno.watchFs(rootsToWatch, { recursive: true });
  const it = watcher[Symbol.asyncIterator]();

  const isDbCandidate = (p: string): boolean => {
    const s = resolve(p);
    if (/-wal$/i.test(s) || /-shm$/i.test(s) || /-journal$/i.test(s)) {
      return false;
    }
    return /\.(sqlite(\.db)?|db)$/i.test(s);
  };

  const listDbCandidates = async (
    roots: readonly string[],
  ): Promise<Set<string>> => {
    const out = new Set<string>();

    const walk = async (dir: string) => {
      let entries: Deno.DirEntry[];
      try {
        entries = [];
        for await (const e of Deno.readDir(dir)) entries.push(e);
      } catch {
        return; // permissions, missing dir, etc. => treat as empty
      }

      for (const e of entries) {
        const full = resolve(dir, e.name);
        if (e.isDirectory) {
          await walk(full);
          continue;
        }
        if (!e.isFile) continue;
        if (isDbCandidate(full)) out.add(full);
      }
    };

    for (const r of roots) {
      const rp = resolve(r);
      try {
        const st = await Deno.lstat(rp);
        if (st.isDirectory) {
          await walk(rp);
        } else if (st.isFile) {
          if (isDbCandidate(rp)) out.add(rp);
        }
      } catch {
        // root missing => ignore
      }
    }

    return out;
  };

  const diffSets = (
    prev: ReadonlySet<string>,
    next: ReadonlySet<string>,
  ): { added: string[]; removed: string[] } => {
    const added: string[] = [];
    const removed: string[] = [];

    for (const p of next) if (!prev.has(p)) added.push(p);
    for (const p of prev) if (!next.has(p)) removed.push(p);

    return { added, removed };
  };

  // Snapshot of what we believe exists. This is the ONLY thing that triggers reconcile.
  let snapshot = await listDbCandidates(rootsToWatch);

  // One in-flight next() promise (avoid re-entrant iterator issues)
  let nextP = it.next();

  while (true) {
    // Wait for first event
    const first = await nextP;
    if (first.done) break;

    // Debounce: keep consuming events until quiet for debounceMs
    while (true) {
      const timeoutP = new Promise<"timeout">((r) =>
        setTimeout(() => r("timeout"), debounceMs)
      );

      nextP = it.next();

      const raced = await Promise.race([
        nextP.then((r) => ({ kind: "next" as const, r })),
        timeoutP.then(() => ({ kind: "timeout" as const })),
      ]);

      if (raced.kind === "timeout") break;

      if (raced.r.done) return;
      // Do nothing with event details; they are just wake-ups.
      // We will re-scan and diff after the debounce window.
    }

    // After debounce, compute actual set and diff
    const current = await listDbCandidates(rootsToWatch);
    const { added, removed } = diffSets(snapshot, current);

    // If no structural change in DB set, do NOTHING (this stops the “refresh loop”)
    if (added.length === 0 && removed.length === 0) {
      snapshot = current;
      continue;
    }

    // Kill removed DB-backed processes (Linux only)
    if (removed.length > 0 && Deno.build.os === "linux") {
      const taggedByProv = await buildTaggedByProvenance();
      for (const p of removed) {
        await killByRemovedPath(p, {
          strictKillsOnly,
          sessionId,
          taggedByProv,
        });
      }
    }

    // Reconcile spawn only when DB set changed
    const res = await materializeOnce(
      src,
      { ...opts, smartSpawn: opts.smartSpawn ?? true },
      { sessionHome: session.sessionHome, rootsAbs, sessionId },
    );

    snapshot = current;
    yield res;
  }
}

/* -------------------------------- spawned ledger scan ------------------------------- */

export type SpawnedLedgerEncounter = Readonly<{
  filePath: string;
  context: SpawnedContext;
  pid: number;
  pidAlive: boolean;
  procCmdline?: string;
}>;

export async function* spawnedLedgerStates(
  spawnedLedgerHomeOrSessionHome: string,
) {
  const gen = encounters(
    [{ path: spawnedLedgerHomeOrSessionHome, globs: ["**/*.json"] }],
    fileSystemSource({}, (e) => Deno.readTextFile(e.path)),
    async ({ entry, content }) => {
      const filePath = entry.path;
      if (!filePath.endsWith(".context.json")) return null;

      const text = String(await content());
      const ctx = JSON.parse(text) as SpawnedContext;

      const pid = Number(ctx?.spawned?.pid);
      if (!Number.isFinite(pid) || pid <= 0) {
        throw new Error(`Invalid pid in context file: ${filePath}`);
      }

      const pidAlive = isPidAlive(pid);
      const procCmdline = pidAlive ? await readProcCmdline(pid) : undefined;

      return {
        filePath,
        context: ctx,
        pid,
        pidAlive,
        procCmdline,
      } satisfies SpawnedLedgerEncounter;
    },
  );

  while (true) {
    const next = await gen.next();
    if (next.done) return next.value;
    if (next.value != null) yield next.value as SpawnedLedgerEncounter;
  }
}

/* -------------------------------- reconcile ------------------------------- */

export type ReconcileItem =
  | Readonly<{
    kind: "process_without_ledger";
    pid: number;
    serviceId: string;
    sessionId: string;
    contextPath: string;
    cmdline?: string;
  }>
  | Readonly<{
    kind: "ledger_without_process";
    ledgerContextPath: string;
    pid: number;
    serviceId?: string;
    sessionId?: string;
  }>;

export type ReconcileSummary = Readonly<{
  processWithoutLedger: number;
  ledgerWithoutProcess: number;
}>;

/**
 * Reconcile operational truth (Linux tagged processes) vs spawned ledger (context.json files).
 *
 * Input can be either the ledger home or a specific sessionHome.
 */
export async function* reconcile(
  spawnedLedgerHomeOrSessionHome: string,
): AsyncGenerator<ReconcileItem, ReconcileSummary> {
  const base = resolve(spawnedLedgerHomeOrSessionHome);

  // 1) Build ledger index (by pid, by context path)
  const ledgerByPid = new Map<number, SpawnedLedgerEncounter>();
  const ledgerByContextPath = new Map<string, SpawnedLedgerEncounter>();

  for await (const le of spawnedLedgerStates(base)) {
    ledgerByPid.set(le.pid, le);
    ledgerByContextPath.set(resolve(le.filePath), le);
  }

  // 2) Walk processes and emit process-side discrepancies
  let processWithoutLedger = 0;

  const seenLedgerPids = new Set<number>();

  for await (const tp of taggedProcesses()) {
    const pid = tp.pid;

    // We consider the ledger “present” if the process' contextPath exists in ledger scan OR pid matches.
    const ledgerByCtx = ledgerByContextPath.get(resolve(tp.contextPath));
    const ledger = ledgerByCtx ?? ledgerByPid.get(pid);

    if (!ledger) {
      processWithoutLedger++;
      yield {
        kind: "process_without_ledger",
        pid,
        serviceId: tp.serviceId,
        sessionId: tp.sessionId,
        contextPath: tp.contextPath,
        cmdline: tp.cmdline,
      };
      continue;
    }

    seenLedgerPids.add(ledger.pid);
  }

  // 3) Emit ledger-side discrepancies: contexts whose pid is not alive OR not seen in taggedProcesses
  let ledgerWithoutProcess = 0;

  for (const [pid, le] of ledgerByPid.entries()) {
    const alive = isPidAlive(pid);
    if (alive && seenLedgerPids.has(pid)) continue;

    ledgerWithoutProcess++;
    yield {
      kind: "ledger_without_process",
      ledgerContextPath: resolve(le.filePath),
      pid,
      serviceId: le.context?.service?.id,
      sessionId: le.context?.session?.sessionId,
    };
  }

  return {
    processWithoutLedger,
    ledgerWithoutProcess,
  };
}
