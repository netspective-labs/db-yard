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
  // taggedProcesses() emits real paths in DB_YARD_PROVENANCE; normalize to resolve() for stable compare.
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
  const rootsToWatch = src.map((p) => p.path);

  const watcher = Deno.watchFs(rootsToWatch, { recursive: true });

  let timer: number | undefined;
  let pending = false;

  const touched = new Set<string>();
  const removed = new Set<string>();

  const waitForDebounce = async () => {
    if (!pending) return;
    await new Promise<void>((resolvePromise) => {
      const check = () => {
        if (timer === undefined) resolvePromise();
        else setTimeout(check, 10);
      };
      check();
    });
  };

  const schedule = () => {
    pending = true;
    if (timer !== undefined) clearTimeout(timer);
    timer = setTimeout(() => {
      timer = undefined;
    }, debounceMs) as unknown as number;
  };

  for await (const ev of watcher) {
    // Collect paths
    for (const p of ev.paths ?? []) {
      touched.add(resolve(p));
      if (ev.kind === "remove") removed.add(resolve(p));
    }

    schedule();

    // Fast path: keep accumulating until debounce fires.
    if (timer !== undefined) continue;

    // Debounce completed; process batch.
    pending = false;

    // If any removals happened, kill matches.
    if (removed.size > 0 && Deno.build.os === "linux") {
      const taggedByProv = await buildTaggedByProvenance();

      for (const p of removed) {
        // Kill by provenance key; optionally scope by sessionId.
        await killByRemovedPath(p, {
          strictKillsOnly,
          sessionId,
          taggedByProv,
        });
      }
    }

    // Always rerun one-shot reconcile spawn (smartSpawn makes it safe/idempotent)
    const res = await materializeOnce(
      src,
      { ...opts, smartSpawn: opts.smartSpawn ?? true },
      { sessionHome: session.sessionHome, rootsAbs, sessionId },
    );

    // Reset batch state
    touched.clear();
    removed.clear();

    yield res;
  }

  // Should not normally exit unless watcher closes.
  await waitForDebounce();
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
