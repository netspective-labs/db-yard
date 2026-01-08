// lib/serve/watch.ts
import { ensureDir } from "@std/fs";
import { resolve } from "@std/path";
import type { Path } from "../discover.ts";
import {
  exposable,
  type ExposableService,
  type ExposableServiceConf,
} from "../exposable.ts";
import { type SpawnedStateEncounter, spawnedStates } from "../materialize.ts";
import {
  killPID,
  spawn,
  type SpawnEventListener,
  type SpawnOptions,
} from "../spawn.ts";
import { tabular } from "../tabular.ts";
import {
  createSpawnSessionHome,
  relFromRoots,
  resolveRootsAbs,
  spawnStatePathForEntry,
} from "../session.ts";
import { proxyPrefixFromRel } from "../path.ts";

export type WatchEvent =
  | Readonly<{ type: "watch_start"; roots: string[]; sessionHome: string }>
  | Readonly<{ type: "fs_event"; kind: string; paths: string[] }>
  | Readonly<{
    type: "reconcile_start";
    reason: "fs" | "timer" | "initial";
  }>
  | Readonly<{
    type: "reconcile_end";
    reason: "fs" | "timer" | "initial";
    discovered: number;
    ledger: number;
    killed: number;
    spawned: number;
    durationMs: number;
  }>
  | Readonly<{
    type: "killed";
    serviceId: string;
    pid: number;
    filePath?: string;
    reason: "missing" | "undiscovered" | "dead";
  }>
  | Readonly<{
    type: "error";
    phase:
      | "discover"
      | "read_ledger"
      | "kill"
      | "spawn"
      | "watch"
      | "reconcile";
    error: unknown;
  }>
  | Readonly<{ type: "watch_end"; reason: "aborted" | "closed" | "error" }>;

export type WatchOptions = Readonly<{
  spawnStateHome: string;
  debounceMs?: number;
  reconcileEveryMs?: number;
  signal?: AbortSignal;

  spawn?: Readonly<
    Pick<
      SpawnOptions,
      | "host"
      | "listenHost"
      | "portStart"
      | "sqlpageBin"
      | "sqlpageEnv"
      | "surveilrBin"
      | "defaultStdoutLogPath"
      | "defaultStderrLogPath"
    >
  >;

  onSpawnEvent?: SpawnEventListener;
  onWatchEvent?: (event: WatchEvent) => void | Promise<void>;
}>;

export type WatchSessionArgs = Readonly<{
  sessionHome: string;
  rootsAbs: readonly string[];
}>;

async function emit(opts: WatchOptions, event: WatchEvent): Promise<void> {
  const fn = opts.onWatchEvent;
  if (!fn) return;
  try {
    await fn(event);
  } catch {
    // ignore listener failures
  }
}

function isAborted(signal: AbortSignal | undefined): boolean {
  return signal?.aborted === true;
}

function safeWatcherEventKind(e: Deno.FsEvent): string {
  return String((e as { kind?: unknown }).kind ?? "unknown");
}

function safePathsFromEvent(e: Deno.FsEvent): string[] {
  const p = (e as { paths?: unknown }).paths;
  if (Array.isArray(p)) return p.map((x) => String(x));
  return [];
}

function serviceKey(service: ExposableService): string {
  return service.id;
}

async function discoverServices(
  srcPaths: Iterable<Path>,
): Promise<Map<string, ExposableService>> {
  const out = new Map<string, ExposableService>();
  for await (const svc of exposable(tabular(srcPaths))) {
    out.set(serviceKey(svc), svc);
  }
  return out;
}

async function readLedger(
  sessionHome: string,
): Promise<Map<string, SpawnedStateEncounter>> {
  const out = new Map<string, SpawnedStateEncounter>();
  for await (const st of spawnedStates(sessionHome)) {
    const id = String(st?.context?.service?.id ?? "");
    if (!id) continue;
    out.set(id, st);
  }
  return out;
}

function isPidAliveNow(pid: number): boolean {
  try {
    Deno.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function pickNextPortStart(
  desiredStart: number,
  ledger: Map<string, SpawnedStateEncounter>,
): number {
  const used = new Set<number>();
  for (const st of ledger.values()) {
    const p = Number(st?.context?.listen?.port);
    if (Number.isFinite(p) && p > 0) {
      const alive = Number.isFinite(st.pid) && st.pid > 0
        ? isPidAliveNow(st.pid)
        : false;
      if (alive) used.add(p);
    }
  }
  let port = desiredStart;
  while (used.has(port)) port++;
  return port;
}

async function spawnMissing(
  args: Readonly<{
    srcPaths: Iterable<Path>;
    sessionHome: string;
    rootsAbs: readonly string[];
    toSpawn: ReadonlySet<string>;
    onSpawnEvent?: SpawnEventListener;
    spawnOpts?: WatchOptions["spawn"];
  }>,
): Promise<number> {
  const { srcPaths, sessionHome, rootsAbs, toSpawn, onSpawnEvent, spawnOpts } =
    args;
  if (toSpawn.size === 0) return 0;

  const expose = (entry: ExposableService, _candidate: string) => {
    const id = serviceKey(entry);
    if (!toSpawn.has(id)) return false;

    const fileAbs = Deno.realPathSync(resolve(entry.supplier.location));
    const rel = relFromRoots(fileAbs, rootsAbs);
    const proxyEndpointPrefix = proxyPrefixFromRel(rel);

    return {
      proxyEndpointPrefix,
      exposableServiceConf: {} as ExposableServiceConf,
    } as const;
  };

  const spawnStatePath = (
    entry: ExposableService,
    nature: "context" | "stdout" | "stderr",
  ) => spawnStatePathForEntry(entry, nature, { sessionHome, rootsAbs });

  const desiredPortStart = spawnOpts?.portStart ?? 3000;
  const ledger = await readLedger(sessionHome);
  const portStart = pickNextPortStart(desiredPortStart, ledger);

  const gen = spawn(srcPaths, expose, spawnStatePath, {
    ...(spawnOpts ?? {}),
    portStart,
    onEvent: onSpawnEvent,
    probe: { enabled: false },
  });

  let spawnedCount = 0;
  while (true) {
    const next = await gen.next();
    if (next.done) break;
    spawnedCount++;
  }
  return spawnedCount;
}

async function reconcileOnce(
  args: Readonly<{
    srcPaths: Iterable<Path>;
    sessionHome: string;
    rootsAbs: readonly string[];
    reason: "fs" | "timer" | "initial";
    opts: WatchOptions;
  }>,
): Promise<void> {
  const { srcPaths, sessionHome, rootsAbs, reason, opts } = args;

  const t0 = performance.now();
  await emit(opts, { type: "reconcile_start", reason });

  let discovered: Map<string, ExposableService>;
  try {
    discovered = await discoverServices(srcPaths);
  } catch (error) {
    await emit(opts, { type: "error", phase: "discover", error });
    await emit(opts, {
      type: "reconcile_end",
      reason,
      discovered: 0,
      ledger: 0,
      killed: 0,
      spawned: 0,
      durationMs: performance.now() - t0,
    });
    return;
  }

  let ledger: Map<string, SpawnedStateEncounter>;
  try {
    ledger = await readLedger(sessionHome);
  } catch (error) {
    await emit(opts, { type: "error", phase: "read_ledger", error });
    ledger = new Map();
  }

  let killedCount = 0;

  for (const [id, st] of ledger) {
    const loc = String(st?.context?.supplier?.location ?? "");
    const pid = Number(st?.pid);
    const alive = Number.isFinite(pid) && pid > 0 ? isPidAliveNow(pid) : false;

    const isStillDiscovered = discovered.has(id);

    let fileExists = true;
    if (loc) {
      try {
        await Deno.stat(loc);
      } catch {
        fileExists = false;
      }
    }

    if (!isStillDiscovered || !fileExists) {
      if (alive) {
        try {
          await killPID(pid);
          killedCount++;
          await emit(opts, {
            type: "killed",
            serviceId: id,
            pid,
            filePath: st.filePath,
            reason: !fileExists ? "missing" : "undiscovered",
          });
        } catch (error) {
          await emit(opts, { type: "error", phase: "kill", error });
        }
      }
    }
  }

  const toSpawn = new Set<string>();
  for (const [id] of discovered) {
    const st = ledger.get(id);
    if (!st) {
      toSpawn.add(id);
      continue;
    }
    const pid = Number(st?.pid);
    const alive = Number.isFinite(pid) && pid > 0 ? isPidAliveNow(pid) : false;
    if (!alive) toSpawn.add(id);
  }

  let spawnedCount = 0;
  if (toSpawn.size > 0) {
    try {
      spawnedCount = await spawnMissing({
        srcPaths,
        sessionHome,
        rootsAbs,
        toSpawn,
        onSpawnEvent: opts.onSpawnEvent,
        spawnOpts: opts.spawn,
      });
    } catch (error) {
      await emit(opts, { type: "error", phase: "spawn", error });
    }
  }

  await emit(opts, {
    type: "reconcile_end",
    reason,
    discovered: discovered.size,
    ledger: ledger.size,
    killed: killedCount,
    spawned: spawnedCount,
    durationMs: performance.now() - t0,
  });
}

/**
 * Run watch loop in a pre-created sessionHome (used by web-ui so UI + watcher share one session).
 */
export async function watchYardInSession(
  srcPaths: Iterable<Path>,
  opts: WatchOptions,
  session: WatchSessionArgs,
): Promise<void> {
  const debounceMs = opts.debounceMs ?? 250;

  const srcArr = Array.from(srcPaths);
  const roots = srcArr.map((p) => resolve(p.path));
  await emit(opts, {
    type: "watch_start",
    roots,
    sessionHome: session.sessionHome,
  });

  const signal = opts.signal;

  let reconcileRunning: Promise<void> | undefined;
  let reconcileQueued = false;
  let debounceTimer: number | undefined;

  const runReconcile = (reason: "fs" | "timer" | "initial") => {
    if (isAborted(signal)) return;

    const schedule = () => {
      if (isAborted(signal)) return;
      if (reconcileRunning) {
        reconcileQueued = true;
        return;
      }

      reconcileRunning = (async () => {
        try {
          await reconcileOnce({
            srcPaths: srcArr,
            sessionHome: session.sessionHome,
            rootsAbs: session.rootsAbs,
            reason,
            opts,
          });
        } catch (error) {
          await emit(opts, { type: "error", phase: "reconcile", error });
        } finally {
          reconcileRunning = undefined;
          if (reconcileQueued && !isAborted(signal)) {
            reconcileQueued = false;
            schedule();
          }
        }
      })();
    };

    if (reason === "fs") {
      if (debounceTimer !== undefined) clearTimeout(debounceTimer);
      debounceTimer = setTimeout(() => {
        debounceTimer = undefined;
        schedule();
      }, debounceMs) as unknown as number;
    } else {
      schedule();
    }
  };

  runReconcile("initial");

  let intervalId: number | undefined;
  const every = opts.reconcileEveryMs ?? 0;
  if (every > 0) {
    intervalId = setInterval(
      () => runReconcile("timer"),
      every,
    ) as unknown as number;
  }

  const watcher = Deno.watchFs(roots, { recursive: true });

  const abortHandler = () => {
    try {
      watcher.close();
    } catch {
      // ignore
    }
  };

  if (signal) {
    if (signal.aborted) abortHandler();
    else signal.addEventListener("abort", abortHandler, { once: true });
  }

  try {
    for await (const ev of watcher) {
      if (isAborted(signal)) break;

      await emit(opts, {
        type: "fs_event",
        kind: safeWatcherEventKind(ev),
        paths: safePathsFromEvent(ev),
      });
      runReconcile("fs");
    }

    await emit(opts, {
      type: "watch_end",
      reason: isAborted(signal) ? "aborted" : "closed",
    });
  } catch (error) {
    await emit(opts, { type: "error", phase: "watch", error });
    await emit(opts, { type: "watch_end", reason: "error" });
  } finally {
    if (intervalId !== undefined) {
      try {
        clearInterval(intervalId);
      } catch {
        // ignore
      }
    }
    if (debounceTimer !== undefined) {
      try {
        clearTimeout(debounceTimer);
      } catch {
        // ignore
      }
    }
    if (signal) {
      try {
        signal.removeEventListener("abort", abortHandler);
      } catch {
        // ignore
      }
    }
    try {
      watcher.close();
    } catch {
      // ignore
    }
  }
}

/**
 * Back-compat API: creates a new stamped session dir under spawnStateHome and runs watcher in it.
 */
export async function watchYard(
  srcPaths: Iterable<Path>,
  opts: WatchOptions,
): Promise<void> {
  const spawnStateHome = resolve(opts.spawnStateHome);
  await ensureDir(spawnStateHome);

  const srcArr = Array.from(srcPaths);
  const rootsAbs = resolveRootsAbs(srcArr);
  const session = await createSpawnSessionHome(spawnStateHome);

  await watchYardInSession(srcArr, opts, {
    sessionHome: session.sessionHome,
    rootsAbs,
  });
}
