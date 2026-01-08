// lib/serve/watch.ts
import { ensureDir } from "@std/fs";
import { join, resolve } from "@std/path";
import type { Path } from "../discover.ts";
import {
  exposable,
  type ExposableService,
  type ExposableServiceConf,
} from "../exposable.ts";
import {
  killPID,
  type SpawnedStateEncounter,
  spawnedStates,
} from "../materialize.ts";
import { spawn, type SpawnEventListener, type SpawnOptions } from "../spawn.ts";
import { tabular } from "../tabular.ts";

export type WatchEvent =
  | Readonly<{ type: "watch_start"; roots: string[]; activeDir: string }>
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

  /**
   * Stable session directory name under spawnStateHome used for continuous mode.
   * Default: "active"
   */
  activeDirName?: string;

  /**
   * Debounce file system events before reconciling.
   * Default: 250ms
   */
  debounceMs?: number;

  /**
   * Optional periodic reconcile in addition to FS events.
   * If omitted or <= 0, disabled.
   */
  reconcileEveryMs?: number;

  /**
   * Optional cancellation.
   */
  signal?: AbortSignal;

  /**
   * Forwarded to spawn() for service processes.
   */
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

  /**
   * Wired to spawn()'s onEvent (low-level spawn telemetry).
   */
  onSpawnEvent?: SpawnEventListener;

  /**
   * High-level watch events.
   */
  onWatchEvent?: (event: WatchEvent) => void | Promise<void>;
}>;

async function emit(
  opts: WatchOptions,
  event: WatchEvent,
): Promise<void> {
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
  // Deno.FsEvent.kind is a string union; keep it as a string for logging.
  return String((e as { kind?: unknown }).kind ?? "unknown");
}

function safePathsFromEvent(e: Deno.FsEvent): string[] {
  const p = (e as { paths?: unknown }).paths;
  if (Array.isArray(p)) return p.map((x) => String(x));
  return [];
}

function serviceKey(service: ExposableService): string {
  // Spawn pipeline uses service.id as the canonical id and that is persisted.
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
  activeDir: string,
): Promise<Map<string, SpawnedStateEncounter>> {
  const out = new Map<string, SpawnedStateEncounter>();
  for await (const st of spawnedStates(activeDir)) {
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

/**
 * Deterministic paths for active ledger files.
 * Keeps the ledger stable across reconciles without needing session stamping logic.
 */
function defaultSpawnStatePath(
  activeDir: string,
  entry: ExposableService,
  nature: "context" | "stdout" | "stderr",
): string {
  const base = `${entry.kind}-${entry.id}`;
  if (nature === "context") return join(activeDir, `${base}.context.json`);
  if (nature === "stdout") return join(activeDir, `${base}.stdout.log`);
  return join(activeDir, `${base}.stderr.log`);
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
    activeDir: string;
    toSpawn: ReadonlySet<string>;
    onSpawnEvent?: SpawnEventListener;
    spawnOpts?: WatchOptions["spawn"];
  }>,
): Promise<number> {
  const { srcPaths, activeDir, toSpawn, onSpawnEvent, spawnOpts } = args;
  if (toSpawn.size === 0) return 0;

  const expose = (
    entry: ExposableService,
    proxyEndpointPrefixCandidate: string,
  ) => {
    const id = serviceKey(entry);
    if (!toSpawn.has(id)) return false;

    // Keep proxy prefix deterministic. Use the candidate from spawn() defaulting.
    // If you want stricter stability, consider adding an option to override this.
    const proxyEndpointPrefix = proxyEndpointPrefixCandidate;

    return {
      proxyEndpointPrefix,
      exposableServiceConf: {} as ExposableServiceConf,
    } as const;
  };

  const spawnStatePath = (
    entry: ExposableService,
    nature: "context" | "stdout" | "stderr",
  ) => defaultSpawnStatePath(activeDir, entry, nature);

  const desiredPortStart = spawnOpts?.portStart ?? 3000;
  const ledger = await readLedger(activeDir);
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
    activeDir: string;
    reason: "fs" | "timer" | "initial";
    opts: WatchOptions;
  }>,
): Promise<void> {
  const { srcPaths, activeDir, reason, opts } = args;

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
    ledger = await readLedger(activeDir);
  } catch (error) {
    await emit(opts, { type: "error", phase: "read_ledger", error });
    ledger = new Map();
  }

  let killedCount = 0;

  // 3) Kill ledger entries whose source file is deleted or no longer discovered.
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

  // 4) Spawn missing services or services with dead PIDs.
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
        activeDir,
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
 * Robust “db-yard watcher” that keeps spawned services in sync with filesystem state.
 */
export async function watchYard(
  srcPaths: Iterable<Path>,
  opts: WatchOptions,
): Promise<void> {
  const debounceMs = opts.debounceMs ?? 250;
  const activeDirName = opts.activeDirName ?? "active";

  const spawnStateHome = resolve(opts.spawnStateHome);
  const activeDir = resolve(join(spawnStateHome, activeDirName));
  await ensureDir(activeDir);

  const roots = Array.from(srcPaths).map((p) => resolve(p.path));
  await emit(opts, { type: "watch_start", roots, activeDir });

  const signal = opts.signal;

  // Serialize reconciles, coalesce triggers.
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
          await reconcileOnce({ srcPaths, activeDir, reason, opts });
        } catch (error) {
          await emit(opts, { type: "error", phase: "reconcile", error });
        } finally {
          reconcileRunning = undefined;
          if (reconcileQueued && !isAborted(signal)) {
            reconcileQueued = false;
            // subsequent runs after coalescing are treated as fs-triggered if any were queued
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

  // Initial reconcile.
  runReconcile("initial");

  // Optional periodic reconcile.
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

    if (isAborted(signal)) {
      await emit(opts, { type: "watch_end", reason: "aborted" });
    } else {
      await emit(opts, { type: "watch_end", reason: "closed" });
    }
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

/*
Suggestions for minor exports to reduce duplication / improve stability:

1) materialize.ts has useful, tested path logic for mapping supplier.location to a
   stable proxy prefix and hierarchical state paths. None of that is exported.
   Consider exporting small helpers such as:
   - proxyPrefixFromRel(relFromRoot: string)
   - relFromRoots(fileAbs: string, rootsAbs: readonly string[])
   - a reusable spawnStatePath factory

2) exposable.ts uses safeServiceId(dbPath) based on basename(dbPath). If you need
   globally unique service ids across directories, consider exporting an alternate
   id strategy (e.g., hashing dbPath) or exposing an option in exposable().
*/
