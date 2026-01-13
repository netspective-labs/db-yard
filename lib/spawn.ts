// lib/spawn.ts
import { resolve } from "@std/path";

import type { Path } from "./discover.ts";
import {
  exposable,
  type ExposableService,
  type ExposableServiceConf,
  type SpawnedProcess,
  type SpawnHost,
  type SpawnLogTarget,
} from "./exposable.ts";
import { ensureParentDir, joinUrl, safeRelFromRoot } from "./path.ts";
import { tabular, TabularDataSupplier } from "./tabular.ts";

export type SpawnLedgerNature = "context" | "stdout" | "stderr";

export type SpawnLedgerPath<Entry> = (
  entry: Entry,
  nature: SpawnLedgerNature,
) => string | undefined;

export type SpawnSummary = Readonly<{
  spawned: string[];
  skipped: string[];
  errored: string[];
  errors: ReadonlyArray<Readonly<{ id: string; error: unknown }>>;
}>;

/* -------------------------------- events -------------------------------- */

export type SpawnSession = Readonly<{
  sessionId: string;
  host: SpawnHost;
  startedAt: string;
}>;

export type SpawnEventBase = Readonly<{
  session: SpawnSession;
  ts: string;
  tMs: number;
}>;

export type SpawnEvent =
  | (SpawnEventBase & Readonly<{ type: "session_start" }>)
  | (SpawnEventBase & Readonly<{ type: "discovered"; serviceId: string }>)
  | (
    & SpawnEventBase
    & Readonly<{
      type: "expose_decision";
      serviceId: string;
      shouldSpawn: boolean;
    }>
  )
  | (
    & SpawnEventBase
    & Readonly<{
      type: "port_allocated";
      serviceId: string;
      listenHost: string;
      port: number;
      baseUrl: string;
    }>
  )
  | (
    & SpawnEventBase
    & Readonly<{
      type: "paths_resolved";
      serviceId: string;
      paths: Readonly<{ context?: string; stdout?: string; stderr?: string }>;
    }>
  )
  | (
    & SpawnEventBase
    & Readonly<{
      type: "spawning";
      serviceId: string;
      proxyEndpointPrefix: string;
    }>
  )
  | (
    & SpawnEventBase
    & Readonly<{
      type: "spawned";
      serviceId: string;
      pid: number;
    }>
  )
  | (
    & SpawnEventBase
    & Readonly<{
      type: "context_written";
      serviceId: string;
      path: string;
    }>
  )
  | (
    & SpawnEventBase
    & Readonly<{
      type: "reachability_probe_started";
      serviceId: string;
      url: string;
      timeoutMs: number;
    }>
  )
  | (
    & SpawnEventBase
    & Readonly<{
      type: "service_reachable";
      serviceId: string;
      url: string;
      status: number;
      durationMs: number;
    }>
  )
  | (
    & SpawnEventBase
    & Readonly<{
      type: "service_unreachable";
      serviceId: string;
      url: string;
      durationMs: number;
      error: unknown;
    }>
  )
  | (
    & SpawnEventBase
    & Readonly<{
      type: "reachability_probe_skipped";
      serviceId: string;
      reason: "disabled";
    }>
  )
  | (
    & SpawnEventBase
    & Readonly<{
      type: "error";
      serviceId: string;
      phase: "expose" | "spawn" | "write_context" | "probe";
      error: unknown;
    }>
  )
  | (SpawnEventBase & Readonly<{ type: "complete"; summary: SpawnSummary }>)
  | (
    & SpawnEventBase
    & Readonly<{
      type: "session_end";
      summary: SpawnSummary;
      totalMs: number;
    }>
  );

export type SpawnEventListener = (event: SpawnEvent) => void | Promise<void>;

/* -------------------------------- expose -------------------------------- */

export type ExposeDecision =
  | false
  | Readonly<{
    proxyEndpointPrefix: string;
    exposableServiceConf?: ExposableServiceConf;
  }>;

export type ExposeFn = (
  entry: ExposableService,
  proxyEndpointPrefixCandidate: string,
) => ExposeDecision | Promise<ExposeDecision>;

/* -------------------------------- options -------------------------------- */

export type ReachabilityProbe = Readonly<{
  enabled?: boolean;
  timeoutMs?: number;
  url?: (
    args: Readonly<{
      baseUrl: string;
      proxyEndpointPrefix: string;
      service: ExposableService;
    }>,
  ) => string;
}>;

export type SpawnOptions = Readonly<{
  host?: SpawnHost;
  listenHost?: string;
  portStart?: number;
  findFreePort?: boolean;
  portMax?: number; // Optional upper bound for scanning. If omitted, defaults to 65535.

  sqlpageBin?: string;
  sqlpageEnv?: string;
  surveilrBin?: string;

  onEvent?: SpawnEventListener;
  sessionId?: string;

  probe?: ReachabilityProbe;

  defaultStdoutLogPath?: SpawnLogTarget;
  defaultStderrLogPath?: SpawnLogTarget;
}>;

/* -------------------------------- context -------------------------------- */

export type SpawnedContext = Readonly<{
  startedAt: string;

  service: Readonly<{
    id: string;
    kind: ExposableService["kind"];
    label: string;
    proxyEndpointPrefix: string;
    upstreamUrl: string;
  }>;

  supplier: TabularDataSupplier;

  session: SpawnSession;

  listen: Readonly<{
    host: string;
    port: number;
    baseUrl: string;
    probeUrl: string;
  }>;

  spawned: Readonly<{
    pid: number;
    plan: SpawnedProcess["plan"];
  }>;

  paths: Readonly<{
    context?: string;
    stdout?: string;
    stderr?: string;
  }>;
}>;

/* ------------------------------ typing helpers --------------------------- */

type DistributiveOmit<T, K extends PropertyKey> = T extends unknown ? Omit<T, K>
  : never;

/* --------------------------- pid/process helpers -------------------------- */

export function isPidAlive(pid: number): boolean {
  try {
    Deno.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

export async function readProcCmdline(
  pid: number,
): Promise<string | undefined> {
  // Linux-only; return undefined elsewhere or if missing.
  const path = `/proc/${pid}/cmdline`;
  try {
    const bytes = await Deno.readFile(path);
    const raw = new TextDecoder().decode(bytes);
    const cleaned = raw.replaceAll("\u0000", " ").trim();
    return cleaned.length ? cleaned : undefined;
  } catch {
    return undefined;
  }
}

function parseProcEnviron(bytes: Uint8Array): Record<string, string> {
  const text = new TextDecoder().decode(bytes);
  const out: Record<string, string> = {};
  // /proc/<pid>/environ is NUL-separated KEY=VAL strings
  for (const part of text.split("\u0000")) {
    if (!part) continue;
    const eq = part.indexOf("=");
    if (eq <= 0) continue;
    const k = part.slice(0, eq);
    const v = part.slice(eq + 1);
    out[k] = v;
  }
  return out;
}

export async function readProcEnviron(
  pid: number,
): Promise<Record<string, string>> {
  const path = `/proc/${pid}/environ`;
  const bytes = await Deno.readFile(path);
  return parseProcEnviron(bytes);
}

export async function killPID(pid: number): Promise<void> {
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

/* ------------------------------ port helpers ------------------------------ */

// deno-lint-ignore require-await
async function isPortAvailable(
  listenHost: string,
  port: number,
): Promise<boolean> {
  let listener: Deno.Listener | undefined;
  try {
    listener = Deno.listen({ hostname: listenHost, port });
    return true;
  } catch {
    return false;
  } finally {
    try {
      listener?.close();
    } catch {
      // ignore
    }
  }
}

async function allocatePort(
  args: Readonly<{
    listenHost: string;
    candidatePort: number;
    findFreePort: boolean;
    portMax: number;
  }>,
): Promise<number> {
  const { listenHost, candidatePort, findFreePort, portMax } = args;

  const start = Math.max(1, Math.floor(candidatePort));
  const max = Math.min(65535, Math.max(start, Math.floor(portMax)));

  if (!findFreePort) return start;

  for (let p = start; p <= max; p++) {
    if (await isPortAvailable(listenHost, p)) return p;
  }

  throw new Error(
    `No available port found on ${listenHost} in range ${start}-${max}`,
  );
}

/* --------------------------------- api ---------------------------------- */

export async function* spawn(
  srcPaths: Iterable<Path>,
  expose: ExposeFn,
  spawnedLedgerPath: SpawnLedgerPath<ExposableService>,
  opts: SpawnOptions,
): AsyncGenerator<SpawnedContext, SpawnSummary> {
  const host: SpawnHost = opts.host ?? { identity: "spawn", pid: Deno.pid };

  const session: SpawnSession = {
    sessionId: opts.sessionId ?? crypto.randomUUID(),
    host,
    startedAt: new Date().toISOString(),
  };

  const listenHost = opts.listenHost ?? "127.0.0.1";
  let port = opts.portStart ?? 3000;

  const findFreePort = opts.findFreePort !== false; // default true
  const portMax =
    typeof opts.portMax === "number" && Number.isFinite(opts.portMax)
      ? opts.portMax
      : 65535;

  const sqlpageBin = opts.sqlpageBin ?? "sqlpage";
  const sqlpageEnv = opts.sqlpageEnv ?? "development";
  const surveilrBin = opts.surveilrBin ?? "surveilr";

  const spawned: string[] = [];
  const skipped: string[] = [];
  const errored: string[] = [];
  const errors: Array<{ id: string; error: unknown }> = [];

  const t0 = performance.now();

  type EmitEvent = DistributiveOmit<SpawnEvent, keyof SpawnEventBase>;

  const emit = async (event: EmitEvent) => {
    if (!opts.onEvent) return;
    const e: SpawnEvent = {
      session,
      ts: new Date().toISOString(),
      tMs: performance.now() - t0,
      ...(event as EmitEvent),
    } as SpawnEvent;

    try {
      await opts.onEvent(e);
    } catch {
      // ignore listener failures
    }
  };

  await emit({ type: "session_start" });

  for await (const service of exposable(tabular(srcPaths))) {
    const id = service.id;

    await emit({ type: "discovered", serviceId: id });

    const supplier = service.supplier;

    const rel = safeRelFromRoot(supplier.srcPath?.path, supplier.location);
    const relNoExt = stripTrailingExt(rel);
    const suggestedPrefix = defaultProxyEndpointPrefix(service.kind, relNoExt);

    let decision: ExposeDecision;
    try {
      decision = await expose(service, suggestedPrefix);
    } catch (error) {
      errored.push(id);
      errors.push({ id, error });
      await emit({ type: "error", serviceId: id, phase: "expose", error });
      continue;
    }

    if (decision === false) {
      await emit({
        type: "expose_decision",
        serviceId: id,
        shouldSpawn: false,
      });
      skipped.push(id);
      continue;
    }

    await emit({ type: "expose_decision", serviceId: id, shouldSpawn: true });

    const proxyEndpointPrefix = decision.proxyEndpointPrefix;
    const exposableServiceConf: ExposableServiceConf =
      decision.exposableServiceConf ?? {};

    let allocatedPort: number;
    try {
      allocatedPort = await allocatePort({
        listenHost,
        candidatePort: port,
        findFreePort,
        portMax,
      });
    } catch (error) {
      errored.push(id);
      errors.push({ id, error });
      await emit({ type: "error", serviceId: id, phase: "spawn", error });
      continue;
    }

    const baseUrl = `http://${listenHost}:${allocatedPort}`;
    await emit({
      type: "port_allocated",
      serviceId: id,
      listenHost,
      port: allocatedPort,
      baseUrl,
    });

    const ctxPath = spawnedLedgerPath(service, "context");
    const stdoutPath = spawnedLedgerPath(service, "stdout") ??
      (typeof opts.defaultStdoutLogPath === "string"
        ? opts.defaultStdoutLogPath
        : undefined);
    const stderrPath = spawnedLedgerPath(service, "stderr") ??
      (typeof opts.defaultStderrLogPath === "string"
        ? opts.defaultStderrLogPath
        : undefined);

    if (ctxPath) await ensureParentDir(ctxPath);
    if (typeof stdoutPath === "string") await ensureParentDir(stdoutPath);
    if (typeof stderrPath === "string") await ensureParentDir(stderrPath);

    await emit({
      type: "paths_resolved",
      serviceId: id,
      paths: {
        context: ctxPath,
        stdout: typeof stdoutPath === "string" ? stdoutPath : undefined,
        stderr: typeof stderrPath === "string" ? stderrPath : undefined,
      },
    });

    await emit({ type: "spawning", serviceId: id, proxyEndpointPrefix });

    const upstreamUrl = joinUrl(
      baseUrl,
      proxyEndpointPrefix === "" ? "/" : proxyEndpointPrefix,
    );

    const probeUrl = buildProbeUrl({
      baseUrl,
      proxyEndpointPrefix,
      service,
      probe: opts.probe,
    });

    try {
      let child: SpawnedProcess;
      const contextPathAbs = ctxPath ? resolve(ctxPath) : undefined;
      const provenance = Deno.realPathSync(
        resolve(service.supplier.location),
      );

      const tags = contextPathAbs
        ? {
          provenance,
          sessionId: session.sessionId,
          serviceId: id,
          contextPath: contextPathAbs,

          kind: service.kind,
          label: service.label,
          proxyEndpointPrefix,
          upstreamUrl,

          listenHost,
          port: allocatedPort,
          baseUrl,
          probeUrl,
        }
        : undefined;

      if (service.kind === "sqlpage") {
        child = await service.spawn({
          host,
          init: {
            listenHost,
            port: allocatedPort,
            proxyEndpointPrefix,
            sqlpageBin,
            sqlpageEnv,
            stdoutLogPath: stdoutPath,
            stderrLogPath: stderrPath,
            processTags: tags,
          },
          exposableServiceConf,
        });
      } else {
        child = await service.spawn({
          host,
          init: {
            listenHost,
            port: allocatedPort,
            proxyEndpointPrefix,
            surveilrBin,
            stdoutLogPath: stdoutPath,
            stderrLogPath: stderrPath,
            processTags: tags,
          },
          exposableServiceConf,
        });
      }

      await emit({ type: "spawned", serviceId: id, pid: child.pid });

      const ctx: SpawnedContext = {
        startedAt: new Date().toISOString(),
        service: {
          id,
          kind: service.kind,
          label: service.label,
          proxyEndpointPrefix,
          upstreamUrl,
        },
        supplier,
        session,
        listen: {
          host: listenHost,
          port: allocatedPort,
          baseUrl,
          probeUrl,
        },
        spawned: {
          pid: child.pid,
          plan: child.plan,
        },
        paths: {
          context: ctxPath,
          stdout: typeof stdoutPath === "string" ? stdoutPath : undefined,
          stderr: typeof stderrPath === "string" ? stderrPath : undefined,
        },
      };

      if (ctxPath) {
        try {
          await ensureParentDir(ctxPath);
          await Deno.writeTextFile(
            ctxPath,
            JSON.stringify(ctx, null, 2) + "\n",
          );
          await emit({ type: "context_written", serviceId: id, path: ctxPath });
        } catch (error) {
          await emit({
            type: "error",
            serviceId: id,
            phase: "write_context",
            error,
          });
        }
      }

      if (opts.probe?.enabled) {
        const timeoutMs = opts.probe.timeoutMs ?? 15_000;
        await emit({
          type: "reachability_probe_started",
          serviceId: id,
          url: probeUrl,
          timeoutMs,
        });

        const probeStarted = performance.now();
        try {
          const status = await waitForHttp200OrReturnStatus(
            probeUrl,
            timeoutMs,
          );
          const durationMs = performance.now() - probeStarted;
          await emit({
            type: "service_reachable",
            serviceId: id,
            url: probeUrl,
            status,
            durationMs,
          });
        } catch (error) {
          const durationMs = performance.now() - probeStarted;
          await emit({
            type: "service_unreachable",
            serviceId: id,
            url: probeUrl,
            durationMs,
            error,
          });
          await emit({ type: "error", serviceId: id, phase: "probe", error });
        }
      } else {
        await emit({
          type: "reachability_probe_skipped",
          serviceId: id,
          reason: "disabled",
        });
      }

      spawned.push(id);
      yield ctx;

      // Advance the global port cursor to the next port after the allocated one.
      port = allocatedPort + 1;
    } catch (error) {
      errored.push(id);
      errors.push({ id, error });
      await emit({ type: "error", serviceId: id, phase: "spawn", error });
      // Keep existing semantics: only advance port cursor on success.
    }
  }

  const summary: SpawnSummary = { spawned, skipped, errored, errors };
  await emit({ type: "complete", summary });
  await emit({ type: "session_end", summary, totalMs: performance.now() - t0 });

  return summary;
}

/* -------------------------- Linux tagged process ls -------------------------- */

export type TaggedProcess = Readonly<{
  pid: number;

  // Always sourced from env tags (source of truth for “owned by Truth Yard”)
  provenance: string;
  contextPath: string;
  sessionId: string;
  serviceId: string;

  kind?: string;
  label?: string;
  proxyEndpointPrefix?: string;
  upstreamUrl?: string;

  // Full env (best-effort; includes all tags)
  env: Record<string, string>;

  // Best-effort enrichments
  context?: SpawnedContext;
  cmdline?: string;

  // If we found a tagged process but could not fully enrich it.
  issue?: Error | unknown;
}>;

/**
 * Linux-only: yield all processes "owned" by Truth Yard using env tags:
 * - DB_YARD_PROVENANCE
 * - DB_YARD_CONTEXT_PATH
 * - DB_YARD_SESSION_ID
 * - DB_YARD_SERVICE_ID
 * - DB_YARD_KIND
 * - DB_YARD_LABEL
 * - DB_YARD_PROXY_ENDPOINT_PREFIX
 * - DB_YARD_UPSTREAM_URL
 *
 * Notes:
 * - Requires permission to read /proc/<pid>/environ for target processes.
 * - Skips processes we cannot inspect (/proc perms) or that don't include CONTEXT_PATH.
 * - Reads cmdline and context.json best-effort; yields even if those enrichments fail.
 */
export async function* taggedProcesses(): AsyncGenerator<TaggedProcess> {
  if (Deno.build.os !== "linux") {
    throw new Error("taggedProcesses() is Linux-only (requires /proc).");
  }

  let dir: AsyncIterable<Deno.DirEntry>;
  try {
    dir = Deno.readDir("/proc");
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    throw new Error(`taggedProcesses(): cannot read /proc: ${msg}`);
  }

  for await (const e of dir) {
    if (!e.isDirectory) continue;

    const name = e.name;
    if (!/^\d+$/.test(name)) continue;

    const pid = Number(name);
    if (!Number.isFinite(pid) || pid <= 0) continue;

    let env: Record<string, string>;
    try {
      env = await readProcEnviron(pid);
    } catch {
      continue;
    }

    const envProvenance = env["DB_YARD_PROVENANCE"];
    if (typeof envProvenance !== "string" || envProvenance.length === 0) {
      continue;
    }

    const envContextPath = env["DB_YARD_CONTEXT_PATH"];
    if (typeof envContextPath !== "string" || envContextPath.length === 0) {
      continue;
    }

    const sessionId = env["DB_YARD_SESSION_ID"];
    if (typeof sessionId !== "string" || sessionId.length === 0) {
      continue;
    }

    const serviceId = env["DB_YARD_SERVICE_ID"];
    if (typeof serviceId !== "string" || serviceId.length === 0) {
      continue;
    }

    const contextPath = envContextPath;
    const provenance = envProvenance;

    let cmdline: string | undefined;
    try {
      cmdline = await readProcCmdline(pid);
    } catch {
      cmdline = undefined;
    }

    let issue: Error | unknown;
    let context: SpawnedContext | undefined;

    try {
      const ctxContent = await Deno.readTextFile(contextPath);
      context = JSON.parse(ctxContent) as SpawnedContext;
    } catch (e) {
      issue = e;
      context = undefined;
    }

    const kind = typeof env["DB_YARD_KIND"] === "string"
      ? env["DB_YARD_KIND"]
      : undefined;
    const label = typeof env["DB_YARD_LABEL"] === "string"
      ? env["DB_YARD_LABEL"]
      : undefined;
    const proxyEndpointPrefix =
      typeof env["DB_YARD_PROXY_ENDPOINT_PREFIX"] === "string"
        ? env["DB_YARD_PROXY_ENDPOINT_PREFIX"]
        : undefined;
    const upstreamUrl = typeof env["DB_YARD_UPSTREAM_URL"] === "string"
      ? env["DB_YARD_UPSTREAM_URL"]
      : undefined;

    // Validate pid consistency: /proc/<pid> vs context.spawned.pid
    const ctxPidRaw = (context as unknown as { spawned?: { pid?: unknown } })
      ?.spawned?.pid;
    const ctxPid = typeof ctxPidRaw === "number"
      ? ctxPidRaw
      : Number(ctxPidRaw);

    if (context && Number.isFinite(ctxPid) && ctxPid > 0 && ctxPid !== pid) {
      const pidIssue = new Error(
        `PID mismatch: /proc pid=${pid} but context.spawned.pid=${ctxPid} (contextPath=${contextPath})`,
      );
      if (issue) {
        issue = new AggregateError(
          [issue, pidIssue],
          "taggedProcesses(): issues detected",
        );
      } else {
        issue = pidIssue;
      }
    }

    yield {
      pid,
      sessionId,
      serviceId,
      contextPath,
      provenance,
      env,
      context,
      cmdline,
      issue,
      kind,
      label,
      proxyEndpointPrefix,
      upstreamUrl,
    } satisfies TaggedProcess;
  }
}

export async function killSpawnedProcesses() {
  for await (const { pid } of taggedProcesses()) {
    await killPID(pid);
  }
}

/* -------------------------------- helpers -------------------------------- */

function stripTrailingExt(p: string): string {
  const i = p.lastIndexOf(".");
  if (i <= 0) return p;
  return p.slice(0, i);
}

function defaultProxyEndpointPrefix(kind: string, relNoExt: string): string {
  const norm = relNoExt.replaceAll("\\", "/").replaceAll(/\/+/g, "/").trim();
  const clean = norm.length === 0 ? kind : norm;
  return `/apps/${kind}/${clean}`.replaceAll(/\/+/g, "/");
}

function buildProbeUrl(
  args: Readonly<{
    baseUrl: string;
    proxyEndpointPrefix: string;
    service: ExposableService;
    probe: ReachabilityProbe | undefined;
  }>,
): string {
  const { baseUrl, proxyEndpointPrefix, service, probe } = args;
  if (probe?.url) return probe.url({ baseUrl, proxyEndpointPrefix, service });
  return joinUrl(
    baseUrl,
    proxyEndpointPrefix === "" ? "/" : proxyEndpointPrefix,
  );
}

async function waitForHttp200OrReturnStatus(
  url: string,
  timeoutMs: number,
): Promise<number> {
  const started = Date.now();
  let lastErr: unknown;

  while (Date.now() - started < timeoutMs) {
    let res: Response | undefined;

    try {
      res = await fetch(url, { redirect: "manual" });
      const status = res.status;
      await res.body?.cancel();
      if (status === 200) return status;
      lastErr = new Error(`HTTP ${status}`);
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
