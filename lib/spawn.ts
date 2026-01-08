// lib/spawn.ts
import { ensureDir } from "@std/fs";
import { dirname, relative, resolve } from "@std/path";

import type { Path } from "./discover.ts";
import { tabular, type TabularDataSupplier } from "./tabular.ts";
import {
  exposable,
  type ExposableService,
  type ExposableServiceConf,
  type SpawnedProcess,
  type SpawnHost,
  type SpawnLogTarget,
} from "./exposable.ts";

export type SpawnStateNature = "context" | "stdout" | "stderr";

export type SpawnStatePath<Entry> = (
  entry: Entry,
  nature: SpawnStateNature,
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

/* --------------------------------- api ---------------------------------- */

export async function* spawn(
  srcPaths: Iterable<Path>,
  expose: ExposeFn,
  spawnStatePath: SpawnStatePath<ExposableService>,
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

    // decision is now narrowed to the object branch
    await emit({ type: "expose_decision", serviceId: id, shouldSpawn: true });

    const proxyEndpointPrefix = decision.proxyEndpointPrefix;
    const exposableServiceConf: ExposableServiceConf =
      decision.exposableServiceConf ?? {};

    const baseUrl = `http://${listenHost}:${port}`;
    await emit({
      type: "port_allocated",
      serviceId: id,
      listenHost,
      port,
      baseUrl,
    });

    const ctxPath = spawnStatePath(service, "context");
    const stdoutPath = spawnStatePath(service, "stdout") ??
      (typeof opts.defaultStdoutLogPath === "string"
        ? opts.defaultStdoutLogPath
        : undefined);
    const stderrPath = spawnStatePath(service, "stderr") ??
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

    const probeUrl = buildProbeUrl({
      baseUrl,
      proxyEndpointPrefix,
      service,
      probe: opts.probe,
    });

    try {
      let child: SpawnedProcess;
      const contextPathAbs = ctxPath ? resolve(ctxPath) : undefined;

      if (service.kind === "sqlpage") {
        child = await service.spawn({
          host,
          init: {
            listenHost,
            port,
            proxyEndpointPrefix,
            sqlpageBin,
            sqlpageEnv,
            stdoutLogPath: stdoutPath,
            stderrLogPath: stderrPath,
            processTags: contextPathAbs
              ? {
                sessionId: session.sessionId,
                serviceId: id,
                contextPath: contextPathAbs,
              }
              : undefined,
          },
          exposableServiceConf,
        });
      } else {
        child = await service.spawn({
          host,
          init: {
            listenHost,
            port,
            proxyEndpointPrefix,
            surveilrBin,
            stdoutLogPath: stdoutPath,
            stderrLogPath: stderrPath,
            processTags: contextPathAbs
              ? {
                sessionId: session.sessionId,
                serviceId: id,
                contextPath: contextPathAbs,
              }
              : undefined,
          },
          exposableServiceConf,
        });
      }

      await emit({ type: "spawned", serviceId: id, pid: child.pid });
      const upstreamUrl = joinUrl(
        baseUrl,
        proxyEndpointPrefix === "" ? "/" : proxyEndpointPrefix,
      );

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
          port,
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

      port++;
    } catch (error) {
      errored.push(id);
      errors.push({ id, error });
      await emit({ type: "error", serviceId: id, phase: "spawn", error });
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

  // Always sourced from env tags (source of truth for “owned by db-yard”)
  sessionId: string;
  serviceId: string;
  contextPath: string;

  // Full env (best-effort; includes the three tags)
  env: Record<string, string>;

  // Best-effort enrichments
  context?: SpawnedContext;
  cmdline?: string;

  // If we found a tagged process but could not fully enrich it.
  issue?: Error | unknown;

  // If context could be read, indicate whether it matches env tags.
  tagMismatch?: Readonly<{
    sessionId?: { env: string; ctx?: string };
    serviceId?: { env: string; ctx?: string };
    contextPath?: { env: string; ctx?: string };
  }>;
}>;

/**
 * Linux-only: yield all processes "owned" by db-yard using env tags:
 * - DB_YARD_CONTEXT_PATH
 * - DB_YARD_SESSION_ID
 * - DB_YARD_SERVICE_ID
 *
 * Notes:
 * - Requires permission to read /proc/<pid>/environ for target processes.
 * - Skips processes we cannot inspect (/proc perms) or that don't include all tags.
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

    const envContextPath = env["DB_YARD_CONTEXT_PATH"];
    if (typeof envContextPath !== "string" || envContextPath.length === 0) {
      continue;
    }

    const contextPath = envContextPath;

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

    // Prefer context.json values when available, fall back to env.
    let sessionId = "";
    if (context?.session?.sessionId) sessionId = context.session.sessionId;
    else if (typeof env["DB_YARD_SESSION_ID"] === "string") {
      sessionId = env["DB_YARD_SESSION_ID"];
    }

    let serviceId = "";
    if (context?.service?.id) serviceId = context.service.id;
    else if (typeof env["DB_YARD_SERVICE_ID"] === "string") {
      serviceId = env["DB_YARD_SERVICE_ID"];
    }

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
      env,
      contextPath,
      context,
      cmdline,
      issue,
    } satisfies TaggedProcess;
  }
}

export async function killSpawnedProcesses() {
  for await (const { pid } of taggedProcesses()) {
    await killPID(pid);
  }
}

/* -------------------------------- helpers -------------------------------- */

async function ensureParentDir(filePath: string): Promise<void> {
  const dir = dirname(filePath);
  if (dir && dir !== "." && dir !== "/") await ensureDir(dir);
}

function safeRelFromRoot(root: string | undefined, filePath: string): string {
  try {
    if (!root || root.trim().length === 0) return filePath;
    const rel = relative(root, filePath);
    if (rel.startsWith("..") || rel === "") return filePath;
    return rel;
  } catch {
    return filePath;
  }
}

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

function normalizePathForUrl(path: string): string {
  const p = path.replaceAll("\\", "/").trim();
  if (!p) return "/";
  if (!p.startsWith("/")) return `/${p}`;
  return p;
}

function joinUrl(baseUrl: string, path: string): string {
  const b = baseUrl.replace(/\/+$/, "");
  const p = normalizePathForUrl(path);
  return `${b}${p}`;
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
