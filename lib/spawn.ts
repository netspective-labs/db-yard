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

export type ProcessEnumerationStrategy = "/proc" | "ps";

export type TaggedProcessesOptions = Readonly<{
  strategy?: ProcessEnumerationStrategy; // caller override
  psBin?: string; // default "ps"
}>;

type ProcLikeProvider = Readonly<{
  strategy: ProcessEnumerationStrategy;
  taggedProcesses(): AsyncGenerator<TaggedProcess>;
  readCmdline(pid: number): Promise<string | undefined>;
  readEnviron(pid: number): Promise<Record<string, string>>;
}>;

/* --------------------------- strategy selection -------------------------- */

function isProbablyInContainer(): boolean {
  // Heuristics. We try to avoid throwing and avoid hard dependencies.
  // It’s fine if this returns a false negative; caller can override.
  try {
    // Common envs set by runtimes.
    const v = Deno.env.get("container");
    if (v && v !== "0") return true;
  } catch {
    // ignore (no env permission)
  }

  try {
    // Docker commonly creates this file.
    Deno.statSync("/.dockerenv");
    return true;
  } catch {
    // ignore
  }

  // cgroup markers; works on many Linux hosts/containers.
  // If /proc is not accessible, we just give up quietly.
  try {
    const cgroup = Deno.readTextFileSync("/proc/1/cgroup");
    const s = cgroup.toLowerCase();
    if (
      s.includes("docker") ||
      s.includes("containerd") ||
      s.includes("kubepods") ||
      s.includes("podman") ||
      s.includes("lxc")
    ) return true;
  } catch {
    // ignore
  }

  return false;
}

function selectStrategy(
  opts?: TaggedProcessesOptions,
): ProcessEnumerationStrategy {
  if (opts?.strategy) return opts.strategy;
  if (Deno.build.os !== "linux") return "ps"; // only one that can degrade safely elsewhere
  return isProbablyInContainer() ? "ps" : "/proc";
}

/* ------------------------------ ps provider ------------------------------ */

type PsRun = Readonly<{
  ok: boolean;
  stdout: string;
  stderr: string;
  code: number;
}>;

async function runPs(psBin: string, args: string[]): Promise<PsRun> {
  try {
    const cmd = new Deno.Command(psBin, {
      args,
      stdin: "null",
      stdout: "piped",
      stderr: "piped",
    });
    const out = await cmd.output();
    return {
      ok: out.success,
      stdout: new TextDecoder().decode(out.stdout),
      stderr: new TextDecoder().decode(out.stderr),
      code: out.code,
    };
  } catch (e) {
    return {
      ok: false,
      stdout: "",
      stderr: e instanceof Error ? e.message : String(e),
      code: 127,
    };
  }
}

async function ensurePsUsable(psBin: string): Promise<void> {
  // procps supports --version; busybox often does not.
  const r1 = await runPs(psBin, ["--version"]);
  if (r1.ok) return;

  // Fallback: see if "ps -eo pid=" works
  const r2 = await runPs(psBin, ["-eo", "pid="]);
  if (r2.ok) return;

  throw new Error(
    `ps is not available or not usable (bin=${psBin}). stderr=${
      r1.stderr || r2.stderr
    }`,
  );
}

function parsePidPrefixedLine(
  line: string,
): { pid: number; rest: string } | undefined {
  const m = line.trim().match(/^(\d+)\s+(.*)$/);
  if (!m) return undefined;
  const pid = Number(m[1]);
  if (!Number.isFinite(pid) || pid <= 0) return undefined;
  return { pid, rest: m[2] ?? "" };
}

function extractTruthYardEnvFromPsArgs(args: string): Record<string, string> {
  // Best-effort. Assumes values do not contain whitespace.
  const out: Record<string, string> = {};
  const re = /(?:^|\s)(TRUTH_YARD_[A-Z0-9_]+)=([^\s]*)/g;
  for (const match of args.matchAll(re)) {
    const k = match[1];
    const v = match[2] ?? "";
    if (k) out[k] = v;
  }
  return out;
}

function bestEffortCmdlineFromPsArgs(args: string): string | undefined {
  const i = args.indexOf("TRUTH_YARD_");
  const cmd = (i >= 0 ? args.slice(0, i) : args).trim();
  return cmd.length ? cmd : undefined;
}

function psProvider(psBin = "ps"): ProcLikeProvider {
  return {
    strategy: "ps",

    async readCmdline(pid: number): Promise<string | undefined> {
      if (Deno.build.os !== "linux") return undefined;
      await ensurePsUsable(psBin);

      const r = await runPs(psBin, ["ww", "-p", String(pid), "-o", "args="]);
      if (!r.ok) return undefined;

      const cmd = r.stdout.trim();
      return cmd.length ? cmd : undefined;
    },

    async readEnviron(pid: number): Promise<Record<string, string>> {
      if (Deno.build.os !== "linux") return {};
      await ensurePsUsable(psBin);

      const r = await runPs(psBin, [
        "e",
        "ww",
        "-p",
        String(pid),
        "-o",
        "args=",
      ]);
      if (!r.ok) return {};
      return extractTruthYardEnvFromPsArgs(r.stdout);
    },

    async *taggedProcesses(): AsyncGenerator<TaggedProcess> {
      if (Deno.build.os !== "linux") {
        throw new Error("taggedProcesses() is Linux-only.");
      }

      await ensurePsUsable(psBin);

      const r = await runPs(psBin, ["e", "ww", "-eo", "pid=,args="]);
      if (!r.ok) {
        const msg = (r.stderr || r.stdout || "").trim();
        throw new Error(
          `taggedProcesses(): ps failed: ${msg || `exit ${r.code}`}`,
        );
      }

      const lines = r.stdout.split("\n").map((s) => s.trim()).filter(Boolean);

      for (const line of lines) {
        const parsed = parsePidPrefixedLine(line);
        if (!parsed) continue;

        const { pid, rest: args } = parsed;

        const env = extractTruthYardEnvFromPsArgs(args);

        const provenance = env["TRUTH_YARD_PROVENANCE"];
        const contextPath = env["TRUTH_YARD_CONTEXT_PATH"];
        const sessionId = env["TRUTH_YARD_SESSION_ID"];
        const serviceId = env["TRUTH_YARD_SERVICE_ID"];

        if (!provenance || !contextPath || !sessionId || !serviceId) continue;

        const cmdline = bestEffortCmdlineFromPsArgs(args);

        let issue: Error | unknown;
        let context: SpawnedContext | undefined;

        try {
          const ctxContent = await Deno.readTextFile(contextPath);
          context = JSON.parse(ctxContent) as SpawnedContext;
        } catch (e) {
          issue = e;
          context = undefined;
        }

        const kind = env["TRUTH_YARD_KIND"];
        const label = env["TRUTH_YARD_LABEL"];
        const proxyEndpointPrefix = env["TRUTH_YARD_PROXY_ENDPOINT_PREFIX"];
        const upstreamUrl = env["TRUTH_YARD_UPSTREAM_URL"];

        const ctxPidRaw =
          (context as unknown as { spawned?: { pid?: unknown } })?.spawned?.pid;
        const ctxPid = typeof ctxPidRaw === "number"
          ? ctxPidRaw
          : Number(ctxPidRaw);

        if (
          context && Number.isFinite(ctxPid) && ctxPid > 0 && ctxPid !== pid
        ) {
          const pidIssue = new Error(
            `PID mismatch: ps pid=${pid} but context.spawned.pid=${ctxPid} (contextPath=${contextPath})`,
          );
          issue = issue
            ? new AggregateError(
              [issue, pidIssue],
              "taggedProcesses(): issues detected",
            )
            : pidIssue;
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
    },
  };
}

/* ----------------------------- /proc provider ---------------------------- */

function parseProcEnviron(bytes: Uint8Array): Record<string, string> {
  const text = new TextDecoder().decode(bytes);
  const out: Record<string, string> = {};
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

async function readProcCmdlineDirect(pid: number): Promise<string | undefined> {
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

async function readProcEnvironDirect(
  pid: number,
): Promise<Record<string, string>> {
  const path = `/proc/${pid}/environ`;
  try {
    const bytes = await Deno.readFile(path);
    return parseProcEnviron(bytes);
  } catch {
    return {};
  }
}

function procProvider(): ProcLikeProvider {
  return {
    strategy: "/proc",

    async readCmdline(pid: number): Promise<string | undefined> {
      if (Deno.build.os !== "linux") return undefined;
      return await readProcCmdlineDirect(pid);
    },

    async readEnviron(pid: number): Promise<Record<string, string>> {
      if (Deno.build.os !== "linux") return {};
      return await readProcEnvironDirect(pid);
    },

    async *taggedProcesses(): AsyncGenerator<TaggedProcess> {
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

        const env = await readProcEnvironDirect(pid);

        const provenance = env["TRUTH_YARD_PROVENANCE"];
        const contextPath = env["TRUTH_YARD_CONTEXT_PATH"];
        const sessionId = env["TRUTH_YARD_SESSION_ID"];
        const serviceId = env["TRUTH_YARD_SERVICE_ID"];

        if (!provenance || !contextPath || !sessionId || !serviceId) continue;

        const cmdline = await readProcCmdlineDirect(pid);

        let issue: Error | unknown;
        let context: SpawnedContext | undefined;

        try {
          const ctxContent = await Deno.readTextFile(contextPath);
          context = JSON.parse(ctxContent) as SpawnedContext;
        } catch (e2) {
          issue = e2;
          context = undefined;
        }

        const kind = env["TRUTH_YARD_KIND"];
        const label = env["TRUTH_YARD_LABEL"];
        const proxyEndpointPrefix = env["TRUTH_YARD_PROXY_ENDPOINT_PREFIX"];
        const upstreamUrl = env["TRUTH_YARD_UPSTREAM_URL"];

        const ctxPidRaw =
          (context as unknown as { spawned?: { pid?: unknown } })?.spawned?.pid;
        const ctxPid = typeof ctxPidRaw === "number"
          ? ctxPidRaw
          : Number(ctxPidRaw);

        if (
          context && Number.isFinite(ctxPid) && ctxPid > 0 && ctxPid !== pid
        ) {
          const pidIssue = new Error(
            `PID mismatch: /proc pid=${pid} but context.spawned.pid=${ctxPid} (contextPath=${contextPath})`,
          );
          issue = issue
            ? new AggregateError(
              [issue, pidIssue],
              "taggedProcesses(): issues detected",
            )
            : pidIssue;
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
    },
  };
}

/* ---------------------------- exported API shim --------------------------- */

// Keep your existing exports, but make them dispatch through the selected strategy.
// taggedProcesses() now accepts options.

export async function readProcCmdline(
  pid: number,
): Promise<string | undefined> {
  // Default to the selected strategy for cmdline reads too.
  const strat = selectStrategy();
  if (strat === "ps") return await psProvider().readCmdline(pid);
  return await procProvider().readCmdline(pid);
}

export async function readProcEnviron(
  pid: number,
): Promise<Record<string, string>> {
  const strat = selectStrategy();
  if (strat === "ps") return await psProvider().readEnviron(pid);
  return await procProvider().readEnviron(pid);
}

/**
 * Linux-only: yield all processes "owned" by Truth Yard using env tags:
 * - TRUTH_YARD_PROVENANCE
 * - TRUTH_YARD_CONTEXT_PATH
 * - TRUTH_YARD_SESSION_ID
 * - TRUTH_YARD_SERVICE_ID
 * - TRUTH_YARD_KIND
 * - TRUTH_YARD_LABEL
 * - TRUTH_YARD_PROXY_ENDPOINT_PREFIX
 * - TRUTH_YARD_UPSTREAM_URL
 *
 * Notes:
 * - Requires permission to read /proc/<pid>/environ for target processes.
 * - Skips processes we cannot inspect (/proc perms) or that don't include CONTEXT_PATH.
 * - Reads cmdline and context.json best-effort; yields even if those enrichments fail.
 */
export async function* taggedProcesses(
  opts?: TaggedProcessesOptions,
): AsyncGenerator<TaggedProcess> {
  const strat = selectStrategy(opts);
  const provider = strat === "ps"
    ? psProvider(opts?.psBin ?? "ps")
    : procProvider();
  yield* provider.taggedProcesses();
}

/* --------------------------- pid/process helpers -------------------------- */

export function isPidAlive(pid: number): boolean {
  try {
    Deno.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
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
