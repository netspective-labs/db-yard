// lib/exposable.ts
import { resolve } from "@std/path";
import type {
  SqlPageDataSupplier,
  SurveilrDataSupplier,
  TabularDataSupplier,
} from "./tabular.ts";
import { ensureParentDir } from "./path.ts";

/**
 * Identifies the process that is acting as the "host" for spawned services.
 * Useful when multiple orchestrators may run, or for ownership/telemetry.
 */
export type SpawnHost = Readonly<{
  identity: string;
  pid: number;
}>;

/**
 * Where to send a spawned child's output (append).
 *
 * Intentionally only a file path string for robust background spawning.
 */
export type SpawnLogTarget = string;

/**
 * A generic process plan.
 *
 * Service-specific knowledge (sqlpage/surveilr) must not live here.
 */
export type SpawnPlan = Readonly<{
  command: string;
  args: readonly string[];
  env?: Readonly<Record<string, string>>;
  cwd?: string;
  tag?: string;

  /**
   * If provided, stdout is appended to this file path.
   * If omitted, stdout is redirected to /dev/null.
   */
  stdoutLogPath?: SpawnLogTarget;

  /**
   * If provided, stderr is appended to this file path.
   * If omitted, stderr is redirected to /dev/null.
   */
  stderrLogPath?: SpawnLogTarget;
}>;

/**
 * A handle to a spawned process.
 *
 * Note: process is the short-lived launcher process (sh), not the service itself.
 * The service pid is exposed as `pid` and is the one you should manage.
 */
export type SpawnedProcess = Readonly<{
  host: SpawnHost;
  plan: SpawnPlan;
  pid: number;
  process: Deno.ChildProcess;
  kill: (signal?: Deno.Signal) => Promise<void>;
}>;

/* -------------------------------- helpers -------------------------------- */

function shQuote(s: string): string {
  // POSIX shell single-quote escaping: ' -> '\''.
  return `'${s.replaceAll("'", `'\\''`)}'`;
}

function buildShellCommandLine(
  command: string,
  args: readonly string[],
): string {
  // Quote each token safely for sh -c.
  return [command, ...args].map(shQuote).join(" ");
}

function decodeText(u8: Uint8Array): string {
  return new TextDecoder().decode(u8);
}

function parsePidFromStdout(stdout: Uint8Array): number {
  const t = decodeText(stdout).trim();
  // stdout might contain extra whitespace/newlines; take first token that is an int
  const m = t.match(/(\d+)/);
  if (!m) throw new Error(`spawnPlan: could not parse pid from stdout: ${t}`);
  const pid = Number(m[1]);
  if (!Number.isFinite(pid) || pid <= 0) {
    throw new Error(`spawnPlan: invalid pid parsed from stdout: ${t}`);
  }
  return pid;
}

/**
 * Spawn a plan in "true background" mode:
 * - Uses `nohup` + backgrounding (`&`) to detach from the parent/session.
 * - Redirects stdout/stderr to files (append) or /dev/null.
 * - Prints the spawned service PID via `echo $!` and returns it.
 *
 * This is POSIX-oriented (Linux/macOS/WSL). On Windows-native without a POSIX shell,
 * you would need a different launcher.
 */
export async function spawnPlan(
  host: SpawnHost,
  plan: SpawnPlan,
): Promise<SpawnedProcess> {
  const stdoutPath = plan.stdoutLogPath ?? "/dev/null";
  const stderrPath = plan.stderrLogPath ?? "/dev/null";

  if (plan.stdoutLogPath) await ensureParentDir(plan.stdoutLogPath);
  if (plan.stderrLogPath) await ensureParentDir(plan.stderrLogPath);

  const cmdline = buildShellCommandLine(plan.command, plan.args);

  // Detach and emit the real service pid.
  // - </dev/null prevents the service from holding stdin open.
  // - nohup avoids SIGHUP teardown when parent/launcher ends.
  // - echo $! returns the background job PID (the service).
  const shell = `nohup ${cmdline} </dev/null 1>>${shQuote(stdoutPath)} 2>>${shQuote(stderrPath)
    } & echo $!`;

  const launcher = new Deno.Command("sh", {
    args: ["-c", shell],
    env: plan.env ? { ...plan.env } : undefined,
    cwd: plan.cwd,
    stdin: "null",
    stdout: "piped",
    stderr: "piped",
  });

  const child = launcher.spawn();
  const { code, stdout, stderr } = await child.output();

  if (code !== 0) {
    const errText = decodeText(stderr).trim();
    const msg = errText.length > 0
      ? `spawnPlan: launcher failed code=${code}: ${errText}`
      : `spawnPlan: launcher failed code=${code}`;
    throw new Error(msg);
  }

  const pid = parsePidFromStdout(stdout);

  return {
    host,
    plan,
    pid,
    process: child,
    // deno-lint-ignore require-await
    kill: async (signal: Deno.Signal = "SIGTERM") => {
      try {
        Deno.kill(pid, signal);
      } catch {
        // ignore (already dead or permissions)
      }
    },
  };
}

/* --------------------------------- init ---------------------------------- */

/**
 * Base init shared across all exposable services.
 *
 * Contains only platform-level inputs (no service-specific binaries/env).
 */
export type ExposableInit = Readonly<{
  listenHost: string;
  port: number;

  /**
   * Prefix to mount the app behind a reverse proxy (if you use one).
   * Example: "/apps/sqlpage/northwind"
   */
  proxyEndpointPrefix: string;

  /**
   * Optional output targets for the spawned child.
   * If omitted, output is redirected to /dev/null.
   */
  stdoutLogPath?: SpawnLogTarget;
  stderrLogPath?: SpawnLogTarget;

  /**
   * Linux-native-ish process ownership tags via env vars.
   * These link the spawned process back to the ledger context JSON,
   * plus additional context to make /proc inspection self-describing.
   */
  processTags?: Readonly<{
    provenance: string;
    sessionId: string;
    serviceId: string;
    contextPath: string;

    kind: ExposableKind;
    label: string;
    proxyEndpointPrefix: string;
    upstreamUrl: string;

    listenHost?: string;
    port?: number;
    baseUrl?: string;
    probeUrl?: string;
  }>;
}>;

/**
 * Service-specific init for SqlPage.
 */
export type SqlPageInit =
  & ExposableInit
  & Readonly<{
    sqlpageBin: string;

    /**
     * SqlPage environment name ("development", "production", etc.).
     */
    sqlpageEnv: string;
  }>;

/**
 * Service-specific init for Surveilr.
 */
export type SurveilrInit =
  & ExposableInit
  & Readonly<{
    surveilrBin: string;
  }>;

/**
 * Optional per-service configuration that can be attached to a supplier.
 *
 * Supported keys (today):
 * - ".env": string env block
 * - "sqlpage.bin": string
 * - "sqlpage.args": string[]
 * - "sqlpage.env": object
 * - "surveilr.bin": string
 * - "surveilr.args": string[]
 */
export type ExposableServiceConf = Readonly<Record<string, unknown>>;

export type ExposableKind = "sqlpage" | "surveilr";

export type ExposableService =
  | SqlPageExposableService
  | SurveilrExposableService;

export type ExposableBase = Readonly<{
  nature: "service";
  kind: ExposableKind;

  supplier: SqlPageDataSupplier | SurveilrDataSupplier;

  id: string;
  label: string;
}>;

export type SqlPageExposableService =
  & ExposableBase
  & Readonly<{
    kind: "sqlpage";
    supplier: SqlPageDataSupplier;

    spawn: (
      args: Readonly<{
        host: SpawnHost;
        init: SqlPageInit;
        exposableServiceConf?: ExposableServiceConf;
      }>,
    ) => Promise<SpawnedProcess>;
  }>;

export type SurveilrExposableService =
  & ExposableBase
  & Readonly<{
    kind: "surveilr";
    supplier: SurveilrDataSupplier;

    spawn: (
      args: Readonly<{
        host: SpawnHost;
        init: SurveilrInit;
        exposableServiceConf?: ExposableServiceConf;
      }>,
    ) => Promise<SpawnedProcess>;
  }>;

/**
 * Convert TabularDataSupplier items into ExposableService items.
 *
 * This is a pure transformation: it does not spawn anything.
 */
export async function* exposable(
  suppliers: Iterable<TabularDataSupplier> | AsyncIterable<TabularDataSupplier>,
): AsyncGenerator<ExposableService> {
  for await (const s of suppliers as AsyncIterable<TabularDataSupplier>) {
    if (s.kind === "sqlpage") yield makeSqlPageService(s);
    else if (s.kind === "surveilr") yield makeSurveilrService(s);
  }
}

/* --------------------------- exposable builders --------------------------- */

function makeSqlPageService(s: SqlPageDataSupplier): SqlPageExposableService {
  return {
    nature: "service",
    kind: "sqlpage",
    supplier: s,
    id: s.dbPath,
    label: `sqlpage:${s.dbPath}`,
    spawn: async ({ host, init, exposableServiceConf }) => {
      const plan = buildSqlPageSpawnPlan({
        dbPath: s.dbPath,
        init,
        conf: exposableServiceConf ?? {},
      });
      return await spawnPlan(host, plan);
    },
  };
}

function makeSurveilrService(
  s: SurveilrDataSupplier,
): SurveilrExposableService {
  return {
    nature: "service",
    kind: "surveilr",
    supplier: s,
    id: s.dbPath,
    label: `surveilr:${s.dbPath}`,
    spawn: async ({ host, init, exposableServiceConf }) => {
      const plan = buildSurveilrSpawnPlan({
        dbPath: s.dbPath,
        init,
        conf: exposableServiceConf ?? {},
      });
      return await spawnPlan(host, plan);
    },
  };
}

/* ----------------------- service-specific spawn plans ---------------------- */

function buildSqlpageDatabaseUrl(dbAbsPath: string): string {
  return `sqlite://${dbAbsPath}`;
}

function processTagsEnv(init: ExposableInit): Record<string, string> {
  const t = init.processTags;
  if (!t) return {};
  return {
    TRUTH_YARD_PROVENANCE: resolve(t.provenance),
    TRUTH_YARD_CONTEXT_PATH: t.contextPath,
    TRUTH_YARD_SESSION_ID: t.sessionId,
    TRUTH_YARD_SERVICE_ID: t.serviceId,

    TRUTH_YARD_KIND: t.kind,
    TRUTH_YARD_LABEL: t.label,
    TRUTH_YARD_PROXY_ENDPOINT_PREFIX: t.proxyEndpointPrefix,
    TRUTH_YARD_UPSTREAM_URL: t.upstreamUrl,

    ...(typeof t.listenHost === "string"
      ? { TRUTH_YARD_LISTEN_HOST: t.listenHost }
      : {}),
    ...(typeof t.port === "number" ? { TRUTH_YARD_PORT: String(t.port) } : {}),
    ...(typeof t.baseUrl === "string"
      ? { TRUTH_YARD_BASE_URL: t.baseUrl }
      : {}),
    ...(typeof t.probeUrl === "string"
      ? { TRUTH_YARD_PROBE_URL: t.probeUrl }
      : {}),
  };
}

function buildSqlPageSpawnPlan(args: {
  dbPath: string;
  init: SqlPageInit;
  conf: ExposableServiceConf;
}): SpawnPlan {
  const { dbPath, init, conf } = args;

  const command = typeof conf["sqlpage.bin"] === "string"
    ? String(conf["sqlpage.bin"])
    : init.sqlpageBin;

  const baseEnv: Record<string, string> = {
    DATABASE_URL: buildSqlpageDatabaseUrl(dbPath),
    LISTEN_ON: `${init.listenHost}:${init.port}`,
    SQLPAGE_ENVIRONMENT: init.sqlpageEnv,
    SQLPAGE_SITE_PREFIX: init.proxyEndpointPrefix,
    ...processTagsEnv(init),
  };

  const extraEnv = readEnvObject(conf["sqlpage.env"]);
  const envBlock = envFromConf(conf);
  const extraArgs = readStringArray(conf["sqlpage.args"]);

  return {
    command,
    args: extraArgs,
    env: {
      ...baseEnv,
      ...extraEnv,
      ...envBlock,
    },
    tag: `sqlpage:${dbPath}`,
    stdoutLogPath: init.stdoutLogPath,
    stderrLogPath: init.stderrLogPath,
  };
}

function buildSurveilrSpawnPlan(args: {
  dbPath: string;
  init: SurveilrInit;
  conf: ExposableServiceConf;
}): SpawnPlan {
  const { dbPath, init, conf } = args;

  const command = typeof conf["surveilr.bin"] === "string"
    ? String(conf["surveilr.bin"])
    : init.surveilrBin;

  const extraArgs = readStringArray(conf["surveilr.args"]);
  const envBlock = envFromConf(conf);

  return {
    command,
    args: [
      "web-ui",
      "-d",
      dbPath,
      "--port",
      String(init.port),
      "--host",
      init.listenHost,
      ...extraArgs,
    ],
    env: {
      SQLPAGE_SITE_PREFIX: init.proxyEndpointPrefix,
      ...processTagsEnv(init),
      ...envBlock,
    },
    tag: `surveilr:${dbPath}`,
    stdoutLogPath: init.stdoutLogPath,
    stderrLogPath: init.stderrLogPath,
  };
}

/* ------------------------------ conf helpers ------------------------------ */

function readStringArray(v: unknown): string[] {
  if (!Array.isArray(v)) return [];
  return v.map((x) => String(x));
}

function readEnvObject(v: unknown): Record<string, string> {
  const out: Record<string, string> = {};
  if (!v || typeof v !== "object" || Array.isArray(v)) return out;
  for (const [k, vv] of Object.entries(v as Record<string, unknown>)) {
    out[k] = String(vv);
  }
  return out;
}

function envFromConf(conf: ExposableServiceConf): Record<string, string> {
  const v = conf[".env"];
  if (typeof v !== "string") return {};
  const block = v.trim();
  if (!block) return {};
  return parseEnvBlock(block);
}

function parseEnvBlock(block: string): Record<string, string> {
  const out: Record<string, string> = {};
  for (const line0 of block.split(/\r?\n/)) {
    const line = line0.trim();
    if (!line) continue;
    if (line.startsWith("#")) continue;

    const s = line.startsWith("export ")
      ? line.slice("export ".length).trim()
      : line;

    const eq = s.indexOf("=");
    if (eq <= 0) continue;

    const key = s.slice(0, eq).trim();
    if (!key) continue;
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) continue;

    const rawVal = s.slice(eq + 1);
    const val = unquoteAndUnescape(rawVal);

    out[key] = val;
  }
  return out;
}

function unquoteAndUnescape(v: string): string {
  const t = v.trim();
  if (t.length >= 2) {
    const q = t[0];
    const last = t[t.length - 1];
    if ((q === "'" || q === '"') && last === q) {
      const inner = t.slice(1, -1);
      if (q === '"') {
        return inner
          .replaceAll("\\n", "\n")
          .replaceAll("\\r", "\r")
          .replaceAll("\\t", "\t")
          .replaceAll('\\"', '"')
          .replaceAll("\\\\", "\\");
      }
      return inner.replaceAll("\\'", "'").replaceAll("\\\\", "\\");
    }
  }
  return t;
}
