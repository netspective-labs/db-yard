// lib/governance.ts
// Types and shared structures only (no operational logic).

export type SpawnKind = "rssd" | "sqlpage";
export type SqlpageEnv = "production" | "development";

export type OwnerIdentity = {
  ownerToken: string;
  watcherPid: number;
  host: string;
  startedAtMs: number;
};

export type SpawnedCtxSnapshot = {
  exec: string;
  sql: string;
  ranAtMs: number;
  ok: boolean;
  exitCode?: number;
  output?: unknown; // parsed JSON (array/object) or string/number
  stderr?: string;
  note?: string;
};

export type SpawnedProcess = {
  version: 1;
  kind: SpawnKind;

  // Instance identity (may be overridden via .db-yard)
  id: string;

  watchRoots: string[];

  dbPath: string;
  dbRelPath?: string;
  dbBasename: string;

  // Proxy prefix computed from dbRelPath (or dbBasename fallback)
  // Example: dbRelPath "abc/def/my.sqlite.db" => "/abc/def/my/"
  proxyEndpointPrefix: string;

  listenHost: string;
  port: number;

  spawnedAtMs: number;
  lastSeenAtMs: number;

  fileSize: number;
  fileMtimeMs: number;

  pid: number;
  command: string;
  args: string[];
  env: Record<string, string>;
  cwd?: string;

  // Log files (stdout/stderr)
  stdoutLogPath?: string;
  stderrLogPath?: string;

  owner: OwnerIdentity;

  // Per-sql snapshots, keyed by sql text
  spawnedCtx?: Record<string, SpawnedCtxSnapshot | undefined>;

  // Parsed config from ".db-yard" (if present)
  dbYardConfig?: Record<string, unknown>;

  notes?: string[];
};

export type Running = {
  record: SpawnedProcess;
};

export type VerboseKind =
  | "detect"
  | "spawn"
  | "stop"
  | "refresh"
  | "skip"
  | "reconcile";

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
    proxyEndpointPrefix: string;
  }): SpawnPlan;
};

export type OrchestratorConfig = {
  // if non-empty, only env var NAMES matching any regex are inherited
  inheritEnvRegex?: string[];

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
