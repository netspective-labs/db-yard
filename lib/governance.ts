// lib/governance.ts
import { cyan, gray, green, magenta, red, yellow } from "@std/fmt/colors";
import { dirname as stdDirname } from "@std/path";

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

export type SpawnedRecord = {
  version: 1;
  kind: SpawnKind;

  // Instance identity (may be overridden via .db-yard)
  id: string;

  watchRoots: string[];

  dbPath: string;
  dbRelPath?: string;
  dbBasename: string;

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
  record: SpawnedRecord;
};

const text = new TextDecoder();

export function nowMs() {
  return Date.now();
}

export function normalizePath(p: string) {
  return p.replaceAll("\\", "/");
}

export async function ensureDir(dir: string) {
  await Deno.mkdir(dir, { recursive: true }).catch(() => {});
}

export async function fileStatSafe(
  path: string,
): Promise<Deno.FileInfo | undefined> {
  try {
    return await Deno.stat(path);
  } catch {
    return undefined;
  }
}

export function fnv1a32Hex(s: string): string {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return (h >>> 0).toString(16).padStart(8, "0");
}

export function safeBaseName(path: string): string {
  const p = normalizePath(path);
  const i = p.lastIndexOf("/");
  return i >= 0 ? p.slice(i + 1) : p;
}

export function isPidAlive(pid: number): boolean {
  try {
    Deno.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

export function pickFreePort(listenHost: string): number {
  const listener = Deno.listen({ hostname: listenHost, port: 0 });
  try {
    return (listener.addr as Deno.NetAddr).port;
  } finally {
    listener.close();
  }
}

export function parseListenHost(s: string): string {
  const trimmed = s.trim();
  if (!trimmed) return "127.0.0.1";
  if (/[^\w.\-:[\]]/.test(trimmed)) return "127.0.0.1";
  return trimmed;
}

export function toSqlpageEnv(s: string): SqlpageEnv {
  return s === "development" ? "development" : "production";
}

export function toPositiveInt(n: unknown, fallback: number): number {
  const v = typeof n === "number" ? n : Number(n);
  if (!Number.isFinite(v) || v <= 0) return fallback;
  return Math.floor(v);
}

export function buildSqlpageDatabaseUrl(dbAbsPath: string): string {
  const p = normalizePath(dbAbsPath);
  return `sqlite://${p}`;
}

export function jsonResponse(obj: unknown, status = 200): Response {
  return new Response(JSON.stringify(obj, null, 2), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

export function computeRelPath(
  watchRootsAbs: readonly string[],
  dbAbsPath: string,
): string | undefined {
  const db = normalizePath(dbAbsPath);
  for (const r0 of watchRootsAbs) {
    const r = normalizePath(r0).replace(/\/+$/, "");
    if (db === r) return ".";
    if (db.startsWith(r + "/")) return db.slice(r.length + 1);
  }
  return undefined;
}

export function defaultRelativeInstanceId(relOrAbs: string): string {
  // Keep it stable and human-ish
  const s = normalizePath(relOrAbs).replace(/^\.\/+/, "");
  return s || "db";
}

export async function loadOrCreateOwnerToken(
  spawnedDir: string,
): Promise<string> {
  const p = `${normalizePath(spawnedDir)}/.db-yard.owner-token`;
  try {
    const existing = (await Deno.readTextFile(p)).trim();
    if (existing) return existing;
  } catch {
    // ignore
  }
  const tok = crypto.randomUUID();
  await Deno.writeTextFile(p, tok);
  return tok;
}

function looksLikeJsonText(s: string): boolean {
  const t = s.trim();
  if (t.length < 2) return false;
  if (t.startsWith("{") && t.endsWith("}")) return true;
  if (t.startsWith("[") && t.endsWith("]")) return true;
  return false;
}

function coerceValue(raw: string): unknown {
  const t = raw.trim();
  if (!t) return "";
  if (/^[+-]?\d+(\.\d+)?$/.test(t)) {
    const n = Number(t);
    if (Number.isFinite(n)) return n;
  }
  if (looksLikeJsonText(t)) {
    try {
      return JSON.parse(t);
    } catch {
      return raw;
    }
  }
  return raw;
}

export async function runSqliteQueryViaCli(opts: {
  exec: string;
  dbPath: string;
  sql: string;
}): Promise<SpawnedCtxSnapshot> {
  const ranAtMs = nowMs();
  const tryModes: { args: string[]; mode: "json" | "line" }[] = [
    { args: ["-json", opts.dbPath, opts.sql], mode: "json" },
    { args: ["-line", opts.dbPath, opts.sql], mode: "line" },
  ];

  for (const m of tryModes) {
    try {
      const cmd = new Deno.Command(opts.exec, {
        args: m.args,
        stdin: "null",
        stdout: "piped",
        stderr: "piped",
      });
      const out = await cmd.output();
      const stdout = text.decode(out.stdout).trim();
      const stderr = text.decode(out.stderr).trim();

      if (!out.success) {
        if (
          m.mode === "json" &&
          /unknown option|unrecognized option|-json/i.test(stderr)
        ) {
          continue;
        }
        return {
          exec: opts.exec,
          sql: opts.sql,
          ranAtMs,
          ok: false,
          exitCode: out.code,
          stderr: stderr || undefined,
          output: stdout || undefined,
        };
      }

      if (m.mode === "json") {
        try {
          const parsed = stdout.length ? JSON.parse(stdout) : [];
          return {
            exec: opts.exec,
            sql: opts.sql,
            ranAtMs,
            ok: true,
            exitCode: out.code,
            stderr: stderr || undefined,
            output: parsed,
          };
        } catch {
          return {
            exec: opts.exec,
            sql: opts.sql,
            ranAtMs,
            ok: true,
            exitCode: out.code,
            stderr: stderr || undefined,
            output: stdout,
            note: "sqlite3 -json output could not be parsed; stored as text",
          };
        }
      }

      return {
        exec: opts.exec,
        sql: opts.sql,
        ranAtMs,
        ok: true,
        exitCode: out.code,
        stderr: stderr || undefined,
        output: stdout,
        note: "sqlite3 -line output stored as text",
      };
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      return {
        exec: opts.exec,
        sql: opts.sql,
        ranAtMs,
        ok: false,
        stderr: msg,
      };
    }
  }

  return {
    exec: opts.exec,
    sql: opts.sql,
    ranAtMs,
    ok: false,
    stderr: "No sqlite exec mode succeeded",
  };
}

export async function readDbYardConfig(args: {
  sqliteExec: string;
  dbPath: string;
}): Promise<Record<string, unknown>> {
  // If .db-yard table doesn't exist, return {} (do not crash)
  const sql = `select key as k, value as v from ".db-yard" order by key`;

  const snap = await runSqliteQueryViaCli({
    exec: args.sqliteExec,
    dbPath: args.dbPath,
    sql,
  });

  if (!snap.ok) return {};

  const cfg: Record<string, unknown> = {};

  if (Array.isArray(snap.output)) {
    for (const row of snap.output as unknown[]) {
      if (!row || typeof row !== "object") continue;
      const r = row as Record<string, unknown>;
      const k = typeof r.k === "string"
        ? r.k
        : (typeof r.key === "string" ? r.key : "");
      const v = typeof r.v === "string"
        ? r.v
        : (typeof r.value === "string" ? r.value : "");
      if (!k) continue;
      cfg[k] = coerceValue(v);
    }
    return cfg;
  }

  // If sqlite3 -line is used, we won't parse rows reliably; treat as empty.
  return {};
}

export { stdDirname as dirname };

export type VerboseKind =
  | "detect"
  | "spawn"
  | "stop"
  | "refresh"
  | "skip"
  | "reconcile";

export function vTag(kind: VerboseKind): string {
  switch (kind) {
    case "detect":
      return `[${cyan(kind)}]`;
    case "spawn":
      return `[${green(kind)}]`;
    case "stop":
      return `[${red(kind)}]`;
    case "refresh":
      return `[${yellow(kind)}]`;
    case "skip":
      return `[${gray(kind)}]`;
    case "reconcile":
      return `[${magenta(kind)}]`;
    default:
      return `[${kind}]`;
  }
}

export function vlog(
  enabled: boolean,
  kind: VerboseKind,
  msg: string,
  extra?: Record<string, unknown>,
) {
  if (!enabled) return;

  const head = `${vTag(kind)} ${msg}`;
  if (!extra) {
    console.log(head);
    return;
  }
  const details = Object.entries(extra)
    .map(([k, v]) => `${gray(k)}=${String(v)}`)
    .join(" ");
  console.log(details ? `${head} ${details}` : head);
}

export async function writeSpawnedPidsFile(
  spawnedDir: string,
  pids: number[],
): Promise<void> {
  const unique = [...new Set(pids.filter((p) => Number.isFinite(p) && p > 0))]
    .sort((a, b) => a - b);

  const path = `${normalizePath(spawnedDir)}/spawned-pids.txt`;
  const content = unique.join(" ");

  // Only rewrite if changed (avoids touching file on every reconcile tick)
  try {
    const prev = await Deno.readTextFile(path);
    if (prev.trim() === content.trim()) return;
  } catch {
    // ignore
  }

  const tmp = `${path}.tmp`;
  await Deno.writeTextFile(tmp, content);
  await Deno.rename(tmp, path);
}
