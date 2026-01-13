/**
 * @module lib/composite
 *
 * Deterministic composite SQL generator for truth-yard embedded composite connections.
 *
 * This module discovers embedded database files using globs and generates a deterministic
 * `composite.sql` file containing:
 * - ATTACH statements for each discovered DB (dialect-aware: SQLite or DuckDB)
 * - optional PRAGMAs (SQLite) or SET/PRAGMA-like statements (DuckDB) emitted deterministically
 * - optional extra SQL (typically views) emitted deterministically
 *
 * Execution model notes
 * - Most composites are executed against an ephemeral database (often `:memory:` for SQLite, or an
 *   in-memory DuckDB connection) when you only need a transient “single-connection” view.
 * - You can also execute the generated SQL against a persistent composite database file
 *   (e.g. `composite.sqlite.auto.db` or `composite.duckdb.auto.db`). In that case:
 *   - The ATTACH statements are not “saved” as permanent mounts. They run per-connection, so each
 *     time you open the composite DB you must execute the composite.sql again (unless your runtime
 *     always bootstraps the connection with the SQL).
 *   - Any CREATE VIEW / CREATE TABLE emitted by composite.sql *is* persisted in that composite DB file.
 *     This can be desirable for stable views or materialized rollups, but it also means schema changes
 *     require regeneration or migration.
 *
 * Determinism contract (core invariants)
 * 1) Discovery order must never influence aliasing or ATTACH output.
 *    - All discovered inputs are normalized, deduped, and canonically sorted BEFORE alias assignment.
 *    - Alias generation MUST be a pure function of a stable identifier (default: relative path from baseDir).
 * 2) Optional PRAGMAs / extra SQL are emitted in a fixed, documented order.
 *    - Pragmas are normalized, deduped, and sorted lexicographically by default.
 *    - Extra SQL is normalized and (optionally) sorted by default to avoid nondeterminism.
 *      For multi-line DDL that must preserve author order (e.g. views), set `extraSqlOrder: "asProvided"`.
 * 3) Default SQL header is deterministic (no timestamps).
 *
 * The golden-string tests in `composite_test.ts` are the best reference for exact output shapes.
 */

// deno-lint-ignore no-explicit-any
type Any = any;

import { expandGlob } from "jsr:@std/fs@^1.0.0/expand-glob";
import { basename, join, normalize, relative } from "jsr:@std/path@^1.0.0";

export type CompositeScope = "admin" | "cross-tenant" | "tenant";

/**
 * Dialect determines ATTACH syntax and conventions.
 *
 * - SQLite: `ATTACH DATABASE 'path' AS alias;`
 * - DuckDB: `ATTACH 'path' AS alias (TYPE sqlite);` for SQLite files by default.
 *
 * Notes:
 * - DuckDB requires `INSTALL sqlite; LOAD sqlite;` before attaching SQLite DBs.
 *   You can emit these via `pragmas()` (or rename in your app layer if preferred).
 */
export type CompositeDialect = "SQLite" | "DuckDB";

export interface CompositeLayout {
  readonly volumeRoot: string;
  readonly embeddedDir?: string;
  readonly adminDir?: string;
  readonly crossTenantDir?: string;
  readonly tenantDir?: string;
  readonly compositeSqlName?: string;
  readonly compositeDbAutoName?: string;
}

/**
 * A discovered DB file that will be ATTACHed.
 */
export interface DiscoveredDb<TMeta = unknown> {
  readonly path: string; // absolute path on disk
  readonly relKey: string; // stable identifier: canonical relative path from baseDir (or fallback)
  readonly alias: string; // schema name used in ATTACH
  readonly readOnly?: boolean;
  readonly meta?: TMeta;
}

/**
 * You can swap this out if you want Node (fast-glob, globby, etc.).
 */
export interface GlobWalker {
  walk(
    globs: readonly string[],
    opts?: { readonly cwd?: string; readonly ignore?: readonly string[] },
  ): AsyncIterable<string>;
}

/**
 * Default Deno glob walker (std/fs expandGlob).
 */
export const denoGlobWalker: GlobWalker = {
  async *walk(globs, opts) {
    const cwd = opts?.cwd ?? Deno.cwd();
    const ignore = new Set(opts?.ignore ?? []);
    for (const pattern of globs) {
      for await (
        const entry of expandGlob(pattern, { root: cwd, globstar: true })
      ) {
        if (!entry.isFile) continue;
        const p = normalize(entry.path);
        if (ignore.has(p)) continue;
        yield p;
      }
    }
  },
};

/**
 * Stable alias from a stable key (default is relKey).
 * You can override via config.aliasForKey.
 */
export function defaultAliasForKey(stableKey: string): string {
  // stableKey might be "db3.sqlite.db" or "qualityfolio/db.sqlite.db"
  const file = basename(stableKey);
  const withoutExt = file
    .replace(/\.sqlite(\.db)?$/i, "")
    .replace(/\.db$/i, "");
  return withoutExt.replace(/[^A-Za-z0-9_]/g, "_");
}

/**
 * Emits dialect-aware ATTACH statements.
 */
export interface SqlEmitter {
  header?(ctx: ComposeContext<Any>): string | string[];
  attach?(db: DiscoveredDb<Any>, ctx: ComposeContext<Any>): string | string[];
  footer?(ctx: ComposeContext<Any>): string | string[];
}

/**
 * Default deterministic emitter (no timestamps), dialect-aware.
 */
export const defaultEmitter: SqlEmitter = {
  header(ctx) {
    const lines: string[] = [];
    lines.push(
      "-- Auto-generated by compose(); DO NOT EDIT derived composite DB directly",
    );
    lines.push(
      `-- dialect: ${ctx.dialect} | scope: ${ctx.scope}${
        ctx.tenantId ? ` tenantId=${ctx.tenantId}` : ""
      }`,
    );
    lines.push("");

    for (const pragma of ctx.pragmas) lines.push(pragma);
    if (ctx.pragmas.length) lines.push("");

    return lines;
  },
  attach(db, ctx) {
    return emitAttach(db, ctx);
  },
  footer(ctx) {
    const lines: string[] = [];
    if (ctx.extraSql.length) {
      lines.push("");
      lines.push("-- extra SQL (views, etc.)");
      lines.push(...ctx.extraSql);
    }
    lines.push("");
    return lines;
  },
};

function sqlQuoteSingle(s: string): string {
  return s.replaceAll("'", "''");
}

export type DeterministicOrder = "sorted" | "asProvided";

export interface ComposeContext<TMeta = unknown> {
  layout: Required<CompositeLayout>;
  scope: CompositeScope;
  tenantId?: string;

  dialect: CompositeDialect;

  baseDir: string;

  // Emitted in deterministic order per config
  pragmas: string[];
  extraSql: string[];

  // Turn an absolute db file path into the path to put in ATTACH.
  makeAttachPath: (absDbPath: string) => string;

  // Stable identifier for aliasing and sorting (default: relative-to-baseDir)
  stableKeyForPath: (absDbPath: string) => string;

  pathInScope: (...parts: string[]) => string;
  defaultIgnores: readonly string[];
}

export interface ComposeConfig<TMeta = unknown> {
  globs: readonly string[];
  ignore?: readonly string[];

  // Determinism controls
  pragmaOrder?: DeterministicOrder; // default: "sorted"
  extraSqlOrder?: DeterministicOrder; // default: "sorted"

  // Alias MUST be derived from stable identifiers, not iteration order.
  aliasForKey?: (stableKey: string, ctx: ComposeContext<TMeta>) => string;

  include?: (
    absDbPath: string,
    ctx: ComposeContext<TMeta>,
  ) => boolean | Promise<boolean>;
  metaForPath?: (
    absDbPath: string,
    ctx: ComposeContext<TMeta>,
  ) => TMeta | Promise<TMeta>;

  /**
   * Dialect-specific “preamble” statements.
   * - SQLite: PRAGMA journal_mode, synchronous, foreign_keys, etc.
   * - DuckDB: INSTALL/LOAD sqlite, SET threads, etc.
   */
  pragmas?: (ctx: ComposeContext<TMeta>) => string[] | Promise<string[]>;

  /**
   * Extra DDL such as CREATE VIEW statements. If you return multi-line SQL that
   * must preserve ordering, set extraSqlOrder="asProvided".
   */
  extraSql?: (
    dbs: readonly DiscoveredDb<TMeta>[],
    ctx: ComposeContext<TMeta>,
  ) => string[] | Promise<string[]>;

  emitter?: SqlEmitter;
}

export interface ComposeResult<TMeta = unknown> {
  ctx: ComposeContext<TMeta>;
  dbs: DiscoveredDb<TMeta>[];
  sql: string;
}

export async function compose<TMeta = unknown>(args: {
  layout: CompositeLayout;
  scope: CompositeScope;
  tenantId?: string;

  /**
   * Dialect selects ATTACH syntax and conventions.
   * Default is "SQLite".
   */
  dialect?: CompositeDialect;

  configure: (
    ctx: ComposeContext<TMeta>,
  ) => ComposeConfig<TMeta> | Promise<ComposeConfig<TMeta>>;
  walker?: GlobWalker;
}): Promise<ComposeResult<TMeta>> {
  const layout = withDefaults(args.layout);
  const scope = args.scope;
  const tenantId = args.tenantId;
  const dialect: CompositeDialect = args.dialect ?? "SQLite";

  const baseDir = scopeBaseDir(layout, scope, tenantId);

  const ctx: ComposeContext<TMeta> = {
    layout,
    scope,
    tenantId,
    dialect,
    baseDir,
    pragmas: [],
    extraSql: [],
    makeAttachPath: (absDbPath: string) => {
      const absBase = normalize(baseDir);
      const absDb = normalize(absDbPath);
      if (absDb.startsWith(absBase + "/") || absDb === absBase) {
        return relative(absBase, absDb);
      }
      return absDbPath;
    },
    stableKeyForPath: (absDbPath: string) => {
      const absBase = normalize(baseDir);
      const absDb = normalize(absDbPath);
      if (absDb.startsWith(absBase + "/") || absDb === absBase) {
        return normalize(relative(absBase, absDb));
      }
      return absDb;
    },
    pathInScope: (...parts) => join(baseDir, ...parts),
    defaultIgnores: [
      normalize(join(baseDir, layout.compositeDbAutoName)),
      normalize(join(baseDir, layout.compositeSqlName)),
    ],
  };

  const config = await args.configure(ctx);
  const walker = args.walker ?? denoGlobWalker;
  const emitter = config.emitter ?? defaultEmitter;

  const pragmaOrder: DeterministicOrder = config.pragmaOrder ?? "sorted";
  const extraSqlOrder: DeterministicOrder = config.extraSqlOrder ?? "sorted";

  // Pragmas (deterministic)
  const pragmasRaw = (await config.pragmas?.(ctx)) ?? [];
  ctx.pragmas = canonicalizeLines(pragmasRaw, pragmaOrder);

  // Walk and collect candidates (do not alias yet)
  const ignore = new Set<string>(
    [
      ...ctx.defaultIgnores,
      ...(config.ignore ?? []),
    ].map(normalize),
  );

  const candidatesAbs: string[] = [];
  for await (const p of walker.walk(config.globs, { cwd: baseDir })) {
    const abs = normalize(isProbablyAbsolute(p) ? p : join(baseDir, p));
    if (ignore.has(abs)) continue;
    candidatesAbs.push(abs);
  }

  // Canonicalize candidates (deterministic)
  const seen = new Set<string>();
  const filtered: { absPath: string; stableKey: string }[] = [];

  for (const absPath of candidatesAbs) {
    const abs = normalize(absPath);
    if (seen.has(abs)) continue;
    seen.add(abs);

    if (ignore.has(abs)) continue;
    if (!looksLikeEmbeddedDb(abs)) continue;
    if (config.include && !(await config.include(abs, ctx))) continue;

    const stableKey = ctx.stableKeyForPath(abs);
    filtered.push({ absPath: abs, stableKey });
  }

  filtered.sort((a, b) => a.stableKey.localeCompare(b.stableKey));

  // Alias assignment (pure function of stableKey)
  const aliasForKey = config.aliasForKey ??
    ((k, _ctx) => defaultAliasForKey(k));

  const dbs: DiscoveredDb<TMeta>[] = [];
  for (const item of filtered) {
    const alias = aliasForKey(item.stableKey, ctx);
    const meta = config.metaForPath
      ? await config.metaForPath(item.absPath, ctx)
      : undefined;
    dbs.push({ path: item.absPath, relKey: item.stableKey, alias, meta });
  }

  // Extra SQL (deterministic)
  const extraSqlRaw = (await config.extraSql?.(dbs, ctx)) ?? [];
  ctx.extraSql = canonicalizeLines(extraSqlRaw, extraSqlOrder);

  // Emit SQL (deterministic order)
  const lines: string[] = [];
  const push = (s: string | string[]) =>
    Array.isArray(s) ? lines.push(...s) : lines.push(s);

  if (emitter.header) push(emitter.header(ctx));
  for (const db of dbs) {
    const stmt = emitter.attach ? emitter.attach(db, ctx) : emitAttach(
      db as unknown as DiscoveredDb<Any>,
      ctx as unknown as ComposeContext<Any>,
    );
    push(stmt);
  }
  if (emitter.footer) push(emitter.footer(ctx));

  const sql = lines
    .flatMap((l) => l.split("\n"))
    .map((l) => l.trimEnd())
    .join("\n")
    .replace(/\n{3,}/g, "\n\n")
    .trimEnd() + "\n";

  return { ctx, dbs, sql };
}

/**
 * Dialect-aware ATTACH.
 *
 * SQLite:
 *   ATTACH DATABASE 'path' AS alias;
 *
 * DuckDB (attaching SQLite DB files):
 *   ATTACH 'path' AS alias (TYPE sqlite);
 *
 * If you later want DuckDB-to-DuckDB attaching, extend this to use
 * TYPE duckdb (or omit TYPE) based on a per-db meta flag.
 */
export function emitAttach(
  db: DiscoveredDb<Any>,
  ctx: ComposeContext<Any>,
): string {
  const attachPath = ctx.makeAttachPath(db.path);

  if (ctx.dialect === "SQLite") {
    return `ATTACH DATABASE '${sqlQuoteSingle(attachPath)}' AS ${db.alias};`;
  }

  // DuckDB
  // For now we assume the discovered files are SQLite databases.
  // Callers should include `INSTALL sqlite; LOAD sqlite;` in ctx.pragmas via config.pragmas().
  return `ATTACH '${sqlQuoteSingle(attachPath)}' AS ${db.alias} (TYPE sqlite);`;
}

function canonicalizeLines(
  input: string[],
  order: DeterministicOrder,
): string[] {
  const normalized = input
    .flatMap((s) => s.split("\n"))
    .map((s) => s.trimEnd())
    .filter((s) => s.length > 0);

  if (order === "asProvided") {
    const seen = new Set<string>();
    const out: string[] = [];
    for (const s of normalized) {
      if (seen.has(s)) continue;
      seen.add(s);
      out.push(s);
    }
    return out;
  }

  return [...new Set(normalized)].sort((a, b) => a.localeCompare(b));
}

function withDefaults(layout: CompositeLayout): Required<CompositeLayout> {
  return {
    volumeRoot: layout.volumeRoot,
    embeddedDir: layout.embeddedDir ?? "embedded",
    adminDir: layout.adminDir ?? "admin",
    crossTenantDir: layout.crossTenantDir ?? "cross-tenant",
    tenantDir: layout.tenantDir ?? "tenant",
    compositeSqlName: layout.compositeSqlName ?? "composite.sql",
    compositeDbAutoName: layout.compositeDbAutoName ??
      "composite.sqlite.auto.db",
  };
}

function scopeBaseDir(
  layout: Required<CompositeLayout>,
  scope: CompositeScope,
  tenantId?: string,
): string {
  const root = join(layout.volumeRoot, layout.embeddedDir);
  if (scope === "admin") return join(root, layout.adminDir);
  if (scope === "cross-tenant") return join(root, layout.crossTenantDir);
  if (scope === "tenant") {
    if (!tenantId) throw new Error("tenantId is required when scope='tenant'");
    return join(root, layout.tenantDir, tenantId);
  }
  throw new Error(`Unsupported scope: ${scope}`);
}

function isProbablyAbsolute(p: string): boolean {
  return p.startsWith("/") || /^[A-Za-z]:[\\/]/.test(p);
}

function looksLikeEmbeddedDb(p: string): boolean {
  const b = basename(p).toLowerCase();
  if (b.endsWith(".sqlite")) return true;
  if (b.endsWith(".sqlite.db")) return true;
  if (b.endsWith(".db")) return true;
  return false;
}
