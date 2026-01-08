// tabular.ts
import { basename, extname } from "@std/path";
import { type WalkEntry } from "@std/fs";

import {
  type EncounterErrorContext,
  encounters,
  type EncounterSummary,
  fileSystemSource,
  type Path,
} from "./discover.ts";

export type TabularDataSupplier =
  | SqliteDataSupplier
  | SqlPageDataSupplier
  | SurveilrDataSupplier
  | ExcelDataSupplier
  | DuckDbDataSupplier
  | ServerTabularDataSupplier;

export type TabularNature = "embedded" | "server";
export type TabularKind =
  | "sqlite"
  | "sqlpage"
  | "surveilr"
  | "excel"
  | "duckdb"
  | "server";

export type TabularBase = Readonly<{
  nature: TabularNature;
  kind: TabularKind;
  srcPath: Path;
  location: string;
  label: string;
}>;

export type SqliteDataSupplier =
  & TabularBase
  & Readonly<{
    nature: "embedded";
    kind: "sqlite";
    dbPath: string;
  }>;

export type SqlPageDataSupplier =
  & TabularBase
  & Readonly<{
    nature: "embedded";
    kind: "sqlpage";
    dbPath: string;
  }>;

export type SurveilrDataSupplier =
  & TabularBase
  & Readonly<{
    nature: "embedded";
    kind: "surveilr";
    dbPath: string;
  }>;

export type ExcelDataSupplier =
  & TabularBase
  & Readonly<{
    nature: "embedded";
    kind: "excel";
    filePath: string;
  }>;

export type DuckDbDataSupplier =
  & TabularBase
  & Readonly<{
    nature: "embedded";
    kind: "duckdb";
    dbPath: string;
  }>;

export type ServerTabularDataSupplier =
  & TabularBase
  & Readonly<{
    nature: "server";
    kind: "server";
    engine?: "postgres" | "mysql" | "mssql" | "http" | "other";
    dsn: string;
  }>;

export type TabularOnError = (
  ctx:
    | Readonly<{ phase: "discovery"; error: EncounterErrorContext<WalkEntry> }>
    | Readonly<{ phase: "detect"; location: string; error: unknown }>,
) => void | Promise<void>;

export type TabularSummary = Readonly<{
  unclassified: string[];
  errored: string[];
  discovery: EncounterSummary<WalkEntry>;
  detectionErrors: ReadonlyArray<
    Readonly<{ location: string; error: unknown }>
  >;
}>;

export type TabularOptions = Readonly<{
  defaultGlobs?: readonly string[];
  disableProbes?: boolean;
  sqliteCmd?: string;
  onError?: TabularOnError;

  /**
   * How de-duplication is performed.
   * - "location": de-dup by the supplier's canonical `location` (default)
   * - "basename": de-dup by file name only
   */
  dedupeBy?: "location" | "basename";
}>;

const DEFAULT_GLOBS = [
  "**/*.db",
  "**/*.sqlite",
  "**/*.sqlite3",
  "**/*.sqlite.db",
  "**/*.duckdb",
  "**/*.xlsx",
] as const;

/**
 * Discover tabular data suppliers from Path descriptors and yield a de-duplicated stream.
 */
export async function* tabular(
  srcPaths: Iterable<Path>,
  opts: TabularOptions = {},
): AsyncGenerator<TabularDataSupplier, TabularSummary> {
  const defaultGlobs = opts.defaultGlobs ?? DEFAULT_GLOBS;
  const dedupeBy = opts.dedupeBy ?? "location";

  // Apply default globs where missing.
  const expanded: Path[] = [];
  for (const p of srcPaths) {
    if (p.globs && p.globs.length > 0) expanded.push(p);
    else expanded.push({ path: p.path, globs: defaultGlobs });
  }

  const detectionErrors: Array<{ location: string; error: unknown }> = [];
  const unclassifiedSet = new Set<string>();

  // De-dupe file paths before probing (handles overlapping globs).
  const seenPaths = new Set<string>();

  // De-dupe suppliers after classification (belt-and-suspenders).
  const seenSuppliers = new Set<string>();

  const discoveryGen = encounters(
    expanded,
    fileSystemSource(),
    ({ entry }) => entry.path,
    async (e: EncounterErrorContext<WalkEntry>) => {
      if (opts.onError) await opts.onError({ phase: "discovery", error: e });
    },
  );

  while (true) {
    const next = await discoveryGen.next();
    if (next.done) {
      const discoverySummary: EncounterSummary<WalkEntry> = next.value;

      const erroredPaths = discoverySummary.errored.map((we: WalkEntry) =>
        we.path
      );

      const summary: TabularSummary = {
        unclassified: [...unclassifiedSet],
        errored: uniqueStrings([
          ...erroredPaths,
          ...detectionErrors.map((d) => d.location),
        ]),
        discovery: discoverySummary,
        detectionErrors,
      };
      return summary;
    }

    const filePath: string = next.value;

    if (seenPaths.has(filePath)) continue;
    seenPaths.add(filePath);

    try {
      const supplier = await detectSupplierForFile(filePath, expanded, opts);
      if (!supplier) {
        unclassifiedSet.add(filePath);
        continue;
      }

      const supplierKey = dedupeBy === "basename"
        ? basename(supplier.location)
        : supplier.location;

      if (seenSuppliers.has(supplierKey)) continue;
      seenSuppliers.add(supplierKey);

      yield supplier;
    } catch (error) {
      detectionErrors.push({ location: filePath, error });
      if (opts.onError) {
        await opts.onError({ phase: "detect", location: filePath, error });
      }
      unclassifiedSet.add(filePath);
    }
  }
}

async function detectSupplierForFile(
  filePath: string,
  srcPaths: readonly Path[],
  opts: TabularOptions,
): Promise<TabularDataSupplier | null> {
  const srcPath = pickSrcPathForFile(filePath, srcPaths) ?? { path: filePath };
  const ext = extname(filePath).toLowerCase();

  if (ext === ".xlsx") {
    return {
      nature: "embedded",
      kind: "excel",
      srcPath,
      location: filePath,
      label: basename(filePath),
      filePath,
    };
  }

  if (isDuckDbExtension(ext, filePath)) {
    return {
      nature: "embedded",
      kind: "duckdb",
      srcPath,
      location: filePath,
      label: basename(filePath),
      dbPath: filePath,
    };
  }

  if (isSqliteLikeExtension(ext, filePath)) {
    if (opts.disableProbes) {
      return {
        nature: "embedded",
        kind: "sqlite",
        srcPath,
        location: filePath,
        label: basename(filePath),
        dbPath: filePath,
      };
    }

    const sqliteCmd = opts.sqliteCmd ?? "sqlite3";
    const hasSqlite = await commandExists(sqliteCmd);

    if (!hasSqlite) {
      return {
        nature: "embedded",
        kind: "sqlite",
        srcPath,
        location: filePath,
        label: basename(filePath),
        dbPath: filePath,
      };
    }

    // reminder: surveilr RSSDs have both `uniform_resource` and `sqlpage_files`
    // tables so we check for surveilr classification first
    const hasUniformResource = await sqliteHasTable(
      sqliteCmd,
      filePath,
      "uniform_resource",
    );
    if (hasUniformResource) {
      return {
        nature: "embedded",
        kind: "surveilr",
        srcPath,
        location: filePath,
        label: basename(filePath),
        dbPath: filePath,
      };
    }

    const hasSqlpageFiles = await sqliteHasTable(
      sqliteCmd,
      filePath,
      "sqlpage_files",
    );
    if (hasSqlpageFiles) {
      return {
        nature: "embedded",
        kind: "sqlpage",
        srcPath,
        location: filePath,
        label: basename(filePath),
        dbPath: filePath,
      };
    }

    return {
      nature: "embedded",
      kind: "sqlite",
      srcPath,
      location: filePath,
      label: basename(filePath),
      dbPath: filePath,
    };
  }

  if (looksLikeServerDsn(filePath)) {
    return {
      nature: "server",
      kind: "server",
      srcPath,
      location: filePath,
      label: filePath,
      dsn: filePath,
      engine: inferServerEngine(filePath),
    };
  }

  return null;
}

function pickSrcPathForFile(
  filePath: string,
  srcPaths: readonly Path[],
): Path | undefined {
  for (const p of srcPaths) {
    if (filePath.startsWith(p.path)) return p;
  }
  return undefined;
}

function isSqliteLikeExtension(ext: string, filePath: string): boolean {
  if (ext === ".sqlite" || ext === ".sqlite3" || ext === ".db") return true;
  const lower = filePath.toLowerCase();
  return lower.endsWith(".sqlite.db") || lower.endsWith(".sqlite3.db");
}

function isDuckDbExtension(ext: string, filePath: string): boolean {
  if (ext === ".duckdb") return true;
  const lower = filePath.toLowerCase();
  return lower.endsWith(".duckdb.db") || lower.endsWith(".ddb");
}

function looksLikeServerDsn(s: string): boolean {
  const lower = s.toLowerCase();
  return lower.startsWith("postgres://") ||
    lower.startsWith("postgresql://") ||
    lower.startsWith("mysql://") ||
    lower.startsWith("mssql://") ||
    lower.startsWith("http://") ||
    lower.startsWith("https://");
}

function inferServerEngine(dsn: string): ServerTabularDataSupplier["engine"] {
  const lower = dsn.toLowerCase();
  if (lower.startsWith("postgres://") || lower.startsWith("postgresql://")) {
    return "postgres";
  }
  if (lower.startsWith("mysql://")) return "mysql";
  if (lower.startsWith("mssql://")) return "mssql";
  if (lower.startsWith("http://") || lower.startsWith("https://")) {
    return "http";
  }
  return "other";
}

async function commandExists(cmd: string): Promise<boolean> {
  try {
    const p = new Deno.Command(cmd, {
      args: ["-version"],
      stdout: "null",
      stderr: "null",
    });
    const { code } = await p.output();
    return code === 0;
  } catch {
    return false;
  }
}

async function sqliteHasTable(
  sqliteCmd: string,
  dbPath: string,
  tableName: string,
): Promise<boolean> {
  const sql = `select 1 from sqlite_master where type='table' and name='${
    escapeSqlLiteral(tableName)
  }' limit 1;`;

  const p = new Deno.Command(sqliteCmd, {
    args: ["-batch", "-noheader", dbPath, sql],
    stdout: "piped",
    stderr: "piped",
  });

  const { code, stdout } = await p.output();
  if (code !== 0) return false;

  const out = new TextDecoder().decode(stdout).trim();
  return out === "1";
}

function escapeSqlLiteral(s: string): string {
  return s.replaceAll("'", "''");
}

function uniqueStrings(xs: string[]): string[] {
  return [...new Set(xs)];
}
