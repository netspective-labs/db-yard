// discover.ts
import { walk, type WalkEntry, type WalkOptions } from "@std/fs";
import { globToRegExp, relative } from "@std/path";

/**
 * A "path-like" input descriptor.
 *
 * - `path` identifies the origin of entries for a source.
 *   Filesystem sources treat it as a directory (or file) path.
 *   Spawned/SQL sources often treat it as a database file path, but it can be any origin string.
 * - `globs` optionally filters entries by a match string defined by the source (usually a relative
 *   path or logical path).
 */
export type Path = Readonly<{
  path: string;
  globs?: readonly string[];
}>;

export function assertValidPath(p: unknown): asserts p is Path {
  if (!p || typeof p !== "object") {
    throw new TypeError("Path must be an object");
  }
  const o = p as Record<string, unknown>;
  if (typeof o.path !== "string" || o.path.trim().length === 0) {
    throw new TypeError("Path.path must be a non-empty string");
  }
  if (o.globs !== undefined) {
    if (!Array.isArray(o.globs)) {
      throw new TypeError("Path.globs must be an array of strings");
    }
    for (const g of o.globs) {
      if (typeof g !== "string" || g.trim().length === 0) {
        throw new TypeError("Path.globs entries must be non-empty strings");
      }
    }
  }
}

export function normalizePaths(srcPaths: Iterable<Path>): Path[] {
  const arr = [...srcPaths];
  for (const p of arr) assertValidPath(p);
  return arr;
}

/**
 * Arguments provided to onMatch().
 *
 * Note: content is lazy. The source is not asked for content unless you call getContent().
 */
export type EncounterArgs<Entry, Content> = Readonly<{
  srcPath: Path;
  entry: Entry;
  glob?: string;
  content: () => Promise<Content>;
}>;

export type EncounterCallback<Entry, Content, Encountered> = (
  args: EncounterArgs<Entry, Content>,
) => Encountered | null | undefined | Promise<Encountered | null | undefined>;

/**
 * Adapter that makes any "path-like" source discoverable.
 *
 * Implement a new source by providing these functions:
 * - list: enumerate entries for a srcPath
 * - id: stable id for caching/handled tracking
 * - matchPath: string used for glob matching (relative path, URL path, logical path, etc.)
 * - content: fetch/compute content (only called if getContent() is invoked)
 */
export type EncounterSource<Entry, Content> = Readonly<{
  list: (srcPath: Path) => AsyncIterable<Entry> | Iterable<Entry>;
  id: (entry: Entry) => string;
  matchPath: (srcPath: Path, entry: Entry) => string;
  content: (entry: Entry) => Content | Promise<Content>;
}>;

export type EncounterErrorContext<Entry> = Readonly<{
  srcPath: Path;
  entry?: Entry;
  phase: "list" | "glob-match" | "content" | "onMatch";
  error: unknown;
}>;

/**
 * Optional hook to observe errors without stopping discovery.
 *
 * Keep this lightweight:
 * - log to console
 * - push into an array
 * - count by phase
 *
 * Avoid throwing from onError. If you do, it will be ignored so discovery can continue.
 */
export type EncounterOnError<Entry> = (
  ctx: EncounterErrorContext<Entry>,
) => void | Promise<void>;

export type EncounterSummary<Entry> = Readonly<{
  /**
   * Entries that were seen but never produced an Encountered value.
   */
  unhandled: Entry[];

  /**
   * Entries that had at least one error in any phase.
   */
  errored: Entry[];

  /**
   * Full error detail (includes srcPath-level errors where entry is undefined).
   */
  errors: EncounterErrorContext<Entry>[];
}>;

type CompiledGlob = Readonly<{ glob: string; re: RegExp }>;

function compileGlobs(globs: readonly string[] | undefined): CompiledGlob[] {
  if (!globs) return [];
  return [...globs].map((glob) => ({
    glob,
    re: globToRegExp(glob, { globstar: true, extended: true }),
  }));
}

/**
 * Discover "encounters" from any source.
 *
 * How it works:
 * - You pass srcPaths and a source adapter.
 * - The source enumerates entries; encounters() optionally filters by Path.globs.
 * - Your onMatch decides what to yield. Returning null/undefined means "not handled".
 * - Content is lazy: only call getContent() when you need it.
 *
 * Error handling:
 * - If you pass onError, errors from major phases are reported and discovery continues.
 * - The generator's return value includes both unhandled entries and errored entries.
 *
 * Typical usage (ignore summary):
 * ```ts
 * const src = fileSystemSource({}, (e) => Deno.readTextFile(e.path));
 * for await (const item of encounters(
 *   [{ path: "./src", globs: ["**\/*.ts"] }],
 *   src,
 *   async ({ entry, getContent }) => {
 *     const text = await getContent();
 *     return { path: entry.path, lines: String(text).split("\n").length };
 *   },
 * )) console.log(item);
 * ```
 *
 * Capturing the summary:
 * ```ts
 * const gen = encounters(
 *   [{ path: "./src" }],
 *   fileSystemSource(),
 *   ({ entry }) => entry.path,
 *   (e) => console.warn(e.phase, e.srcPath.path, e.error),
 * );
 *
 * while (true) {
 *   const { value, done } = await gen.next();
 *   if (done) {
 *     console.log("unhandled:", value.unhandled.length);
 *     console.log("errored:", value.errored.length);
 *     console.log("errors:", value.errors.length);
 *     break;
 *   }
 *   console.log("encountered:", value);
 * }
 * ```
 */
export async function* encounters<Entry, Content, Encountered>(
  srcPaths: Iterable<Path>,
  source: EncounterSource<Entry, Content>,
  onMatch: EncounterCallback<Entry, Content, Encountered>,
  onError?: EncounterOnError<Entry>,
): AsyncGenerator<Encountered, EncounterSummary<Entry>> {
  const parsed = normalizePaths(srcPaths);

  const firstSeenEntryById = new Map<string, Entry>();
  const handledIds = new Set<string>();
  const contentById = new Map<string, Promise<Content>>();

  const errors: EncounterErrorContext<Entry>[] = [];
  const erroredIds = new Set<string>();

  const recordError = async (ctx: EncounterErrorContext<Entry>) => {
    errors.push(ctx);

    if (ctx.entry !== undefined) {
      try {
        erroredIds.add(source.id(ctx.entry));
      } catch {
        // keep the error record even if id() itself fails
      }
    }

    if (onError) {
      try {
        await onError(ctx);
      } catch {
        // ignore errors from onError to keep discovery running
      }
    }
  };

  for (const srcPath of parsed) {
    const compiled = compileGlobs(srcPath.globs);

    let iterable: AsyncIterable<Entry> | Iterable<Entry>;
    try {
      iterable = source.list(srcPath);
    } catch (error) {
      await recordError({ srcPath, phase: "list", error });
      continue;
    }

    try {
      for await (const entry of iterable as AsyncIterable<Entry>) {
        let id: string;
        try {
          id = source.id(entry);
        } catch (error) {
          await recordError({ srcPath, entry, phase: "glob-match", error });
          continue;
        }

        if (!firstSeenEntryById.has(id)) firstSeenEntryById.set(id, entry);

        let globHits: (string | undefined)[];
        try {
          if (srcPath.globs === undefined) {
            globHits = [undefined];
          } else {
            const mp = source.matchPath(srcPath, entry);
            const hits: string[] = [];
            for (const { glob, re } of compiled) {
              if (re.test(mp)) hits.push(glob);
            }
            globHits = hits;
          }
        } catch (error) {
          await recordError({ srcPath, entry, phase: "glob-match", error });
          continue;
        }

        if (globHits.length === 0) continue;

        const getContent = async () => {
          const existing = contentById.get(id);
          if (existing) return existing;

          try {
            const created = Promise.resolve(source.content(entry));
            contentById.set(id, created);
            return await created;
          } catch (error) {
            await recordError({ srcPath, entry, phase: "content", error });
            throw error;
          }
        };

        for (const glob of globHits) {
          let encountered: Encountered | null | undefined;
          try {
            encountered = await onMatch({
              srcPath,
              entry,
              glob,
              content: getContent,
            });
          } catch (error) {
            await recordError({ srcPath, entry, phase: "onMatch", error });
            continue;
          }

          if (encountered != null) {
            handledIds.add(id);
            yield encountered;
          }
        }
      }
    } catch (error) {
      await recordError({ srcPath, phase: "list", error });
      continue;
    }
  }

  const unhandled: Entry[] = [];
  for (const [id, entry] of firstSeenEntryById) {
    if (!handledIds.has(id)) unhandled.push(entry);
  }

  const errored: Entry[] = [];
  for (const [id, entry] of firstSeenEntryById) {
    if (erroredIds.has(id)) errored.push(entry);
  }

  return { unhandled, errored, errors };
}

/**
 * Create a filesystem-backed source adapter using @std/fs walk.
 *
 * - matchPath is relative(srcPath.path, entry.path), enabling globs like "**\/*.ts".
 * - Provide a contentSupplier when you need file content (text, bytes, parsed JSON, etc.).
 * - If you do not provide a contentSupplier, getContent() resolves to undefined.
 */
export function fileSystemSource(
  walkOptions: WalkOptions = {},
  contentSupplier?: (entry: WalkEntry) => unknown | Promise<unknown>,
): EncounterSource<WalkEntry, unknown> {
  const options: WalkOptions = { includeDirs: false, ...walkOptions };

  return {
    list: (srcPath) => walk(srcPath.path, options),
    id: (entry) => entry.path,
    matchPath: (srcPath, entry) => relative(srcPath.path, entry.path),
    content: (entry) => contentSupplier ? contentSupplier(entry) : undefined,
  };
}

/**
 * Describes how to spawn an external command that emits JSON on stdout.
 *
 * The spawned process must output a JSON array. Supported shapes:
 * - array of strings:
 *   ["a/b", "c/d"]
 * - array of arrays:
 *   [["a/b", <content>, <elaboration?>], ...]
 * - array of objects:
 *   [{ "path": "a/b", "content": <any>, "elaboration": <any> }, ...]
 *
 * The SQL passed to spawnableSqlSource() is available in ctx.sql.
 */
export type SpawnableJsonSupplier = Readonly<{
  cmd: string;
  args: (ctx: Readonly<{ srcPath: Path; sql: string }>) => string[];
  env?: Record<string, string>;
  cwd?: string;
  stdin?: (
    ctx: Readonly<{ srcPath: Path; sql: string }>,
  ) => string | Uint8Array;
}>;

export type SpawnableSqlRowEntry = Readonly<{
  origin: string;
  path: string;
  content: unknown;
  elaboration?: unknown;
}>;

/**
 * Create a source adapter by spawning a command per srcPath that returns JSON rows.
 *
 * Use this when your data lives behind an external CLI, such as:
 * - sqlite3 or duckdb CLI
 * - a custom extractor tool
 * - an HTTP client wrapper that prints JSON
 *
 * Each srcPath spawns the command once. The JSON array it prints becomes entries.
 * Glob matching uses the row's `path`.
 */
export function spawnableSqlSource(
  sql: string,
  supplier: SpawnableJsonSupplier,
): EncounterSource<SpawnableSqlRowEntry, unknown> {
  return {
    list: (srcPath) => {
      async function* iter(): AsyncGenerator<SpawnableSqlRowEntry> {
        const ctx = { srcPath, sql } as const;
        const args = supplier.args(ctx);
        const stdin = supplier.stdin ? supplier.stdin(ctx) : undefined;

        const cmd = new Deno.Command(supplier.cmd, {
          args,
          cwd: supplier.cwd,
          env: supplier.env,
          stdin: stdin === undefined ? "null" : "piped",
          stdout: "piped",
          stderr: "piped",
        });

        const child = cmd.spawn();

        if (stdin !== undefined) {
          const w = child.stdin;
          if (w) {
            const writer = w.getWriter();
            try {
              if (typeof stdin === "string") {
                await writer.write(new TextEncoder().encode(stdin));
              } else {
                await writer.write(stdin);
              }
            } finally {
              try {
                await writer.close();
              } catch {
                // ignore
              }
              writer.releaseLock();
            }
          }
        }

        const { code, stdout, stderr } = await child.output();

        if (code !== 0) {
          const errText = new TextDecoder().decode(stderr).trim();
          const msg = errText.length > 0
            ? `spawnableSqlSource: command failed (${supplier.cmd}) code=${code}: ${errText}`
            : `spawnableSqlSource: command failed (${supplier.cmd}) code=${code}`;
          throw new Error(msg);
        }

        const text = new TextDecoder().decode(stdout).trim();
        if (text.length === 0) return;

        let parsed: unknown;
        try {
          parsed = JSON.parse(text);
        } catch (e) {
          const errText = new TextDecoder().decode(stderr).trim();
          const suffix = errText ? `; stderr=${errText}` : "";
          throw new Error(
            `spawnableSqlSource: stdout was not valid JSON from (${supplier.cmd}): ${
              e instanceof Error ? e.message : String(e)
            }${suffix}`,
          );
        }

        if (!Array.isArray(parsed)) {
          throw new Error(
            `spawnableSqlSource: expected JSON array from (${supplier.cmd}), got ${typeof parsed}`,
          );
        }

        for (const item of parsed) {
          const row = normalizeSpawnedRow(item);
          if (!row.path) continue;

          yield {
            origin: srcPath.path,
            path: row.path,
            content: row.content,
            elaboration: row.elaboration,
          };
        }
      }

      return iter();
    },

    id: (e) => `spawned:${e.origin}#${e.path}`,
    matchPath: (_srcPath, e) => e.path,
    content: (e) => e.content,
  };
}

/**
 * Convenience source: SQLite implemented via spawnableSqlSource using `sqlite3 -json`.
 *
 * Your SQL should yield column aliases:
 * - path (required)
 * - content (required)
 * - elaboration (optional)
 *
 * Example SQL:
 * ```sql
 * select
 *   'patients/' || patient_id as path,
 *   json_object('id', patient_id, 'name', name) as content,
 *   json_object('table', 'patients') as elaboration
 * from patients;
 * ```
 */
export function sqliteSource(
  sql: string,
  opts: Readonly<{ sqliteCmd?: string }> = {},
): EncounterSource<SpawnableSqlRowEntry, unknown> {
  const sqliteCmd = opts.sqliteCmd ?? "sqlite3";
  return spawnableSqlSource(sql, {
    cmd: sqliteCmd,
    args: ({ srcPath, sql }) => ["-json", srcPath.path, sql],
  });
}

function normalizeSpawnedRow(
  item: unknown,
): { path: string; content: unknown; elaboration?: unknown } {
  if (typeof item === "string") return { path: item, content: undefined };

  if (Array.isArray(item)) {
    const path = item.length >= 1 ? String(item[0] ?? "") : "";
    const content = item.length >= 2 ? item[1] : undefined;
    const elaboration = item.length >= 3 ? item[2] : undefined;
    return { path, content, elaboration };
  }

  if (item && typeof item === "object") {
    const o = item as Record<string, unknown>;
    const path = typeof o.path === "string" ? o.path : String(o.path ?? "");
    const content = "content" in o ? o.content : undefined;
    const elaboration = "elaboration" in o ? o.elaboration : undefined;
    return { path, content, elaboration };
  }

  return { path: "", content: undefined };
}
