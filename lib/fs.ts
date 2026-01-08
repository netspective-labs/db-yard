// lib/fs.ts
import { ensureDir } from "@std/fs";
import { dirname, resolve } from "@std/path";
import { encounters, fileSystemSource } from "./discover.ts";
import { normalizeSlash } from "./path.ts";

export async function ensureParentDir(filePath: string): Promise<void> {
  const dir = dirname(filePath);
  if (dir && dir !== "." && dir !== "/") await ensureDir(dir);
}

export type ListedFile = Readonly<{
  name: string; // relative to root
  absPath: string;
  size: number;
  mtimeMs: number;
  kind: "json" | "log" | "other";
}>;

export async function listFilesRecursiveViaEncounters(
  args: Readonly<{
    rootDir: string;
    globs?: readonly string[];
    hide?: (rel: string) => boolean;
  }>,
): Promise<ListedFile[]> {
  const root = resolve(args.rootDir);
  const globs = args.globs ?? ["**/*"];

  const out: ListedFile[] = [];

  const gen = encounters(
    [{ path: root, globs: [...globs] }],
    fileSystemSource({}, (e) => {
      // We don’t need content for listing, but encounters wants a supplier.
      // Keep it cheap: stat is done below.
      return e.path;
    }),
    async ({ entry }) => {
      const abs = resolve(entry.path);
      const rel = normalizeSlash(abs).startsWith(normalizeSlash(root) + "/")
        ? normalizeSlash(abs).slice(normalizeSlash(root).length + 1)
        : normalizeSlash(abs);

      if (args.hide && args.hide(rel)) return null;

      let st: Deno.FileInfo;
      try {
        st = await Deno.stat(abs);
      } catch {
        return null;
      }
      if (!st.isFile) return null;

      const kind = rel.endsWith(".json")
        ? "json"
        : (rel.endsWith(".stdout.log") || rel.endsWith(".stderr.log")
          ? "log"
          : "other");

      out.push({
        name: rel,
        absPath: abs,
        size: st.size,
        mtimeMs: st.mtime?.getTime() ?? 0,
        kind,
      });

      return null;
    },
  );

  while (true) {
    const next = await gen.next();
    if (next.done) break;
  }

  out.sort((a, b) => a.name.localeCompare(b.name));
  return out;
}

export function formatBytes(n: number): string {
  if (!Number.isFinite(n) || n < 0) return "-";
  if (n < 1024) return `${n} B`;
  const kb = n / 1024;
  if (kb < 1024) return `${kb.toFixed(1)} KB`;
  const mb = kb / 1024;
  if (mb < 1024) return `${mb.toFixed(1)} MB`;
  const gb = mb / 1024;
  return `${gb.toFixed(1)} GB`;
}

export function formatWhen(ms: number): string {
  if (!Number.isFinite(ms) || ms <= 0) return "-";
  try {
    return new Date(ms).toISOString();
  } catch {
    return "-";
  }
}
