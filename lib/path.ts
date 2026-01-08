// lib/path.ts
import { ensureDir } from "@std/fs";
import { basename, dirname, extname, relative } from "@std/path";

export async function ensureParentDir(filePath: string): Promise<void> {
  const dir = dirname(filePath);
  if (dir && dir !== "." && dir !== "/") await ensureDir(dir);
}

export function normalizeSlash(p: string): string {
  return p.replaceAll("\\", "/").replaceAll(/\/+/g, "/");
}

export function stripOneExt(p: string): string {
  const ext = extname(p);
  return ext ? p.slice(0, -ext.length) : p;
}

export function normalizePathForUrl(path: string): string {
  const p = normalizeSlash(String(path ?? "")).trim();
  if (!p) return "/";
  if (!p.startsWith("/")) return `/${p}`;
  return p;
}

export function joinUrl(baseUrl: string, path: string): string {
  const b = String(baseUrl ?? "").replace(/\/+$/, "");
  const p = normalizePathForUrl(path);
  return `${b}${p}`;
}

export function isSafeRelativeSubpath(rel: string): boolean {
  const s = normalizeSlash(String(rel ?? "")).replace(/^\/+/, "");
  if (!s) return false;
  if (s.includes("\0")) return false;
  const parts = s.split("/").filter((x) => x.length > 0);
  if (!parts.length) return false;
  for (const part of parts) {
    if (part === "." || part === "..") return false;
  }
  return true;
}

export function contentTypeByName(name: string): string {
  const n = String(name ?? "").toLowerCase();
  if (n.endsWith(".json")) return "application/json; charset=utf-8";
  if (n.endsWith(".html") || n.endsWith(".htm")) {
    return "text/html; charset=utf-8";
  }
  if (n.endsWith(".css")) return "text/css; charset=utf-8";
  if (n.endsWith(".js")) return "text/javascript; charset=utf-8";
  if (n.endsWith(".log") || n.endsWith(".txt")) {
    return "text/plain; charset=utf-8";
  }
  return "application/octet-stream";
}

function stripCompoundDbExt(p: string): string {
  const lower = p.toLowerCase();

  const compounds = [
    ".sqlite.db",
    ".sqlite3.db",
    ".duckdb.db",
  ];

  for (const suf of compounds) {
    if (lower.endsWith(suf)) return p.slice(0, -suf.length);
  }

  return stripOneExt(p);
}

/**
 * materialize/watch/web-ui canonical proxy prefix:
 * derived from the discovered DB path relative to its best root.
 */
export function proxyPrefixFromRel(relFromRoot: string): string {
  const relNoExt = stripCompoundDbExt(relFromRoot);
  const clean = normalizeSlash(relNoExt).replaceAll(/^\.\//g, "").trim();
  if (!clean) return "/";
  return `/${clean.startsWith("/") ? clean.slice(1) : clean}`.replaceAll(
    /\/+/g,
    "/",
  );
}

function bestRootForFile(
  fileAbs: string,
  rootsAbs: readonly string[],
): string | undefined {
  const candidates = rootsAbs
    .filter((r) => fileAbs === r || fileAbs.startsWith(r + "/"))
    .sort((a, b) => b.length - a.length);
  return candidates[0];
}

export function relFromRoots(
  fileAbs: string,
  rootsAbs: readonly string[],
): string {
  const root = bestRootForFile(fileAbs, rootsAbs);
  if (!root) return basename(fileAbs);

  let rel = relative(root, fileAbs);
  rel = normalizeSlash(rel).replaceAll(/^\.\//g, "");

  // Defensive: strip root-name prefix if it leaks into rel.
  const rootName = basename(root);
  const prefix = `${rootName}/`;
  if (rel.startsWith(prefix)) rel = rel.slice(prefix.length);

  if (!rel || rel.startsWith("..")) return basename(fileAbs);
  return rel;
}

export function relDirFromRoots(
  fileAbs: string,
  rootsAbs: readonly string[],
): string {
  const rel = relFromRoots(fileAbs, rootsAbs);
  const d = dirname(rel);
  if (d === "." || d === "/" || d.trim() === "") return "";
  return normalizeSlash(d).replaceAll(/\/+$/g, "");
}

/**
 * Small helper for “best-effort” rel paths in UIs and logs.
 */
export function safeRelFromRoot(
  rootAbs: string | undefined,
  fileAbs: string,
): string {
  try {
    if (!rootAbs || rootAbs.trim().length === 0) return fileAbs;
    const rel = normalizeSlash(relative(rootAbs, fileAbs));
    if (rel.startsWith("..") || rel === "") return fileAbs;
    return rel;
  } catch {
    return fileAbs;
  }
}

export function safeBasename(p: string): string {
  try {
    return basename(p);
  } catch {
    return String(p);
  }
}
