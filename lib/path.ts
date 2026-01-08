// lib/path.ts
import { basename, extname, relative } from "@std/path";

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

/**
 * materialize/watch/web-ui canonical proxy prefix:
 * derived from the discovered DB path relative to its best root.
 */
export function proxyPrefixFromRel(relFromRoot: string): string {
  const relNoExt = stripOneExt(relFromRoot);
  const clean = normalizeSlash(relNoExt).replaceAll(/^\.\//g, "").trim();
  if (!clean) return "/";
  return `/${clean.startsWith("/") ? clean.slice(1) : clean}`.replaceAll(
    /\/+/g,
    "/",
  );
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
