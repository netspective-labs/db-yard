// lib/session.ts
import { ensureDir } from "@std/fs";
import { basename, dirname, join, relative, resolve } from "@std/path";
import type { Path } from "./discover.ts";
import type { ExposableService } from "./exposable.ts";
import { normalizeSlash } from "./path.ts";

export type SpawnLedgerNature = "context" | "stdout" | "stderr";

function fmt2(n: number): string {
  return String(n).padStart(2, "0");
}

export function sessionStamp(d = new Date()): string {
  const yyyy = d.getFullYear();
  const mm = fmt2(d.getMonth() + 1);
  const dd = fmt2(d.getDate());
  const hh = fmt2(d.getHours());
  const mi = fmt2(d.getMinutes());
  const ss = fmt2(d.getSeconds());
  return `${yyyy}-${mm}-${dd}-${hh}-${mi}-${ss}`;
}

export function resolveRootsAbs(srcPaths: Iterable<Path>): string[] {
  const src = Array.from(srcPaths);
  return src.map((p) => Deno.realPathSync(resolve(p.path)));
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

export async function ensureSpawnedLedgerHome(
  spawnedLedgerHome: string,
): Promise<string> {
  const home = resolve(spawnedLedgerHome);
  await ensureDir(home);
  return home;
}

export type SpawnSessionHome = Readonly<{
  spawnedLedgerHome: string;
  sessionHome: string;
  sessionName: string;
}>;

export async function createSpawnSessionHome(
  spawnedLedgerHome: string,
): Promise<SpawnSessionHome> {
  const home = await ensureSpawnedLedgerHome(spawnedLedgerHome);
  const sessionName = sessionStamp();
  const sessionHome = join(home, sessionName);
  await ensureDir(sessionHome);

  // Pointer file for “current session” (portable, no symlinks).
  await Deno.writeTextFile(join(home, ".current-session"), `${sessionName}\n`);

  return { spawnedLedgerHome: home, sessionHome, sessionName };
}

export async function resolveCurrentSessionHome(
  spawnedLedgerHome: string,
): Promise<SpawnSessionHome | undefined> {
  const home = resolve(spawnedLedgerHome);
  const pointer = join(home, ".current-session");
  try {
    const name = (await Deno.readTextFile(pointer)).trim();
    if (!name) return undefined;
    const sessionHome = join(home, name);
    const st = await Deno.stat(sessionHome);
    if (!st.isDirectory) return undefined;
    return { spawnedLedgerHome: home, sessionHome, sessionName: name };
  } catch {
    return undefined;
  }
}

export async function pickLatestSessionHome(
  spawnedLedgerHome: string,
): Promise<SpawnSessionHome | undefined> {
  const home = resolve(spawnedLedgerHome);

  let best: string | undefined;
  try {
    for await (const e of Deno.readDir(home)) {
      if (!e.isDirectory) continue;
      if (!/^\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}$/.test(e.name)) continue;
      if (!best || e.name > best) best = e.name;
    }
  } catch {
    return undefined;
  }

  if (!best) return undefined;
  return {
    spawnedLedgerHome: home,
    sessionHome: join(home, best),
    sessionName: best,
  };
}

export function spawnedLedgerPathForEntry(
  entry: ExposableService,
  nature: SpawnLedgerNature,
  args: Readonly<{ sessionHome: string; rootsAbs: readonly string[] }>,
): string | undefined {
  const fileAbs = Deno.realPathSync(resolve(entry.supplier.location));

  const relFromRoot = relFromRoots(fileAbs, args.rootsAbs);
  const relDir = relDirFromRoots(fileAbs, args.rootsAbs);

  const outDir = relDir ? join(args.sessionHome, relDir) : args.sessionHome;
  const fileName = basename(relFromRoot);

  if (nature === "context") return join(outDir, `${fileName}.context.json`);
  if (nature === "stdout") return join(outDir, `${fileName}.stdout.log`);
  if (nature === "stderr") return join(outDir, `${fileName}.stderr.log`);
  return undefined;
}
