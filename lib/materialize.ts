// lib/yard.ts
import { ensureDir } from "@std/fs";
import { basename, dirname, extname, join, relative, resolve } from "@std/path";
import type { Path } from "./discover.ts";
import { encounters, fileSystemSource } from "./discover.ts";
import type { ExposableService } from "./exposable.ts";
import { richTextUISpawnEvents } from "./spawn-event.ts";
import {
  spawn,
  type SpawnedContext,
  SpawnEventListener,
  type SpawnSummary,
} from "./spawn.ts";

export type MaterializeVerbose = false | "essential" | "comprehensive";

export type MaterializeOptions = Readonly<{
  verbose: MaterializeVerbose;
  spawnStateHome: string;
}>;

export type MaterializeResult = Readonly<{
  sessionHome: string;
  summary: SpawnSummary;
  spawned: SpawnedContext[];
}>;

function fmt2(n: number): string {
  return String(n).padStart(2, "0");
}

function sessionStamp(d = new Date()): string {
  const yyyy = d.getFullYear();
  const mm = fmt2(d.getMonth() + 1);
  const dd = fmt2(d.getDate());
  const hh = fmt2(d.getHours());
  const mi = fmt2(d.getMinutes());
  const ss = fmt2(d.getSeconds());
  return `${yyyy}-${mm}-${dd}-${hh}-${mi}-${ss}`;
}

function normalizeSlash(p: string): string {
  return p.replaceAll("\\", "/").replaceAll(/\/+/g, "/");
}

function stripOneExt(p: string): string {
  const ext = extname(p);
  return ext ? p.slice(0, -ext.length) : p;
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

function relFromRoots(fileAbs: string, rootsAbs: readonly string[]): string {
  const root = bestRootForFile(fileAbs, rootsAbs);
  if (!root) return basename(fileAbs);

  let rel = relative(root, fileAbs);
  rel = normalizeSlash(rel).replaceAll(/^\.\//g, "");

  // Defensive guard: if rel still includes the root dir name (your reported symptom),
  // strip that segment. Example: "cargo.d/controls/x.db" -> "controls/x.db".
  const rootName = basename(root);
  const prefix = `${rootName}/`;
  if (rel.startsWith(prefix)) rel = rel.slice(prefix.length);

  if (!rel || rel.startsWith("..")) return basename(fileAbs);
  return rel;
}

function relDirFromRoots(fileAbs: string, rootsAbs: readonly string[]): string {
  const rel = relFromRoots(fileAbs, rootsAbs);
  const d = dirname(rel);
  if (d === "." || d === "/" || d.trim() === "") return "";
  return normalizeSlash(d).replaceAll(/\/+$/g, "");
}

function proxyPrefixFromRel(relFromRoot: string): string {
  const relNoExt = stripOneExt(relFromRoot);
  const clean = normalizeSlash(relNoExt).replaceAll(/^\.\//g, "").trim();
  if (!clean) return "/";
  return `/${clean.startsWith("/") ? clean.slice(1) : clean}`.replaceAll(
    /\/+/g,
    "/",
  );
}

export async function materialize(
  srcPaths: Iterable<Path>,
  opts: MaterializeOptions,
): Promise<MaterializeResult> {
  const src = Array.from(srcPaths);

  // Canonicalize roots so prefix checks work even with symlinks.
  const rootsAbs = src.map((p) => Deno.realPathSync(resolve(p.path)));

  const spawnStateHome = resolve(opts.spawnStateHome);
  await ensureDir(spawnStateHome);

  const sessionHome = join(spawnStateHome, sessionStamp());
  await ensureDir(sessionHome);

  const onEvent: SpawnEventListener | undefined = opts.verbose === false
    ? undefined
    : richTextUISpawnEvents(opts.verbose);

  const spawned: SpawnedContext[] = [];

  const spawnStatePath = (
    entry: ExposableService,
    nature: "context" | "stdout" | "stderr",
  ): string | undefined => {
    // Canonicalize discovered file too.
    const fileAbs = Deno.realPathSync(resolve(entry.supplier.location));

    const relFromRoot = relFromRoots(fileAbs, rootsAbs); // should NOT include cargo.d
    const relDir = relDirFromRoots(fileAbs, rootsAbs);

    const outDir = relDir ? join(sessionHome, relDir) : sessionHome;
    const fileName = basename(relFromRoot);

    if (nature === "context") return join(outDir, `${fileName}.context.json`);
    if (nature === "stdout") return join(outDir, `${fileName}.stdout.log`);
    if (nature === "stderr") return join(outDir, `${fileName}.stderr.log`);
    return undefined;
  };

  const expose = (entry: ExposableService, _candidate: string) => {
    const fileAbs = Deno.realPathSync(resolve(entry.supplier.location));
    const relFromRoot = relFromRoots(fileAbs, rootsAbs);
    const proxyEndpointPrefix = proxyPrefixFromRel(relFromRoot);

    return { proxyEndpointPrefix, exposableServiceConf: {} } as const;
  };

  const gen = spawn(src, expose, spawnStatePath, {
    onEvent,
    probe: { enabled: false },
  });

  while (true) {
    const next = await gen.next();
    if (next.done) {
      return {
        sessionHome,
        summary: next.value as SpawnSummary,
        spawned,
      };
    }
    spawned.push(next.value);
  }
}

export type SpawnedStateEncounter = Readonly<{
  filePath: string;
  context: SpawnedContext;
  pid: number;
  pidAlive: boolean;
  procCmdline?: string;
  upstreamUrl: string;
}>;

function normalizePathForUrl(path: string): string {
  const p = path.replaceAll("\\", "/").trim();
  if (!p) return "/";
  if (!p.startsWith("/")) return `/${p}`;
  return p;
}

function joinUrl(baseUrl: string, path: string): string {
  const b = baseUrl.replace(/\/+$/, "");
  const p = normalizePathForUrl(path);
  return `${b}${p}`;
}

export async function* spawnedStates(spawnStateHome: string) {
  const gen = encounters(
    [{ path: spawnStateHome, globs: ["**/*.json"] }],
    fileSystemSource({}, (e) => Deno.readTextFile(e.path)),
    async ({ entry, content }) => {
      const filePath = entry.path;
      if (!filePath.endsWith(".context.json")) return null;

      const text = String(await content());
      const ctx = JSON.parse(text) as SpawnedContext;

      const pid = Number(ctx?.spawned?.pid);
      if (!Number.isFinite(pid) || pid <= 0) {
        throw new Error(`Invalid pid in context file: ${filePath}`);
      }

      const pidAlive = isPidAlive(pid);
      const procCmdline = pidAlive ? await readProcCmdline(pid) : undefined;

      const upstreamUrl = joinUrl(
        ctx.listen.baseUrl,
        ctx.service.proxyEndpointPrefix === ""
          ? "/"
          : ctx.service.proxyEndpointPrefix,
      );

      return {
        filePath,
        context: ctx,
        pid,
        pidAlive,
        procCmdline,
        upstreamUrl,
      };
    },
  );

  while (true) {
    const next = await gen.next();
    if (next.done) return next.value;
    if (next.value != null) yield next.value;
  }
}

export async function killSpawnedStates(
  spawnStateHome: string,
): Promise<void> {
  for await (const state of spawnedStates(spawnStateHome)) {
    const { pid, pidAlive } = state;
    if (pidAlive) {
      await killPID(pid);
    }
  }
}

function isPidAlive(pid: number): boolean {
  try {
    // On Unix, signal 0 checks existence/permission without sending a signal.
    Deno.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

export async function killPID(pid: number) {
  // Platform-specific: on POSIX we try process-group kill first (negative pid).
  const killGroup = () => {
    try {
      Deno.kill(-pid, "SIGTERM");
      return true;
    } catch {
      return false;
    }
  };

  const killSingle = (sig: Deno.Signal) => {
    try {
      Deno.kill(pid, sig);
      return true;
    } catch {
      return false;
    }
  };

  const triedGroup = Deno.build.os !== "windows" ? killGroup() : false;
  if (!triedGroup) {
    if (!killSingle("SIGTERM")) return;
  }

  for (let i = 0; i < 20; i++) {
    if (!isPidAlive(pid)) return;
    await new Promise((r) => setTimeout(r, 100));
  }

  if (Deno.build.os !== "windows") {
    try {
      Deno.kill(-pid, "SIGKILL");
      return;
    } catch {
      // fall back
    }
  }
  killSingle("SIGKILL");
}

async function readProcCmdline(pid: number): Promise<string | undefined> {
  // Linux-only; return undefined elsewhere or if missing.
  const path = `/proc/${pid}/cmdline`;
  try {
    const bytes = await Deno.readFile(path);
    const raw = new TextDecoder().decode(bytes);
    const cleaned = raw.replaceAll("\u0000", " ").trim();
    return cleaned.length ? cleaned : undefined;
  } catch {
    return undefined;
  }
}
