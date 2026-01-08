// lib/materialize.ts
import { ensureDir } from "@std/fs";
import { resolve } from "@std/path";
import type { Path } from "./discover.ts";
import { encounters, fileSystemSource } from "./discover.ts";
import type { ExposableService } from "./exposable.ts";
import { richTextUISpawnEvents } from "./spawn-event.ts";
import {
  isPidAlive,
  killPID,
  readProcCmdline,
  spawn,
  type SpawnedContext,
  type SpawnEventListener,
  type SpawnSummary,
} from "./spawn.ts";
import { createSpawnSessionHome, resolveRootsAbs } from "./session.ts";
import { joinUrl } from "./path.ts";

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

export async function materialize(
  srcPaths: Iterable<Path>,
  opts: MaterializeOptions,
): Promise<MaterializeResult> {
  const src = Array.from(srcPaths);

  const rootsAbs = resolveRootsAbs(src);

  const spawnStateHome = resolve(opts.spawnStateHome);
  await ensureDir(spawnStateHome);

  const session = await createSpawnSessionHome(spawnStateHome);

  const onEvent: SpawnEventListener | undefined = opts.verbose === false
    ? undefined
    : richTextUISpawnEvents(opts.verbose);

  const spawned: SpawnedContext[] = [];

  const { spawnStatePathForEntry } = await import("./session.ts");
  const spawnStatePath = (
    entry: ExposableService,
    nature: "context" | "stdout" | "stderr",
  ): string | undefined =>
    spawnStatePathForEntry(entry, nature, {
      sessionHome: session.sessionHome,
      rootsAbs,
    });

  const { relFromRoots } = await import("./session.ts");
  const { proxyPrefixFromRel } = await import("./path.ts");
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
        sessionHome: session.sessionHome,
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

export async function* spawnedStates(spawnStateHomeOrSessionHome: string) {
  const gen = encounters(
    [{ path: spawnStateHomeOrSessionHome, globs: ["**/*.json"] }],
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
  spawnStateHomeOrSessionHome: string,
): Promise<void> {
  for await (const state of spawnedStates(spawnStateHomeOrSessionHome)) {
    const { pid, pidAlive } = state;
    if (pidAlive) await killPID(pid);
  }
}
