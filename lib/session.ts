import { isPidAlive, normalizeSlash, pidsFromFile } from "./fs.ts";
import { SpawnedProcess } from "./governance.ts";
import { stopByPid } from "./orchestrate.ts";

async function listSessionDirs(spawnedStatePath: string): Promise<string[]> {
  const out: string[] = [];
  try {
    for await (const e of Deno.readDir(spawnedStatePath)) {
      if (!e.isDirectory) continue;
      out.push(`${spawnedStatePath}/${e.name}`);
    }
  } catch {
    // ignore
  }
  out.sort();
  return out;
}

async function isOwnedSessionDir(sessionDir: string): Promise<boolean> {
  // “owned by db-yard” heuristic: presence of the owner token file
  const p = `${sessionDir}/.db-yard.owner-token`;
  try {
    const st = await Deno.stat(p);
    return st.isFile;
  } catch {
    return false;
  }
}

export async function killAllOwnedSessions(args: {
  spawnedStatePath: string;
  verbose: boolean;
}) {
  const sessionDirs = await listSessionDirs(args.spawnedStatePath);
  if (!sessionDirs.length) return;

  const pidToSources = new Map<number, string[]>();

  for (const sessionDir of sessionDirs) {
    if (!(await isOwnedSessionDir(sessionDir))) continue;

    const pidFile = `${sessionDir}/spawned-pids.txt`;
    const pids = await pidsFromFile(pidFile);
    for (const pid of pids) {
      const arr = pidToSources.get(pid) ?? [];
      arr.push(pidFile);
      pidToSources.set(pid, arr);
    }
  }

  const uniquePids = [...pidToSources.keys()].sort((a, b) => a - b);
  if (!uniquePids.length) return;

  if (args.verbose) {
    console.log(
      `[exit] kill-all-on-exit: killing ${uniquePids.length} PID(s) from owned sessions under ${args.spawnedStatePath}`,
    );
  }

  for (const pid of uniquePids) {
    if (pid === Deno.pid) continue;

    const alive = isPidAlive(pid);
    if (!alive) continue;

    await stopByPid(pid);
  }
}

/**
 * Scan a spawned-state root directory, read all v1 SpawnedRecord JSON files,
 * and return only those whose PID is still alive.
 */
export async function liveSpawnedRecords(
  root: string,
): Promise<SpawnedProcess[]> {
  const out: SpawnedProcess[] = [];
  const rootN = normalizeSlash(root);

  async function walk(dir: string) {
    let it: AsyncIterable<Deno.DirEntry>;
    try {
      it = Deno.readDir(dir);
    } catch {
      return;
    }

    for await (const e of it) {
      const p = normalizeSlash(`${dir}/${e.name}`);

      if (e.isDirectory) {
        await walk(p);
        continue;
      }

      if (!e.isFile || !p.endsWith(".json")) continue;

      let rec: SpawnedProcess | undefined;
      try {
        const raw = await Deno.readTextFile(p);
        const obj = JSON.parse(raw);
        if (obj && obj.version === 1) rec = obj as SpawnedProcess;
      } catch {
        rec = undefined;
      }

      if (!rec?.pid || !rec.id) continue;
      if (!isPidAlive(rec.pid)) continue;

      out.push(rec);
    }
  }

  await walk(rootN);

  out.sort((a, b) => a.id.localeCompare(b.id));
  return out;
}
