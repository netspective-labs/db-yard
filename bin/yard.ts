#!/usr/bin/env -S deno run -A --node-modules-dir=auto
// bin/yard.ts
import { Command } from "@cliffy/command";
import { CompletionsCommand } from "@cliffy/completions";
import { HelpCommand } from "@cliffy/help";

import { startWebUiServer } from "../lib/web-ui.ts";
import {
  ensureDir,
  isPidAlive,
  listSpawnedPidFiles,
  normalizeSlash,
  parseListenHost,
  readPidsFromFile,
  readProcCmdline,
  resolveGlob,
  resolvePath,
  sessionStamp,
} from "../lib/fs.ts";
import { SqlpageEnv } from "../lib/governance.ts";
import { startOrchestrator, stopByPid } from "../lib/orchestrate.ts";
import {
  formatKillResult,
  formatKillSkipDead,
  formatPidSkipSelf,
  formatPidStatusLine,
} from "../lib/text-ui.ts";
import { deriveWatchRootsFromGlobs } from "../lib/watch.ts";

export function toSqlpageEnv(s: string): SqlpageEnv {
  return s === "development" ? "development" : "production";
}

type CliOptions = {
  watch?: string[];
  spawnedStatePath?: string;

  // allowlist regex for inherited env var NAMES (repeatable)
  // if not provided, spawned processes inherit all from yard.ts
  env?: string[];

  spawnedCtxExec: string;
  spawnedCtx?: string[];
  sqlpageEnv: string;
  sqlpageBin: string;
  surveilrBin: string;

  reconcileMs: number;
  listen: string;

  adoptForeignState: boolean;

  webUiPort?: number;
  webUiHost: string;

  verbose: boolean;
  killAllOnExit: boolean;
};

const defaultWatch = `./cargo.d/**/*.db`;
const defaultSpawned = `./spawned.d`;

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

async function killAllOwnedSessions(args: {
  spawnedStatePath: string;
  verbose: boolean;
}) {
  const sessionDirs = await listSessionDirs(args.spawnedStatePath);
  if (!sessionDirs.length) return;

  const pidToSources = new Map<number, string[]>();

  for (const sessionDir of sessionDirs) {
    if (!(await isOwnedSessionDir(sessionDir))) continue;

    const pidFile = `${sessionDir}/spawned-pids.txt`;
    const pids = await readPidsFromFile(pidFile);
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

if (import.meta.main) {
  await new Command()
    .name("yard.ts")
    .description("File-driven process yard for SQLite DB cargo.")
    .example(
      "Watch all SQLite DBs under cargo.d (default behavior)",
      "yard.ts",
    )
    .example(
      "Watch with explicit glob (recommended)",
      "yard.ts --watch './cargo.d/**/*.db'",
    )
    .example(
      "Run with verbose colored output",
      "yard.ts --watch './cargo.d/**/*.db' --verbose",
    )
    .example(
      "Use a custom spawned state directory",
      "yard.ts --watch './cargo.d/**/*.db' --spawned-state-path ./spawned.d",
    )
    .example(
      "Enable admin server",
      "yard.ts --admin-port 9090 --admin-host 127.0.0.1",
    )
    .option(
      "--watch <glob:string>",
      "Watch glob(s) (repeatable). Example: ./cargo.d/**/*.db",
      { collect: true },
    )
    .option(
      "--spawned-state-path <dir:string>",
      "Directory for spawned state JSON files (a session subdir is created per run)",
      { default: defaultSpawned },
    )
    .option(
      "--env <re:string>",
      "Allowlist env var name regex (repeatable). If set, only matching env vars are inherited by spawned processes.",
      { collect: true },
    )
    .option(
      "--spawned-ctx <sql:string>",
      "Optional SQL query to run against DB; output stored in JSON (repeatable)",
      { collect: true },
    )
    .option(
      "--spawned-ctx-exec <exec:string>",
      "SQLite CLI used to query DB configuration/context",
      { default: "sqlite3" },
    )
    .option(
      "--sqlpage-env <env:string>",
      "SQLPAGE_ENVIRONMENT: production|development",
      { default: "production" },
    )
    .option("--sqlpage-bin <path:string>", "sqlpage executable", {
      default: "sqlpage",
    })
    .option("--surveilr-bin <path:string>", "surveilr executable", {
      default: "surveilr",
    })
    .option(
      "--reconcile-ms <ms:number>",
      "Periodic reconciliation interval ms",
      { default: 3000 },
    )
    .option("--listen <host:string>", "Listener host for spawned services", {
      default: "127.0.0.1",
    })
    .option(
      "--adopt-foreign-state",
      "Adopt existing state owned by another yard token (unsafe)",
      { default: false },
    )
    .option("--web-ui-port <port:number>", "Optional admin HTTP server port", {
      required: false,
    })
    .option("--web-ui-host <host:string>", "Admin host (default: 127.0.0.1)", {
      default: "127.0.0.1",
    })
    .option("--verbose", "Verbose pretty logging (color)", { default: false })
    .option(
      "--kill-all-on-exit",
      "On exit, kill all spawned PIDs across all sessions that appear to be owned by db-yard",
      { default: false },
    )
    .action(async (options: CliOptions) => {
      const watchGlobs =
        (options.watch?.length ? options.watch : [defaultWatch])
          .map(resolveGlob);

      const watchRoots = await deriveWatchRootsFromGlobs(watchGlobs);

      const spawnedBase = resolvePath(
        options.spawnedStatePath ?? defaultSpawned,
      );
      await ensureDir(spawnedBase);

      const sessionDir = normalizeSlash(`${spawnedBase}/${sessionStamp()}`);
      await ensureDir(sessionDir);

      const orch = await startOrchestrator({
        watchGlobs,
        watchRoots,
        spawnedDir: sessionDir,
        listenHost: parseListenHost(options.listen),
        reconcileMs:
          Number.isFinite(options.reconcileMs) && options.reconcileMs > 0
            ? Math.floor(options.reconcileMs)
            : 3000,
        sqlpageEnv: toSqlpageEnv(options.sqlpageEnv),
        sqlpageBin: options.sqlpageBin,
        surveilrBin: options.surveilrBin,
        spawnedCtxExec: options.spawnedCtxExec,
        spawnedCtxSqls: options.spawnedCtx ?? [],
        adoptForeignState: !!options.adoptForeignState,
        inheritEnvRegex: options.env ?? [],
        verbose: !!options.verbose,
      });

      // bin/yard.ts (where you currently start the admin server, swap to web-ui server)
      const webUiPort = options.webUiPort;
      if (
        typeof webUiPort === "number" && Number.isFinite(webUiPort) &&
        webUiPort > 0
      ) {
        startWebUiServer({
          webHost: parseListenHost(options.webUiHost || "127.0.0.1"),
          webPort: Math.floor(webUiPort),
          spawnedDir: sessionDir,
          sqliteExec: options.spawnedCtxExec,
          getRunning: () => [...orch.runningByDb.values()],
        });
      }

      console.log(
        `db-yard session started\n  state: ${sessionDir}\n  json:  ${sessionDir}/*.json\n  logs: ${sessionDir}/*.stdout.log, *.stderr.log`,
      );

      // bin/yard.ts (inside .action(...) after orch/start + optional web-ui start, before the final console.log)

      const spawnedRootForExitKill = spawnedBase; // root containing session dirs

      let exitKillRan = false;
      const runExitKillOnce = async () => {
        if (exitKillRan) return;
        exitKillRan = true;

        try {
          orch.close();
        } catch {
          // ignore
        }

        if (options.killAllOnExit) {
          await killAllOwnedSessions({
            spawnedStatePath: spawnedRootForExitKill,
            verbose: !!options.verbose,
          });
        }
      };

      // handle Ctrl+C / SIGTERM
      try {
        Deno.addSignalListener("SIGINT", () => {
          runExitKillOnce().finally(() => Deno.exit(130));
        });
        Deno.addSignalListener("SIGTERM", () => {
          runExitKillOnce().finally(() => Deno.exit(143));
        });
      } catch {
        // signals may be unavailable on some platforms
      }

      // also run on normal unload (best-effort)
      globalThis.addEventListener("unload", () => {
        // no await allowed here; best-effort fire-and-forget
        void runExitKillOnce();
      });
    })
    .command("spawned", "Inspect (and optionally kill) spawned processes")
    .example("List all managed processes across sessions", "yard.ts spawned")
    .example(
      "List processes from a specific spawned state directory",
      "yard.ts spawned --spawned-state-path ./spawned.d",
    )
    .example(
      "Kill all managed processes (dangerous)",
      "yard.ts spawned --kill",
    )
    .option(
      "--spawned-state-path <dir:string>",
      "Root directory containing session dirs (each with spawned-pids.txt)",
      { default: defaultSpawned },
    )
    .option(
      "--kill",
      "Kill all PIDs found across all session spawned-pids.txt files",
      { default: false },
    )
    .action(async (options) => {
      const spawnedStatePath = options.spawnedStatePath
        ? normalizeSlash(Deno.realPathSync(options.spawnedStatePath))
        : normalizeSlash(Deno.realPathSync(defaultSpawned));

      const pidFiles = await listSpawnedPidFiles(spawnedStatePath);
      if (!pidFiles.length) {
        console.log(
          `No session spawned-pids.txt files found under: ${spawnedStatePath}`,
        );
        return;
      }

      const pidToSources = new Map<number, string[]>();
      for (const f of pidFiles) {
        const pids = await readPidsFromFile(f);
        for (const pid of pids) {
          const arr = pidToSources.get(pid) ?? [];
          arr.push(f);
          pidToSources.set(pid, arr);
        }
      }

      const uniquePids = [...pidToSources.keys()].sort((a, b) => a - b);
      if (!uniquePids.length) {
        console.log(`No PIDs found in: ${spawnedStatePath}`);
        return;
      }

      const kill = !!options.kill;
      if (kill) {
        console.log(
          `Killing ${uniquePids.length} PID(s) discovered under: ${spawnedStatePath}`,
        );
      } else {
        console.log(
          `Found ${uniquePids.length} unique PID(s) under: ${spawnedStatePath}`,
        );
      }

      for (const pid of uniquePids) {
        if (pid === Deno.pid) {
          console.log(
            formatPidSkipSelf({
              pid,
              sourcesCount: pidToSources.get(pid)?.length ?? 0,
            }),
          );
          continue;
        }

        const alive = isPidAlive(pid);
        const cmdline = await readProcCmdline(pid);

        if (!kill) {
          console.log(
            formatPidStatusLine({
              pid,
              alive,
              sourcesCount: pidToSources.get(pid)?.length ?? 0,
              cmdline,
            }),
          );
          continue;
        }

        if (!alive) {
          console.log(formatKillSkipDead(pid));
          continue;
        }

        await stopByPid(pid);
        const after = isPidAlive(pid);
        console.log(
          formatKillResult({ pid, stillAlive: after, cmdline }),
        );
      }
    })
    .command("help", new HelpCommand())
    .command("completions", new CompletionsCommand())
    .parse(Deno.args);
}
