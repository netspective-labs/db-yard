#!/usr/bin/env -S deno run -A --node-modules-dir=auto
import { Command } from "@cliffy/command";
import { CompletionsCommand } from "@cliffy/completions";
import { HelpCommand } from "@cliffy/help";
import { brightGreen, brightRed, dim, yellow } from "@std/fmt/colors";
import { dirname } from "@std/path/dirname";
import { startAdminServer } from "../lib/admin.ts";
import {
  ensureDir,
  isPidAlive,
  parseListenHost,
  toPositiveInt,
  toSqlpageEnv,
} from "../lib/governance.ts";
import { startOrchestrator, stopByPid } from "../lib/orchestrate.ts";
import { expandGlob } from "@std/fs";

async function readProcCmdline(pid: number): Promise<string | undefined> {
  // Linux-first best-effort
  const p = `/proc/${pid}/cmdline`;
  try {
    const raw = await Deno.readFile(p);
    const s = new TextDecoder().decode(raw);
    const parts = s.split("\0").filter((x) => x.length);
    if (!parts.length) return undefined;
    return parts.join(" ");
  } catch {
    return undefined;
  }
}

async function listSpawnedPidFiles(
  spawnedStatePath: string,
): Promise<string[]> {
  const out: string[] = [];
  try {
    for await (const e of Deno.readDir(spawnedStatePath)) {
      if (!e.isDirectory) continue;
      const p = `${spawnedStatePath}/${e.name}/spawned-pids.txt`;
      try {
        const st = await Deno.stat(p);
        if (st.isFile) out.push(p);
      } catch {
        // ignore
      }
    }
  } catch {
    // ignore
  }
  return out;
}

async function readPidsFromFile(path: string): Promise<number[]> {
  try {
    const raw = (await Deno.readTextFile(path)).trim();
    if (!raw) return [];
    const nums = raw.split(/\s+/)
      .map((x) => Number(x))
      .filter((n) => Number.isFinite(n) && n > 0)
      .map((n) => Math.floor(n));
    return nums;
  } catch {
    return [];
  }
}
type CliOptions = {
  watch?: string[];
  spawnedStatePath?: string;

  spawnedCtxExec: string;
  spawnedCtx?: string[];
  sqlpageEnv: string;
  sqlpageBin: string;
  surveilrBin: string;

  reconcileMs: number;
  listen: string;

  adoptForeignState: boolean;

  adminPort?: number;
  adminHost: string;

  verbose: boolean;
};

const defaultWatch = `./cargo.d/**/*.db`;
const defaultSpawned = `./spawned.d`;

function normalizeSlash(p: string) {
  return p.replaceAll("\\", "/");
}

function isAbsPath(p: string) {
  const s = normalizeSlash(p);
  if (s.startsWith("/")) return true;
  if (/^[A-Za-z]:\//.test(s)) return true;
  if (s.startsWith("//")) return true;
  return false;
}

function resolvePath(p: string) {
  const s = normalizeSlash(p.trim());
  return isAbsPath(s) ? s : normalizeSlash(`${Deno.cwd()}/${s}`);
}

function resolveGlob(g: string) {
  const s = normalizeSlash(g.trim());
  return isAbsPath(s) ? s : normalizeSlash(`${Deno.cwd()}/${s}`);
}

function pad2(n: number) {
  return String(n).padStart(2, "0");
}

function sessionStamp(d = new Date()): string {
  // yyyy-mm-dd-hh-mi-ss (local time)
  const yyyy = d.getFullYear();
  const mm = pad2(d.getMonth() + 1);
  const dd = pad2(d.getDate());
  const hh = pad2(d.getHours());
  const mi = pad2(d.getMinutes());
  const ss = pad2(d.getSeconds());
  return `${yyyy}-${mm}-${dd}-${hh}-${mi}-${ss}`;
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
    .option("--admin-port <port:number>", "Optional admin HTTP server port", {
      required: false,
    })
    .option("--admin-host <host:string>", "Admin host (default: 127.0.0.1)", {
      default: "127.0.0.1",
    })
    .option("--verbose", "Verbose pretty logging (color)", { default: false })
    .action(async (options: CliOptions) => {
      const watchGlobs =
        (options.watch?.length ? options.watch : [defaultWatch])
          .map(resolveGlob);

      // Do NOT derive roots from glob syntax.
      // Derive roots only from real filesystem matches.
      const watchRootsSet = new Set<string>();

      for (const g of watchGlobs) {
        for await (const e of expandGlob(g, { globstar: true })) {
          if (!e.isFile) continue;
          watchRootsSet.add(normalizeSlash(dirname(e.path)));
        }
      }

      // Fallback: if no matches yet, use cwd (do NOT mkdir)
      if (watchRootsSet.size === 0) {
        watchRootsSet.add(normalizeSlash(Deno.cwd()));
      }

      const watchRoots = [...watchRootsSet];

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
        reconcileMs: toPositiveInt(options.reconcileMs, 3000),
        sqlpageEnv: toSqlpageEnv(options.sqlpageEnv),
        sqlpageBin: options.sqlpageBin,
        surveilrBin: options.surveilrBin,
        spawnedCtxExec: options.spawnedCtxExec,
        spawnedCtxSqls: options.spawnedCtx ?? [],
        adoptForeignState: !!options.adoptForeignState,
        verbose: !!options.verbose,
      });

      const adminPort = options.adminPort;
      if (
        typeof adminPort === "number" && Number.isFinite(adminPort) &&
        adminPort > 0
      ) {
        startAdminServer({
          adminHost: parseListenHost(options.adminHost || "127.0.0.1"),
          adminPort: Math.floor(adminPort),
          spawnedDir: sessionDir,
          sqliteExec: options.spawnedCtxExec,
          getRunning: () => [...orch.runningByDb.values()],
        });
      }

      // bin/yard.ts (only the relevant part)
      console.log(
        `db-yard session started\n  state: ${sessionDir}\n  json:  ${sessionDir}/*.json\n  logs: ${sessionDir}/*.stdout.log, *.stderr.log`,
      );
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
            `${yellow(String(pid))} ${dim("(skipping self)")} sources=${
              pidToSources.get(pid)?.length ?? 0
            }`,
          );
          continue;
        }

        const alive = isPidAlive(pid);
        const cmdline = await readProcCmdline(pid);

        if (!kill) {
          const status = alive ? brightGreen("alive") : brightRed("dead");
          const sources = pidToSources.get(pid) ?? [];
          const srcHint = sources.length ? ` sources=${sources.length}` : "";
          const cmdHint = cmdline ? ` ${dim(cmdline)}` : "";
          console.log(`${pid} ${status}${srcHint}${cmdHint}`);
          continue;
        }

        if (!alive) {
          console.log(`${pid} ${brightRed("dead")} ${dim("(skip kill)")}`);
          continue;
        }

        await stopByPid(pid);
        const after = isPidAlive(pid);
        console.log(
          `${pid} ${after ? brightRed("still-alive") : brightGreen("killed")}${
            cmdline ? ` ${dim(cmdline)}` : ""
          }`,
        );
      }
    })
    .command("help", new HelpCommand())
    .command("completions", new CompletionsCommand())
    .parse(Deno.args);
}
