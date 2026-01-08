#!/usr/bin/env -S deno run -A --node-modules-dir=auto
// bin/yard.ts
import { Command, EnumType } from "@cliffy/command";
import { CompletionsCommand } from "@cliffy/completions";
import { HelpCommand } from "@cliffy/help";
import { cyan, dim, green, red, yellow } from "@std/fmt/colors";
import {
  killSpawnedStates,
  materialize,
  spawnedStates,
} from "../lib/materialize.ts";
import {
  generateReverseProxyConfsFromSpawnedStates,
} from "../lib/reverse-proxy-conf.ts";
import { type WatchEvent, watchYard } from "../lib/serve/watch.ts";
import { richTextUISpawnEvents } from "../lib/spawn-event.ts";

export async function lsSpawnedStates(
  spawnStateHome: string,
): Promise<void> {
  for await (const state of spawnedStates(spawnStateHome)) {
    const { pid, pidAlive, upstreamUrl, context } = state;

    const kind = context.service.kind;
    const nature = context.supplier.nature;

    const statusIcon = pidAlive ? "🟢" : "🔴";
    const pidLabel = pidAlive ? green(String(pid)) : red(`${pid} (dead)`);

    const kindLabel = cyan(kind);
    const natureLabel = dim(nature);

    const urlLabel = pidAlive ? yellow(upstreamUrl) : dim(upstreamUrl);

    console.log(
      `${statusIcon} [${pidLabel}] ${urlLabel} ` +
        `${dim("(")}${kindLabel}${dim("/")}${natureLabel}${dim(")")}`,
    );
  }
}

const verboseType = new EnumType(["essential", "comprehensive"] as const);
const proxyType = new EnumType(["nginx", "traefik", "both"] as const);

// Defaults (overridable per command)
const defaultCargoHome = "./cargo.d";
const defaultSpawnStateHome = "./spawned.d";

function printWatchEvent(e: WatchEvent) {
  if (e.type === "watch_start") {
    console.log(
      `${green("watch")} roots=${dim(e.roots.join(", "))} active=${
        dim(e.activeDir)
      }`,
    );
    return;
  }
  if (e.type === "fs_event") {
    console.log(`${dim("fs")} ${e.kind} ${dim(e.paths.join(", "))}`);
    return;
  }
  if (e.type === "reconcile_start") {
    console.log(`${yellow("reconcile")} start ${dim(`(${e.reason})`)}`);
    return;
  }
  if (e.type === "reconcile_end") {
    console.log(
      `${yellow("reconcile")} end ${dim(`(${e.reason})`)} ` +
        `discovered=${e.discovered} ledger=${e.ledger} ` +
        `killed=${e.killed} spawned=${e.spawned} ${
          dim(`${Math.round(e.durationMs)}ms`)
        }`,
    );
    return;
  }
  if (e.type === "killed") {
    console.log(
      `${red("killed")} ${e.serviceId} pid=${e.pid} ${dim(e.reason)}`,
    );
    return;
  }
  if (e.type === "error") {
    console.log(`${red("error")} ${dim(e.phase)} ${String(e.error)}`);
    return;
  }
  if (e.type === "watch_end") {
    console.log(`${green("watch")} end ${dim(`(${e.reason})`)}`);
    return;
  }
}

await new Command()
  .name("yard.ts")
  .description("File-driven process yard for SQLite DB cargo.")
  .example(
    `Start all exposable databases in ${defaultCargoHome}`,
    "yard.ts start",
  )
  .example(
    `Start with essential verbosity`,
    "yard.ts start --verbose essential",
  )
  .example(
    `List all managed processes in ${defaultSpawnStateHome}`,
    "yard.ts ls",
  )
  .example(
    `Stop (kill) all managed processes in ${defaultSpawnStateHome}`,
    "yard.ts kill",
  )
  .example(
    `Continuously watch ${defaultCargoHome} and keep services in sync`,
    "yard.ts watch",
  )
  .command(
    "start",
    `Start exposable databases (default root ${defaultCargoHome}) and exit`,
  )
  .type("verbose", verboseType)
  .option(
    "--cargo-home <dir:string>",
    `Cargo root directory (default ${defaultCargoHome})`,
    { default: defaultCargoHome },
  )
  .option(
    "--spawn-state-home <dir:string>",
    `Spawn state home (default ${defaultSpawnStateHome})`,
    { default: defaultSpawnStateHome },
  )
  .option("--verbose <level:verbose>", "Spawn/materialize verbosity")
  .option("--summarize", "Summarize after spawning")
  .option("--no-ls", "Don't list after spawning")
  .action(async ({ summarize, verbose, ls, cargoHome, spawnStateHome }) => {
    const result = await materialize([{ path: cargoHome }], {
      verbose: verbose ? verbose : false,
      spawnStateHome,
    });

    if (summarize) {
      console.log(`sessionHome: ${result.sessionHome}`);
      console.log("summary:", result.summary);
    }

    if (ls) {
      await lsSpawnedStates(spawnStateHome);
    }
  })
  .command(
    "watch [roots...:string]",
    `Watch roots (default ${defaultCargoHome}) and keep services in sync (ledger in spawn-state-home/active)`,
  )
  .type("verbose", verboseType)
  .option(
    "--cargo-home <dir:string>",
    `Cargo root directory (default ${defaultCargoHome})`,
    { default: defaultCargoHome },
  )
  .option(
    "--spawn-state-home <dir:string>",
    `Spawn state home (default ${defaultSpawnStateHome})`,
    { default: defaultSpawnStateHome },
  )
  .option(
    "--active-dir-name <name:string>",
    "Stable active session dir name (default 'active')",
    { default: "active" },
  )
  .option(
    "--debounce-ms <ms:number>",
    "Debounce filesystem events before reconciling (default 250)",
    { default: 250 },
  )
  .option(
    "--reconcile-every-ms <ms:number>",
    "Optional periodic full reconcile (0 disables)",
    { default: 0 },
  )
  .option("--watch-verbose", "Print high-level watch events to stdout")
  .option(
    "--spawn-events <level:verbose>",
    "Emit spawn() rich UI events (essential|comprehensive)",
  )
  .option("--listen-host <host:string>", "Listen host (default 127.0.0.1)")
  .option("--port-start <port:number>", "Starting port (default 3000)")
  .option("--sqlpage-bin <bin:string>", "sqlpage binary (default 'sqlpage')")
  .option(
    "--sqlpage-env <env:string>",
    "SQLPAGE_ENVIRONMENT value (default 'development')",
  )
  .option("--surveilr-bin <bin:string>", "surveilr binary (default 'surveilr')")
  .action(async (o, ...roots: string[]) => {
    const srcRoots: string[] = (roots.length > 0) ? roots : [o.cargoHome];

    const ac = new AbortController();
    const stop = () => {
      try {
        ac.abort();
      } catch {
        // ignore
      }
    };

    try {
      Deno.addSignalListener("SIGINT", stop);
    } catch {
      // ignore
    }
    try {
      Deno.addSignalListener("SIGTERM", stop);
    } catch {
      // ignore
    }

    const onWatchEvent = o.watchVerbose
      ? (e: WatchEvent) => printWatchEvent(e)
      : undefined;

    const onSpawnEvent = o.spawnEvents
      ? richTextUISpawnEvents(o.spawnEvents)
      : undefined;

    try {
      await watchYard(srcRoots.map((p: string) => ({ path: p })), {
        spawnStateHome: o.spawnStateHome,
        activeDirName: o.activeDirName,
        debounceMs: o.debounceMs,
        reconcileEveryMs: o.reconcileEveryMs > 0
          ? o.reconcileEveryMs
          : undefined,
        signal: ac.signal,
        onWatchEvent,
        onSpawnEvent,
        spawn: {
          listenHost: o.listenHost,
          portStart: o.portStart,
          sqlpageBin: o.sqlpageBin,
          sqlpageEnv: o.sqlpageEnv,
          surveilrBin: o.surveilrBin,
        },
      });
    } finally {
      try {
        Deno.removeSignalListener("SIGINT", stop);
      } catch {
        // ignore
      }
      try {
        Deno.removeSignalListener("SIGTERM", stop);
      } catch {
        // ignore
      }
    }
  })
  .command("ls", `List managed processes (default ${defaultSpawnStateHome})`)
  .option(
    "--spawn-state-home <dir:string>",
    `Spawn state home (default ${defaultSpawnStateHome})`,
    { default: defaultSpawnStateHome },
  )
  .action(async ({ spawnStateHome }) => {
    await lsSpawnedStates(spawnStateHome);
  })
  .command(
    "proxy-conf",
    `NGINX, Traefik, etc. proxy configs from upstream URLs in spawn-state home`,
  )
  .type("proxy", proxyType)
  .option(
    "--spawn-state-home <dir:string>",
    `Spawn state home (default ${defaultSpawnStateHome})`,
    { default: defaultSpawnStateHome },
  )
  .option("--type <type:proxy>", "Which config(s) to generate", {
    default: "nginx",
  })
  .option("--nginx-out <dir:string>", "Write nginx confs into this dir")
  .option("--traefik-out <dir:string>", "Write traefik confs into this dir")
  .option("--include-dead", "Include dead PIDs when generating configs")
  .option("--verbose", "Print where configs were written")
  .option(
    "--location-prefix <prefix:string>",
    "Override proxy location prefix for ALL services (leading slash recommended)",
  )
  .option(
    "--strip-prefix",
    "Enable stripPrefix middleware/rewrite (default is off)",
  )
  .option(
    "--server-name <name:string>",
    "nginx: server_name value (default '_')",
  )
  .option("--listen <listen:string>", "nginx: listen value (default '80')")
  .option(
    "--entrypoints <csv:string>",
    "traefik: entryPoints CSV (default 'web')",
  )
  .option(
    "--rule <rule:string>",
    "traefik: router rule override (default PathPrefix(`<prefix>/`))",
  )
  .option(
    "--nginx-extra <text:string>",
    "nginx: extra snippet appended into server block",
  )
  .option(
    "--traefik-extra <text:string>",
    "traefik: extra yaml appended at end",
  )
  .action(async (o) => {
    const wantNginx = o.type === "nginx" || o.type === "both";
    const wantTraefik = o.type === "traefik" || o.type === "both";

    const overrides = {
      nginx: {
        locationPrefix: o.locationPrefix,
        serverName: o.serverName,
        listen: o.listen,
        stripPrefix: o.stripPrefix ? true : undefined,
        extra: o.nginxExtra,
      },
      traefik: {
        locationPrefix: o.locationPrefix,
        entrypoints: o.entrypoints,
        rule: o.rule,
        stripPrefix: o.stripPrefix ? true : undefined,
        extra: o.traefikExtra,
      },
    } as const;

    await generateReverseProxyConfsFromSpawnedStates({
      spawnStateHome: o.spawnStateHome,
      nginxConfHome: wantNginx ? o.nginxOut : undefined,
      traefikConfHome: wantTraefik ? o.traefikOut : undefined,
      includeDead: o.includeDead ? true : undefined,
      verbose: o.verbose ? true : undefined,
      overrides,
    });
  })
  .command(
    "kill",
    `Stop (kill) managed processes (default ${defaultSpawnStateHome})`,
  )
  .option(
    "--spawn-state-home <dir:string>",
    `Spawn state home (default ${defaultSpawnStateHome})`,
    { default: defaultSpawnStateHome },
  )
  .option("--clean", "Remove spawn-state home after killing processes")
  .action(async ({ clean, spawnStateHome }) => {
    await killSpawnedStates(spawnStateHome);
    if (clean) {
      Deno.remove(spawnStateHome, { recursive: true }).catch(() => undefined);
    } else {
      await lsSpawnedStates(spawnStateHome);
    }
  })
  .command("help", new HelpCommand())
  .command("completions", new CompletionsCommand())
  .parse(Deno.args);
