#!/usr/bin/env -S deno run -A --node-modules-dir=auto
// bin/yard.ts
import { Command, EnumType } from "@cliffy/command";
import { CompletionsCommand } from "@cliffy/completions";
import { HelpCommand } from "@cliffy/help";
import { blue, cyan, dim, green, red, yellow } from "@std/fmt/colors";
import {
  materialize,
  materializeWatch,
  spawnedLedgerStates,
} from "../lib/materialize.ts";
import { generateReverseProxyConfsFromSpawnedStates } from "../lib/reverse-proxy-conf.ts";
import { killSpawnedProcesses, taggedProcesses } from "../lib/spawn.ts";

export async function lsLedgers(
  spawnStateHomeOrSessionHome: string,
): Promise<void> {
  for await (const state of spawnedLedgerStates(spawnStateHomeOrSessionHome)) {
    const { pid, pidAlive, context, context: { service: { upstreamUrl } } } =
      state;

    const kind = context.service.kind;
    const nature = context.supplier.nature;

    const statusIcon = pidAlive ? "🟢" : "🔴";
    const pidLabel = pidAlive ? green(String(pid)) : red(`${pid} (dead)`);

    const kindLabel = cyan(kind);
    const natureLabel = dim(nature);

    const urlLabel = pidAlive ? yellow(upstreamUrl) : dim(upstreamUrl);

    console.log(
      `${statusIcon} [${pidLabel}] ${urlLabel} ${dim("(")}${kindLabel}${
        dim("/")
      }${natureLabel}${dim(")")}`,
    );
  }
}

export async function lsProcesses(
  opts: Readonly<{ extended?: boolean }> = {},
): Promise<void> {
  const extended = opts.extended === true;

  const kv = (key: string, value: unknown) => {
    const v = value == null || value === "" ? "(none)" : String(value);
    return `  ${dim(key)}: ${blue(v)}`;
  };

  const issueToString = (issue: unknown): string => {
    if (!issue) return "";
    if (issue instanceof AggregateError) {
      const parts = issue.errors.map((e) =>
        e instanceof Error ? e.message : String(e)
      );
      return parts.join(" | ");
    }
    if (issue instanceof Error) return issue.message;
    return String(issue);
  };

  for await (const p of taggedProcesses()) {
    const pidLabel = green(String(p.pid));

    const kind = p.context?.service?.kind ?? p.kind ?? "unknown";
    const nature = p.context?.supplier?.nature ?? "unknown";

    const kindLabel = cyan(kind);
    const natureLabel = dim(nature);

    const upstreamUrl = p.context?.service?.upstreamUrl ??
      p.upstreamUrl ??
      "(no context)";

    const urlLabel = yellow(upstreamUrl);

    console.log(
      `${extended ? "" : "🟢 "}[${pidLabel}] ${urlLabel} ${
        dim("(")
      }${kindLabel}${dim("/")}${natureLabel}${dim(")")}`,
    );

    if (!extended) continue;

    const issueStr = issueToString(p.issue);

    // Extended: one essential field per line as "  <key>: <value>"
    console.log(kv("provenance", p.provenance));
    console.log(kv("sessionId", p.sessionId));
    console.log(kv("serviceId", p.serviceId));
    console.log(kv("kind", p.kind ?? p.context?.service?.kind));
    console.log(kv("label", p.label ?? p.context?.service?.label));
    console.log(
      kv(
        "proxyEndpointPrefix",
        p.proxyEndpointPrefix ?? p.context?.service?.proxyEndpointPrefix,
      ),
    );
    console.log(kv("upstreamUrl", upstreamUrl));
    console.log(kv("contextPath", p.contextPath));
    console.log(kv("cmdline", p.cmdline));
    if (issueStr) console.log(kv("issue", issueStr));
  }
}

const verboseType = new EnumType(["essential", "comprehensive"] as const);
const proxyType = new EnumType(["nginx", "traefik", "both"] as const);

const defaultCargoHome = "./cargo.d";
const defaultSpawnStateHome = "./spawned.d";

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
  .example(`Start and keep running in watch mode`, "yard.ts start --watch")
  .example(
    `Watch with strict kill scoping (only kill processes started in this watch session)`,
    "yard.ts start --watch --strict-kills-only",
  )
  .example(`List Linux processes started by yard.ts`, "yard.ts ps -e")
  .example(
    `List all managed processes in ${defaultSpawnStateHome}`,
    "yard.ts ls",
  )
  .example(
    `Stop (kill) all managed processes in ${defaultSpawnStateHome}`,
    "yard.ts kill",
  )
  .example(`Start web UI + watcher`, "yard.ts web-ui --watch")
  .command(
    "start",
    `Start exposable databases (default root ${defaultCargoHome}) and exit (or --watch to keep in sync)`,
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
  .option(
    "--summarize",
    "Summarize after spawning (and after each watch cycle)",
  )
  .option("--no-ls", "Don't list after spawning")
  // ---- new: watch + useful knobs ----
  .option(
    "--watch",
    `Continuously watch cargo and keep services in sync (spawns new, kills removed)`,
  )
  .option(
    "--watch-debounce-ms <ms:number>",
    "Watch debounce window (default 750ms)",
    { default: 750 },
  )
  .option(
    "--strict-kills-only",
    "Watch: only kill processes whose provenance AND sessionId match this watch session (default off)",
  )
  .option(
    "--no-smart-spawn",
    "Disable smartSpawn (default on). When on, avoids spawning a DB if already running (Linux taggedProcesses).",
  )
  .action(
    async (
      {
        summarize,
        verbose,
        ls,
        cargoHome,
        spawnStateHome,
        watch,
        watchDebounceMs,
        strictKillsOnly,
        smartSpawn,
      },
    ) => {
      const v = verbose ? verbose : false;

      if (watch) {
        for await (
          const result of materializeWatch([{ path: cargoHome }], {
            verbose: v,
            spawnedLedgerHome: spawnStateHome,
            smartSpawn: smartSpawn !== false, // default true; --no-smart-spawn flips it to false
            watch: {
              enabled: true,
              debounceMs: watchDebounceMs,
              strictKillsOnly: strictKillsOnly ? true : false,
            },
          })
        ) {
          if (summarize) {
            console.log(`sessionHome: ${result.sessionHome}`);
            if (result.sessionId) console.log(`sessionId: ${result.sessionId}`);
            console.log("summary:", result.summary);
          }

          if (ls) {
            await lsProcesses();
          }
        }

        return;
      }

      const result = await materialize([{ path: cargoHome }], {
        verbose: v,
        spawnedLedgerHome: spawnStateHome,
        smartSpawn: smartSpawn !== false, // default true
      });

      if (summarize) {
        console.log(`sessionHome: ${result.sessionHome}`);
        if (result.sessionId) console.log(`sessionId: ${result.sessionId}`);
        console.log("summary:", result.summary);
      }

      if (ls) {
        await lsProcesses();
      }
    },
  )
  .command(
    "ls",
    `List upstream URLs and PIDs from spawned states (default ${defaultSpawnStateHome})`,
  )
  .option(
    "--spawn-state-home <dir:string>",
    `Spawn state home (default ${defaultSpawnStateHome})`,
    { default: defaultSpawnStateHome },
  )
  .action(async ({ spawnStateHome }) => {
    await lsLedgers(spawnStateHome);
  })
  .command("ps", `List Linux tagged processes`)
  .option("-e, --extended", `Show provenance details`)
  .action(async (options) => {
    await lsProcesses(options);
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
      nginxConfHome: wantNginx ? o.nginxOut : undefined,
      traefikConfHome: wantTraefik ? o.traefikOut : undefined,
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
    await killSpawnedProcesses();
    if (clean) {
      Deno.remove(spawnStateHome, { recursive: true }).catch(() => undefined);
    } else {
      await lsLedgers(spawnStateHome);
    }
  })
  .command("help", new HelpCommand())
  .command("completions", new CompletionsCommand())
  .parse(Deno.args);
