#!/usr/bin/env -S deno run -A --node-modules-dir=auto
// bin/yard.ts
import { Command, EnumType } from "@cliffy/command";
import { CompletionsCommand } from "@cliffy/completions";
import { HelpCommand } from "@cliffy/help";
import { cyan, dim, green, red, yellow } from "@std/fmt/colors";
import { materialize, spawnedLedgerStates } from "../lib/materialize.ts";
import { generateReverseProxyConfsFromSpawnedStates } from "../lib/reverse-proxy-conf.ts";
import { killSpawnedProcesses, taggedProcesses } from "../lib/spawn.ts";

export async function lsSpawnedStates(
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

  for await (const p of taggedProcesses()) {
    const pidLabel = green(String(p.pid));

    const kind = p.context?.service?.kind ?? "unknown";
    const nature = p.context?.supplier?.nature ?? "unknown";

    const kindLabel = cyan(kind);
    const natureLabel = dim(nature);

    const upstreamUrl = p.context
      ? p.context.service.upstreamUrl
      : "(no context)";

    const urlLabel = yellow(upstreamUrl);

    console.log(
      `🟢 [${pidLabel}] ${urlLabel} ${dim("(")}${kindLabel}${
        dim("/")
      }${natureLabel}${dim(")")}`,
    );

    if (!extended) continue;

    const extras: string[] = [];

    if (p.issue) {
      if (p.issue instanceof AggregateError) {
        extras.push(
          `issue=${
            p.issue.errors.map((e) =>
              e instanceof Error ? e.message : String(e)
            ).join(" | ")
          }`,
        );
      } else if (p.issue instanceof Error) {
        extras.push(`issue=${p.issue.message}`);
      } else {
        extras.push(`issue=${String(p.issue)}`);
      }
    }

    if (p.sessionId) extras.push(`sessionId=${p.sessionId}`);
    if (p.serviceId) extras.push(`serviceId=${p.serviceId}`);
    if (p.contextPath) extras.push(`contextPath=${p.contextPath}`);
    if (p.cmdline) extras.push(`cmdline=${p.cmdline}`);

    if (extras.length > 0) {
      console.log(dim(`  ${extras.join("  ")}`));
    } else {
      console.log(dim(`  (no extra details)`));
    }
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
  .example(`List Linux processes started by yard.ts`, "yard.ts ps -e")
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
  .example(`Start web UI + watcher`, "yard.ts web-ui --watch")
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
      spawnedLedgerHome: spawnStateHome,
    });

    if (summarize) {
      console.log(`sessionHome: ${result.sessionHome}`);
      console.log("summary:", result.summary);
    }

    if (ls) {
      await lsProcesses();
    }
  })
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
    await lsSpawnedStates(spawnStateHome);
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
    await killSpawnedProcesses();
    if (clean) {
      Deno.remove(spawnStateHome, { recursive: true }).catch(() => undefined);
    } else {
      await lsSpawnedStates(spawnStateHome);
    }
  })
  .command("help", new HelpCommand())
  .command("completions", new CompletionsCommand())
  .parse(Deno.args);
