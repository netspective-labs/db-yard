#!/usr/bin/env -S deno run -A --node-modules-dir=auto
// bin/yard.ts
import { Command, EnumType } from "@cliffy/command";
import { CompletionsCommand } from "@cliffy/completions";
import { HelpCommand } from "@cliffy/help";
import {
  killSpawnedStates,
  materialize,
  spawnedStates,
} from "../lib/materialize.ts";

import { cyan, dim, green, red, yellow } from "@std/fmt/colors";
import {
  generateReverseProxyConfsFromSpawnedStates,
} from "../lib/reverse-proxy-conf.ts";

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

const cargoHome = "./cargo.d";
const spawnStateHome = "./spawned.d";

await new Command()
  .name("yard.ts")
  .description("File-driven process yard for SQLite DB cargo.")
  .example(
    `Start all exposable databases in ${cargoHome}`,
    "yard.ts start",
  )
  .example(
    `Start with essential verbosity`,
    "yard.ts start --verbose essential",
  )
  .example(
    `Start with comprehensive verbosity`,
    "yard.ts start --verbose comprehensive",
  )
  .example(
    `List all managed processes in ${spawnStateHome}`,
    "yard.ts ls",
  )
  .example(
    `Stop (kill) all managed processes in ${spawnStateHome}`,
    "yard.ts kill",
  )
  .example(
    `Stop (kill) processes and remove ${spawnStateHome}`,
    "yard.ts kill --clean",
  )
  .example(
    `Write nginx bundle to ./out/nginx`,
    "yard.ts proxy-conf --type nginx --nginx-out ./out/nginx",
  )
  .example(
    `Write traefik bundle to ./out/traefik`,
    "yard.ts proxy-conf --type traefik --traefik-out ./out/traefik",
  )
  .example(
    `Write both nginx + traefik bundles`,
    "yard.ts proxy-conf --type both --nginx-out ./out/nginx --traefik-out ./out/traefik",
  )
  .example(
    `Emit nginx config to stdout (default)`,
    "yard.ts proxy-conf --type nginx",
  )
  .command("start", `Start all exposable databases in ${cargoHome} and exit`)
  .type("verbose", verboseType)
  .option("--verbose <level:verbose>", "Spawn/materialize verbosity")
  .option("--summarize", "Summarize after spawning")
  .option("--no-ls", "Don't list after spawning")
  .action(async ({ summarize, verbose, ls }) => {
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
  .command("ls", `List all managed processes in ${spawnStateHome}`)
  .action(async () => {
    await lsSpawnedStates(spawnStateHome);
  })
  .command(
    "proxy-conf",
    `NGINX, Traefik, etc. proxy configs from upstream URLs in ${spawnStateHome}`,
  )
  .type("proxy", proxyType)
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
  .action(
    async ({
      type,
      nginxOut,
      traefikOut,
      includeDead,
      verbose,
      locationPrefix,
      stripPrefix,
      serverName,
      listen,
      entrypoints,
      rule,
      nginxExtra,
      traefikExtra,
    }) => {
      const wantNginx = type === "nginx" || type === "both";
      const wantTraefik = type === "traefik" || type === "both";

      const overrides = {
        nginx: {
          locationPrefix: locationPrefix,
          serverName,
          listen,
          stripPrefix: stripPrefix ? true : undefined,
          extra: nginxExtra,
        },
        traefik: {
          locationPrefix: locationPrefix,
          entrypoints,
          rule,
          stripPrefix: stripPrefix ? true : undefined,
          extra: traefikExtra,
        },
      } as const;

      await generateReverseProxyConfsFromSpawnedStates({
        spawnStateHome,
        nginxConfHome: wantNginx ? nginxOut : undefined,
        traefikConfHome: wantTraefik ? traefikOut : undefined,
        includeDead: includeDead ? true : undefined,
        verbose: verbose ? true : undefined,
        overrides,
      });
    },
  )
  .command("kill", `Stop (kill) all managed processes in ${spawnStateHome}`)
  .option("--clean", `Remove ${spawnStateHome} after killing processes`)
  .action(async ({ clean }) => {
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
