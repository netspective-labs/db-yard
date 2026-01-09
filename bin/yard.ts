#!/usr/bin/env -S deno run -A --node-modules-dir=auto
// bin/yard.ts
import { Command, EnumType } from "@cliffy/command";
import { CompletionsCommand } from "@cliffy/completions";
import { HelpCommand } from "@cliffy/help";
import { blue, cyan, dim, green, red, yellow } from "@std/fmt/colors";
import { compose } from "../lib/composite.ts";
import {
  materialize,
  reconcile,
  type ReconcileItem,
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

async function psReconcile(
  spawnStateHomeOrSessionHome: string,
): Promise<void> {
  const base = spawnStateHomeOrSessionHome;

  const fmt = (item: ReconcileItem): string => {
    if (item.kind === "process_without_ledger") {
      const pid = green(String(item.pid));
      const sid = cyan(item.serviceId);
      const sess = dim(item.sessionId);
      const ctx = blue(item.contextPath);
      const cmd = item.cmdline ? dim(item.cmdline) : dim("(no cmdline)");
      return `🟡 ${
        yellow("process without ledger")
      } [${pid}] serviceId=${sid} sessionId=${sess}\n  contextPath=${ctx}\n  cmdline=${cmd}`;
    }

    const pid = red(String(item.pid));
    const ctx = blue(item.ledgerContextPath);
    const sid = item.serviceId ? cyan(item.serviceId) : dim("(unknown)");
    const sess = item.sessionId ? dim(item.sessionId) : dim("(unknown)");
    return `🟠 ${
      yellow("ledger without process")
    } [${pid}] serviceId=${sid} sessionId=${sess}\n  ledgerContextPath=${ctx}`;
  };

  let any = false;

  const gen = reconcile(base);
  while (true) {
    const next = await gen.next();
    if (next.done) {
      const s = next.value;
      const ok = s.processWithoutLedger === 0 && s.ledgerWithoutProcess === 0;

      const headline = ok
        ? `🟢 ${green("reconcile OK")} (no discrepancies)`
        : `🔶 ${yellow("reconcile found discrepancies")}`;

      console.log(headline);
      console.log(
        `  ${dim("processWithoutLedger")}: ${
          blue(String(s.processWithoutLedger))
        }`,
      );
      console.log(
        `  ${dim("ledgerWithoutProcess")}: ${
          blue(String(s.ledgerWithoutProcess))
        }`,
      );

      if (!any && !ok) {
        console.log(dim("  (no items emitted)"));
      }
      return;
    }

    any = true;
    console.log(fmt(next.value));
  }
}

const verboseType = new EnumType(["essential", "comprehensive"] as const);
const proxyType = new EnumType(["nginx", "traefik", "both"] as const);
const dialectType = new EnumType(["SQLite", "DuckDB"] as const);
const scopeType = new EnumType(["admin", "cross-tenant", "tenant"] as const);

const defaultCargoHome = "./cargo.d";
const defaultLedgerHome = "./ledger.d";

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
    `List all managed processes in ${defaultLedgerHome}`,
    "yard.ts ls",
  )
  .example(
    `Stop (kill) all managed processes in ${defaultLedgerHome}`,
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
    "--ledger-home <dir:string>",
    `Spawn state home (default ${defaultLedgerHome})`,
    { default: defaultLedgerHome },
  )
  .option("--verbose <level:verbose>", "Spawn/materialize verbosity")
  .option("--summarize", "Summarize after spawning")
  .option("--no-ls", "Don't list after spawning")
  .action(async ({ summarize, verbose, ls, cargoHome, ledgerHome }) => {
    const result = await materialize([{ path: cargoHome }], {
      verbose: verbose ? verbose : false,
      spawnedLedgerHome: ledgerHome,
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
    "cc",
    "Generate SQL DDL for Composite Connections (CC) and emit to STDOUT",
  )
  .type("dialect", dialectType)
  .type("scope", scopeType)
  .option(
    "--volume-root <dir:string>",
    "Root directory containing embedded/ (default '.')",
    { default: "." },
  )
  .option(
    "--scope <scope:scope>",
    "Which composite scope to target (admin, cross-tenant, tenant)",
    { default: "admin" },
  )
  .option(
    "--tenant-id <id:string>",
    "Tenant ID (required when --scope tenant)",
  )
  .option(
    "--dialect <d:dialect>",
    "SQL dialect for ATTACH statements",
    { default: "SQLite" },
  )
  .option(
    "--globs <csv:string>",
    "CSV list of glob patterns to discover embedded DB files",
    { default: "**/*.sqlite.db,**/*.sqlite,**/*.db" },
  )
  .option(
    "--ignore <csv:string>",
    "CSV list of additional ignore paths (relative to baseDir or absolute)",
  )
  .option(
    "--pragma-order <order:string>",
    "Pragma emission order: sorted | asProvided",
    { default: "sorted" },
  )
  .option(
    "--extra-sql-order <order:string>",
    "Extra SQL emission order: sorted | asProvided",
    { default: "sorted" },
  )
  .option(
    "--duckdb-sqlite-ext",
    "DuckDB only: emit INSTALL sqlite; LOAD sqlite; preamble (order-preserved)",
  )
  .action(async (o) => {
    const parseCsv = (s: string): string[] =>
      s.split(",").map((x) => x.trim()).filter((x) => x.length > 0);

    const globs = parseCsv(o.globs);

    const ignore = o.ignore
      ? parseCsv(o.ignore).map((p) =>
        // allow relative ignores; resolve them later under baseDir
        p
      )
      : undefined;

    if (o.scope === "tenant" && !o.tenantId) {
      console.error(red("cc: --tenant-id is required when --scope tenant"));
      Deno.exit(2);
    }

    const result = await compose({
      layout: { volumeRoot: o.volumeRoot },
      scope: o.scope,
      tenantId: o.tenantId,
      dialect: o.dialect,
      configure: (_ctx) => ({
        globs,
        ignore,
        pragmaOrder: o.pragmaOrder === "asProvided" ? "asProvided" : "sorted",
        extraSqlOrder: o.extraSqlOrder === "asProvided"
          ? "asProvided"
          : "sorted",
        // Keep aliasing deterministic and stable by default.
        // If you want a different alias policy (e.g. cross_ prefixes), extend with another CLI option.
        aliasForKey: (stableKey: string) => {
          const file = stableKey.split("/").pop() ?? stableKey;
          return file.replace(/\.sqlite(\.db)?$/i, "").replace(/\.db$/i, "");
        },
        pragmas: (ctx) => {
          // In this CLI command we treat pragmas as a “preamble”. We keep it minimal and deterministic.
          // For DuckDB attaching SQLite files, users typically need sqlite extension loaded.
          if (ctx.dialect === "DuckDB" && o.duckdbSqliteExt) {
            // INSTALL then LOAD order matters.
            return ["INSTALL sqlite;", "LOAD sqlite;"];
          }
          return [];
        },
      }),
    });

    // Emit to STDOUT
    console.log(result.sql);
  })
  .command(
    "ls",
    `List upstream URLs and PIDs from spawned states (default ${defaultLedgerHome})`,
  )
  .option(
    "--ledger-home <dir:string>",
    `Spawn state home (default ${defaultLedgerHome})`,
    { default: defaultLedgerHome },
  )
  .action(async ({ ledgerHome }) => {
    await lsLedgers(ledgerHome);
  })
  .command("ps", `List Linux tagged processes`)
  .option("-e, --extended", `Show provenance details`)
  .option(
    "--reconcile",
    `Also reconcile tagged processes vs spawned ledger (context.json files)`,
  )
  .option(
    "--ledger-home <dir:string>",
    `Spawn state home OR a specific sessionHome (default ${defaultLedgerHome})`,
    { default: defaultLedgerHome },
  )
  .action(async (options) => {
    if (options.reconcile) {
      await psReconcile(options.ledgerHome);
    } else {
      await lsProcesses(options);
    }
  })
  .command(
    "proxy-conf",
    `NGINX, Traefik, etc. proxy configs from upstream URLs in spawn-state home`,
  )
  .type("proxy", proxyType)
  .option(
    "--ledger-home <dir:string>",
    `Spawn state home (default ${defaultLedgerHome})`,
    { default: defaultLedgerHome },
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
    `Stop (kill) managed processes (default ${defaultLedgerHome})`,
  )
  .option(
    "--ledger-home <dir:string>",
    `Spawn state home (default ${defaultLedgerHome})`,
    { default: defaultLedgerHome },
  )
  .option("--clean", "Remove ledger root after killing processes")
  .action(async ({ clean, ledgerHome }) => {
    await killSpawnedProcesses();
    if (clean) {
      Deno.remove(ledgerHome, { recursive: true }).catch(() => undefined);
    } else {
      await lsLedgers(ledgerHome);
    }
  })
  .command("help", new HelpCommand())
  .command("completions", new CompletionsCommand())
  .parse(Deno.args);
