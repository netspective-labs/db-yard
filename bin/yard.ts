#!/usr/bin/env -S deno run -A --node-modules-dir=auto
// bin/yard.ts
import { Command, EnumType } from "@cliffy/command";
import { CompletionsCommand } from "@cliffy/completions";
import { HelpCommand } from "@cliffy/help";
import { blue, cyan, dim, green, red, yellow } from "@std/fmt/colors";
import { compose } from "../lib/composite.ts";
import {
  materialize,
  materializeWatch,
  reconcile,
  type ReconcileItem,
  spawnedLedgerStates,
} from "../lib/materialize.ts";
import {
  generateReverseProxyConfsFromSpawnedStates,
  nginxProxyManagerJSON,
} from "../lib/reverse-proxy-conf.ts";
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

const psStrategyType = new EnumType(["container", "native"] as const);

const YARD_VERSION = "dev";

function escapeHtml(s: string): string {
  return s
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
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

export async function lsProcesses(
  opts: Readonly<{ extended?: boolean; strategy?: "container" | "native" }> =
    {},
): Promise<void> {
  const extended = opts.extended === true;

  const spawnStrategy = opts.strategy === "container"
    ? "ps"
    : opts.strategy === "native"
    ? "/proc"
    : undefined;

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

  for await (
    const p of taggedProcesses(
      spawnStrategy ? { strategy: spawnStrategy } : undefined,
    )
  ) {
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

// 4) Update htmlFromLsProcesses to accept an optional strategy and pass through
export async function htmlFromLsProcesses(
  opts: Readonly<{ strategy?: "container" | "native" }> = {},
): Promise<void> {
  const rows: string[] = [];
  let count = 0;

  const spawnStrategy = opts.strategy === "container"
    ? "ps"
    : opts.strategy === "native"
    ? "/proc"
    : undefined;

  for await (
    const p of taggedProcesses(
      spawnStrategy ? { strategy: spawnStrategy } : undefined,
    )
  ) {
    count++;

    const pid = (typeof p.pid === "number") ? p.pid : undefined;
    const alive = true;

    const proxyPrefix = p.proxyEndpointPrefix ?? "?";
    const proxyHref = p.proxyEndpointPrefix ?? "#";
    const upstreamHref = p.upstreamUrl ?? "#";
    const upstreamLabel = p.upstreamUrl ?? "?";

    const serviceName = p.serviceId;
    const statusEmoji = alive === true ? "🟢" : alive === false ? "🔴" : "⚪";

    let detailsHtml = "";
    const extras = [
      ["cmdline", p.cmdline],
      ["kind", p.kind],
      ["contextPath", p.contextPath],
      ["label", p.label],
      [
        "env",
        JSON.stringify(
          Object.entries(p.env).filter(([k]) => k.startsWith("TRUTH_YARD")),
        ),
      ],
    ].filter(([, v]) =>
      v !== undefined && v !== null && String(v).trim() !== ""
    );

    if (extras.length > 0) {
      detailsHtml = `
<details>
  <summary>⚙️ Process</summary>
  <ul>
    ${
        extras
          .map(([k, v]) =>
            `<li><strong>${escapeHtml(k ?? "")}</strong>: <code>${
              escapeHtml(String(v))
            }</code></li>`
          )
          .join("\n")
      }
  </ul>
</details>`.trim();
    } else {
      detailsHtml = `<span class="secondary">—</span>`;
    }

    const proxyCell = proxyPrefix
      ? `<a href="${proxyHref}">${escapeHtml(proxyPrefix)}</a>`
      : `<span class="secondary">—</span>`;

    const upstreamCell = upstreamHref
      ? `<a href="${escapeHtml(upstreamHref)}">${escapeHtml(upstreamLabel)}</a>`
      : (upstreamLabel
        ? `<code>${escapeHtml(upstreamLabel)}</code>`
        : `<span class="secondary">—</span>`);

    const pidCell = pid
      ? `<code>${escapeHtml(String(pid))}</code> ${statusEmoji}`
      : `<span class="secondary">—</span>`;

    rows.push(`
<tr>
  <td>🧩 ${escapeHtml(serviceName)}</td>
  <td>${proxyCell}</td>
  <td>${upstreamCell}</td>
  <td>${detailsHtml}</td>
  <td>${pidCell}</td>
</tr>`.trim());
  }

  rows.sort((a, b) => a.localeCompare(b));

  const generatedAt = new Date().toISOString();

  const html = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Operational Truth Yard Services</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@picocss/pico@2/css/pico.min.css" />
  <style>
    :root { --pico-font-size: 16px; }
    header { margin-top: 1.5rem; }
    .secondary { opacity: 0.7; }
    code { font-size: 0.9em; }
    details summary { cursor: pointer; }
    footer { margin-top: 2.5rem; opacity: 0.8; }
    td:last-child { text-align: right; white-space: nowrap; }
  </style>
</head>
<body>
  <main class="container">
    <header>
      <h1>
        <img src="https://raw.githubusercontent.com/netspective-labs/truth-yard/refs/heads/main/support/oty-logo-189x72.png">
        Operational Truth Yard Services
      </h1>
      <p class="secondary">${count} service${
    count === 1 ? "" : "s"
  } discovered</p>
    </header>

    <section>
      <figure>
        <table role="grid">
          <thead>
            <tr>
              <th scope="col">Service</th>
              <th scope="col">🧭 Proxy</th>
              <th scope="col">🔗 Upstream</th>
              <th scope="col">🛠️ Process</th>
              <th scope="col">PID</th>
            </tr>
          </thead>
          <tbody>
            ${
    rows.length
      ? rows.join("\n")
      : `<tr><td colspan="5" class="secondary">No services found.</td></tr>`
  }
          </tbody>
        </table>
      </figure>
    </section>

    <footer>
      <small>Generated ${escapeHtml(generatedAt)} · yard version ${
    escapeHtml(YARD_VERSION)
  }</small>
    </footer>
  </main>
</body>
</html>`;

  console.log(html);
}

const verboseType = new EnumType(["essential", "comprehensive"] as const);
const proxyType = new EnumType(
  ["nginx", "traefik", "both", "nginx-proxy-manager"] as const,
);
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
    "yard.ts start --watch",
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
  .option(
    "--listen-host <host:string>",
    "Listent host for spawned services (default 127.0.0.1)",
    { default: "127.0.0.1" },
  )
  .option("--verbose <level:verbose>", "Spawn/materialize verbosity")
  .option("--summarize", "Summarize after spawning")
  .option("--no-ls", "Don't list after spawning")
  .option(
    "--watch",
    "Watch cargo-home and keep services in sync (remove => kill, change => reconcile spawn)",
  )
  .option(
    "--watch-debounce-ms <ms:number>",
    "Watch mode: debounce window in ms (default 750)",
  )
  .option(
    "--watch-strict-kills-only",
    "Watch mode: only kill processes from THIS watch loop session (Linux only)",
  )
  .option(
    "--start-port <port:number>",
    "Starting port for upstream services (default 3000)",
  )
  .action(
    async (
      {
        summarize,
        verbose,
        ls,
        cargoHome,
        ledgerHome,
        watch,
        watchDebounceMs,
        watchStrictKillsOnly,
        listenHost,
        startPort,
      },
    ) => {
      const optsBase = {
        verbose: verbose ? verbose : false,
        spawnedLedgerHome: ledgerHome,
        listenHost,
        startPort,
      } as const;

      if (watch) {
        console.log(
          `👀 ${green("Watch mode enabled")} — monitoring ${yellow(cargoHome)}`,
        );
        console.log(
          `⏱️  debounce=${
            blue(
              String(watchDebounceMs ?? 750),
            )
          }ms  strictKillsOnly=${
            blue(
              String(!!watchStrictKillsOnly),
            )
          }`,
        );
        console.log(dim("Press Ctrl+C to stop watching.\n"));

        const gen = materializeWatch([{ path: cargoHome }], {
          ...optsBase,
          watch: {
            enabled: true,
            debounceMs: typeof watchDebounceMs === "number"
              ? watchDebounceMs
              : undefined,
            strictKillsOnly: watchStrictKillsOnly ? true : undefined,
          },
        });

        let first = true;

        for await (const result of gen) {
          if (first) {
            console.log(`🚀 ${green("Initial materialization complete")}`);
            first = false;
          } else {
            console.log(
              `🔁 ${cyan("Change detected")} — reconciling services…`,
            );
          }

          console.log(
            `📦 sessionHome: ${yellow(result.sessionHome)}`,
          );
          console.log(
            `📊 summary: ${blue(JSON.stringify(result.summary))}`,
          );

          if (ls) {
            console.log(dim("\n🔍 Listing active services:"));
            await lsProcesses();
          }

          console.log(dim("\n⏳ Waiting for next change…\n"));
        }

        return;
      }

      const result = await materialize([{ path: cargoHome }], {
        ...optsBase,
      });

      if (summarize) {
        console.log(`sessionHome: ${result.sessionHome}`);
        console.log("summary:", result.summary);
      }

      if (ls) {
        await lsProcesses();
      }
    },
  )
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

    const ignore = o.ignore ? parseCsv(o.ignore).map((p) => p) : undefined;

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
  .type("ps_strategy", psStrategyType)
  .option(
    "--strategy <s:ps_strategy>",
    "Process enumeration strategy: container=ps, native=/proc (default: auto)",
  )
  .option("--html", "Emit a static HTML index page to STDOUT")
  .action(async (options) => {
    if (options.html) {
      if (options.reconcile) {
        throw new Error("ps --html cannot be used with --reconcile");
      }
      await htmlFromLsProcesses({ strategy: options.strategy });
      return;
    }

    if (options.reconcile) {
      await psReconcile(options.ledgerHome);
    } else {
      await lsProcesses({
        extended: options.extended,
        strategy: options.strategy,
      });
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
  .option(
    "--upstream-scheme <scheme:string>",
    "nginx-proxy-manager: forward_scheme (default 'http')",
  )
  .option(
    "--upstream-host <host:string>",
    "nginx-proxy-manager: forward_host (default '0.0.0.0')",
    { default: "0.0.0.0" },
  )
  .action(async (o) => {
    const wantNginx = o.type === "nginx" || o.type === "both";
    const wantTraefik = o.type === "traefik" || o.type === "both";
    const wantNPM = o.type === "nginx-proxy-manager";

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
      nginxProxyManager: {
        locationPrefix: o.locationPrefix,
        upstreamScheme: o.upstreamScheme,
        upstreamHost: o.upstreamHost,
      },
    } as const;

    if (wantNPM) {
      const states = [];
      for await (const s of taggedProcesses()) {
        states.push(s);
      }
      console.log(nginxProxyManagerJSON(states, overrides));
      return;
    }

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
