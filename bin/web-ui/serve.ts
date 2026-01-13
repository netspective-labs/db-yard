#!/usr/bin/env -S deno run --watch -A --node-modules-dir=auto
// bin/web-ui/serve.ts

import { Command } from "@cliffy/command";
import { normalize, resolve } from "@std/path";
import { Hono } from "jsr:@hono/hono@4.11.3";
import { taggedProcesses } from "../../lib/spawn.ts";

import {
  type AppCfg,
  computeMounts,
  logStartup,
  registerAssetsRoutes,
  registerLedgerBrowser,
  registerProcessesApi,
  registerReconcileApi,
  registerRootRedirects,
  registerUiRoutes,
  requireDir,
} from "./app.ts";

import { registerCatchAllProxy, registerProxyApiRoutes } from "./proxy.ts";

async function main() {
  const cmd = await new Command()
    .name("truth-yard-web-ui")
    .description(
      "Operational Truth Yard web UI + ledger browser + simple proxy to tagged processes",
    )
    .option("--host <host:string>", "Host to bind", { default: "127.0.0.1" })
    .option("--port <port:number>", "Port to listen on", { default: 8787 })
    .option(
      "--ledger-dir <path:string>",
      "Path to the Truth Yard ledger directory (ledger.d parent)",
      { default: "./ledger.d" },
    )
    .option("--assets-dir <path:string>", "Path to assets directory", {
      default: "bin/web-ui/asset",
    })
    .option("--no-proxy", "Disable proxy behavior for non /.truth-yard paths", {
      default: false,
    })
    .option(
      "--refresh-ms <ms:number>",
      "UI polling interval for tagged processes",
      { default: 2000 },
    )
    .parse(Deno.args);

  const cfg: AppCfg = {
    host: cmd.options.host,
    port: cmd.options.port,
    ledgerDirAbs: normalize(resolve(cmd.options.ledgerDir)),
    assetsDir: cmd.options.assetsDir,
    proxyEnabled: !cmd.options.proxy,
  };

  await requireDir(cfg.assetsDir);

  const app = new Hono();
  const mounts = computeMounts();
  const getProcesses = () => Array.fromAsync(taggedProcesses());

  const shared = { app, cfg, mounts, getProcesses };

  registerRootRedirects(shared);
  registerAssetsRoutes(shared);
  registerUiRoutes(shared);
  registerProcessesApi(shared);
  registerReconcileApi(shared);
  registerLedgerBrowser(shared);

  registerProxyApiRoutes({
    app,
    apiMount: mounts.apiMount,
    uiMount: mounts.uiMount,
    mount: mounts.mount,
    ledgerDir: cfg.ledgerDirAbs,
    getProcesses,
  });

  if (cfg.proxyEnabled) {
    registerCatchAllProxy({
      app,
      apiMount: mounts.apiMount,
      uiMount: mounts.uiMount,
      mount: mounts.mount,
      ledgerDir: cfg.ledgerDirAbs,
      getProcesses,
    });
  }

  logStartup(cfg, mounts);
  Deno.serve({ hostname: cfg.host, port: cfg.port }, app.fetch);
}

if (import.meta.main) {
  await main();
}
