// bin/web-ui/app.ts

import { normalize, resolve } from "@std/path";
import type { Hono } from "jsr:@hono/hono@4.11.3";
import { serveStatic } from "jsr:@hono/hono@4.11.3/deno";
import { reconcile, type ReconcileItem } from "../../lib/materialize.ts";
import type { TaggedProcess } from "../../lib/spawn.ts";

export type Mounts = {
  mount: string;
  assetsMount: string;
  uiMount: string;
  apiMount: string;
  ledgerMount: string;
};

export type AppCfg = {
  host: string;
  port: number;
  ledgerDirAbs: string;
  assetsDir: string;
  proxyEnabled: boolean;
};

export type SharedDeps = {
  app: Hono;
  cfg: AppCfg;
  mounts: Mounts;
  getProcesses: () => Promise<TaggedProcess[]>;
};

/* ---------------- shared utilities ---------------- */

export function toPosixPath(p: string) {
  return p.replaceAll("\\", "/");
}

export function stripLeadingSlash(p: string) {
  return p.startsWith("/") ? p.slice(1) : p;
}

export function safeJoin(rootAbs: string, requestPath: string) {
  const rp = stripLeadingSlash(toPosixPath(requestPath));
  const fullAbs = normalize(resolve(rootAbs, rp));
  if (fullAbs === rootAbs) return fullAbs;
  if (!fullAbs.startsWith(rootAbs + "/")) return null;
  return fullAbs;
}

export async function exists(p: string) {
  try {
    await Deno.stat(p);
    return true;
  } catch {
    return false;
  }
}

export async function readTextIfSmall(path: string, maxBytes = 2_000_000) {
  const st = await Deno.stat(path);
  if (!st.isFile) return null;
  if (st.size > maxBytes) return null;
  return await Deno.readTextFile(path);
}

export function guessContentType(path: string) {
  const p = path.toLowerCase();
  if (p.endsWith(".html")) return "text/html; charset=utf-8";
  if (p.endsWith(".css")) return "text/css; charset=utf-8";
  if (p.endsWith(".js") || p.endsWith(".mjs")) {
    return "text/javascript; charset=utf-8";
  }
  if (p.endsWith(".json")) return "application/json; charset=utf-8";
  if (p.endsWith(".log") || p.endsWith(".txt")) {
    return "text/plain; charset=utf-8";
  }
  if (p.endsWith(".svg")) return "image/svg+xml";
  if (p.endsWith(".png")) return "image/png";
  if (p.endsWith(".jpg") || p.endsWith(".jpeg")) return "image/jpeg";
  if (p.endsWith(".wasm")) return "application/wasm";
  return "application/octet-stream";
}

export function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

export function jsonError(message: string, status = 400) {
  return jsonResponse({ ok: false, error: message }, status);
}

export function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), ms);
  return Promise.race([
    promise,
    new Promise<T>((_, rej) =>
      ac.signal.addEventListener(
        "abort",
        () => rej(new Error(`timeout after ${ms}ms`)),
        { once: true },
      )
    ),
  ]).finally(() => clearTimeout(t));
}

/* ---------------- ledger HTML ---------------- */

export function htmlEscape(s: string) {
  return s
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

export function renderDirListing(params: {
  mountUrl: string;
  relPath: string;
  entries: Array<{ name: string; isDir: boolean; size?: number }>;
}) {
  const { mountUrl, relPath, entries } = params;
  const parts = relPath.split("/").filter(Boolean);
  const crumbs = [`<a href="${mountUrl}/">ledger.d</a>`]
    .concat(
      parts.map((p, i) => {
        const sub = parts.slice(0, i + 1).join("/");
        return `<a href="${mountUrl}/${sub}">${htmlEscape(p)}</a>`;
      }),
    )
    .join(" / ");

  const rows = entries
    .map((e) => {
      const href = `${mountUrl}/${[relPath, e.name].filter(Boolean).join("/")}`;
      const label = e.isDir ? `${e.name}/` : e.name;
      const meta = e.isDir
        ? "dir"
        : typeof e.size === "number"
        ? `${e.size} bytes`
        : "file";
      return `<tr>
        <td><a href="${href}">${htmlEscape(label)}</a></td>
        <td class="mono">${htmlEscape(meta)}</td>
      </tr>`;
    })
    .join("");

  return `<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Truth Yard ledger browser</title>
</head>
<body>
  <div>${crumbs}</div>
  <table>
    <thead><tr><th>Name</th><th>Type/Size</th></tr></thead>
    <tbody>${rows}</tbody>
  </table>
</body>
</html>`;
}

/* ---------------- shared route registration ---------------- */

export function registerRootRedirects(deps: SharedDeps) {
  const { app, mounts } = deps;
  const { uiMount, mount } = mounts;

  app.get("/", (c) => c.redirect(`${uiMount}/`));
  app.get(`${mount}`, (c) => c.redirect(`${uiMount}/`));
  app.get(`${mount}/`, (c) => c.redirect(`${uiMount}/`));
}

export function registerAssetsRoutes(deps: SharedDeps) {
  const { app, cfg, mounts } = deps;
  const { assetsMount } = mounts;
  const { assetsDir } = cfg;

  app.get(
    `${assetsMount}/*`,
    serveStatic({
      root: ".",
      rewriteRequestPath: (path) => {
        const rel = path.slice((assetsMount + "/").length);
        return `/${assetsDir}/${rel}`;
      },
    }),
  );
}

export function registerUiRoutes(deps: SharedDeps) {
  const { app, cfg, mounts } = deps;
  const { uiMount } = mounts;
  const { assetsDir } = cfg;

  app.get(`${uiMount}`, (c) => c.redirect(`${uiMount}/`));
  app.get(`${uiMount}/`, async (c) => {
    const indexPath = `${assetsDir}/index.html`;
    if (!(await exists(indexPath))) return c.text(`Missing ${indexPath}`, 500);
    return c.html(await Deno.readTextFile(indexPath));
  });
}

export function registerProcessesApi(deps: SharedDeps) {
  const { app, cfg, mounts, getProcesses } = deps;
  const { apiMount } = mounts;
  const { ledgerDirAbs } = cfg;

  app.get(`${apiMount}/tagged-processes.json`, async (c) => {
    const processes = await getProcesses();
    return c.json({
      now: new Date().toISOString(),
      ledgerDir: ledgerDirAbs,
      count: processes.length,
      taggedProcesses: processes,
    });
  });
}

export function registerReconcileApi(deps: SharedDeps) {
  const { app, cfg, mounts } = deps;
  const { apiMount } = mounts;
  const { ledgerDirAbs } = cfg;

  app.get(`${apiMount}/reconcile.json`, async (c) => {
    const items: ReconcileItem[] = [];
    const gen = reconcile(ledgerDirAbs);
    while (true) {
      const next = await gen.next();
      if (next.done) {
        return c.json({
          now: new Date().toISOString(),
          ledgerDir: ledgerDirAbs,
          summary: next.value,
          items,
        });
      }
      items.push(next.value);
    }
  });
}

export function registerLedgerBrowser(deps: SharedDeps) {
  const { app, cfg, mounts } = deps;
  const { ledgerMount } = mounts;
  const { ledgerDirAbs } = cfg;

  app.get(`${ledgerMount}`, (c) => c.redirect(`${ledgerMount}/`));
  app.get(`${ledgerMount}/*`, async (c) => {
    const rel = c.req.path.slice((ledgerMount + "/").length);
    const fsPath = safeJoin(ledgerDirAbs, rel);
    if (!fsPath) return c.text("Invalid path", 400);

    let st: Deno.FileInfo;
    try {
      st = await Deno.stat(fsPath);
    } catch {
      return c.text("Not found", 404);
    }

    if (st.isDirectory) {
      const entries: Array<{ name: string; isDir: boolean; size?: number }> =
        [];
      for await (const e of Deno.readDir(fsPath)) {
        const p = `${fsPath}/${e.name}`;
        let size: number | undefined;
        if (e.isFile) {
          try {
            size = (await Deno.stat(p)).size;
          } catch {
            // ignore
          }
        }
        entries.push({ name: e.name, isDir: e.isDirectory, size });
      }
      entries.sort((a, b) =>
        Number(b.isDir) - Number(a.isDir) || a.name.localeCompare(b.name)
      );
      return c.html(
        renderDirListing({ mountUrl: ledgerMount, relPath: rel, entries }),
      );
    }

    const ct = guessContentType(fsPath);
    if (
      ct.startsWith("text/") || ct.includes("json") ||
      ct.includes("javascript") || ct.includes("svg+xml")
    ) {
      const text = await readTextIfSmall(fsPath, 5_000_000);
      if (text !== null) return c.text(text, 200, { "content-type": ct });
    }

    const data = await Deno.readFile(fsPath);
    return new Response(data, { status: 200, headers: { "content-type": ct } });
  });
}

/* ---------------- wiring helpers ---------------- */

export function computeMounts(): Mounts {
  const mount = "/.truth-yard";
  return {
    mount,
    assetsMount: `${mount}/asset`,
    uiMount: `${mount}/ui`,
    apiMount: `${mount}/api`,
    ledgerMount: `${mount}/ledger.d`,
  };
}

export async function requireDir(path: string) {
  try {
    const st = await Deno.stat(path);
    if (!st.isDirectory) {
      console.error(`expected directory: ${path}`);
      Deno.exit(2);
    }
  } catch (e) {
    console.error(`not found: ${path}`);
    console.error(e);
    Deno.exit(2);
  }
}

export function logStartup(cfg: AppCfg, mounts: Mounts) {
  console.log(
    `Operational Truth Yard (OTY) web-ui listening on http://${cfg.host}:${cfg.port}`,
  );
  console.log(`UI: http://${cfg.host}:${cfg.port}${mounts.uiMount}/`);
  console.log(`Assets: http://${cfg.host}:${cfg.port}${mounts.assetsMount}/`);
  console.log(
    `API: http://${cfg.host}:${cfg.port}${mounts.apiMount}/tagged-processes.json`,
  );
  console.log(`Ledger: http://${cfg.host}:${cfg.port}${mounts.ledgerMount}/`);
}
