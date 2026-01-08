// lib/serve/web-ui.ts
import { dirname, fromFileUrl, join, resolve } from "@std/path";
import type { Path } from "../discover.ts";
import {
  formatBytes,
  formatWhen,
  listFilesRecursiveViaEncounters,
} from "../fs.ts";
import { type SpawnedStateEncounter, spawnedStates } from "../materialize.ts";
import { contentTypeByName, isSafeRelativeSubpath, joinUrl } from "../path.ts";
import {
  createSpawnSessionHome,
  pickLatestSessionHome,
  resolveCurrentSessionHome,
  resolveRootsAbs,
} from "../session.ts";
import { isPidAlive } from "../spawn.ts";
import {
  type WatchEvent,
  type WatchOptions,
  watchYardInSession,
} from "./watch.ts";

function nowMs() {
  return Date.now();
}

function jsonResponse(obj: unknown, status = 200): Response {
  return new Response(JSON.stringify(obj, null, 2), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

function escapeHtml(s: string) {
  return String(s ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

async function readAssetText(assetFileName: string): Promise<string> {
  const here = dirname(fromFileUrl(import.meta.url));
  const p = join(here, assetFileName);
  return await Deno.readTextFile(p);
}

type Running = SpawnedStateEncounter;

function proxyPrefixForRunning(r: Running): string {
  return r.context.service.proxyEndpointPrefix;
}

function pickRunningByPrefix(
  running: Running[],
  pathname: string,
): { running: Running; prefix: string } | undefined {
  let best: { running: Running; prefix: string } | undefined;

  for (const r of running) {
    const pfx = proxyPrefixForRunning(r);
    if (!pathname.startsWith(pfx)) continue;
    if (!best || pfx.length > best.prefix.length) {
      best = { running: r, prefix: pfx };
    }
  }

  return best;
}

async function proxyToTarget(
  req: Request,
  targetBase: URL,
  proxyEndpointPrefix: string,
): Promise<Response> {
  const u = new URL(req.url);

  const outUrl = new URL(targetBase.toString());
  outUrl.pathname = u.pathname;
  outUrl.search = u.search;

  const headers = new Headers(req.headers);
  headers.set("SQLPAGE_SITE_PREFIX", proxyEndpointPrefix);
  headers.set("yard-proxyEndpointPrefix", proxyEndpointPrefix);
  headers.set("host", outUrl.host);

  const init: RequestInit = {
    method: req.method,
    headers,
    redirect: "manual",
    body: (req.method === "GET" || req.method === "HEAD")
      ? undefined
      : req.body,
  };

  let resp: Response;
  try {
    resp = await fetch(outUrl, init);
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    return jsonResponse({
      ok: false,
      error: "proxy failed",
      target: outUrl.toString(),
      message: msg,
    }, 502);
  }

  return new Response(resp.body, {
    status: resp.status,
    headers: new Headers(resp.headers),
  });
}

async function runSqliteQueryViaCli(opts: {
  exec: string;
  dbPath: string;
  sql: string;
}): Promise<{
  exec: string;
  sql: string;
  ranAtMs: number;
  ok: boolean;
  exitCode?: number;
  output?: unknown;
  stderr?: string;
  note?: string;
}> {
  const ranAtMs = nowMs();
  const text = new TextDecoder();

  const tryModes: { args: string[]; mode: "json" | "line" }[] = [
    { args: ["-json", opts.dbPath, opts.sql], mode: "json" },
    { args: ["-line", opts.dbPath, opts.sql], mode: "line" },
  ];

  for (const m of tryModes) {
    try {
      const cmd = new Deno.Command(opts.exec, {
        args: m.args,
        stdin: "null",
        stdout: "piped",
        stderr: "piped",
      });
      const out = await cmd.output();
      const stdout = text.decode(out.stdout).trim();
      const stderr = text.decode(out.stderr).trim();

      if (!out.success) {
        if (
          m.mode === "json" &&
          /unknown option|unrecognized option|-json/i.test(stderr)
        ) continue;
        return {
          exec: opts.exec,
          sql: opts.sql,
          ranAtMs,
          ok: false,
          exitCode: out.code,
          stderr: stderr || undefined,
          output: stdout || undefined,
        };
      }

      if (m.mode === "json") {
        try {
          const parsed = stdout.length ? JSON.parse(stdout) : [];
          return {
            exec: opts.exec,
            sql: opts.sql,
            ranAtMs,
            ok: true,
            exitCode: out.code,
            stderr: stderr || undefined,
            output: parsed,
          };
        } catch {
          return {
            exec: opts.exec,
            sql: opts.sql,
            ranAtMs,
            ok: true,
            exitCode: out.code,
            stderr: stderr || undefined,
            output: stdout,
            note: "sqlite3 -json output could not be parsed; stored as text",
          };
        }
      }

      return {
        exec: opts.exec,
        sql: opts.sql,
        ranAtMs,
        ok: true,
        exitCode: out.code,
        stderr: stderr || undefined,
        output: stdout,
        note: "sqlite3 -line output stored as text",
      };
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      return {
        exec: opts.exec,
        sql: opts.sql,
        ranAtMs,
        ok: false,
        stderr: msg,
      };
    }
  }

  return {
    exec: opts.exec,
    sql: opts.sql,
    ranAtMs,
    ok: false,
    stderr: "No sqlite exec mode succeeded",
  };
}

function buildRootIndexHtml(args: { running: Running[] }): string {
  const running = args.running.slice().sort((a, b) =>
    proxyPrefixForRunning(a).localeCompare(proxyPrefixForRunning(b))
  );

  const rows = running.map((r) => {
    const prefix = proxyPrefixForRunning(r);
    const href = prefix;
    const upstream = `${r.context.listen.host}:${r.context.listen.port}`;
    const rel = r.context.supplier.location;
    return `<div class="row">
  <span class="col col-prefix"><a href="${escapeHtml(href)}">${
      escapeHtml(href)
    }</a></span>
  <span class="col col-up">${escapeHtml(upstream)}</span>
  <span class="col col-kind">${escapeHtml(r.context.service.kind)}</span>
  <span class="col col-rel">${escapeHtml(rel)}</span>
</div>`;
  }).join("\n");

  return `<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>yard</title>
  <link rel="stylesheet" href="/.web-ui/web-ui.css" />
</head>
<body>
  <div class="hdr">yard</div>
  <div class="muted">Available proxy paths</div>

  <div class="note">
    <a href="/.web-ui/web-ui.html">Admin UI</a> • <a href="/.admin">/.admin</a> (json) • <a href="/.admin/index.html">/.admin/index.html</a>
  </div>

  <div class="grid">
    <div class="row head">
      <span class="col col-prefix">proxy (click)</span>
      <span class="col col-up">upstream</span>
      <span class="col col-kind">kind</span>
      <span class="col col-rel">supplier location</span>
    </div>
    ${rows || `<div class="row"><span class="col muted">none</span></div>`}
  </div>
</body>
</html>`;
}

function buildAdminIndexHtml(args: {
  spawnedDir: string;
  files: {
    name: string;
    size: number;
    mtimeMs: number;
    kind: "json" | "log" | "other";
  }[];
  running: Running[];
}): string {
  const { spawnedDir, files, running } = args;

  const runningRows = running.map((r) => {
    const id = escapeHtml(r.context.service.id);
    const kind = escapeHtml(r.context.service.kind);
    const host = escapeHtml(
      `${r.context.listen.host}:${r.context.listen.port}`,
    );
    const prefix = escapeHtml(proxyPrefixForRunning(r));
    const db = escapeHtml(r.context.supplier.location);
    return `<div class="row">
  <span class="col col-id">${id}</span>
  <span class="col col-kind">${kind}</span>
  <span class="col col-host">${host}</span>
  <span class="col col-prefix"><a href="${
      escapeHtml(proxyPrefixForRunning(r))
    }">${prefix}</a></span>
  <span class="col col-db">${db}</span>
</div>`;
  }).join("\n");

  const fileRows = files.map((f) => {
    const nameEsc = escapeHtml(f.name);
    const href = `/.admin/files/${encodeURIComponent(f.name)}`;
    const size = escapeHtml(formatBytes(f.size));
    const mtime = escapeHtml(formatWhen(f.mtimeMs));
    const kind = escapeHtml(f.kind);
    return `<div class="row">
  <span class="col col-name"><a href="${href}">${nameEsc}</a></span>
  <span class="col col-kind">${kind}</span>
  <span class="col col-size">${size}</span>
  <span class="col col-mtime">${mtime}</span>
</div>`;
  }).join("\n");

  return `<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>yard admin</title>
  <link rel="stylesheet" href="/.web-ui/web-ui.css" />
</head>
<body>
  <div class="hdr">yard admin</div>
  <div class="muted">sessionHome: ${escapeHtml(spawnedDir)}</div>
  <div class="note"><a href="/">/</a> (proxy index) • <a href="/.web-ui/web-ui.html">Admin UI</a></div>

  <div class="section">
    <div class="hdr2">Running instances</div>
    <div class="grid">
      <div class="row head">
        <span class="col col-id">id</span>
        <span class="col col-kind">kind</span>
        <span class="col col-host">host</span>
        <span class="col col-prefix">proxy (click)</span>
        <span class="col col-db">supplier location</span>
      </div>
      ${
    runningRows || `<div class="row"><span class="col muted">none</span></div>`
  }
    </div>
  </div>

  <div class="section">
    <div class="hdr2">Session files (JSON + logs)</div>
    <div class="grid">
      <div class="row head">
        <span class="col col-name">name</span>
        <span class="col col-kind">kind</span>
        <span class="col col-size">size</span>
        <span class="col col-mtime">mtime</span>
      </div>
      ${
    fileRows || `<div class="row"><span class="col muted">none</span></div>`
  }
    </div>
  </div>

  <div class="section">
    <div class="hdr2">Unsafe SQL endpoint</div>
    <div class="muted">POST <code>/SQL/unsafe/&lt;serviceId&gt;.json</code> with JSON body <code>{"sql":"select 1"}</code>.</div>
  </div>

  <script src="/.web-ui/web-ui.js"></script>
</body>
</html>`;
}

export type WebUiOptions = Readonly<{
  webHost: string;
  webPort: number;

  spawnStateHome: string;
  srcRoots: string[];
  sqliteExec: string;

  /**
   * If true, start watcher in this process and share one stamped sessionHome with the UI.
   * If false, UI is read-only and will attach to current/latest session.
   */
  watch?: boolean;

  /**
   * Watcher options (debounce, periodic reconcile, spawn options, etc.)
   */
  watchOptions?: Omit<WatchOptions, "spawnStateHome">;

  /**
   * Optional hook for watch events.
   */
  onWatchEvent?: (e: WatchEvent) => void | Promise<void>;
}>;

export async function startWebUiServer(opts: WebUiOptions) {
  const spawnStateHome = resolve(opts.spawnStateHome);

  const srcPaths: Path[] = opts.srcRoots.map((p) => ({ path: p }));
  const rootsAbs = resolveRootsAbs(srcPaths);

  // Determine or create the session.
  const session =
    (opts.watch ? await createSpawnSessionHome(spawnStateHome) : undefined) ??
      (await resolveCurrentSessionHome(spawnStateHome)) ??
      (await pickLatestSessionHome(spawnStateHome)) ??
      (await createSpawnSessionHome(spawnStateHome));

  const sessionHome = session.sessionHome;

  const ac = new AbortController();

  // Start watcher (optional) in same process/session.
  let watcherPromise: Promise<void> | undefined;
  if (opts.watch) {
    const wo: WatchOptions = {
      spawnStateHome,
      ...(opts.watchOptions ?? {}),
      signal: ac.signal,
      onWatchEvent: opts.onWatchEvent,
    };

    watcherPromise = watchYardInSession(srcPaths, wo, {
      sessionHome,
      rootsAbs,
    });
  }

  const getRunning = async (): Promise<Running[]> => {
    const running: Running[] = [];
    for await (const st of spawnedStates(sessionHome)) running.push(st);
    // keep only those whose pid is alive if you want; for admin, show all.
    return running;
  };

  const handler = async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const pathname = url.pathname;

    // Serve web UI assets under /.web-ui
    if (
      req.method === "GET" &&
      (pathname === "/.web-ui" || pathname === "/.web-ui/")
    ) {
      return new Response("", {
        status: 302,
        headers: { location: "/.web-ui/web-ui.html" },
      });
    }
    if (req.method === "GET" && pathname.startsWith("/.web-ui/")) {
      const rel = pathname.slice("/.web-ui/".length);
      if (!isSafeRelativeSubpath(rel)) {
        return jsonResponse({ ok: false, error: "invalid asset name" }, 400);
      }

      if (rel === "web-ui.html") {
        const html = await readAssetText("web-ui.html");
        return new Response(html, {
          status: 200,
          headers: { "content-type": "text/html; charset=utf-8" },
        });
      }
      if (rel === "web-ui.css") {
        const css = await readAssetText("web-ui.css");
        return new Response(css, {
          status: 200,
          headers: { "content-type": "text/css; charset=utf-8" },
        });
      }
      if (rel === "web-ui.js") {
        const js = await readAssetText("web-ui.js");
        return new Response(js, {
          status: 200,
          headers: { "content-type": "text/javascript; charset=utf-8" },
        });
      }

      return jsonResponse({ ok: false, error: "not found" }, 404);
    }

    if (req.method === "GET" && pathname === "/") {
      const running = await getRunning();
      const html = buildRootIndexHtml({ running });
      return new Response(html, {
        status: 200,
        headers: { "content-type": "text/html; charset=utf-8" },
      });
    }

    if (req.method === "GET" && pathname === "/.admin") {
      const running = await getRunning();
      const items = running.map((r) => ({
        id: r.context.service.id,
        kind: r.context.service.kind,
        label: r.context.service.label,
        proxyEndpointPrefix: r.context.service.proxyEndpointPrefix,
        supplier: r.context.supplier,
        listen: r.context.listen,
        pid: r.pid,
        pidAlive: isPidAlive(r.pid),
        upstreamUrl: joinUrl(
          r.context.listen.baseUrl,
          r.context.service.proxyEndpointPrefix || "/",
        ),
        filePath: r.filePath,
      })).sort((a, b) =>
        `${a.kind}:${a.id}`.localeCompare(`${b.kind}:${b.id}`)
      );

      return jsonResponse({
        ok: true,
        nowMs: nowMs(),
        spawnStateHome,
        sessionHome,
        count: items.length,
        items,
      });
    }

    if (
      req.method === "GET" &&
      (pathname === "/.admin/index.html" || pathname === "/.admin/")
    ) {
      const hide = (name: string) =>
        name.startsWith(".db-yard.") ||
        name.endsWith(".tmp") ||
        name === "spawned-pids.txt";

      const files = await listFilesRecursiveViaEncounters({
        rootDir: sessionHome,
        globs: ["**/*"],
        hide,
      });

      const running = await getRunning();
      const html = buildAdminIndexHtml({
        spawnedDir: sessionHome,
        files: files.map((f) => ({
          name: f.name,
          size: f.size,
          mtimeMs: f.mtimeMs,
          kind: f.kind,
        })),
        running,
      });

      return new Response(html, {
        status: 200,
        headers: { "content-type": "text/html; charset=utf-8" },
      });
    }

    if (req.method === "GET" && pathname.startsWith("/.admin/files/")) {
      const rel = decodeURIComponent(pathname.slice("/.admin/files/".length))
        .replaceAll("\\", "/").replace(/^\/+/, "");
      if (!isSafeRelativeSubpath(rel)) {
        return jsonResponse({ ok: false, error: "invalid file name" }, 400);
      }

      const abs = resolve(`${sessionHome}/${rel}`);

      let st: Deno.FileInfo | undefined;
      try {
        st = await Deno.stat(abs);
      } catch {
        return jsonResponse({ ok: false, error: "not found" }, 404);
      }
      if (!st.isFile) {
        return jsonResponse({ ok: false, error: "not a file" }, 400);
      }

      let f: Deno.FsFile | undefined;
      try {
        f = await Deno.open(abs, { read: true });
        return new Response(f.readable, {
          status: 200,
          headers: { "content-type": contentTypeByName(rel) },
        });
      } catch (e) {
        try {
          f?.close();
        } catch { /* ignore */ }
        const msg = e instanceof Error ? e.message : String(e);
        return jsonResponse({
          ok: false,
          error: "failed to read file",
          message: msg,
        }, 500);
      }
    }

    if (pathname.startsWith("/SQL/unsafe/") && pathname.endsWith(".json")) {
      if (req.method !== "POST") {
        return jsonResponse({ ok: false, error: "POST required" }, 405);
      }

      const serviceId = pathname.slice("/SQL/unsafe/".length, -".json".length)
        .trim();
      if (!serviceId) {
        return jsonResponse({ ok: false, error: "missing id" }, 400);
      }

      const running = await getRunning();
      const picked = running.find((r) => r.context.service.id === serviceId);
      if (!picked) return jsonResponse({ ok: false, error: "unknown id" }, 404);

      let body: { sql?: unknown };
      try {
        body = await req.json();
      } catch {
        return jsonResponse({ ok: false, error: "invalid JSON body" }, 400);
      }

      const sql = typeof body.sql === "string" ? body.sql : "";
      if (!sql.trim()) {
        return jsonResponse({ ok: false, error: "missing sql" }, 400);
      }
      if (sql.length > 200_000) {
        return jsonResponse({ ok: false, error: "sql too large" }, 413);
      }

      const snap = await runSqliteQueryViaCli({
        exec: opts.sqliteExec,
        dbPatheno: undefined as never,
        dbPath: picked.context.supplier.location,
        sql,
        // deno-lint-ignore no-explicit-any
      } as any);

      return jsonResponse(
        {
          ok: snap.ok,
          db: {
            id: picked.context.service.id,
            path: picked.context.supplier.location,
            kind: picked.context.service.kind,
          },
          result: snap,
        },
        snap.ok ? 200 : 500,
      );
    }

    // Reverse proxy (all other paths)
    const running = await getRunning();
    const target = pickRunningByPrefix(running, pathname);
    if (target) {
      const { running: r } = target;
      const base = new URL(
        `http://${r.context.listen.host}:${r.context.listen.port}`,
      );
      return await proxyToTarget(
        req,
        base,
        r.context.service.proxyEndpointPrefix,
      );
    }

    return jsonResponse(
      {
        ok: false,
        error: "no proxy target",
        hint:
          "See '/' for prefixes, '/.web-ui/web-ui.html' for admin UI, or '/.admin/index.html'.",
      },
      404,
    );
  };

  Deno.serve(
    { hostname: opts.webHost, port: opts.webPort, signal: ac.signal },
    handler,
  );

  return {
    sessionHome,
    close: () => {
      try {
        ac.abort();
      } catch {
        // ignore
      }
    },
    watcher: watcherPromise,
  };
}
