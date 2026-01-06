// lib/web-ui.ts
/**
 * Optional web UI + reverse proxy.
 *
 * Endpoints:
 * - GET  /.admin                 -> JSON runtime state
 * - GET  /.admin/index.html       -> HTML directory-style listing of JSON + logs in spawnedDir
 * - GET  /.admin/files/<name>     -> serve a file from spawnedDir (json/log/txt)
 * - POST /SQL/unsafe/<id>.json    -> run ad-hoc SQL against a known DB (UNSAFE)
 * - ALL OTHER PATHS              -> reverse proxy to a spawned service
 *
 * Reverse proxy routing:
 * - If path starts with "/<id>/...", proxies to that instance id (strips "/<id>")
 * - Else if exactly one instance is running, proxies to it (path unchanged)
 * - Else 404 with hint to use "/<id>/..."
 *
 * Platform notes:
 * - Reverse proxy uses fetch() streaming and assumes local http targets.
 */
import type { Running } from "./governance.ts";

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
  return s
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function contentTypeByName(name: string): string {
  const n = name.toLowerCase();
  if (n.endsWith(".json")) return "application/json; charset=utf-8";
  if (n.endsWith(".html") || n.endsWith(".htm")) {
    return "text/html; charset=utf-8";
  }
  if (n.endsWith(".log") || n.endsWith(".txt")) {
    return "text/plain; charset=utf-8";
  }
  return "application/octet-stream";
}

async function safeListSpawnedFiles(spawnedDir: string): Promise<
  {
    name: string;
    size: number;
    mtimeMs: number;
    kind: "json" | "log" | "other";
  }[]
> {
  const out: {
    name: string;
    size: number;
    mtimeMs: number;
    kind: "json" | "log" | "other";
  }[] = [];
  try {
    for await (const e of Deno.readDir(spawnedDir)) {
      if (!e.isFile) continue;
      const name = e.name;
      // hide owner token file, pid file, temp files
      if (name.startsWith(".db-yard.")) continue;
      if (name.endsWith(".tmp")) continue;
      if (name === "spawned-pids.txt") continue;

      const p = `${spawnedDir}/${name}`;
      let st: Deno.FileInfo | undefined;
      try {
        st = await Deno.stat(p);
      } catch {
        continue;
      }
      const kind = name.endsWith(".json")
        ? "json"
        : (name.endsWith(".stdout.log") || name.endsWith(".stderr.log")
          ? "log"
          : "other");
      out.push({
        name,
        size: st.size,
        mtimeMs: st.mtime?.getTime() ?? 0,
        kind,
      });
    }
  } catch {
    // ignore
  }

  out.sort((a, b) => a.name.localeCompare(b.name));
  return out;
}

function formatBytes(n: number): string {
  if (!Number.isFinite(n) || n < 0) return "-";
  if (n < 1024) return `${n} B`;
  const kb = n / 1024;
  if (kb < 1024) return `${kb.toFixed(1)} KB`;
  const mb = kb / 1024;
  if (mb < 1024) return `${mb.toFixed(1)} MB`;
  const gb = mb / 1024;
  return `${gb.toFixed(1)} GB`;
}

function formatWhen(ms: number): string {
  if (!Number.isFinite(ms) || ms <= 0) return "-";
  try {
    return new Date(ms).toISOString();
  } catch {
    return "-";
  }
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

  const runningRows = running
    .map((r) => {
      const rec = r.record;
      const id = escapeHtml(rec.id);
      const kind = escapeHtml(rec.kind);
      const host = escapeHtml(`${rec.listenHost}:${rec.port}`);
      const db = escapeHtml(rec.dbPath);
      const href = `/${encodeURIComponent(rec.id)}/`;
      return `<div class="row">
  <span class="col col-id"><a href="${href}">${id}</a></span>
  <span class="col col-kind">${kind}</span>
  <span class="col col-host">${host}</span>
  <span class="col col-db">${db}</span>
</div>`;
    })
    .join("\n");

  const fileRows = files
    .map((f) => {
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
    })
    .join("\n");

  return `<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>db-yard admin</title>
  <style>
    body { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; margin: 18px; }
    .muted { color: #666; }
    .section { margin-top: 18px; }
    .hdr { font-size: 14px; margin: 8px 0; }
    .grid { border: 1px solid #ddd; border-radius: 8px; overflow: hidden; }
    .row { display: flex; gap: 10px; padding: 8px 10px; border-top: 1px solid #eee; }
    .row:first-child { border-top: none; }
    .row.head { background: #f7f7f7; font-weight: 600; }
    .col { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .col-id { width: 240px; }
    .col-kind { width: 90px; }
    .col-host { width: 160px; }
    .col-db { flex: 1; }
    .col-name { flex: 1; }
    .col-size { width: 110px; text-align: right; }
    .col-mtime { width: 220px; }
    a { color: #0645ad; text-decoration: none; }
    a:hover { text-decoration: underline; }
    .note { font-size: 12px; margin-top: 8px; }
    code { background: #f4f4f4; padding: 2px 4px; border-radius: 4px; }
  </style>
</head>
<body>
  <div class="hdr">db-yard admin</div>
  <div class="muted">spawnedDir: ${escapeHtml(spawnedDir)}</div>

  <div class="section">
    <div class="hdr">Running instances</div>
    <div class="grid">
      <div class="row head">
        <span class="col col-id">id (click to proxy)</span>
        <span class="col col-kind">kind</span>
        <span class="col col-host">host</span>
        <span class="col col-db">dbPath</span>
      </div>
      ${
    runningRows || `<div class="row"><span class="col muted">none</span></div>`
  }
    </div>
    <div class="note muted">
      Proxy routing: <code>/${
    escapeHtml("<id>")
  }/...</code> goes to that instance. If only one instance is running, <code>/...</code> proxies to it.
    </div>
  </div>

  <div class="section">
    <div class="hdr">Session files (JSON + logs)</div>
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
    <div class="note muted">
      Raw JSON and logs are served under <code>/.admin/files/&lt;name&gt;</code>.
    </div>
  </div>

  <div class="section">
    <div class="hdr">Unsafe SQL endpoint</div>
    <div class="muted">
      POST <code>/SQL/unsafe/&lt;id&gt;.json</code> with JSON body <code>{"sql":"select 1"}</code>.
    </div>
  </div>
</body>
</html>`;
}

function findRunningById(
  getRunning: () => Running[],
  id: string,
): Running | undefined {
  const t = id.trim();
  if (!t) return undefined;
  return getRunning().find((r) => r.record.id === t);
}

function pickDefaultRunning(getRunning: () => Running[]): Running | undefined {
  const items = getRunning();
  if (items.length === 1) return items[0];
  return undefined;
}

async function proxyToTarget(req: Request, targetBase: URL): Promise<Response> {
  const u = new URL(req.url);

  const outUrl = new URL(targetBase.toString());
  outUrl.pathname = u.pathname;
  outUrl.search = u.search;

  // copy headers, but set Host to target host
  const headers = new Headers(req.headers);
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

  // passthrough response
  const respHeaders = new Headers(resp.headers);
  return new Response(resp.body, { status: resp.status, headers: respHeaders });
}

export function startWebUiServer(args: {
  webHost: string;
  webPort: number;
  getRunning(): Running[];
  spawnedDir: string;
  sqliteExec: string;
}) {
  const { webHost, webPort, getRunning, spawnedDir, sqliteExec } = args;

  type SqlUnsafeBody = { sql?: unknown };

  const handler = async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const pathname = url.pathname;

    // /.admin JSON
    if (req.method === "GET" && pathname === "/.admin") {
      const items = getRunning().map((r) => r.record).sort((a, b) => {
        const ak = `${a.kind}:${a.dbBasename}:${a.dbPath}`;
        const bk = `${b.kind}:${b.dbBasename}:${b.dbPath}`;
        return ak.localeCompare(bk);
      });

      return jsonResponse({
        ok: true,
        nowMs: nowMs(),
        spawnedDir,
        count: items.length,
        items,
      });
    }

    // /.admin/index.html
    if (
      req.method === "GET" &&
      (pathname === "/.admin/index.html" || pathname === "/.admin/")
    ) {
      const files = await safeListSpawnedFiles(spawnedDir);
      const running = [...getRunning()].sort((a, b) =>
        a.record.id.localeCompare(b.record.id)
      );
      const html = buildAdminIndexHtml({ spawnedDir, files, running });
      return new Response(html, {
        status: 200,
        headers: { "content-type": "text/html; charset=utf-8" },
      });
    }

    // serve a file from spawnedDir
    if (req.method === "GET" && pathname.startsWith("/.admin/files/")) {
      const name = decodeURIComponent(pathname.slice("/.admin/files/".length));
      if (
        !name || name.includes("/") || name.includes("\\") ||
        name.includes("\0")
      ) {
        return jsonResponse({ ok: false, error: "invalid file name" }, 400);
      }
      const p = `${spawnedDir}/${name}`;
      let st: Deno.FileInfo | undefined;
      try {
        st = await Deno.stat(p);
      } catch {
        return jsonResponse({ ok: false, error: "not found" }, 404);
      }
      if (!st.isFile) {
        return jsonResponse({ ok: false, error: "not a file" }, 400);
      }

      // stream file
      let f: Deno.FsFile | undefined;
      try {
        f = await Deno.open(p, { read: true });
        return new Response(f.readable, {
          status: 200,
          headers: { "content-type": contentTypeByName(name) },
        });
      } catch (e) {
        try {
          f?.close();
        } catch {
          // ignore
        }
        const msg = e instanceof Error ? e.message : String(e);
        return jsonResponse({
          ok: false,
          error: "failed to read file",
          message: msg,
        }, 500);
      }
    }

    // Unsafe SQL
    // POST /SQL/unsafe/<id>.json
    if (pathname.startsWith("/SQL/unsafe/") && pathname.endsWith(".json")) {
      if (req.method !== "POST") {
        return jsonResponse({ ok: false, error: "POST required" }, 405);
      }

      const id = pathname.slice("/SQL/unsafe/".length, -".json".length).trim();
      if (!id) return jsonResponse({ ok: false, error: "missing id" }, 400);

      const running = findRunningById(getRunning, id);
      if (!running) {
        return jsonResponse({ ok: false, error: "unknown id" }, 404);
      }

      let body: SqlUnsafeBody;
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
        exec: sqliteExec,
        dbPath: running.record.dbPath,
        sql,
      });

      return jsonResponse(
        {
          ok: snap.ok,
          db: {
            id: running.record.id,
            path: running.record.dbPath,
            kind: running.record.kind,
          },
          result: snap,
        },
        snap.ok ? 200 : 500,
      );
    }

    // Reverse proxy (all other paths)
    const segments = pathname.split("/").filter(Boolean);
    const first = segments[0] ?? "";

    let target = findRunningById(getRunning, first);
    if (target) {
      // strip /<id> prefix
      const rest = "/" + segments.slice(1).join("/");
      const u2 = new URL(req.url);
      u2.pathname = rest === "/" ? "/" : rest;
      const req2 = new Request(u2.toString(), req);
      const base = new URL(
        `http://${target.record.listenHost}:${target.record.port}`,
      );
      return await proxyToTarget(req2, base);
    }

    target = pickDefaultRunning(getRunning);
    if (target) {
      const base = new URL(
        `http://${target.record.listenHost}:${target.record.port}`,
      );
      return await proxyToTarget(req, base);
    }

    return jsonResponse(
      {
        ok: false,
        error: "no proxy target",
        hint:
          "Use '/.admin/index.html' or '/<id>/' when multiple instances are running.",
      },
      404,
    );
  };

  const ac = new AbortController();

  try {
    Deno.serve(
      { hostname: webHost, port: webPort, signal: ac.signal },
      handler,
    );
    console.log(
      `[web-ui] listening on http://${webHost}:${webPort} (/.admin, /.admin/index.html, /SQL/unsafe/<id>.json, proxy /<id>/...)`,
    );
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e);
    console.warn(`[web-ui] failed to start on ${webHost}:${webPort}: ${msg}`);
  }

  return { close: () => ac.abort() };
}
