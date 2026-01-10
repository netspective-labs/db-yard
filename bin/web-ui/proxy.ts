// bin/web-ui/proxy.ts

import type { Context, Hono } from "jsr:@hono/hono@4.11.3";
import { proxy } from "jsr:@hono/hono@4.11.3/proxy";
import type { TaggedProcess } from "../../lib/spawn.ts";
import { jsonError, jsonResponse, withTimeout } from "./app.ts";

export type ProxyRoute = { basePath: string; upstreamUrl: string };
export type ProxyConflict = { basePath: string; upstreamUrls: string[] };

export type ProxyDeps = {
  app: Hono;
  mount: string;
  uiMount: string;
  apiMount: string;
  ledgerDir: string;
  getProcesses: () => Promise<TaggedProcess[]>;
};

const REDACT = new Set([
  "authorization",
  "proxy-authorization",
  "cookie",
  "set-cookie",
  "x-api-key",
  "x-amz-security-token",
  "x-forwarded-client-cert",
]);

function normalizeBasePath(p: string) {
  const s = p.trim();
  if (!s) return "";
  const withSlash = s.startsWith("/") ? s : `/${s}`;
  return withSlash.length > 1 ? withSlash.replace(/\/+$/, "") : withSlash;
}

export function buildProxyTableWithConflicts(processes: TaggedProcess[]) {
  const raw: ProxyRoute[] = [];
  const byBase = new Map<string, Set<string>>();

  for (const p of processes) {
    const upstreamUrl = typeof p.upstreamUrl === "string" ? p.upstreamUrl : "";
    if (!upstreamUrl) continue;

    const pep = typeof p.proxyEndpointPrefix === "string"
      ? String(p.proxyEndpointPrefix)
      : "";
    const basePath = normalizeBasePath(pep);
    if (basePath) raw.push({ basePath, upstreamUrl });

    const sid = typeof p.serviceId === "string" ? p.serviceId.trim() : "";
    if (sid && !sid.includes("/")) {
      raw.push({ basePath: `/${sid}`, upstreamUrl });
    }
  }

  const seen = new Set<string>();
  const table = raw
    .map((r) => ({ ...r, basePath: r.basePath.replace(/\/+$/, "") || "/" }))
    .filter((r) => {
      if (!byBase.has(r.basePath)) byBase.set(r.basePath, new Set());
      byBase.get(r.basePath)!.add(r.upstreamUrl);
      if (seen.has(r.basePath)) return false;
      seen.add(r.basePath);
      return true;
    })
    .sort((a, b) => b.basePath.length - a.basePath.length);

  const conflicts: ProxyConflict[] = [];
  for (const [basePath, urls] of byBase.entries()) {
    if (urls.size > 1) {
      conflicts.push({ basePath, upstreamUrls: [...urls].sort() });
    }
  }
  conflicts.sort((a, b) => b.basePath.length - a.basePath.length);

  return { table, conflicts };
}

export function resolveProxyPath(pathname: string, routes: ProxyRoute[]) {
  const path = pathname.startsWith("/") ? pathname : `/${pathname}`;
  const route = routes.find((r) =>
    path === r.basePath || path.startsWith(r.basePath + "/")
  );
  if (!route) return null;
  const rest = path.slice(route.basePath.length) || "/";
  return { route, rest };
}

function headersToObject(h: Headers, opts?: { redact?: boolean }) {
  const obj: Record<string, string> = {};
  for (const [k, v] of h.entries()) {
    const key = k.toLowerCase();
    if (opts?.redact && REDACT.has(key)) obj[key] = "(redacted)";
    else obj[key] = v;
  }
  return obj;
}

function traceRequested(c: Context) {
  const u = new URL(c.req.url);
  if (u.searchParams.get("__db_yard_trace") === "1") return true;
  const hv = c.req.header("x-db-yard-trace");
  if (!hv) return false;
  return hv === "1" || hv.toLowerCase() === "true" ||
    hv.toLowerCase() === "yes";
}

function getOrMakeTraceId(c: Context) {
  const provided = c.req.header("x-db-yard-trace-id");
  return (provided && provided.trim()) ? provided.trim() : crypto.randomUUID();
}

function applyTraceHeaders(
  resp: Response,
  trace: {
    traceId: string;
    matchedBasePath?: string;
    upstreamUrl?: string;
    rest?: string;
  },
) {
  const out = new Headers(resp.headers);
  out.set("x-db-yard-trace-id", trace.traceId);
  if (trace.matchedBasePath) {
    out.set("x-db-yard-matched-basepath", trace.matchedBasePath);
  }
  if (trace.upstreamUrl) out.set("x-db-yard-upstream", trace.upstreamUrl);
  if (trace.rest) out.set("x-db-yard-rest", trace.rest);

  return new Response(resp.body, {
    status: resp.status,
    statusText: resp.statusText,
    headers: out,
  });
}

async function fetchWithTimeout(
  url: string,
  init: RequestInit,
  timeoutMs: number,
) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), timeoutMs);
  try {
    return await fetch(url, { ...init, signal: ac.signal });
  } finally {
    clearTimeout(t);
  }
}

function previewBodyIfSmall(bodyText: string, maxChars = 4000) {
  if (bodyText.length <= maxChars) return bodyText;
  return bodyText.slice(0, maxChars) + "\n…(truncated)…";
}

function makeProxiedUrl(upstreamUrl: string, rest: string, reqUrl?: string) {
  const target = new URL(upstreamUrl);
  const proxied = new URL(rest, target);
  if (reqUrl) proxied.search = new URL(reqUrl).search;
  return proxied.toString();
}

/*
Fix for issue #11:
If the browser navigates to an absolute path like "/foo" from inside a proxied page,
the request loses the basePath prefix. We infer the basePath from Referer and retry.
*/
function inferBasePathFromReferer(c: Context, routes: ProxyRoute[]) {
  const ref = c.req.header("referer");
  if (!ref) return null;

  try {
    const u = new URL(ref);
    const resolved = resolveProxyPath(u.pathname, routes);
    return resolved?.route.basePath ?? null;
  } catch {
    return null;
  }
}

export function registerProxyApiRoutes(deps: ProxyDeps) {
  const { app, apiMount, uiMount, ledgerDir, getProcesses } = deps;

  app.get(`${apiMount}/proxy-table.json`, async (c) => {
    const processes = await getProcesses();
    const { table, conflicts } = buildProxyTableWithConflicts(processes);
    return c.json({
      now: new Date().toISOString(),
      ledgerDir,
      count: table.length,
      proxyTable: table,
      proxyConflicts: conflicts,
    });
  });

  app.get(`${apiMount}/proxy-resolve.json`, async (c) => {
    const url = new URL(c.req.url);
    const path = url.searchParams.get("path") ?? "";
    if (!path) return jsonError('missing query param "path"', 400);

    const processes = await getProcesses();
    const { table } = buildProxyTableWithConflicts(processes);

    const resolved = resolveProxyPath(path, table);
    if (!resolved) {
      return jsonResponse({ ok: false, input: path, resolved: null }, 200);
    }

    return jsonResponse({
      ok: true,
      input: path,
      matchBasePath: resolved.route.basePath,
      upstreamUrl: resolved.route.upstreamUrl,
      rest: resolved.rest,
      proxiedUrl: makeProxiedUrl(
        resolved.route.upstreamUrl,
        resolved.rest,
        c.req.url,
      ),
    });
  });

  app.get(`${apiMount}/health.json`, async (c) => {
    const url = new URL(c.req.url);
    const timeoutMs = Number(url.searchParams.get("timeoutMs") ?? "1500");
    const max = Number(url.searchParams.get("max") ?? "50");

    const processes = await getProcesses();
    const { table } = buildProxyTableWithConflicts(processes);
    const checks = table.slice(0, Math.max(0, Math.min(max, table.length)));

    const results = await Promise.all(
      checks.map(async (r) => {
        const started = Date.now();
        try {
          const res = await withTimeout(
            fetch(new URL(r.upstreamUrl).toString(), { method: "GET" }),
            timeoutMs,
          );
          const ms = Date.now() - started;
          return {
            basePath: r.basePath,
            upstreamUrl: r.upstreamUrl,
            ok: res.ok,
            status: res.status,
            ms,
          };
        } catch (e) {
          const ms = Date.now() - started;
          return {
            basePath: r.basePath,
            upstreamUrl: r.upstreamUrl,
            ok: false,
            error: e instanceof Error ? e.message : String(e),
            ms,
          };
        }
      }),
    );

    return c.json({
      now: new Date().toISOString(),
      timeoutMs,
      count: results.length,
      results,
    });
  });

  app.get(`${apiMount}/proxy-debug.json`, async (c) => {
    const url = new URL(c.req.url);
    const path = url.searchParams.get("path") ?? "";
    if (!path) {
      return c.json({ ok: false, error: 'missing query param "path"' }, 400);
    }

    const processes = await getProcesses();
    const { table, conflicts } = buildProxyTableWithConflicts(processes);
    const resolved = resolveProxyPath(path, table);

    const inbound = headersToObject(c.req.raw.headers, { redact: true });
    const forwarded = headersToObject(c.req.raw.headers, { redact: true });

    return c.json({
      ok: true,
      now: new Date().toISOString(),
      ledgerDir,
      input: path,
      uiHint: `${uiMount}/`,
      matched: resolved
        ? {
          basePath: resolved.route.basePath,
          upstreamUrl: resolved.route.upstreamUrl,
          rest: resolved.rest,
          proxiedUrl: makeProxiedUrl(resolved.route.upstreamUrl, resolved.rest),
        }
        : null,
      proxyConflicts: conflicts,
      inboundHeaders: inbound,
      forwardedHeaders: forwarded,
      note:
        "Headers are redacted for safety. This shows what db-yard received and what it will forward upstream.",
    });
  });

  app.get(`${apiMount}/proxy-roundtrip.json`, async (c) => {
    const url = new URL(c.req.url);
    const path = url.searchParams.get("path") ?? "";
    const timeoutMs = Number(url.searchParams.get("timeoutMs") ?? "1500");
    if (!path) {
      return c.json({ ok: false, error: 'missing query param "path"' }, 400);
    }

    const processes = await getProcesses();
    const { table } = buildProxyTableWithConflicts(processes);
    const resolved = resolveProxyPath(path, table);
    if (!resolved) {
      return c.json({ ok: false, input: path, resolved: null }, 404);
    }

    const proxiedUrl = makeProxiedUrl(
      resolved.route.upstreamUrl,
      resolved.rest,
    );

    const started = Date.now();
    try {
      const res = await fetchWithTimeout(
        proxiedUrl,
        { method: "GET", headers: c.req.raw.headers },
        timeoutMs,
      );
      const ms = Date.now() - started;

      const ct = res.headers.get("content-type") ?? "";
      let preview: string | null = null;

      if (
        ct.includes("text/") || ct.includes("json") || ct.includes("xml") ||
        ct.includes("html")
      ) {
        const txt = await res.text();
        preview = previewBodyIfSmall(txt);
      }

      return c.json({
        ok: true,
        now: new Date().toISOString(),
        input: path,
        matchedBasePath: resolved.route.basePath,
        upstreamUrl: resolved.route.upstreamUrl,
        proxiedUrl,
        ms,
        status: res.status,
        responseHeaders: headersToObject(res.headers, { redact: true }),
        bodyPreview: preview,
      });
    } catch (e) {
      const ms = Date.now() - started;
      return c.json(
        {
          ok: false,
          input: path,
          matchedBasePath: resolved.route.basePath,
          upstreamUrl: resolved.route.upstreamUrl,
          proxiedUrl,
          ms,
          error: e instanceof Error ? e.message : String(e),
        },
        502,
      );
    }
  });

  app.get(`${apiMount}/trace-help.json`, (c) => {
    return c.json({
      ok: true,
      howTo: [
        "Add ?__db_yard_trace=1 to any proxied request URL, OR send header: x-db-yard-trace: 1",
        "db-yard will add response headers like x-db-yard-trace-id, x-db-yard-matched-basepath, x-db-yard-upstream, x-db-yard-rest",
        "Use x-db-yard-trace-id to correlate with server logs (one JSON line per traced request).",
      ],
      redaction: [...REDACT.values()],
    });
  });
}

export function registerCatchAllProxy(deps: ProxyDeps) {
  const { app, mount, uiMount, getProcesses } = deps;

  app.all("*", async (c) => {
    if (c.req.path.startsWith(`${mount}/`)) return c.notFound();

    const processes = await getProcesses();
    const { table } = buildProxyTableWithConflicts(processes);

    const reqPath = c.req.path;
    let resolved = resolveProxyPath(reqPath, table);

    // Issue #11 fix: retry using Referer-derived basePath if the current path has no mapping.
    if (!resolved) {
      const inferredBase = inferBasePathFromReferer(c, table);
      if (
        inferredBase && !reqPath.startsWith(inferredBase + "/") &&
        reqPath !== inferredBase
      ) {
        resolved = resolveProxyPath(inferredBase + reqPath, table);
      }
    }

    if (!resolved) {
      return c.text(
        `No upstream mapping for "${reqPath}". Try ${uiMount}/`,
        502,
      );
    }

    const proxiedUrl = makeProxiedUrl(
      resolved.route.upstreamUrl,
      resolved.rest,
      c.req.url,
    );

    const traceOn = traceRequested(c);
    const traceId = traceOn ? getOrMakeTraceId(c) : "";
    const started = Date.now();

    const resp = await proxy(proxiedUrl, {
      method: c.req.method,
      headers: c.req.raw.headers,
      body: c.req.raw.body,
    });

    if (!traceOn) return resp;

    const traced = applyTraceHeaders(resp, {
      traceId,
      matchedBasePath: resolved.route.basePath,
      upstreamUrl: resolved.route.upstreamUrl,
      rest: resolved.rest,
    });

    const ms = Date.now() - started;
    console.log(JSON.stringify({
      ts: new Date().toISOString(),
      traceId,
      method: c.req.method,
      path: c.req.path,
      matchedBasePath: resolved.route.basePath,
      upstreamUrl: resolved.route.upstreamUrl,
      rest: resolved.rest,
      status: traced.status,
      ms,
    }));

    return traced;
  });
}
