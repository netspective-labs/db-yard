/**
 * lib/admin.ts
 *
 * Optional admin server, isolated.
 * Reads running state via accessor and runs ad-hoc SQL via sqlite CLI runner.
 */
import type { Running } from "./governance.ts";
import { jsonResponse, nowMs, runSqliteQueryViaCli } from "./governance.ts";

export function startAdminServer(args: {
  adminHost: string;
  adminPort: number;
  getRunning(): Running[];
  spawnedDir: string;
  sqliteExec: string;
}) {
  const { adminHost, adminPort, getRunning, spawnedDir, sqliteExec } = args;

  type SqlUnsafeBody = { sql?: unknown };

  const handler = async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const pathname = url.pathname;

    if (req.method === "GET" && pathname === "/admin") {
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

    if (pathname.startsWith("/sql-unsafe/") && pathname.endsWith(".json")) {
      if (req.method !== "POST") {
        return jsonResponse({ ok: false, error: "POST required" }, 405);
      }

      const id = pathname.slice("/sql-unsafe/".length, -".json".length).trim();
      if (!id) return jsonResponse({ ok: false, error: "missing id" }, 400);

      const running = getRunning().find((r) => r.record.id === id);
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

    return jsonResponse({ ok: false, error: "not found" }, 404);
  };

  const ac = new AbortController();

  try {
    Deno.serve(
      { hostname: adminHost, port: adminPort, signal: ac.signal },
      handler,
    );
    console.log(
      `[admin] listening on http://${adminHost}:${adminPort} (/admin, /sql-unsafe/<id>.json)`,
    );
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e);
    console.warn(
      `[admin] failed to start on ${adminHost}:${adminPort}: ${msg}`,
    );
  }

  return { close: () => ac.abort() };
}
