// spawn-event.ts
import { bold, cyan, dim, green, magenta, red, yellow } from "@std/fmt/colors";

import type { SpawnEvent, SpawnEventListener } from "./spawn.ts";

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

function getString(o: Record<string, unknown>, k: string): string | undefined {
  const v = o[k];
  return typeof v === "string" ? v : undefined;
}

function getNumber(o: Record<string, unknown>, k: string): number | undefined {
  const v = o[k];
  return typeof v === "number" ? v : undefined;
}

function getRecord(
  o: Record<string, unknown>,
  k: string,
): Record<string, unknown> | undefined {
  const v = o[k];
  return isRecord(v) ? v : undefined;
}

function getArray(
  o: Record<string, unknown>,
  k: string,
): unknown[] | undefined {
  const v = o[k];
  return Array.isArray(v) ? v : undefined;
}

function fmtTime(ev: Record<string, unknown>): string {
  const tMs = getNumber(ev, "tMs") ?? 0;
  return dim(`[+${Math.round(tMs)}ms]`);
}

function fmtSessionId(ev: Record<string, unknown>): string {
  const session = getRecord(ev, "session");
  if (!session) return "";
  return dim(String(getString(session, "sessionId") ?? ""));
}

function pickServiceId(ev: Record<string, unknown>): string | undefined {
  const direct = getString(ev, "serviceId") ??
    getString(ev, "id") ??
    undefined;

  if (direct) return direct;

  const exposable = getRecord(ev, "exposable");
  const service = getRecord(ev, "service");
  const entry = getRecord(ev, "entry");
  const supplier = getRecord(ev, "supplier");

  return (exposable ? getString(exposable, "id") : undefined) ??
    (service ? getString(service, "id") : undefined) ??
    (entry ? getString(entry, "id") : undefined) ??
    (supplier ? getString(supplier, "id") : undefined) ??
    undefined;
}

function svc(ev: Record<string, unknown>): string {
  const id = pickServiceId(ev);
  return id ? cyan(id) : dim("(service)");
}

function pickPath(ev: Record<string, unknown>): string | undefined {
  return getString(ev, "path") ??
    getString(ev, "location") ??
    getString(ev, "filePath") ??
    getString(ev, "dbPath") ??
    undefined;
}

function pickUrl(ev: Record<string, unknown>): string | undefined {
  return getString(ev, "url") ??
    getString(ev, "baseUrl") ??
    getString(ev, "endpoint") ??
    getString(ev, "reachableUrl") ??
    undefined;
}

function fmtMaybe(v: unknown): string {
  if (v === undefined || v === null) return dim("(n/a)");
  return dim(String(v));
}

function stringifyError(err: unknown): string {
  if (err instanceof Error) return err.message;
  return String(err);
}

/**
 * A human-friendly SpawnEventListener that prints colored, emoji-rich output
 * suitable for terminal UIs.
 *
 * strategy:
 * - "essential": lifecycle + errors only
 * - "comprehensive": includes discovery, decisions, paths, probes, summaries
 */
export function richTextUISpawnEvents(
  strategy: "essential" | "comprehensive",
): SpawnEventListener {
  const showAll = strategy === "comprehensive";

  return (e: SpawnEvent) => {
    const ev: Record<string, unknown> = isRecord(e) ? e : {};
    const type = String(ev["type"] ?? "");

    // core lifecycle
    if (type === "session_start") {
      console.log(
        `${fmtTime(ev)} 🚀 ${bold("spawn session")} ${fmtSessionId(ev)}`,
      );
      return;
    }

    if (type === "session_end") {
      console.log(
        `${fmtTime(ev)} 🏁 ${bold("session end")} ${fmtSessionId(ev)}`,
      );

      const summary = getRecord(ev, "summary") ?? getRecord(ev, "result");
      if (summary && showAll) {
        const spawned = getArray(summary, "spawned");
        const skipped = getArray(summary, "skipped");
        const errored = getArray(summary, "errored");

        const spawnedN = spawned?.length ?? Number(summary["spawned"] ?? 0);
        const skippedN = skipped?.length ?? Number(summary["skipped"] ?? 0);
        const erroredN = errored?.length ?? Number(summary["errored"] ?? 0);

        console.log(
          `${fmtTime(ev)} 📊 ${bold("summary")} ` +
            `${green(String(spawnedN) + " spawned")}, ` +
            `${yellow(String(skippedN) + " skipped")}, ` +
            `${
              erroredN > 0
                ? red(String(erroredN) + " errored")
                : green("0 errored")
            }`,
        );

        const errors = getArray(summary, "errors");
        if (errors && errors.length > 0) {
          for (const item of errors) {
            if (!isRecord(item)) continue;
            const id = getString(item, "id") ?? "error";
            const err = stringifyError(item["error"]);
            console.error(`  ${red("•")} ${magenta(id)} ${red(err)}`);
          }
        }
      }
      return;
    }

    // discovery + decision
    if (type === "discovered") {
      if (!showAll) return;
      console.log(
        `${fmtTime(ev)} 🔍 discovered ${svc(ev)} ${fmtMaybe(pickPath(ev))}`,
      );
      return;
    }

    if (type === "expose_decision") {
      if (!showAll) return;

      const shouldSpawn = (typeof ev["shouldSpawn"] === "boolean")
        ? (ev["shouldSpawn"] as boolean)
        : (ev["decision"] !== false && ev["decision"] !== undefined);

      console.log(
        `${fmtTime(ev)} ${shouldSpawn ? "✅" : "⏭️"} expose ${svc(ev)} ${
          shouldSpawn ? green("spawn") : yellow("skip")
        }`,
      );
      return;
    }

    // port / spawning / spawned
    if (type === "port_allocated") {
      const port = getNumber(ev, "port") ??
        getNumber(getRecord(ev, "listen") ?? {}, "port");
      console.log(
        `${fmtTime(ev)} 🔌 ${svc(ev)} port ${cyan(String(port ?? "?"))}`,
      );
      return;
    }

    if (type === "spawning") {
      console.log(`${fmtTime(ev)} 🧬 spawning ${svc(ev)}`);
      return;
    }

    if (type === "spawned") {
      const pid = getNumber(ev, "pid") ??
        getNumber(getRecord(ev, "spawned") ?? {}, "pid");
      console.log(
        `${fmtTime(ev)} 🟢 spawned ${svc(ev)} pid=${dim(String(pid ?? "?"))}`,
      );
      return;
    }

    // paths + context
    if (type === "paths_resolved") {
      if (!showAll) return;

      const stdoutPath = getString(ev, "stdoutPath") ?? getString(ev, "stdout");
      const stderrPath = getString(ev, "stderrPath") ?? getString(ev, "stderr");
      const contextPath = getString(ev, "contextPath") ??
        getString(ev, "context");

      if (stdoutPath) {
        console.log(`${fmtTime(ev)} 📄 stdout → ${dim(stdoutPath)}`);
      }
      if (stderrPath) {
        console.log(`${fmtTime(ev)} 📄 stderr → ${dim(stderrPath)}`);
      }
      if (contextPath) {
        console.log(`${fmtTime(ev)} 🧾 context → ${dim(contextPath)}`);
      }
      return;
    }

    if (type === "context_written") {
      if (!showAll) return;
      console.log(
        `${fmtTime(ev)} 🧾 context written ${svc(ev)} → ${
          fmtMaybe(pickPath(ev))
        }`,
      );
      return;
    }

    // reachability and probe events (names may vary; handle generically)
    if (type === "reachability_probe_started") {
      if (!showAll) return;
      console.log(
        `${fmtTime(ev)} 🧪 probe start ${svc(ev)} ${
          fmtMaybe(pickUrl(ev) ?? pickPath(ev))
        }`,
      );
      return;
    }

    // If your spawn.ts uses a different event name, it will still land here as a generic event.
    if (
      type.includes("reachability") || type.includes("probe") ||
      type.includes("reachable")
    ) {
      if (!showAll && !type.includes("reachable")) return;

      const url = pickUrl(ev);
      const err = ev["error"];
      if (err !== undefined) {
        console.log(
          `${fmtTime(ev)} ⚠️ ${svc(ev)} ${yellow(type)} ${
            dim(stringifyError(err))
          }`,
        );
      } else {
        console.log(
          `${fmtTime(ev)} 🌐 ${svc(ev)} ${green(type)} ${fmtMaybe(url)}`,
        );
      }
      return;
    }

    // error
    if (type === "error") {
      const id = getString(ev, "id") ?? getString(ev, "where") ?? "error";
      console.error(
        `${fmtTime(ev)} 💥 ${red(id)} ${magenta(stringifyError(ev["error"]))}`,
      );
      return;
    }

    // fallback
    if (showAll) {
      console.log(
        `${fmtTime(ev)} ℹ️ ${dim("event")} ${dim(type)} ${
          fmtMaybe(pickPath(ev))
        }`,
      );
    }
  };
}
