#!/usr/bin/env -S deno run --allow-all

import { doctor } from "jsr:@spry/universal";

const api = doctor([
  "deno --version",
  {
    type: "group",
    label: "Git dependencies",
    items: [
      {
        type: "custom",
        label: "Git hooks setup",
        run: async (ctx) => {
          const hooksResult = await ctx.spawnText("git config core.hooksPath");
          const hooksPath = hooksResult.stdout.trim();

          if (hooksPath.length === 0) {
            return { kind: "warn" as const, message: "Git hooks not setup, run `deno task init`" };
          }

          try {
            const findResult = await ctx.spawnText(`find ${hooksPath} -maxdepth 1 -type f`);
            const hookFiles = findResult.stdout.split('\n').filter(f => f.trim().length > 0);

            if (hookFiles.length === 0) {
              return { kind: "suggest" as const, message: `No hooks found in ${hooksPath}` };
            }

            const reports = [];
            for (const hook of hookFiles) {
              try {
                const info = await Deno.stat(hook);
                const isExecutable = info.mode ? (info.mode & 0o111) !== 0 : false;
                if (isExecutable) {
                  reports.push({ kind: "ok" as const, message: `Git hook executable: ${hook}` });
                } else {
                  reports.push({
                    kind: "warn" as const,
                    message: `Git hook NOT executable: ${hook} (run \`chmod +x ${hook}\`)`
                  });
                }
              } catch {
                reports.push({ kind: "warn" as const, message: `Could not check ${hook}` });
              }
            }

            // Return the first non-ok report, or the first ok report
            return reports.find(r => r.kind !== "ok") || reports[0];
          } catch {
            return { kind: "warn" as const, message: `Could not access hooks path: ${hooksPath}` };
          }
        },
      },
    ],
  },
  {
    type: "group",
    label: "Core dependencies",
    items: [
      { type: "version", cmd: "sqlite3 --version", label: "sqlite3" },
      { type: "version", cmd: "sqlpage --version", label: "sqlpage" },
      { type: "version", cmd: "surveilr --version", label: "surveilr" },
    ],
  },
  {
    type: "group",
    label: "Optional runtime dependencies",
    items: [
      {
        type: "exists",
        cmd: "nginx",
        onFound: async (_ctx) => [
          { type: "version", cmd: "nginx -v", label: "nginx" },
        ],
        onMissing: () => ({
          kind: "suggest" as const,
          message: "nginx not found in PATH, install it if you want to use nginx as a reverse proxy"
        }),
      },
      {
        type: "exists",
        cmd: "psql",
        onFound: async (_ctx) => [
          { type: "version", cmd: "psql --version", label: "PostgreSQL" },
        ],
        onMissing: () => ({ kind: "suggest" as const, message: "PostgreSQL psql not found in PATH, optional" }),
      },
    ],
  },
  {
    type: "group",
    label: "Project structure",
    items: [
      {
        type: "custom",
        label: "bin/yard.ts",
        run: async () => {
          try {
            await Deno.stat("bin/yard.ts");
            return { kind: "ok" as const, message: "bin/yard.ts exists" };
          } catch {
            return { kind: "warn" as const, message: "bin/yard.ts is missing" };
          }
        },
      },
      {
        type: "custom",
        label: "deno.jsonc",
        run: async () => {
          try {
            await Deno.stat("deno.jsonc");
            return { kind: "ok" as const, message: "deno.jsonc exists" };
          } catch {
            return { kind: "warn" as const, message: "deno.jsonc is missing" };
          }
        },
      },
    ],
  },
]);

if (import.meta.main) {
  const result = await api.run();
  api.render.cli(result);

  // Also demonstrate JSON output
  if (Deno.args.includes("--json")) {
    console.log(JSON.stringify(api.render.json(result), null, 2));
  }
}