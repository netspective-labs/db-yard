#!/usr/bin/env -S deno run --allow-all

import * as colors from "@std/fmt/colors";
import { build$, CommandBuilder } from "@david/dax";

const $ = build$({ commandBuilder: new CommandBuilder().noThrow() });

export type ReportResult = {
  readonly ok: string;
} | {
  readonly warn: string;
} | {
  readonly suggest: string;
};

export interface DoctorReporter {
  (
    args: ReportResult | {
      test: () => ReportResult | Promise<ReportResult>;
    },
  ): Promise<void>;
}

export interface DoctorDiagnostic {
  readonly diagnose: (report: DoctorReporter) => Promise<void>;
}

export interface DoctorCategory {
  readonly label: string;
  readonly diagnostics: () => Generator<DoctorDiagnostic, void>;
}

export function doctorCategory(
  label: string,
  diagnostics: () => Generator<DoctorDiagnostic, void>,
): DoctorCategory {
  return {
    label,
    diagnostics,
  };
}

export function denoDoctor(): DoctorCategory {
  return doctorCategory("Deno", function* () {
    const deno: DoctorDiagnostic = {
      diagnose: async (report: DoctorReporter) => {
        report({ ok: (await $`deno --version`.lines())[0] });
      },
    };
    yield deno;
  });
}

/**
 * Doctor task legend:
 * - 🚫 is used to indicate a warning or error and should be corrected
 * - 💡 is used to indicate an (optional) _suggestion_
 * - 🆗 is used to indicate success
 * @param categories
 * @returns
 */
export function doctor(categories: () => Generator<DoctorCategory>) {
  // deno-lint-ignore require-await
  const report = async (options: ReportResult) => {
    if ("ok" in options) {
      console.info("  🆗", colors.green(options.ok));
    } else if ("suggest" in options) {
      console.info("  💡", colors.yellow(options.suggest));
    } else {
      console.warn("  🚫", colors.brightRed(options.warn));
    }
  };

  return async () => {
    for (const cat of categories()) {
      console.info(colors.dim(cat.label));
      for (const diag of cat.diagnostics()) {
        await diag.diagnose(async (options) => {
          if ("test" in options) {
            try {
              report(await options.test());
            } catch (err) {
              report({ warn: err.toString() });
            }
          } else {
            report(options);
          }
        });
      }
    }
  };
}

export const checkup = doctor(function* () {
  yield doctorCategory("Build dependencies", function* () {
    yield* denoDoctor().diagnostics();
  });

  yield doctorCategory("Git dependencies", function* () {
    yield {
      diagnose: async (report) => {
        const hooksPathLines = await $`git config core.hooksPath`.lines();
        const hooksPath = hooksPathLines.length > 0 ? hooksPathLines[0] : "";

        if (hooksPath.trim().length > 0) {
          try {
            const hookFiles =
              (await $`find ${hooksPath} -maxdepth 1 -type f`.noThrow().lines())
                .filter((f) => f.trim().length > 0);
            if (hookFiles.length > 0) {
              for (const hook of hookFiles) {
                const info = await Deno.stat(hook);
                const isExecutable = info.mode
                  ? (info.mode & 0o111) !== 0
                  : false;
                if (isExecutable) {
                  report({ ok: `Git hook executable: ${hook}` });
                } else {
                  report({
                    warn:
                      `Git hook NOT executable: ${hook} (run \`chmod +x ${hook}\`)`,
                  });
                }
              }
            } else {
              report({ suggest: `No hooks found in ${hooksPath}` });
            }
          } catch {
            report({ warn: `Could not access hooks path: ${hooksPath}` });
          }
        } else {
          report({
            test: () => ({ warn: "Git hooks not setup, run `deno task init`" }),
          });
        }
      },
    };
  });

  yield doctorCategory("Core dependencies", function* () {
    yield {
      diagnose: async (report) => {
        await report({
          test: async () => (await $.commandExists("sqlite3")
            ? { ok: `sqlite3: ${(await $`sqlite3 --version`.lines())[0]}` }
            : {
              warn:
                "sqlite3 not found in PATH, but it is required for truth-yard",
            }),
        });
        await report({
          test: async () => (await $.commandExists("sqlpage")
            ? { ok: `sqlpage: ${(await $`sqlpage --version`.lines())[0]}` }
            : { warn: "sqlpage not found in PATH, required for web-ui" }),
        });
        await report({
          test: async () => (await $.commandExists("surveilr")
            ? { ok: `surveilr: ${(await $`surveilr --version`.lines())[0]}` }
            : { warn: "surveilr not found in PATH, required for ingestion" }),
        });
      },
    };
  });

  yield doctorCategory("Optional runtime dependencies", function* () {
    yield {
      diagnose: async (report) => {
        await report({
          test: async () => (await $.commandExists("nginx")
            ? {
              ok: `nginx: ${
                (await $`nginx -v`.noThrow().captureCombined()).combined.trim()
              }`,
            }
            : {
              suggest:
                "nginx not found in PATH, install it if you want to use nginx as a reverse proxy",
            }),
        });
        await report({
          test: async () => (await $.commandExists("psql")
            ? { ok: `psql: ${(await $`psql --version`.lines())[0]}` }
            : { suggest: "PostgreSQL psql not found in PATH, optional" }),
        });
      },
    };
  });

  yield doctorCategory("Project structure", function* () {
    yield {
      diagnose: async (report) => {
        const checkFile = async (path: string) => {
          try {
            await Deno.stat(path);
            return { ok: `${path} exists` };
          } catch {
            return { warn: `${path} is missing` };
          }
        };
        await report({ test: () => checkFile("bin/yard.ts") });
        await report({ test: () => checkFile("deno.jsonc") });
      },
    };
  });
});

if (import.meta.main) {
  await checkup();
}
