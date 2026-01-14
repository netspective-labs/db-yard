#!/usr/bin/env -S deno run -A --node-modules-dir=auto
// ai-workforce.ts
//
// Deterministically emits an “AI context prompt” to STDOUT.
// - Project description first
// - Then each relevant file (from FILES below) in a stable order
// - Output depends only on FILES + file contents (no timestamps)
//
// Usage:
//   deno run -A ai-workforce.ts > .ai-workforce.txt
//   deno run -A ai-workforce.ts --root . --no-hash
//
// Notes:
// - Missing files are reported as a short, deterministic stub in the output.
// - Newlines are normalized to LF for stability across platforms.

import { isAbsolute, normalize, relative, resolve } from "@std/path";

// 1) Put the project-relevant files here.
//    Keep this list curated and reasonably small.
//    TODO: group them in the future to allow context groups to be emitted
//          (e.g., server-side only or browser-side only or just assurance)
const FILES = [
  "README.md",
  "deno.jsonc",
  "./lib/discover.ts",
  "./lib/exposable.ts",
  "./lib/spawn-event.ts",
  "./lib/spawn.ts",
  "./lib/materialize.ts",
  "./bin/yard.ts",
] as const;

// 2) Brief project blurb. Keep it stable and factual.
const PROJECT_BRIEF = [
  "Project context (brief):",
  "- This repository is a Deno TypeScript 2.6 codebase which is described by README.md provided.",
].join("\n");

function normalizeLf(s: string) {
  return s.replace(/\r\n?/g, "\n");
}

function stablePathKey(p: string) {
  // Normalize separators and case-insensitive oddities in a simple, stable way.
  // We intentionally do NOT lower-case because that can break on case-sensitive FS.
  return normalize(p);
}

async function sha256Hex(input: string) {
  const data = new TextEncoder().encode(input);
  const digest = await crypto.subtle.digest("SHA-256", data);
  const bytes = new Uint8Array(digest);
  let out = "";
  for (const b of bytes) out += b.toString(16).padStart(2, "0");
  return out;
}

class AiContextBuilder {
  #root: string;
  #includeHash: boolean;

  constructor(root: string, includeHash: boolean) {
    this.#root = root;
    this.#includeHash = includeHash;
  }

  async build() {
    const lines: string[] = [];

    lines.push(
      "You are an AI assistant helping with a Deno TypeScript 2.5 repository.",
      "Follow existing code style and be careful about deterministic, reproducible output.",
      "",
      PROJECT_BRIEF,
      "",
      "Relevant files (verbatim), read them and don't explain them but be ready for edits:",
    );

    const sorted = [...FILES].sort((a, b) =>
      stablePathKey(a).localeCompare(stablePathKey(b), "en")
    );

    for (const p of sorted) {
      const abs = isAbsolute(p) ? p : resolve(this.#root, p);
      const rel = normalizeLf(relative(this.#root, abs)).replaceAll("\\", "/");

      let body: string | undefined;
      let missing = false;

      try {
        body = await Deno.readTextFile(abs);
      } catch {
        missing = true;
      }

      if (missing || body === undefined) {
        lines.push(
          "",
          `FILE: ${rel}`,
          "STATUS: MISSING (not found at runtime)",
          "```",
          "",
          "```",
        );
        continue;
      }

      const normalized = normalizeLf(body);

      if (this.#includeHash) {
        const hash = await sha256Hex(normalized);
        lines.push("", `FILE: ${rel}`, `SHA256: ${hash}`);
      } else {
        lines.push("", `FILE: ${rel}`);
      }

      // Always include as fenced text (not language-specific) to avoid accidental formatting differences.
      lines.push("```text");
      lines.push(normalized);
      // Ensure the closing fence is on its own line even if file ends without newline.
      if (!normalized.endsWith("\n")) lines.push("");
      lines.push("```");
    }

    lines.push(
      "",
      "Instructions:",
      "- Use only the files above as your ground truth.",
      "- If you propose changes, be specific: file paths, edits, and concise relevant rationale (not educational).",
      "- Prefer Deno/JSR imports, strict TypeScript, no explit types that can be inferred (focus on inference for type-safety), and lint-friendly code.",
    );

    return lines.join("\n");
  }
}

function parseArgs(args: string[]) {
  let root = ".";
  let includeHash = true;

  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a === "--root" && args[i + 1]) {
      root = args[++i];
      continue;
    }
    if (a === "--no-hash") {
      includeHash = false;
      continue;
    }
  }

  return { root, includeHash };
}

export async function emitAiContextToStdout(
  opts?: { root?: string; includeHash?: boolean },
) {
  const root = resolve(opts?.root ?? ".");
  const includeHash = opts?.includeHash ?? true;

  const builder = new AiContextBuilder(root, includeHash);
  const prompt = await builder.build();

  // Write once for stability.
  await Deno.stdout.write(new TextEncoder().encode(prompt));
}

if (import.meta.main) {
  const { root, includeHash } = parseArgs(Deno.args);
  await emitAiContextToStdout({ root, includeHash });
}
