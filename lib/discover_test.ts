// discover_test.ts
import { assert, assertEquals, assertRejects } from "@std/assert";
import { basename, fromFileUrl, join } from "@std/path";
import {
  encounters,
  fileSystemSource,
  type Path,
  sqliteSource,
} from "./discover.ts";

function fixturesDir(): string {
  return fromFileUrl(new URL("../support/fixtures/", import.meta.url));
}

async function collectEncounters<T, R>(
  gen: AsyncGenerator<T, R>,
): Promise<{ items: T[]; result: R }> {
  const items: T[] = [];
  while (true) {
    const { value, done } = await gen.next();
    if (done) return { items, result: value };
    items.push(value);
  }
}

async function hasCommand(cmd: string): Promise<boolean> {
  try {
    const p = new Deno.Command(cmd, {
      args: ["-version"],
      stdout: "null",
      stderr: "null",
    });
    const { code } = await p.output();
    return code === 0;
  } catch {
    return false;
  }
}

Deno.test({
  name: "types and validation",
  permissions: { read: true },
  fn: async (t) => {
    await t.step("Path accepts path and optional globs", async () => {
      const srcPaths: Path[] = [
        { path: fixturesDir() },
        { path: fixturesDir(), globs: ["*.db"] },
      ];

      const gen = encounters(
        srcPaths,
        fileSystemSource(),
        ({ entry }) => entry.path,
      );

      const { result } = await collectEncounters(gen);
      assert(Array.isArray(result.unhandled));
      assert(Array.isArray(result.errored));
      assert(Array.isArray(result.errors));
    });

    await t.step("encounters throws on invalid Path", async () => {
      const bad = [{ path: "" }] as unknown as Path[];

      await assertRejects(
        async () => {
          const gen = encounters(
            bad,
            fileSystemSource(),
            ({ entry }) => entry.path,
          );
          await gen.next();
        },
        TypeError,
      );
    });
  },
});

Deno.test({
  name: "filesystem source",
  permissions: { read: true },
  fn: async (t) => {
    const root = fixturesDir();

    await t.step("discovers *.db under fixtures", async () => {
      // Important: fileSystemSource.list enumerates ALL entries; encounters() filters by globs,
      // but unhandled is "seen entries that never yielded", so it will include non-matching items.
      // To keep the test meaningful and stable, we:
      // - assert the expected matches are present in items
      // - assert there are no errors
      // - do NOT assert unhandled is empty
      const gen = encounters(
        [{ path: root, globs: ["*.db"] }],
        fileSystemSource(),
        ({ entry }) => basename(entry.path),
      );

      const { items, result } = await collectEncounters(gen);

      const expected = new Set([
        "chinook.db",
        "northwind.sqlite.db",
        "sakila.db",
      ]);
      for (const name of items) expected.delete(name);

      assertEquals(
        expected.size,
        0,
        `missing expected db fixtures: ${[...expected].join(", ")}; got: ${
          items.join(", ")
        }`,
      );

      assertEquals(
        result.errored.length,
        0,
        `unexpected errored entries: ${result.errored.length}`,
      );
      assertEquals(
        result.errors.length,
        0,
        `unexpected errors: ${result.errors.length}`,
      );
    });

    await t.step("discovers *.xlsx under fixtures", async () => {
      const gen = encounters(
        [{ path: root, globs: ["*.xlsx"] }],
        fileSystemSource(),
        ({ entry }) => basename(entry.path),
      );

      const { items, result } = await collectEncounters(gen);

      assertEquals(items, ["northwind.xlsx"]);
      assertEquals(result.errored.length, 0);
      assertEquals(result.errors.length, 0);
    });

    await t.step(
      "onError collects failures without stopping discovery",
      async () => {
        const badRoot = join(root, "does-not-exist");

        const seenErrors: string[] = [];

        const gen = encounters(
          [
            { path: badRoot, globs: ["*.db"] },
            { path: root, globs: ["*.db"] },
          ],
          fileSystemSource(),
          ({ entry }) => basename(entry.path),
          (e) => {
            seenErrors.push(`${e.phase}:${e.srcPath.path}`);
          },
        );

        const { items, result } = await collectEncounters(gen);

        assert(items.includes("chinook.db"));
        assert(items.includes("northwind.sqlite.db"));
        assert(items.includes("sakila.db"));

        assert(seenErrors.length >= 1);
        assert(result.errors.length >= 1);
      },
    );
  },
});

Deno.test({
  name: "sqlite source",
  permissions: { read: true, run: true },
  fn: async (t) => {
    await t.step("runs SQL against a fixture DB and yields rows", async () => {
      if (!(await hasCommand("sqlite3"))) {
        console.warn("sqlite3 not found on PATH; skipping sqliteSource test");
        return;
      }

      const dbPath = fromFileUrl(
        new URL("../support/fixtures/chinook.db", import.meta.url),
      );

      const sql = `
        select
          'tables/' || name as path,
          sql as content,
          json_object('type', type) as elaboration
        from sqlite_master
        where type in ('table','view') and name not like 'sqlite_%'
        order by name
        limit 5;
      `.trim();

      const src = sqliteSource(sql);

      const gen = encounters(
        [{ path: dbPath, globs: ["tables/*"] }],
        src,
        async ({ entry, content: getContent }) => {
          const content = await getContent();
          return {
            path: entry.path,
            content,
            elaboration: entry.elaboration,
          };
        },
      );

      const { items, result } = await collectEncounters(gen);

      assert(items.length > 0);
      assertEquals(
        result.errored.length,
        0,
        `errored entries: ${result.errored.length}`,
      );
    });
  },
});
