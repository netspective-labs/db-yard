// tabular_test.ts
import { tabular, type TabularDataSupplier } from "./tabular.ts";
import { type Path } from "./discover.ts";

import { assert, assertEquals } from "@std/assert";
import { basename, fromFileUrl } from "@std/path";

function fixturesDir(): string {
  return fromFileUrl(new URL("../support/fixtures/", import.meta.url));
}

async function collect<T, R>(
  gen: AsyncGenerator<T, R>,
): Promise<{ items: T[]; result: R }> {
  const items: T[] = [];
  while (true) {
    const { value, done } = await gen.next();
    if (done) return { items, result: value };
    items.push(value);
  }
}

function byLocation(
  items: TabularDataSupplier[],
): Map<string, TabularDataSupplier> {
  const m = new Map<string, TabularDataSupplier>();
  for (const i of items) m.set(basename(i.location), i);
  return m;
}

Deno.test({
  name: "tabular: discovery and classification",
  permissions: { read: true, run: true },
  fn: async (t) => {
    const srcPaths: Path[] = [{ path: fixturesDir() }];

    const gen = tabular(srcPaths);
    const { items, result } = await collect(gen);

    await t.step("discovers all expected fixtures", () => {
      const names = items.map((i) => basename(i.location)).sort();
      assertEquals(
        names,
        [
          "chinook.db",
          "empty-rssd.sqlite.db",
          "northwind.sqlite.db",
          "northwind.xlsx",
          "sakila.db",
          "scf-2025.3.sqlite.db",
          "sample.duckdb",
        ].sort(),
      );
    });

    await t.step("classifies SQLite databases", () => {
      const m = byLocation(items);

      const chinook = m.get("chinook.db")!;
      assertEquals(chinook.kind, "sqlite");
      assertEquals(chinook.nature, "embedded");

      const northwind = m.get("northwind.sqlite.db")!;
      assertEquals(northwind.kind, "sqlite");
      assertEquals(northwind.nature, "embedded");

      const sakila = m.get("sakila.db")!;
      assertEquals(sakila.kind, "sqlite");
      assertEquals(sakila.nature, "embedded");
    });

    await t.step(
      "detects Surveilr RSSD SQLite via uniform_resource table",
      () => {
        const m = byLocation(items);

        const rssd = m.get("empty-rssd.sqlite.db")!;
        assertEquals(rssd.kind, "surveilr");
        assertEquals(rssd.nature, "embedded");
      },
    );

    await t.step("detects SQLPage SQLite via sqlpage_files table", () => {
      const m = byLocation(items);

      const sqlpage = m.get("scf-2025.3.sqlite.db")!;
      assertEquals(sqlpage.kind, "sqlpage");
      assertEquals(sqlpage.nature, "embedded");
    });

    await t.step("detects Excel workbooks by extension", () => {
      const m = byLocation(items);

      const excel = m.get("northwind.xlsx")!;
      assertEquals(excel.kind, "excel");
      assertEquals(excel.nature, "embedded");
    });

    await t.step("detects DuckDB databases by extension", () => {
      const m = byLocation(items);

      const duck = m.get("sample.duckdb")!;
      assertEquals(duck.kind, "duckdb");
      assertEquals(duck.nature, "embedded");
    });

    await t.step("no unexpected discovery or detection errors", () => {
      // tabular() summary shape:
      // - result.unclassified: string[]
      // - result.errored: string[]
      // - result.discovery.errors: EncounterErrorContext<WalkEntry>[]
      // - result.detectionErrors: {location, error}[]
      assertEquals(result.unclassified.length, 0);
      assertEquals(result.errored.length, 0);
      assertEquals(result.discovery.errors.length, 0);
      assertEquals(result.detectionErrors.length, 0);
    });

    // A small sanity check: if this fails, you likely have extra fixtures in that folder.
    await t.step("no extra suppliers beyond the expected list", () => {
      assertEquals(items.length, 7);
      assert(items.every((s) => s.nature === "embedded"));
    });
  },
});
