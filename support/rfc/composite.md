# SQLite Composite Connections

Operational Truth Yard (`OTY`) is designed around embedded databases as evidence
warehouses. Rather than forcing tenants into a single monolithic database, Truth
Yard explicitly supports many small, purpose-built SQLite databases per tenant
and per functional area such as evidence warehouses, Qualityfolio, Fleetfolio,
telemetry, and other RSSD-style stores.

This design is intentional. Customers are free to create new databases whenever
it makes operational sense. Databases can be versioned, archived, replicated, or
retired independently. The platform must therefore make it easy to access
multiple databases together without forcing application code to manage many
connections or to understand where data physically lives.

The composite connection pattern described here solves that problem.

## Key concepts

Embedded databases All databases discussed here are embedded databases such as
SQLite or DuckDB. They are file-based and accessed via local paths, not through
a server process like PostgreSQL.

Multiple databases per tenant Each tenant may have many SQLite databases. Some
are created by Truth Yard components, others by customers themselves. There is
no artificial requirement to consolidate them.

Composite databases A composite database is a thin SQLite or DuckDB database
whose primary role is to ATTACH other databases and optionally define views
across them. It does not own the underlying data. It exists solely to provide a
stable, simplified connection surface.

## Terminology and directory layout

Standard directory layout:

```text
<volume>/
  embedded/
    admin/
      db0.sqlite.db
      composite.sql
      composite.sqlite.auto.db

    cross-tenant/
      db1.sqlite.db
      db2.sqlite.db
      composite.sql
      composite.sqlite.auto.db

    tenant/
      <tenantID>/
        db3.sqlite.db
        db4.sqlite.db
        composite.sql
        composite.sqlite.auto.db
```

- `embedded` Indicates an embedded database such as SQLite or DuckDB, not a
  server-based database.
- `admin` Databases used for internal administrative purposes. These are not
  tenant-facing.
- `cross-tenant` Databases intended to be queried across all tenants, typically
  for reporting, observability, governance, or platform-level analytics.
- `tenant` Databases scoped to a single tenant.

## Important naming conventions

- `composite.sqlite.auto.db` An auto-generated database whose contents are
  derived entirely from composite.sql. This file can always be regenerated and
  should not be writeable.
- `composite.sql` The canonical SQL definition for a composite. This file is
  human-readable, auditable, and source-controlled. It may be authored manually
  or generated automatically.

## Why composites exist

Without composites, application code would need to open multiple SQLite
connections, track which database contains which tables, manage locking and WAL
modes per database, and be updated whenever a new database is added.

With composites, the application opens exactly one connection per context. All
ATTACH logic is centralized. Table and view names are stable. Adding or removing
databases becomes a data or configuration change, not an application code
change.

## How SQLite ATTACH works in this design

SQLite allows a single database connection to attach additional database files
under logical schema names. Once attached, tables are referenced using
schema_name.table_name.

Example composite.sql fragment:

```sql
ATTACH DATABASE 'db3.sqlite.db' AS qualityfolio;
ATTACH DATABASE 'db4.sqlite.db' AS fleetfolio;
```

After this, the composite connection can query:

```sql
SELECT * FROM qualityfolio.test_cases;
SELECT * FROM fleetfolio.assets;
```

The composite database itself may define views that unify or normalize data:

```sql
CREATE VIEW all_findings AS
SELECT 'quality' AS source, id, severity, created_at
FROM qualityfolio.findings
UNION ALL
SELECT 'fleet' AS source, id, severity, created_at
FROM fleetfolio.findings;
```

## Lifecycle of a composite

Authoring composite.sql Each composite.sql file is the authoritative definition
of a composite. It may include ATTACH statements, PRAGMA settings, view
definitions, and optional indexes. The file can be written manually or
auto-generated from rules or metadata.

Generation of composite.sqlite.auto.db A script or job performs the following
steps:

- Deletes any existing composite.sqlite.auto.db.
- Creates a new SQLite database file.
- Executes composite.sql against it.
- Optionally validates that all ATTACH targets exist.

The “auto.db” suffix explicitly indicates that this file is derived and
disposable.

Application usage The application never connects directly to db3.sqlite.db or
db4.sqlite.db. It connects only to composite.sqlite.auto.db. This keeps
connection management simple and consistent across admin, cross-tenant, and
tenant-specific contexts.

## Read-only and read-write access patterns

Not all attached databases should be writable through a composite connection.

Recommended approach:

- Open composites in read-only mode by default.
- Enable read-write access only for databases that must be written through the
  composite.
- Enable WAL mode only for databases that require concurrent writes.

Typical composite.sql settings:

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
```

Guidance:

- Use WAL only where writes are expected.
- Keep write transactions short.
- Prefer writes to occur in the database’s owning context rather than through
  broad cross-db queries.

## Application integration

In an application, composites are treated as first-class data sources.

Operational guidance:

- Each page or API route selects the appropriate composite: admin, cross-tenant,
  or tenant-specific.
- Server-side code opens a single SQLite connection to composite.sqlite.auto.db.
- UI and route code never needs to know which underlying databases are attached.
- Schema evolution happens by regenerating composites, not by changing
  application logic.

This aligns naturally with Astro’s server-first execution model and keeps
client-side code unaware of database topology.

### Composite as a thin ATTACH layer

```text
               single SQLite connection
                        |
                        v
          +----------------------------------+
          | composite.sqlite.auto.db          |
          |  - ATTACH qualityfolio           |
          |  - ATTACH fleetfolio             |
          |  - views (optional)              |
          +-----------------+----------------+
                            |
        +-------------------+-------------------+
        |                                       |
        v                                       v
+---------------------+               +---------------------+
| db3.sqlite.db        |               | db4.sqlite.db        |
| Qualityfolio data   |               | Fleetfolio data     |
+---------------------+               +---------------------+
```

### What composite.sql does

```text
composite.sql
  |
  |  ATTACH DATABASE 'db3.sqlite.db' AS qualityfolio;
  |  ATTACH DATABASE 'db4.sqlite.db' AS fleetfolio;
  |  CREATE VIEW ... (optional)
  v
composite.sqlite.auto.db
```

How queries flow

```text
Application query
  SELECT * FROM all_findings;

Composite resolves view
  all_findings = UNION ALL
    SELECT ... FROM qualityfolio.findings
    SELECT ... FROM fleetfolio.findings

Attached DB resolution
  qualityfolio.* -> db3.sqlite.db
  fleetfolio.*   -> db4.sqlite.db
```

## Operational management and safety

- Never accept user input directly as an ATTACH path.
- Maintain a controlled mapping from logical names to filesystem paths.
- Treat composite.sql as configuration and keep it under source control.
- Regenerate composites during deployment, migration, or tenant onboarding.
- Add health checks to verify expected ATTACH targets exist.

## Advanced querying with DuckDB

SQLite composites are ideal for operational queries, light aggregation, and
write-adjacent workloads. For advanced analytics, DuckDB can be layered on top
using the same composite concept.

### DuckDB as an analytics composite

DuckDB supports ATTACH for SQLite databases, allowing it to act as an
analytics-only aggregator while SQLite remains the system of record.

Typical use cases:

- Large cross-tenant scans.
- Heavy aggregations and GROUP BY queries.
- Time-series analytics.
- Materialized rollups for dashboards.

DuckDB composite pattern

- Create a DuckDB database file such as analytics.duckdb.
- Load the SQLite extension.
- ATTACH multiple SQLite databases.
- Define analytics views or materialized tables.

Example:

```sql
INSTALL sqlite;
LOAD sqlite;

ATTACH 'tenant/tenantA/db3.sqlite.db' AS tenantA_quality (TYPE sqlite);
ATTACH 'tenant/tenantB/db3.sqlite.db' AS tenantB_quality (TYPE sqlite);
```

Then define analytics views:

```sql
CREATE VIEW all_test_results AS
SELECT 'tenantA' AS tenant, * FROM tenantA_quality.test_results
UNION ALL
SELECT 'tenantB' AS tenant, * FROM tenantB_quality.test_results;
```

## Separation of concerns

Recommended model:

- SQLite composites provide operational truth and evidence access.
- DuckDB composites provide analytics and reporting.
- DuckDB may materialize derived tables internally.
- DuckDB should not be the primary writer back into SQLite except in carefully
  controlled cases.

## Summary

The composite connection pattern is a foundational architectural element for
Truth Yard's `surveilr`:

- Customers freely create multiple SQLite databases.
- Composites provide a stable, simple connection surface.
- ATTACH logic is centralized, auditable, and regenerable.
- Application code remains clean and topology-agnostic.
- SQLite handles operational workloads.
- DuckDB extends the same model for advanced analytics.

This approach scales in complexity without forcing a move to server-based
databases and fits naturally with embedded, evidence-first system design.
