![db-yard Logo](db-yard-logo.png)

`db-yard` is a file-driven process yard that watches directories for SQLite
database files and automatically turns them into running local services. When a
database file appears, `db-yard` launches the appropriate server process,
assigns it a port, and records its operational state. When the file disappears,
the process is cleanly shut down. The filesystem itself is the control plane.

The core idea is simple: a SQLite database on disk represents deployable cargo.
Dropping that cargo into the yard launches a service. Removing it decommissions
the service. No registries, no internal state databases, and no long-running
supervisors beyond `db-yard` itself.

`db-yard` is designed for developers who want fast, local, deterministic
infrastructure without configuration sprawl. It is especially useful for
workflows built around SQLite-first tools such as SQLPage and surveilr RSSDs,
where databases are the unit of deployment rather than code bundles.

How it works in practice:

You point `db-yard` at one or more directories. It recursively watches for known
database patterns such as *.rssd.db and *.sqlpage.db. Each matching database
file triggers a spawned process bound to a local host and a free port. A JSON
manifest is written for each running instance, describing its PID, port,
command, and metadata. File modifications refresh metadata without restarting
processes. File deletion cleanly terminates the associated process and removes
its manifest.

`db-yard` treats the spawned-state directory as an append-only operational
ledger. Other tools can observe this directory to build reverse proxies,
dashboards, routing tables, or orchestration layers without needing shared
memory or APIs.

The project deliberately avoids being a platform or framework. It does not proxy
HTTP traffic, manage TLS, restart processes on data changes, or impose opinions
about site structure. It focuses narrowly on lifecycle management driven by the
presence or absence of files.

An optional admin HTTP server can be enabled to expose runtime state and, when
explicitly configured, execute ad-hoc SQL against known databases for inspection
or debugging. This interface is intentionally gated and clearly marked as unsafe
where appropriate.

`db-yard` follows a “Navy Yard” mental model:

- The yard is passive until cargo arrives.
- Databases are cargo crates.
- Spawned processes are launched vessels.
- Ports are berths.
- JSON state files are the manifest.

## CLI Overview

```
bin/yard.ts [options]
bin/yard.ts processes [options]
```

- The default command starts the orchestrator.
- The `spawned` subcommand inspects or terminates processes started in previous
  sessions.

## Core Concepts

- **Watch globs** You tell `yard.ts` what to watch using glob patterns, not
  directories. Example: `./cargo.d/**/*.db`

- **Session directories** Each run creates a new session directory under
  `spawned-state-path`:

  ```
  spawned.d/
    2026-01-06-14-16-11/
      spawned-pids.txt
      <db>.<id>.json
      <db>.<id>.stdout.log
      <db>.<id>.stderr.log
  ```

- **Spawn drivers (auto-detected)** For each database file:

  1. If a `.db-yard` table exists with key `spawn-driver`, that driver is used.
  2. Else if `uniform_resource` exists, it is treated as a `surveilr` web UI.
  3. Else if `sqlpage_files` exists, it is treated as a SQLPage app.

- **Crash resilience** Spawned child processes are detached so they survive
  orchestrator crashes. PIDs are tracked in `spawned-pids.txt` for later
  inspection or cleanup.

## Common Usage Patterns

### 1. Default usage (recommended starting point)

Watch all SQLite databases under `cargo.d` recursively.

```
yard.ts
```

Equivalent to:

```
yard.ts --watch './cargo.d/**/*.db'
```

Output is intentionally minimal unless `--verbose` is used.

### 2. Explicit watch glob

Use this when you want to be very clear about what is watched.

```
yard.ts --watch './cargo.d/**/*.db'
```

You can repeat `--watch` multiple times:

```
yard.ts \
  --watch './cargo.d/**/*.db' \
  --watch './other-cargo/**/*.sqlite'
```

### 3. Verbose, operator-friendly output

Shows colored messages when databases are detected, spawned, reconciled, or
stopped.

```
db-yard --verbose
```

Verbose mode also prints:

- which spawn driver was chosen
- stdout/stderr log file locations
- reconciliation actions (only when something actually changes)

### 4. Custom spawned state directory

Useful in CI, containers, or multi-user environments.

```
yard.ts --spawned-state-path /var/db-yard/spawned
```

Each run still creates a timestamped session directory inside this path.

### 5. Enable the admin server (optional)

Starts a lightweight HTTP admin endpoint for introspection.

```
yard.ts --admin-port 9090
```

Optional host override:

```
yard.ts --admin-port 9090 --admin-host 0.0.0.0
```

## Process Management

### List all managed processes (across sessions)

```
yard.ts spawned
```

This scans all session directories and reports:

- PID
- alive/dead status
- command (best-effort)
- how many sessions reference the PID

### Kill all managed processes (dangerous)

```
yard.ts spawned --kill
```

This:

- de-duplicates PIDs across sessions
- skips the current `yard.ts` process
- sends SIGTERM, then SIGKILL if needed

Use with care.
