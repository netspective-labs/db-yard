![db-yard Logo](project-hero.png)

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

```bash
bin/yard.ts [options]
bin/yard.ts spawned [options]

# helpers
bin/yard.ts help
bin/yard.ts completions
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

### Default usage (recommended starting point)

Watch all SQLite databases under `cargo.d` recursively.

```bash
yard.ts
```

Equivalent to:

```bash
yard.ts --watch './cargo.d/**/*.db'
```

Output is intentionally minimal unless `--verbose` is used.

### Explicit watch glob

Use this when you want to be very clear about what is watched.

```bash
yard.ts --watch './cargo.d/**/*.db'
```

You can repeat `--watch` multiple times:

```bash
yard.ts \
  --watch './cargo.d/**/*.db' \
  --watch './other-cargo/**/*.sqlite'
```

### Environment variables

By default, `yard.ts` unsafely passes all env vars to spawned processes. If
that's not secure enough, you can use `--env` to help secure which env vars are
passed into spawned processes:

```bash
bin/yard.ts \
  --env '^(PATH|HOME|USER|SHELL|LANG|LC_|TERM|TZ)$' \
  --env '^DBYARD_' \
  --env '^SQLPAGE_'
```

### Verbose, operator-friendly output

Shows colored messages when databases are detected, spawned, reconciled, or
stopped.

```bash
db-yard --verbose
```

Verbose mode also prints:

- which spawn driver was chosen
- stdout/stderr log file locations
- reconciliation actions (only when something actually changes)

### Custom spawned state directory

Useful in CI, containers, or multi-user environments.

```bash
yard.ts --spawned-state-path /var/db-yard/spawned
```

Each run still creates a timestamped session directory inside this path.

### Enable the admin server (optional)

Starts a lightweight HTTP admin endpoint for introspection.

```bash
yard.ts --web-ui-port 9090
```

Optional host override:

```bash
yard.ts --web-ui-port 9090 --web-ui-host 0.0.0.0
```

## Process Management

### List all managed processes (across sessions)

```bash
yard.ts spawned
```

This scans all session directories and reports:

- PID
- alive/dead status
- command (best-effort)
- how many sessions reference the PID

### Kill all managed processes (dangerous)

```bash
yard.ts spawned --kill
```

This:

- de-duplicates PIDs across sessions
- skips the current `yard.ts` process
- sends SIGTERM, then SIGKILL if needed

Use with care.

## Web UI and Reverse Proxy

`db-yard` includes an optional built-in web UI that provides lightweight
inspection, file access, unsafe debugging tools, and request routing to spawned
services. It is intentionally minimal and local-first.

You enable it by supplying an admin port:

```
yard.ts --web-ui-port 9090
```

By default it binds to `127.0.0.1`. You can override this with `--web-ui-host`,
but exposing it beyond localhost is strongly discouraged.

The web UI provides four distinct behaviors:

1. Hidden admin endpoints (`/.admin`)

These endpoints are intentionally prefixed with `/.admin` to avoid accidental
discovery.

- `GET /.admin` Returns a JSON snapshot of all currently running instances,
  including their IDs, ports, database paths, and metadata.

- `GET /.admin/index.html` Displays a simple, fixed-width, directory-style HTML
  page showing:

  - all running database instances (with links you can click to proxy traffic)
  - all JSON state files and stdout/stderr logs in the current session directory

  The file listing behaves like a classic directory index. Clicking a file
  streams the raw JSON or log directly from disk.

- `GET /.admin/files/<name>` Streams an individual JSON or log file from the
  session directory. This is read-only and limited to files created by
  `db-yard`.

2. Unsafe SQL endpoint (`/SQL/unsafe`)

- `POST /SQL/unsafe/<id>.json` Executes ad-hoc SQL against the SQLite database
  for a running instance.

  Request body:

  ```
  { "sql": "select * from sqlite_master" }
  ```

  This endpoint is explicitly unsafe. It bypasses application logic and executes
  SQL directly via the SQLite CLI. It is intended only for local inspection and
  debugging.

3. Reverse proxy for spawned services

All other paths are treated as reverse-proxy traffic to spawned services:

- `/<id>/...` Proxies the request to the instance with that ID and strips the
  `/<id>` prefix.

- `/...` (no ID prefix) If exactly one instance is running, traffic is proxied
  to it automatically.

If multiple instances are running and no ID is provided, the request returns a
helpful error telling you to use `/<id>/...`.

This makes it easy to run many local SQLite-backed services behind a single port
without configuring a separate reverse proxy.

4. Security notes

The web UI is intentionally powerful and intentionally unsafe in places.

- Do not bind it to `0.0.0.0` unless you fully trust your network.
- `/.admin` and `/SQL/unsafe` should be considered local-only debugging tools.
- Treat the web UI as a development and inspection surface, not a hardened admin
  plane.

### Using `--kill-all-on-exit` During Development

During active development of `yard.ts` itself, it is easy to leave orphaned
processes running if the orchestrator crashes, is restarted frequently, or is
killed abruptly.

To make development safer, `db-yard` provides:

```
--kill-all-on-exit
```

When this flag is enabled:

- On process exit (Ctrl+C, SIGTERM, or normal shutdown)
- `db-yard` scans all session directories it owns
- Collects all PIDs recorded in `spawned-pids.txt`
- Gracefully terminates those processes using the same logic as the
  `spawned --kill` command

Example:

```bash
yard.ts --watch './cargo.d/**/*.db' --kill-all-on-exit
```

This is especially useful when:

- Iterating on `yard.ts` internals
- Repeatedly restarting the orchestrator
- Debugging spawn logic or driver behavior
- Running automated tests or scripts that invoke `db-yard`

Important notes:

- This flag is intended for development and local testing.
- In normal usage, `db-yard` is designed so spawned processes survive
  orchestrator restarts.
- Enabling `--kill-all-on-exit` changes that contract.

Do not use `--kill-all-on-exit` in production or shared environments unless you
explicitly want all spawned services to terminate whenever `db-yard` exits.
