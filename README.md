`db-yard` is a file-driven process yard that watches directories for SQLite database files and automatically turns them into running local services. When a database file appears, `db-yard` launches the appropriate server process, assigns it a port, and records its operational state. When the file disappears, the process is cleanly shut down. The filesystem itself is the control plane.

The core idea is simple: a SQLite database on disk represents deployable cargo. Dropping that cargo into the yard launches a service. Removing it decommissions the service. No registries, no internal state databases, and no long-running supervisors beyond `db-yard` itself.

`db-yard` is designed for developers who want fast, local, deterministic infrastructure without configuration sprawl. It is especially useful for workflows built around SQLite-first tools such as SQLPage and surveilr RSSDs, where databases are the unit of deployment rather than code bundles.

How it works in practice:

You point `db-yard` at one or more directories.
It recursively watches for known database patterns such as *.rssd.db and *.sqlpage.db.
Each matching database file triggers a spawned process bound to a local host and a free port.
A JSON manifest is written for each running instance, describing its PID, port, command, and metadata.
File modifications refresh metadata without restarting processes.
File deletion cleanly terminates the associated process and removes its manifest.

`db-yard` treats the spawned-state directory as an append-only operational ledger. Other tools can observe this directory to build reverse proxies, dashboards, routing tables, or orchestration layers without needing shared memory or APIs.

The project deliberately avoids being a platform or framework. It does not proxy HTTP traffic, manage TLS, restart processes on data changes, or impose opinions about site structure. It focuses narrowly on lifecycle management driven by the presence or absence of files.

An optional admin HTTP server can be enabled to expose runtime state and, when explicitly configured, execute ad-hoc SQL against known databases for inspection or debugging. This interface is intentionally gated and clearly marked as unsafe where appropriate.

`db-yard` follows a “Navy Yard” mental model:

- The yard is passive until cargo arrives.
- Databases are cargo crates.
- Spawned processes are launched vessels.
- Ports are berths.
- JSON state files are the manifest.

In short, `db-yard` turns SQLite databases into living local services using nothing more than the filesystem, processes, and clear conventions.
