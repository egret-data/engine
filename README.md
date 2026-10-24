# Egret Engine

A distributed SQL engine for the lakehouse, written entirely in Rust.

Egret Engine is built on [Apache DataFusion](https://github.com/apache/datafusion) and
[datafusion-distributed](https://github.com/datafusion-contrib/datafusion-distributed), uses
[Apache Iceberg](https://iceberg.apache.org/) as its native table format, and speaks the PostgreSQL wire protocol.
It aims to combine:

- **Trino-style federation and fault-tolerant execution** — query many sources; run long ETL with task-level retry.
- **StarRocks-style Iceberg caching and materialized views** — multi-layer metadata/data caching with cache-affinity
  scheduling, async materialized views with transparent query rewrite.
- **RisingWave-style CDC** — continuous ingestion from PostgreSQL/MySQL into Iceberg.

> **Status:** early development. Milestone M0 serves SQL over the PostgreSQL wire protocol from an in-memory
> DataFusion session; Iceberg support arrives in M1.

## Getting started

Requires [rustup](https://rustup.rs/); the toolchain pinned in `rust-toolchain.toml` is installed automatically.

```bash
cargo run -p egret-engine-server -- --role all
psql -h 127.0.0.1 -p 5432 -U demo -c "SELECT 1 AS one"
```

Configuration is read from built-in defaults, then an optional TOML file (`--config engine.toml`), then
environment variables prefixed with `EGRET_ENGINE__` (nested keys separated by `__`):

```toml
[server]
role = "all"                    # coordinator | worker | all
pgwire_listen = "127.0.0.1:5432"
```

```bash
EGRET_ENGINE__SERVER__PGWIRE_LISTEN=127.0.0.1:15432 cargo run -p egret-engine-server
```

M0 has no authentication: every client that can reach the pgwire port gets full access, so keep it
bound to localhost and do not expose it beyond the local machine.

## Design

| Document | Description |
|---|---|
| [Architecture (P0 → P4)](docs/superpowers/specs/2026-10-17-egret-engine-architecture-design.md) | Overall architecture, decisions, sub-project roadmap |
| [P0: Core read engine](docs/superpowers/specs/2026-10-17-egret-engine-p0-core-design.md) | Detailed design of the first milestone |
| [Research notes](docs/research/2026-10-17-engine-research.md) | Survey of DataFusion, Iceberg, Trino, StarRocks, RisingWave and related projects |

## Roadmap

| Phase | Scope |
|---|---|
| P0 | Distributed Iceberg reads, Iceberg caching, pgwire, authentication, RBAC + RLS + masking + OPA, query retry |
| P1 | Iceberg writes (CTAS/INSERT/DELETE/UPDATE/MERGE), table maintenance, fault-tolerant `TASK` execution |
| P2 | Federation (PostgreSQL, MySQL, ClickHouse, ...) |
| P3 | Materialized views with query rewrite |
| P4 | Change data capture into Iceberg |

## Contributing

Contributions are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) and our
[Code of Conduct](CODE_OF_CONDUCT.md). Report security issues privately as described in [SECURITY.md](SECURITY.md).

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
