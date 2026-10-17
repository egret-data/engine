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

> **Status:** early design phase. Nothing is usable yet. See the design documents below.

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
