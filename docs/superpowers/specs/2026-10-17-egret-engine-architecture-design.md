# Egret Engine — Architecture (P0 → P4)

- **Date:** 2026-10-17
- **Status:** Design approved; spec under review
- **Repository:** `github.com/egret-data/engine`
- **Research:** [`docs/research/2026-10-17-engine-research.md`](../../research/2026-10-17-engine-research.md)
- **P0 detailed design:** [`2026-10-17-egret-engine-p0-core-design.md`](2026-10-17-egret-engine-p0-core-design.md)

## 1. Goals

Build a distributed SQL engine written entirely in Rust, as a **standalone product** (the egret data platform is one
integration among others), combining the strengths of:

| Inspiration | Capability |
|---|---|
| Trino | Federation across many data sources; fault-tolerant execution |
| StarRocks | Multi-layer Iceberg caching; async materialized views with query rewrite |
| RisingWave | CDC from OLTP databases into Iceberg; streaming materialized views (later) |

Foundation: **DataFusion 55 + datafusion-distributed** (core engine) and **Apache Iceberg** (table format) through an
**Iceberg REST catalog** (Lakekeeper, Polaris, Nessie, ...).

Target workload: **both interactive/BI (low latency) and heavy ETL** (CTAS/INSERT/MERGE/compaction running for hours).

## 2. Decisions

| # | Decision | Main reason |
|---|---|---|
| D1 | Client protocol: **PostgreSQL wire protocol** (pgwire), PostgreSQL dialect. Arrow Flight SQL later | Widest client ecosystem (psql, JDBC, psycopg, BI tools); DataFusion's SQL is closest to PostgreSQL |
| D2 | Authentication: JWT in the password field (TLS), OAUTHBEARER (PG18), SCRAM, impersonation with an explicit privilege | Works with every client; pgwire 0.41 already supports SCRAM and OAUTHBEARER |
| D3 | **One engine, two execution modes**: `retry_policy = NONE \| QUERY \| TASK` | Keep streaming latency for BI while providing Trino-FTE-style fault tolerance for ETL |
| D4 | Dedicated **PostgreSQL** metadata store | Transactions, leases (`SKIP LOCKED`), familiar operations |
| D5 | **Standalone product**: pluggable auth/policy/catalog, PostgreSQL-style RBAC, own Helm chart | Not locked to one platform |
| D6 | **StarRocks-style Iceberg caching in P0** | The decisive performance factor on object storage |
| D7 | Authorization: **full internal model** (roles/grants + RLS via `CREATE POLICY` + masking) **plus OPA** (embedded `regorus` and remote). No Ranger in P0 | Self-sufficient engine; OPA can return row filters and masks and fits cloud-native stacks |
| D8 | **Separate Rust workspace**, never linked into the platform | Dependency conflicts (platform: iceberg 0.9 / DataFusion 53 / Arrow 58 vs engine: DataFusion 55 / Arrow 59); integration happens only over protocols |
| D9 | Fork `datafusion-distributed` as a small patch series, rebased regularly | Needed for per-query memory pools, attempt ids and spooled boundaries (`TASK` mode) |
| D10 | Iceberg is the **source of truth** for data, CDC offsets (snapshot summary) and MV lineage (table properties). PostgreSQL holds coordination state only | Avoid two diverging sources of truth |

### Naming and licensing

- GitHub org `egret-data` (formerly `egretdb`): repositories `platform` (the egret platform) and `engine`
  (this repository).
- Rust packages `egret-engine-<x>` in `crates/<x>`; binary `egret-engine`; container image `egret-data/engine`; Helm
  chart `egret-engine`. This avoids clashing with the platform's `egret-*` crates.
- License: Apache-2.0.

## 3. Sub-projects

Each sub-project gets its own spec → implementation plan → implementation cycle.

| # | Sub-project | Outcome | Depends on |
|---|---|---|---|
| **P0** | **Core read engine**: coordinator/worker, distributed Iceberg reads, Iceberg caching, pgwire, authentication + authorization, `QUERY` retry | Fast distributed `SELECT` on Iceberg from any PostgreSQL client | — |
| P1 | **Iceberg writes + `TASK` mode**: CTAS, INSERT/OVERWRITE, DELETE/UPDATE/MERGE (copy-on-write and merge-on-read), idempotent commit layer, compaction/expiration/orphan cleanup, spooled exchange with per-task retry, query history, P1 caching items | Full lakehouse plus fault-tolerant ETL | P0 |
| P2 | **Federation**: `CREATE CATALOG ... USING postgres/mysql/clickhouse/...` on datafusion-federation + datafusion-table-providers, distributed codec for federated scans, P2 caching items (peer cache, distributed manifest planning) | Trino-style cross-source queries | P0 |
| P3 | **Materialized views**: MVs stored as Iceberg tables, partition-change-tracking refresh plus append-only incremental refresh, SPJG query rewrite with union compensation | StarRocks-style MVs | P1 |
| P4 | **CDC**: PostgreSQL (supabase/etl) and MySQL (`mysql_async` binlog) ingestion into Iceberg upsert tables, DBLog-style snapshot backfill; streaming MVs afterwards | RisingWave-style CDC | P1 |

## 4. System architecture

```text
                       clients (pgwire, TLS)
                              │
                     ┌────────▼─────────┐
                     │  Coordinator ×N  │ parse → authz → [MV rewrite P3] → plan → schedule
                     └──┬───────────┬───┘
               gRPC     │           │
               ┌────────▼───┐   ┌───▼──────────────────────────────┐
               │ Worker ×M  │   │ Job runtime (role=maintenance)   │
               │ query exec │   │  • MV refresh scheduler     (P3) │
               │ NVMe cache │   │  • compaction / expiration  (P1) │
               └─────┬──────┘   │    / orphan cleanup              │
                     │          │  • scheduled CACHE SELECT  (P0+) │
                     │          └───────────┬──────────────────────┘
                     │                      │ submits TASK-mode queries to the same cluster
  ┌──────────────────┼──────────────────────┼─────────────────────┐
  │ Ingest ×K (role=ingest, P4)             │                     │
  │  PG pgoutput ────┐                      │                     │
  │  MySQL binlog ───┼─► micro-batch epoch ─► Iceberg upsert commit (offset in snapshot summary)
  │  snapshot backfill (DBLog watermark)    │                     │
  └──────────────────┬──────────────────────┬─────────────────────┘
                     ▼                      ▼
               MinIO / S3          Iceberg REST catalog        PostgreSQL metadata
                                                               (roles, grants, policies, catalogs,
                                                                jobs, leases, MV defs, CDC pipelines)
```

Principles:

1. **One binary, several roles**: `--role coordinator | worker | maintenance | ingest | all`.
2. **Stateless coordinators** (a session is bound to one pgwire connection); several coordinators behind an L4 load
   balancer.
3. **Workers are a StatefulSet** with stable identities and NVMe persistent volumes, so caches survive restarts and
   affinity hashing stays stable.
4. **One shared job framework** (`jobs` table in PostgreSQL, `FOR UPDATE SKIP LOCKED`): scheduled `CACHE SELECT`
   (P0), compaction/expiration (P1), MV refresh (P3).
5. **One Iceberg commit layer** (idempotent: `query-id` in the snapshot summary plus `Idempotency-Key`), used by DML
   (P1), MV refresh (P3) and CDC (P4).
6. **Heavy jobs run as `TASK`-mode queries** on the same cluster; maintenance has no separate execution engine.
7. **Authorization is checked on the coordinator before planning** — mandatory, because data caches are keyed by
   file path, not by credentials.

## 5. Execution modes

| `retry_policy` | Shuffle | On failure | Default for |
|---|---|---|---|
| `NONE` | Streaming, in memory (upstream datafusion-distributed) | Query fails | — (explicit opt-in) |
| `QUERY` | Streaming, in memory | Re-run the whole query while the result still fits the buffer (32 MB) | `SELECT`/BI (P0) |
| `TASK` (P1) | Spooled to object storage (later hybrid NVMe + S3), barriers between stages | Retry individual tasks, select exactly one attempt per task | CTAS/INSERT/MERGE/compaction, large inputs |

`TASK` mode (P1) follows Trino's fault-tolerant execution: `SpoolWriteExec` / `Spool*Exec` boundaries, producers hash
into a fixed number of buckets, a `FaultTolerantStageScheduler` on the coordinator, `TaskKey.attempt`, an attempt
selector, and adaptive sizing from exact spool statistics. Iceberg writers name files
`{queryId}-{stage}-{task}-{attempt}-{uuid}`; only the selected attempt's output reaches `IcebergCommitExec`.

## 6. Extension points P0 must provide for P1–P4

| For | P0 must |
|---|---|
| P1, P4 | Read **positional deletes, equality deletes and deletion vectors** correctly |
| P1 | Add `TaskKey.attempt` in the fork; ship the job framework and `jobs` table; declare `INSERT/UPDATE/DELETE` privileges |
| P3, P4 | Key the metadata cache by `(table, snapshot_id)` and provide a **snapshot diff API** (skipping `REPLACE` snapshots) |
| P3 | Order optimizer rules **authorization rewrite before MV rewrite**; an MV may only be used when the authorized plan still matches |
| P2 | Keep the catalog registry multi-kind (`CREATE CATALOG ... USING <kind>`) behind a `ConnectorFactory` trait |
| All | Keep `PolicySource` (Ranger can be added later), `Authenticator` and `CatalogSource` as traits |

## 7. Major risks

| Risk | Mitigation |
|---|---|
| datafusion-distributed API churn and single dominant maintainer | Small patch series in the fork, upstream generic changes, plan snapshot tests |
| iceberg-rust lacks overwrite/row-delta; commit API is `pub(crate)` | Own commit layer (build manifests + `CommitTableRequest`), using Sail (Apache-2.0) as reference |
| Equality-delete bugs upstream | Differential tests against Spark on fixtures |
| Vended credentials expiring during long queries | Credential manager refreshing via `/credentials`, credentials sent with work units |
| `TASK` mode is large (~8–12k lines) | Isolated in P1, starting with the simplest correct design (object-store spool) |
| Coordinator is a single point of failure for running queries | Accepted in P0/P1; persist `TASK` query DAG state in PostgreSQL in a later phase |
