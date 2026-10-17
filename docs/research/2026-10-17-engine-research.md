# Egret Engine — Research Notes (2026-10-17)

Goal: a database engine written in Rust. Core = datafusion-distributed, table format = Apache Iceberg (via an Iceberg
REST catalog such as Lakekeeper), Trino-style federation, StarRocks-style caching and materialized views,
RisingWave-style CDC.

Projects studied: `datafusion-contrib/*` (datafusion-distributed, datafusion-federation, datafusion-table-providers,
datafusion-postgres, datafusion-materialized-views, liquid-cache, datafusion-functions-json, datafusion-variant),
`apache/iceberg-rust`, `apache/datafusion-iceberg`, `apache/datafusion-ballista`, `lakehq/sail`,
`lakekeeper/lakekeeper`, `trinodb/trino`, `StarRocks/starrocks`, `risingwavelabs/risingwave`,
`ArroyoSystems/arroyo`, `supabase/etl`, `Mooncake-Labs/moonlink`, `sunng87/pgwire`.

Versions and code references reflect the state of those repositories in October 2026.

## 1. datafusion-distributed (post-v4.0.0, DataFusion 55)

- A **library**, not a binary. Any node can coordinate; there is no scheduler and no persisted state.
- `SessionStateBuilder::with_distributed_planner()` rewrites the **physical plan**, inserting `NetworkShuffleExec`,
  `NetworkCoalesceExec` and `NetworkBroadcastExec` at repartition points. The head stage runs on the coordinator.
- Transport: tonic gRPC — a bidirectional `CoordinatorChannel` plus `ExecuteTask` streaming Arrow IPC
  (FlightData framing, lz4/zstd). Shuffle is **pull-based, streaming, in-memory and never persisted**.
- Serialization: datafusion-proto plus `with_distributed_user_codec(C)`, which must be registered on both coordinator
  and workers. Table providers never cross the wire — the catalog lives only on the coordinator.
- Extension hooks: `WorkerResolver`, `ChannelResolver`, `RouteTaskHandler` (affinity), `DesiredTaskCountHandler`,
  `ScaleUpLeafNodeHandler`, `WorkerPlanRewriteHandler`, and **work unit feeds** (the coordinator streams splits to
  workers at runtime).
- Adaptive execution (`dynamic_task_count` with a sampler), distributed dynamic filters, `EXPLAIN ANALYZE` with
  per-stage metrics.
- Ships a **`datafusion-distributed-iceberg` crate** (read-only): `IcebergWorkUnitFeed` streams `FileScanTask`s.
  Open TODOs: filter/sort pushdown, delete files, partitioning. Its codec ships FileIO properties — **S3 credentials
  travel inside the plan**, which must be fixed.
- Limitations: no task retry once data streams (a worker failure fails the query); one memory pool per worker shared
  by all queries; no Prometheus/OTel exporter; API churn (upgrade guides for 3.0, 4.0 and 5.0); one dominant
  maintainer.
- Self-reported benchmark (12× c5n.2xlarge, S3 Parquet): TPC-H SF100 in 42 s vs Trino 93 s and Ballista 237 s.
- Compared with Ballista: Ballista has a scheduler, a persistent sort-based shuffle, task/stage retry and a REST API
  (better for long ETL); datafusion-distributed is better for interactive latency.

**What we must build ourselves:** a coordinator service (protocol, sessions, query lifecycle, cancellation,
timeouts), admission control and per-query memory limits, cluster membership (Kubernetes resolver) and load-aware
routing, the catalog, whole-query retry before the first row, observability (metrics, history), the Iceberg write
path, and a worker binary.

## 2. Iceberg in Rust

- The DataFusion integration moved from iceberg-rust to **`apache/datafusion-iceberg`** (DataFusion 55,
  Arrow 59.2, pinned to iceberg-rust rev `5b82ab99`).
- iceberg-rust read path is solid: manifest/partition/metrics/row-group/page-index/bloom-filter pruning, positional
  deletes, equality deletes (several fixes still open upstream), v3 deletion vectors, schema evolution.
  `FileScanTask` is serde-serializable, which makes distribution straightforward. Missing: `files`/`partitions`/
  `refs` metadata tables, SQL time travel, incremental/changelog scans.
- Write path: writers (data, position delete, equality delete; rolling, fanout) and `fast_append`, `update_schema`,
  `expire_snapshots`. **Missing:** overwrite, row-delta, rewrite-files, and Java-style conflict validation (open PRs
  only). **Blocker:** `TransactionAction`, `SnapshotProducer` and `TableCommit` are `pub(crate)`, so custom snapshot
  operations need either a thin fork or building manifests ourselves (`ManifestWriter` is public) and posting a
  `CommitTableRequest` (`TableUpdate`/`TableRequirement` are public).
- datafusion-iceberg write plan: `IcebergWriteExec` (emits `DataFile`s) → coalesce → `IcebergCommitExec`
  (`fast_append`). INSERT append only.
- **Sail** (`lakehq/sail`, Apache-2.0, DataFusion 55.1): an independent ~54k-line Iceberg implementation with
  DELETE/UPDATE/MERGE (copy-on-write and merge-on-read), commit conflict detection, `rewrite_data_files` and metadata
  tables — **the best reference design**; code can be reused under Apache-2.0.
- Lakekeeper: vended credentials (STS; refresh via `GET .../credentials`, which the iceberg-rust client does not call
  yet), remote signing, `Idempotency-Key` on commits, multi-table transactions, stage-create (for CTAS), view REST
  API. **No** compaction, snapshot expiration or orphan-file cleanup — the engine must provide these.
- Dependency conflict: Lakekeeper's own iceberg-rust fork is on iceberg 0.9 / DataFusion 53 / Arrow 58. The engine
  must be a **separate workspace** that talks to the catalog only over REST.

## 3. Federation and client protocol

- datafusion-federation (alpha, DataFusion 55): an optimizer rule finds the largest subtrees that belong to one
  provider, unparses them to SQL for the source dialect and pushes the whole subtree down (filter, projection,
  aggregate, sort, limit, same-source joins). No splitting (single partition), dynamic filters ignored, **no codec for
  distributed execution** — we must write one (ship SQL + catalog id; workers rebuild the executor).
- datafusion-table-providers 0.14 (DataFusion 55.2): PostgreSQL, MySQL, DuckDB, SQLite (mature), ClickHouse, Oracle,
  ODBC, Flight SQL, MongoDB, ADBC. No Trino provider. No catalog-wide discovery (`table_names` unimplemented).
- Trino SPI → DataFusion mapping: `ConnectorFactory`/`Connector` ≈ `CatalogProvider` (no lifecycle); `applyX`
  pushdown ≈ federation unparse; splits ≈ work unit feeds; dynamic catalogs ≈ `register_catalog` (no persistence or
  propagation to workers).
- Protocol options: **Trino HTTP** (~1.5–3k lines; JSON type encoding, `nextUri` state machine, session headers);
  **pgwire** via datafusion-postgres (usable, but one shared `SessionContext` for all connections); **Arrow Flight
  SQL** for high-throughput clients.
- sqlparser has no Trino dialect; DataFusion's SQL is closest to PostgreSQL.
- Connectors: static crates plus a registry; `datafusion-ffi` only for third-party plugins.

## 4. Caching and materialized views

- StarRocks Data Cache: byte-range block cache (256 KB) below the file reader, key `(hash(path), mtime|size, block)`,
  memory + disk tiers, SLRU eviction, async population, **cache-affinity scheduling** via consistent/rendezvous
  hashing (top-3 candidates, imbalance cap), peer cache.
- StarRocks async MVs: MANUAL / ASYNC / `EVERY` refresh; **PCT** (partition change tracking: map base partitions to
  MV partitions, overwrite stale partitions); newer IVM (delta rules, Iceberg snapshot ancestry diff, skip `REPLACE`
  snapshots); SPJG rewrite (`MaterializedViewRewriter`: predicate split, aggregate rollup, union rewrite with
  partition compensation, cost-based MV selection).
- datafusion-materialized-views (DataFusion 52): single-table SPJ + aggregate view matching (no joins yet),
  `OneOf`/`OneOfExec` cost-based candidate selection, lineage for Hive-partitioned listing tables. A trial port to
  DataFusion 55 showed 37 mechanical compile errors (~1–2 days).
- liquid-cache (DataFusion 55): caches decoded columns and evaluates predicates inside the cache, but hooks
  `ParquetSource`; the Iceberg scan uses iceberg-rust's `ArrowReader`, so it would not intercept it — later phase.
- foyer `HybridCache` (used by RisingWave) fits a memory + NVMe block cache; iceberg-rust has a pluggable `Storage`
  trait to wrap.

## 5. CDC

- RisingWave CDC runs **Debezium in an embedded JVM via JNI** (no native Rust log readers). Snapshot backfill uses the
  DBLog watermark algorithm (`cdc_backfill.rs`). Exactly-once: pre-commit metadata with a pre-assigned snapshot id,
  checked with `is_snapshot_id_in_iceberg` on replay. The Iceberg upsert sink uses equality deletes; a newer
  primary-key-index sink emits deletion vectors instead. Compaction runs separately.
- Arroyo: streaming on a DataFusion 48 fork; its Iceberg sink is append-only. Borrow ideas (retract streams,
  Debezium unrolling), not code.
- **supabase/etl** (Apache-2.0, very active): mature PostgreSQL logical replication (pgoutput), at-least-once,
  `Destination` trait — the base for PostgreSQL capture.
- MySQL: `mysql_async` binlog stream + `mysql_common::binlog` (GTID).
- moonlink: BSL 1.1 (not usable for a managed service); reference only for its PK-index + deletion-vector design.
- Proposal: a **separate ingest role** (not in the query path); 30–60 s micro-batch epochs folded by primary key;
  Iceberg commits carry `ingest.offset` in the snapshot summary (source of truth); single writer per table via a
  lease; `Idempotency-Key`. Deletes: start with equality deletes + aggressive compaction, move to deletion vectors +
  PK index. Streaming MVs later.

## 6. Platform integration contract (egret)

The egret data platform currently manages Trino and StarRocks. To integrate Egret Engine as a third engine kind,
the engine must offer:

1. A query endpoint returning typed columns with clean SQL type names, cancellation, bound parameters, describe
   without execution, and rejection of multi-statement input where requested.
2. Authentication with platform-signed JWTs (issuer/JWKS, `aud = engine`, `preferred_username`, `groups`) over TLS
   with a pinnable certificate; optionally service-user impersonation.
3. An Iceberg REST client using OAuth2 client credentials and vended S3 credentials.
4. Policy enforcement per session user (access, row filters, column masks) from a policy feed published by the
   platform.
5. Kubernetes deployment the platform can provision (provision / status / update / delete / restart).
6. A client protocol with a Python driver (notebooks connect directly).

Platform-side changes (engine kind, connector, catalog DDL rendering, policy bundle endpoint, provisioner) live in the
platform repository.

## 7. StarRocks-style Iceberg caching (deep dive)

StarRocks is fast on Iceberg because about ten caching layers work together, not just the block cache:

| StarRocks layer | Key settings | Rust equivalent | Where | Priority |
|---|---|---|---|---|
| Table cache with async refresh (`CachingIcebergCatalog`) | TTL 24 h, refresh 60 s serve-stale, REST TTL capped at 3000 s (tokens ~1 h) | moka `Cache<TableIdent, Arc<Table>>`, serve-stale + single-flight reload | Coordinator | P0 |
| Manifest content cache (key = manifest path; immutable, reused across snapshots) | 10% of heap per data/delete cache | Global moka cache keyed by manifest path, byte-weighted. iceberg-rust's `ObjectCache` is per-`Table` (32 MB) and not injectable → own cache or patch | Coordinator | P0 |
| Background refresh + pre-parse of new manifests | 600 s, compares `metadata_location` | tokio task | Coordinator | P1 |
| Raw metadata bytes, memory + disk | off by default | foyer via an iceberg `Storage` wrapper | Coordinator | P2 |
| Distributed manifest planning (`plan_mode=auto`) | thresholds on manifest count/size | Manifests as work units; workers return `FileScanTask`s | Both | P2 |
| Streaming splits | incremental scan ranges | Work unit feeds already stream | Coordinator | done |
| CBO statistics (manifest-list row counts, Puffin NDV, `ANALYZE`) | | `TableProvider::statistics`, cached by `(table, snapshot)` | Coordinator | P1 |
| Parquet footer cache | key path + mtime/size | DataFusion 55 `CachedParquetFileReaderFactory` + `FileMetadataCache` (default 50 MB) | Worker | P0 |
| Block cache, memory + disk SLRU, persistent | 256 KB blocks | foyer `HybridCache` wrapping storage reads; key `hash(path) + size + block` | Worker | P0 |
| Decoded page cache | | `RecordBatch` cache per (file, row group, columns) — later | Worker | P2 |
| IO coalescing | gap 1 MB / max 8 MB | arrow-rs `get_ranges` + block alignment | Worker | P1 |
| Population rules (`populate_datacache_mode=auto`) | no population for non-SELECT / full-column scans | Session `cache_populate`, foyer hints (RisingWave `CachePolicy` pattern) | Both | P1 |
| IO adaptor (bypass disk when slower than S3) | | Latency EWMA | Worker | P2 |
| Affinity routing | consistent hash on path + offset, top-3 | Rendezvous hashing in the work unit feed (replacing `i % txs.len()`) + `RouteTaskHandler`, stable worker identity | Coordinator | P0 |
| Peer cache during scaling | historical ring, 600 s | `peer_hint` + gRPC `FetchBlock` | Both | P2 |
| Warm-up (`CACHE SELECT`) | | Plan with forced population and a discarding sink | Both | P1 |
| Metrics (`be_datacache_metrics`) | | foyer metrics + system table | Both | P1 |
| Result cache | none for external tables in StarRocks | Key `(normalized SQL, snapshot ids)` — easy because Iceberg is versioned | Coordinator | P2 |

Reference foyer configuration: RisingWave `src/storage/src/store_impl.rs` (`RecoverMode::Quiet` to keep the cache
across restarts, FIFO picker, throttling).

Hard problems: (1) freshness vs the REST catalog — bounded staleness (60 s) by default plus a strong mode (always
`loadTable` but reuse manifests); (2) vended credential expiry — table TTL below token expiry, credentials sent per
task, no long-lived per-token object stores; **because cache keys are paths, authorization must be checked on the
coordinator before planning**; (3) Iceberg files are immutable, so content caches never go stale — only capacity and
routing matter; (4) warm cache across restarts — fixed-seed key hashing, NVMe persistent volumes, stable pod
identity; (5) caches are outside DataFusion's memory pool — subtract their budget from the pool.

## 8. Fault-tolerant execution (one engine, two modes)

- **Trino FTE** (`EventDrivenFaultTolerantQueryScheduler`): with `retry-policy=TASK` every exchange is spooled
  through the `ExchangeManager` SPI (S3 layout `{hex}.{query}.{exchange}.{sinkPartition}/{attempt}/...` plus a
  `committed` marker), stages are separated by barriers, partitions are retried, `ExchangeSourceOutputSelector`
  picks exactly one attempt per task, exchanges are encrypted per query, and memory retries use
  `peak × growthFactor`. `QUERY` re-runs the whole query with a 32 MB coordinator dedup buffer. Trino recommends a
  separate cluster for `TASK`.
- **Ballista** (DataFusion 55, Apache-2.0): reusable sort-shuffle writer (`data.arrow` + index, spill); lineage-based
  recovery (`reset_stages_on_lost_executor`). Writes with `std::fs` only, no attempt directories.
- **Feasible inside datafusion-distributed, but requires a fork.** Hook: adaptive planning calls an async
  `NetworkBoundaryBuilder` per stage in `inject_network_boundaries`, so the coordinator can await stage completion
  (a barrier). The relevant APIs are `pub(crate)` and boundary types are a closed set (shuffle, coalesce, broadcast).
  Changes: `TaskKey.attempt`; `Spool*Exec` boundaries (producers hash into a fixed number P of buckets);
  `SpoolWriteExec` on `object_store`; workers self-drive stages (`ExecuteToSpool`); a
  `FaultTolerantStageScheduler`; a replay log for work unit feeds; remote dynamic filters disabled in `TASK`; adaptive
  sizing from exact spool statistics. Estimate: ~8–12k lines (3–5 engineer-months) for `TASK`; `QUERY` retry 1–3
  weeks.
- **Idempotent writes:** writers produce `{queryId}-{stage}-{task}-{attempt}-{uuid}.parquet`; `DataFile` output
  flows through a spooled boundary so only the selected attempt reaches `IcebergCommitExec`; one transaction with
  `query-id` in the snapshot summary plus `Idempotency-Key` (verify Lakekeeper support); losing attempts' files are
  deleted by query-id prefix, with orphan cleanup as a backstop.
- **Profiles:** `NONE` (lowest latency), `QUERY` (default for reads and short writes), `TASK` (CTAS/INSERT/MERGE/
  compaction, or estimated input above ~50–100 GB / runtime above ~5 min), ideally on a separate worker pool.
- **Exchange storage:** start with object storage (simplest correct option); later hybrid NVMe + async upload.
- **Hard problems:** skew with fixed buckets, spilling, memory estimation, coordinator as a single point of failure
  (persist DAG state later), spool cleanup, S3 small files (one file per task + index, hex prefixes), determinism on
  retry, broadcast joins and dynamic filters across barriers.

## 9. Authentication options with pgwire

- PostgreSQL 18 added OAuth 2.0 (`oauth` in `pg_hba.conf`, SASL `OAUTHBEARER`, validator modules). PostgreSQL has no
  native JWT method; impersonation is `SET ROLE` / `SET SESSION AUTHORIZATION`.
- `pgwire` 0.41 supports cleartext, MD5, SCRAM-SHA-256 and **PostgreSQL 18 OAUTHBEARER** (`OauthValidator` trait,
  optional `SimpleOidcValidator`). Since the engine implements the server side, it controls authentication fully.

## 10. Decision log

1. Client protocol: **pgwire** (pgwire 0.41 + pieces of datafusion-postgres), PostgreSQL dialect; Flight SQL later.
2. Authentication: JWT in the password field (TLS) as the primary method, OAUTHBEARER (PG18), SCRAM, impersonation
   with an explicit privilege.
3. Workload: **interactive + heavy ETL** → **one engine, two execution modes**, `retry_policy = NONE | QUERY | TASK`.
4. Metadata store: a **dedicated PostgreSQL** database (Iceberg remains the source of truth for data, offsets and
   lineage).
5. Positioning: **standalone product**; egret is one integration (pluggable auth/policy/catalog, PostgreSQL-style
   RBAC, own Helm chart).
6. StarRocks-style Iceberg caching is P0 (table/manifest/block caches + affinity scheduling).
7. Authorization in P0: **full internal model** (roles/grants + `CREATE POLICY` RLS + masking policies) **plus OPA**
   (embedded `regorus` and remote HTTP, Trino-OPA-style contract, batched). Deny wins. The platform publishes an OPA
   bundle instead of a Ranger feed. Ranger is out of P0 (can be added later through the `PolicySource` trait).
   Authorization is enforced on the coordinator at planning time.
8. Naming: GitHub org `egretdb` (planned rename to `egret-data`), repo `engine`, crates `egret-engine-<x>`,
   binary `egret-engine`. License: Apache-2.0.
