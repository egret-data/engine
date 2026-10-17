# Egret Engine — P0: Core Read Engine

- **Date:** 2026-10-17
- **Status:** Design approved; spec under review
- **Architecture:** [`2026-10-17-egret-engine-architecture-design.md`](2026-10-17-egret-engine-architecture-design.md)
- **Research:** [`docs/research/2026-10-17-engine-research.md`](../../research/2026-10-17-engine-research.md)

## 0. Scope

**In P0:** distributed `SELECT` over Iceberg (REST catalog) with StarRocks-style multi-layer caching; pgwire frontend;
authentication (JWT, OAUTHBEARER, SCRAM, impersonation); full internal authorization (roles/grants, RLS, masking)
plus OPA; query lifecycle with `NONE`/`QUERY` retry policies; Helm chart and docker-compose; a minimal job framework.

**Out of P0:** Iceberg writes and `TASK` mode (P1), federation (P2), materialized views (P3), CDC (P4), Arrow Flight
SQL, Apache Ranger.

## 1. Architecture and components

### 1.1 Topology

```text
  psql / DBeaver / Metabase / platform / psycopg
                     │ pgwire (TLS)
            ┌────────▼─────────┐  L4 LB (each connection sticks to one coordinator)
            │  Coordinator ×N  │  stateless: session, parse, authz, plan,
            │                  │  metadata cache, runs the head stage
            └──┬─────┬─────┬───┘
   gRPC (dd)   │     │     │ REST          HTTP (optional)
   ┌───────────▼┐    │   ┌─▼──────────────┐   ┌─────┐
   │ Worker ×M  │    │   │ Iceberg REST   │   │ OPA │
   │ StatefulSet│    │   │ catalog        │   └─────┘
   │ NVMe cache │    │   └────────────────┘
   └─────┬──────┘    │
         │ S3      ┌─▼──────────────┐
         ▼         │ PostgreSQL meta│
     MinIO / S3    └────────────────┘
```

Binary `egret-engine --role coordinator|worker|all` (`all` = coordinator and worker in one process, for development
and single-node deployments).

### 1.2 Query flow

1. pgwire accepts the connection → authentication → `Principal`.
2. Parse (`sqlparser` `PostgreSqlDialect`). Engine commands (`CREATE ROLE/GRANT/POLICY/CATALOG`, `SET`, `SHOW`,
   `CACHE SELECT`) go to a command handler; queries continue to step 3.
3. Logical planning; catalogs resolve through the metadata cache.
4. **Authorization analyzer rule**: privilege checks, then
   `SecurityBarrier(Projection(masks) ← Filter(row filters) ← TableScan)` per table.
5. DataFusion optimizer → physical plan → distributed planner (fork) splits stages.
6. Iceberg scan: the coordinator streams `FileScanTask`s (with credentials) to workers chosen by rendezvous hashing.
7. Workers read through the foyer block cache → S3.
8. Results stream back to the coordinator → `DataRow`s. With `retry_policy=QUERY`, up to 32 MB is buffered so that
   a retry stays invisible to the client.

### 1.3 Workspace

| Crate (`crates/<x>`, package `egret-engine-<x>`) | Responsibility |
|---|---|
| `common` | Configuration (TOML + environment overrides), errors, ids (UUIDv7), telemetry |
| `meta` | PostgreSQL metadata store (sqlx + migrations) |
| `sql` | Extended DDL, PostgreSQL-compatible functions, `pg_catalog` / `information_schema` |
| `auth` | Authenticators (JWT, OAUTHBEARER, SCRAM), JWKS cache, `Principal` |
| `authz` | `PolicyEngine`, `InternalSource`, `OpaSource`, analyzer rule, `SecurityBarrier` |
| `catalog` | Iceberg REST catalog registry, cached `CatalogProvider`, credential manager |
| `iceberg` | Scan planning (manifest cache), work unit feed, data source, codec, snapshot diff |
| `cache` | `CachingStorage` (foyer), population policy, affinity hashing |
| `cluster` | `WorkerResolver` (Kubernetes Endpoints / static), `RouteTaskHandler`, health tracking |
| `coordinator` | Sessions, query lifecycle, admission control, `QUERY` retry, cancellation |
| `worker` | Worker service, runtime environment, per-query memory pool, codec registration |
| `pgwire` | Frontend: per-connection sessions, extended protocol, cancellation, type mapping |
| `jobs` | Minimal job framework (`jobs` table, `SKIP LOCKED`), scheduled `CACHE SELECT` |
| `server` | The `egret-engine` binary |

### 1.4 Dependencies

| Component | Version |
|---|---|
| DataFusion | 55.x |
| arrow / parquet | 59.2 |
| datafusion-distributed | Fork `egretdb/datafusion-distributed`, branch `egret/main`, pinned revision |
| iceberg-rust | Revision `5b82ab99` (same as datafusion-iceberg); thin fork only if needed (see §2.2) |
| pgwire | 0.41 (plus `arrow-pg` and `datafusion-pg-catalog` from datafusion-postgres) |
| foyer | 0.22 |
| regorus, sqlx, tonic, moka | Pinned via `Cargo.lock` |

## 2. Iceberg and caching

### 2.1 Catalogs

```sql
CREATE CATALOG lake USING iceberg_rest WITH (
  uri = 'https://platform.example.com/catalog', warehouse = 'main',
  auth = 'oauth2', credential = secret('lake-client'),
  access_delegation = 'vended-credentials');
DROP CATALOG lake;
SHOW CATALOGS;
```

- Definitions are stored in the PostgreSQL metadata store; secrets are stored only as **references**
  (`secret('name')` → environment variable, file, or Kubernetes Secret).
- In P0 the engine calls the catalog with a **service credential** and enforces authorization itself. Forwarding
  end-user tokens to the catalog is out of scope.
- Code base: absorb `datafusion-distributed-iceberg` into the `iceberg` crate and change it to push filters into the
  scan, read **delete files** (positional, equality, deletion vectors) through `ArrowReader`, report `Statistics`,
  and **never** put `storage_properties`/credentials into the plan protobuf.

### 2.2 Metadata caches (coordinator)

| Cache | Key → value | Behaviour |
|---|---|---|
| Table cache | `(catalog, namespace, table)` → `{metadata, metadata_location, credentials, loaded_at}` | Serve stale within `iceberg.metadata_staleness` (default `60s`) while refreshing in the background (single-flight); TTL ≤ credential expiry − 5 min; `strong` mode always calls `loadTable` but reuses manifests |
| Manifest list cache | `manifest_list_path` → manifest list | Immutable; byte-bounded |
| Manifest cache | `manifest_path` → parsed entries | Global, byte-bounded (`iceberg.manifest_cache_bytes`); column statistics kept only for partition-source, sort-key and identifier columns |
| Statistics | `(table, snapshot_id)` → row count from the manifest list | For the cost-based optimizer |

- `plan_files` is implemented by the engine on top of iceberg-rust's evaluators (`ManifestEvaluator`,
  `ExpressionEvaluator`, `InclusiveMetricsEvaluator`) using the caches above, because iceberg-rust's `ObjectCache` is
  per-`Table` and cannot be injected. **First task of M1:** verify that these evaluators are public; if not, maintain
  a thin iceberg-rust fork that widens their visibility (small patch, proposed upstream).
- **Snapshot diff API**: `diff(table, from_snapshot, to_snapshot) -> {added, removed}` (data and delete files),
  verifying that `from` is an ancestor of `to` and skipping `REPLACE` snapshots. Implemented and tested in P0; used by
  P3 and P4.

### 2.3 Credential manager

- Obtains vended credentials from `loadTable`; refreshes them via `GET .../tables/{t}/credentials` 5 minutes before
  expiry.
- The coordinator attaches credentials to **work units** (gRPC over TLS), never to plans.
- Workers keep a `Storage`/`ObjectStore` per `(bucket, credential_version)`; old versions are dropped once no task uses
  them.

### 2.4 Worker read path

```text
ArrowReader (iceberg-rust: delete files, schema evolution)
   └─ CachingStorage (wraps iceberg Storage/FileRead)
        ├─ foyer HybridCache: memory tier + NVMe tier, RecoverMode::Quiet (survives restarts)
        │    key = (xxh3(path), file_size, block_idx), block size 1 MB by default (cache.block_size)
        └─ miss → S3 range GET (block-aligned) → asynchronous population
```

- The cache wraps the iceberg `Storage` layer (not `ParquetSource`) so delete files and schema evolution stay correct.
  Consequence: parsed Parquet footers are **not** cached in P0 (footer bytes still hit the block cache); a parsed
  footer cache is P1.
- No invalidation: Iceberg files are immutable and paths are never reused; `file_size` in the key guards against
  out-of-contract overwrites.
- Keys are hashed with a fixed seed and the key format is versioned (`cache.key_version`), so the cache stays valid
  across restarts and upgrades.
- The memory tier budget is **subtracted** from the worker's DataFusion memory pool.
- **Population policy:** session setting `cache_populate = auto | always | never`, decided on the coordinator and sent
  in the work unit. `auto` skips population for queries issued by maintenance jobs and for scans that read every
  column of a table with at least 20 columns (`cache.populate_full_scan_min_columns`).
- Configuration: `cache.memory_bytes`, `cache.disk_path`, `cache.disk_bytes`, `cache.block_size`.

### 2.5 Affinity scheduling

- A split is `(path, byte range)` aligned to row groups, sized by
  `distributed.file_scan_config_bytes_per_partition`.
- Splits are assigned to task slots by **rendezvous hashing** over the healthy worker list (identified by StatefulSet
  ordinal). If a worker exceeds 1.5× the average assigned bytes (`cache.affinity_imbalance_ratio`), the split goes to
  the next candidate.
- A `RouteTaskHandler` pins task slots to worker URLs. This replaces the current round-robin assignment
  (`i % txs.len()`).

### 2.6 Cache operations

- `CACHE SELECT cols FROM t [WHERE ...]` forces `cache_populate=always`, discards rows and returns the number of bytes
  loaded. Scheduling uses the job framework:
  `CREATE CACHE JOB name SCHEDULE '<cron>' AS CACHE SELECT ...`.
- Metrics: hit/miss bytes in `EXPLAIN ANALYZE`; Prometheus (`egret_engine_cache_*`); `system.cache_stats`
  aggregated from workers over gRPC.

## 3. Authentication and authorization

### 3.1 Authentication

`pg_hba`-style rules in the configuration (first match wins):

```toml
[[auth.rules]]
users = ["*"]          # pattern
source = "0.0.0.0/0"   # CIDR
tls = true
method = "jwt"         # jwt | oauthbearer | scram | reject
```

- **JWT providers** (`[[auth.jwt.providers]]`): `issuer`, `jwks_uri`, `audience`, `username_claim` (default
  `preferred_username`), `groups_claim` (default `groups`), `algorithms` (allowlist; `none` is always rejected).
  JWKS documents are cached and refetched on an unknown `kid` (rate-limited). `iss`, `aud`, `exp` and `nbf` are
  validated with 60 s leeway.
- With `jwt`, the password field carries the token, and the startup `user` parameter **must equal** the username
  claim.
- `oauthbearer`: SASL OAUTHBEARER (PostgreSQL 18) via pgwire's `OauthValidator`, sharing the JWT validator.
- `scram`: SCRAM-SHA-256 for internal roles created with `LOGIN PASSWORD`; verifiers stored in the metadata store.
- Methods that transmit secrets require TLS; a rule allowing `jwt` without TLS is a startup configuration error.
- Sessions expire at the token's `exp`: any statement after `exp` fails with `28000` and the connection is closed.
- `Principal { user, groups, roles (recursively resolved), claims, auth_method }`.
- **Impersonation**: `SET SESSION AUTHORIZATION 'bob'` requires `GRANT IMPERSONATE ON USER bob TO svc` or
  `GRANT IMPERSONATE TO svc` (any user). The new principal is user `bob` with groups taken from `bob`'s internal role
  memberships and empty claims. `RESET SESSION AUTHORIZATION` restores the original principal. Every impersonation is
  audited.

### 3.2 Roles and privileges (internal source)

- `CREATE ROLE name [LOGIN] [PASSWORD '...'] [SUPERUSER] [BYPASSRLS]`, `DROP ROLE`, `GRANT role TO role`.
- JWT users need not be created upfront: each user implicitly has a role with its own name (if such a role exists,
  its attributes apply). Group `g` maps to role `g` when that role exists (`auth.group_role_prefix`, empty by
  default).
- The first superusers are bootstrapped from configuration (`authz.bootstrap_superusers = ["admin"]`).
- Securables: `CATALOG > SCHEMA > TABLE > COLUMN`, plus `SYSTEM`.
- Privileges: `USAGE` (catalog, schema), `SELECT` (table, column), `CREATE` (catalog, schema), `MANAGE` (manage
  grants/policies on an object), `IMPERSONATE`. `INSERT/UPDATE/DELETE` are declared but take effect only from P1.
- **Grants inherit downward, including objects created later** (unlike PostgreSQL):
  `GRANT SELECT ON SCHEMA lake.sales TO analysts` covers every current and future table in that schema.
- Reading a table requires `USAGE` on its catalog and schema, and `SELECT` on the table (or on every referenced
  column).

### 3.3 Internal RLS and masking

```sql
CREATE POLICY eu_only ON lake.sales.orders AS PERMISSIVE FOR SELECT TO analysts
  USING (region = session_attr('region'));

CREATE MASKING POLICY mask_email AS (v varchar) RETURNS varchar ->
  CASE WHEN is_member_of('pii_readers') THEN v ELSE mask_email_local(v) END;
ALTER TABLE lake.crm.users ALTER COLUMN email SET MASKING POLICY mask_email;
ALTER TABLE lake.crm.users ALTER COLUMN email UNSET MASKING POLICY;
```

- RLS follows PostgreSQL semantics: permissive policies combine with OR, restrictive ones with AND; if a table has at
  least one policy and the principal matches no permissive policy, it sees **zero rows**; `SUPERUSER` and `BYPASSRLS`
  skip RLS.
- P0 policy expressions may use table columns, literals, deterministic built-in functions and session functions
  `current_user`, `current_groups()`, `is_member_of(text)` and `session_attr(text)` (JWT claims). **No subqueries**
  (P1).
- Built-in masks: `mask_hash(v)`, `mask_null(v)`, `mask_show_last(v, n)`, `mask_year(d)`, `mask_email_local(v)`. A
  masking policy's return type must equal the column type. At most one mask per column.
- Order: row filters evaluate on raw values, then masks apply; user expressions only ever see masked values.

### 3.4 OPA source

- Input (Trino-OPA-style contract):

```json
{
  "context": {"identity": {"user": "alice", "groups": ["analysts"]}, "claims": {"region": "eu"}},
  "action": {
    "operation": "SelectFromColumns",
    "resource": {"table": {"catalogName": "lake", "schemaName": "sales", "tableName": "orders",
                           "columns": ["id", "amount"]}}
  }
}
```

- Rules (package `egret_engine`): `allow` (boolean), `batch` (filters object lists for `SHOW`/metadata),
  `row_filters` (`[{"expression": "..."}]`), `column_masks` (`{"expression": "..."}`). Expressions are SQL, parsed in
  the table's scope, with the same restrictions as §3.3.
- `embedded` mode: `regorus` in-process, bundle polling (`opa.bundle_url`, ETag, `opa.poll_interval`), optional
  bundle signature verification. `remote` mode: HTTP `POST /v1/data/egret_engine/...`, batched per query, decisions
  cached for `opa.decision_cache_ttl` (default 5 s).
- The engine ships a **standard Rego library** (`policy/rego/`) that reads a `data.json` containing grants, row
  filters and masks; integrators only provide `data.json`.
- Fail closed: if OPA errors, is unreachable, or no bundle has loaded yet, the query is denied (`42501`, with the cause
  in the message).

### 3.5 Combination and enforcement

- Result = internal **AND** OPA (when OPA is enabled). Row filters from both sources are ANDed. If both sources mask
  the same column, the query fails with `42501` "conflicting masking policies".
- **Analyzer rule** on the `LogicalPlan` (before the optimizer), for every `TableScan`: check table and referenced
  column privileges → replace with `SecurityBarrier(Projection(masks) ← Filter(row filters) ← TableScan)`.
- **`SecurityBarrier`** (new logical node): the optimizer may push a user predicate below the barrier only if it is
  **leakproof** — a comparison / `IN` / `IS NULL` / `BETWEEN` between a column and literals, deterministic, unable to
  raise errors, and not referencing a masked column. Other predicates stay above the barrier.
- Metadata (`SHOW`, `information_schema`, `pg_catalog`) lists only objects on which the principal holds at least one
  privilege.
- Missing `USAGE`/`SELECT` on a table → `42P01` (the table's existence is not revealed); `USAGE` present but column
  privileges missing → `42501`.
- Optimizer ordering reserved for P3: authorization → MV rewrite.
- Audit: one JSON log event per query
  `{query_id, principal, auth_method, impersonated_by, objects, decision, policies_applied, ts}`.

## 4. pgwire frontend and query lifecycle

### 4.1 Frontend

- `pgwire` 0.41 + `arrow-pg` + `datafusion-pg-catalog`; handlers are our own, with **one session per connection**.
- Protocol: simple query (multi-statement executed sequentially), extended query (Parse/Bind/Describe/Execute, `$n`
  parameters, parameter type inference, per-session prepared statement cache), **portals with row limits** (cursor
  fetch, which is also the backpressure mechanism), `CancelRequest` (per-backend secret key), TLS (rustls),
  ParameterStatus: `server_version` (`16.0 (egret-engine <version>)`), `server_encoding`/`client_encoding` `UTF8`,
  `DateStyle` `ISO, MDY`, `TimeZone`, `standard_conforming_strings` `on`, `integer_datetimes` `on`.
- `BEGIN`/`COMMIT`/`ROLLBACK` are no-ops in P0.
- Namespaces: `dbname` = default catalog; `search_path` = default schema; three-part names always work.
- Arrow → PostgreSQL types: Int16/32/64 → int2/4/8; Float32/64 → float4/8; Decimal128/256 → numeric;
  Utf8/LargeUtf8/Utf8View → text; Boolean → bool; Date32 → date; Time64 → time; Timestamp (without/with time zone) →
  timestamp/timestamptz; Binary → bytea; FixedSizeBinary(16) (Iceberg uuid) → uuid; List → array; Struct/Map → json
  (text). Binary format for numeric, boolean, date/time and uuid types.
- `pg_catalog`/`information_schema` filtered by privileges, plus introspection functions: `version()`,
  `current_schema()`, `current_database()`, `format_type`, `has_table_privilege`, `has_schema_privilege`,
  `pg_table_is_visible`, `pg_get_userbyid`.

### 4.2 Session

```text
Session { principal, settings, prepared_stmts, portals, current_query,
          state: SessionState }  // DataFusion + distributed planner + codecs; catalog caches are global
```

`SET` variables: `retry_policy` (`none|query`, default `query`), `cache_populate`, `iceberg.metadata_staleness`,
`statement_timeout`, `query_max_memory`, `search_path`, `distributed.*`.

### 4.3 Query lifecycle

- `query_id` is a UUIDv7; states `QUEUED → PLANNING → RUNNING → FINISHED | FAILED | CANCELED`; an in-memory registry
  per coordinator exposed as `system.running_queries`; cancellation via `SELECT system.cancel_query('<id>')` (allowed
  for the query owner, superusers, or holders of `MANAGE` on `SYSTEM`).
- **Admission control** with simple resource groups assigned by role:

```toml
[[resource_groups]]
name = "bi"
roles = ["analysts"]
max_running = 20
max_queued = 100
queue_timeout = "60s"
```

  Limits apply **per coordinator** in P0. Principals matching no group use group `default`
  (`max_running = 32`, `max_queued = 256`, `queue_timeout = "300s"`, overridable).
- **Memory**: fork patch — a memory pool per `query_id` on each worker (limit `query_max_memory`, default 30% of the
  worker pool); over the limit → spill; if spilling is impossible → `53200`.
- **Cancellation**: `CancelRequest`, `statement_timeout`, or client disconnect → drop `DistributedExec` → workers
  receive end-of-stream and stop their tasks.
- **`QUERY` retry**: only for infrastructure errors (worker unavailable, lost connection, transport error). Results
  are buffered up to `retry_buffer_size` (default 32 MB) before being sent; a failure during buffering re-plans the
  query without the failed worker, up to `query_retry_attempts` (default 2). Once the buffer overflows, results stream
  and the query is no longer retryable. `retry_policy=none` streams immediately.
- **Worker health**: `GetWorkerInfo` heartbeat every 5 s; failed workers are excluded for a 30 s cooldown; affinity
  hashing uses the healthy set.

### 4.4 SQLSTATE mapping

| Situation | SQLSTATE |
|---|---|
| Syntax error | `42601` |
| Table missing, or not visible to the principal | `42P01` |
| Insufficient privilege (object visible) | `42501` |
| Token expired / invalid credentials | `28000` / `28P01` |
| Canceled / timed out | `57014` |
| Out of memory | `53200` |
| Queue full / queue timeout | `53300` |
| Shutting down | `57P01` |
| Internal error | `XX000` |

### 4.5 Operations

- OpenTelemetry tracing (query → stage → task, propagated through gRPC headers); Prometheus `/metrics` on
  coordinators and workers; slow-query log (`log.slow_query_threshold`).
- `EXPLAIN` / `EXPLAIN ANALYZE` for distributed plans, including per-stage metrics and cache hit/miss.
- Graceful shutdown: coordinators stop accepting connections and wait for running queries up to
  `shutdown.grace_period`; workers switch to draining (excluded from new assignments) and finish running tasks.

## 5. Fork, integration, deployment and testing

### 5.1 datafusion-distributed fork

- Repository `egretdb/datafusion-distributed`, branch `egret/main`, rebased on upstream monthly as a small patch
  series.
- P0 patches: (1) per-query memory pools on workers; (2) `TaskKey.attempt` in the protocol (unused until P1).
- Affinity, the Iceberg feed and credentials use public hooks (`RouteTaskHandler`, work unit feeds, codecs) — no fork
  needed.

### 5.2 Platform integration contract

Platform-side work is specified and implemented in the platform repository. The engine provides:

| Area | Engine contract |
|---|---|
| Connectivity | pgwire over TLS with a pinnable certificate; per-user platform-signed JWTs as passwords |
| Queries | `$n` parameters, describe via Parse/Describe without execution, row limits via portals, `CancelRequest` |
| Catalogs | `CREATE/ALTER/DROP CATALOG` DDL that the platform renders and applies |
| Policies | OPA bundle (`data.json` following the engine's Rego library) published by the platform |
| Provisioning | Helm chart `egret-engine`; configuration for the platform's JWT issuer and `opa.bundle_url` |
| Monitoring | `system.running_queries`, `system.cancel_query` |

### 5.3 Deployment

- Helm chart `egret-engine` (in this repository): coordinator Deployment + Service (5432 pgwire, metrics); worker
  StatefulSet + NVMe PVC (cache) + headless Service; `WorkerResolver` reads Kubernetes Endpoints. The PostgreSQL
  metadata store and the catalog are external dependencies.
- Distroless single-binary image; TOML configuration with environment overrides; secrets from environment or files.
- `deploy/docker-compose.yml` for development: PostgreSQL, MinIO, Lakekeeper, engine `--role all`, optional OPA
  profile.

### 5.4 Testing

| Layer | Approach |
|---|---|
| Unit | Per crate, Arrange / Act / Assert |
| SQL correctness | sqllogictest `.slt` files executed **over pgwire** against an in-process cluster |
| Iceberg | Fixtures created with Spark + Iceberg containers (equality deletes, deletion vectors, schema and partition evolution); results compared with Spark |
| Distribution | TPC-H / TPC-DS SF1: distributed results equal single-node results |
| RLS / masking | Differential tests against a real PostgreSQL (same policies and data); leak-test matrix for `SecurityBarrier` |
| OPA | Golden tests for the Rego library; same cases through regorus and real OPA |
| Clients | CI smoke tests: psql, psycopg, pgjdbc, SQLAlchemy |
| Chaos | Kill a worker mid-query (`QUERY` retry); restart a worker (cache retained) |
| Performance | TPC-H SF10/SF100 on MinIO, cold and warm cache, compared with Trino and StarRocks on the same hardware |

### 5.5 Milestones

| Milestone | Content |
|---|---|
| M0 | Workspace, configuration, `--role all` binary, pgwire simple query over in-memory DataFusion, CI |
| M1 | Single-node Iceberg reads via REST, metadata caches, delete files, snapshot diff API (verify evaluator visibility first) |
| M2 | Distribution: fork, workers, resolver, work unit feed, credential manager |
| M3 | Caching: foyer `CachingStorage`, affinity, population policy, `CACHE SELECT`, metrics |
| M4 | Authentication and internal authorization (roles/grants, RLS, masking, `SecurityBarrier`) plus OPA |
| M5 | Lifecycle: admission control, memory pools, `QUERY` retry, cancellation, SQLSTATE, BI compatibility, job framework |
| M6 | Helm chart, docker-compose, operations documentation |

### 5.6 Definition of done

1. Metabase, DBeaver, psql and the egret platform query Iceberg tables (Lakekeeper) over pgwire with JWT
   authentication.
2. Tables with equality deletes and deletion vectors return results identical to Spark across all fixtures.
3. RLS/masking differential tests match PostgreSQL; every `SecurityBarrier` leak test passes.
4. Killing one worker during a query whose result is under 32 MB still lets the query succeed.
5. After a worker restart, the next run's cache hit ratio is at least 90% of the pre-restart ratio.
6. TPC-H SF100 with a warm cache completes in **less total time than Trino** on the same hardware.
