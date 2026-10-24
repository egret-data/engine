# P0 / M1a Iceberg Catalogs and Single-Node Reads Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Register Iceberg REST catalogs with `CREATE CATALOG` (persisted in a PostgreSQL metadata store,
secrets stored as references) and query their tables over pgwire on a single node, with positional deletes,
deletion vectors, schema evolution and partition evolution returning the same rows as Spark.

**Architecture:** Five new crates, following spec §1.3.
- `egret-engine-meta` stores catalog definitions: `CatalogStore` with a PostgreSQL implementation (sqlx and
  embedded migrations) and an in-memory one for when no metadata store is configured.
- `egret-engine-sql` parses the engine's DDL (`CREATE/DROP CATALOG`, `SHOW CATALOGS`) and hands every other
  statement to DataFusion's parser.
- `egret-engine-iceberg` exposes an iceberg-rust `Catalog` as a DataFusion catalog. Tables are loaded per query;
  scans go through iceberg-rust's `TableScan::to_arrow`, which applies delete files and schema evolution.
- `egret-engine-catalog` resolves secrets, maps catalog options to REST catalog properties and keeps the
  shared `CatalogRegistry`.
- `egret-engine-coordinator` owns the `Engine` shared by all connections (one `RuntimeEnv`, the registry) and
  the per-connection `Session`, which executes statements and maps errors to SQLSTATE codes.

The pgwire crate becomes protocol-only and builds each session after the startup handshake, so the `database`
parameter selects the default catalog. This addresses the M0 review notes on issue #8.

**Tech Stack:** Rust 1.99.0, DataFusion 55.2, iceberg-rust / iceberg-catalog-rest / iceberg-storage-opendal at
git rev `5b82ab99aeaf50bf4f5b3b776ac9cd6380a5f3e3` (Arrow 59.3), sqlx 0.9.0 (PostgreSQL), pgwire 0.41.1. The
test environment uses Docker Compose with PostgreSQL 17.6, RustFS 1.0.0, `apache/iceberg-rest-fixture:1.10.1`,
and Spark 4.0.1 with Iceberg 1.10.1.

**Spec:** [`docs/superpowers/specs/2026-10-17-egret-engine-p0-core-design.md`](../specs/2026-10-17-egret-engine-p0-core-design.md)
(§1.3 `meta`/`sql`/`catalog`/`iceberg`/`coordinator`, §2.1, §2.3 first bullet, §4.1 namespaces, §4.4 `42P01`).
**Roadmap:** [`2026-10-17-p0-roadmap.md`](2026-10-17-p0-roadmap.md). M1 is split into M1a (this plan), M1b (the
engine's own scan planning: global manifest caches, table cache, statistics, filter pushdown, equality-delete
fixtures, iceberg-rust fork) and M1c (snapshot diff API, credential refresh).

## Global Constraints

- Package names `egret-engine-<x>` in `crates/<x>`; every crate uses `version/edition/rust-version/license/
  repository/publish.workspace = true` and `[lints] workspace = true`.
- iceberg-rust crates come from `https://github.com/apache/iceberg-rust` at rev
  `5b82ab99aeaf50bf4f5b3b776ac9cd6380a5f3e3`: `iceberg`, `iceberg-catalog-rest`, and `iceberg-storage-opendal`
  with `default-features = false, features = ["opendal-s3"]`.
- `sqlx = 0.9.0`, `default-features = false`, features `json`, `macros`, `migrate`, `postgres`, `runtime-tokio`.
  Only runtime queries (`sqlx::query`, `sqlx::query_as`) are used, never the compile-time-checked `query!`
  macros, so builds never need a database.
- Test environment host ports: PostgreSQL `25432` (user, password and database `egret`), object store `29000`
  (`admin` / `password`, region `us-east-1`, path-style access), REST catalog `28181`. Tests read
  `EGRET_TEST_POSTGRES_URL`, `EGRET_TEST_REST_URI` and `EGRET_TEST_S3_ENDPOINT` to override these defaults.
- Tests that need the environment are marked `#[ignore = "requires the dev/iceberg test environment"]` and run
  with `cargo test --workspace -- --ignored` after `docker compose -f dev/iceberg/docker-compose.yml up -d --wait`.
- SQLSTATE codes:

  | Code | Meaning |
  |---|---|
  | `42601` | Syntax error |
  | `42P01` | Undefined table |
  | `42704` | Undefined catalog |
  | `42710` | Duplicate catalog |
  | `42939` | Reserved name |
  | `22023` | Invalid option |
  | `08001` | Catalog unreachable |
  | `0A000` | Not supported |
  | `XX000` | Everything else |
- Secrets:
  - `secret('<name>')` resolves from `EGRET_SECRET_<NAME>` (upper case; every character other than a letter
    or digit becomes `_`), then from the file `<secrets.dir>/<name>`.
  - Names match `[A-Za-z0-9._-]+` and do not start with `.`.
  - Only references are stored, never values.
- Catalog connector name `iceberg_rest`. The name `datafusion` is reserved: it is each session's in-memory
  catalog. Unqualified tables resolve in schema `public`.
- Configuration keys `meta.url` (optional PostgreSQL URL) and `secrets.dir` (optional directory). Their
  environment overrides are `EGRET_ENGINE__META__URL` and `EGRET_ENGINE__SECRETS__DIR`.
- CI gates (unchanged): `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D
  warnings`, `cargo test --workspace --locked`. CI gains an `integration` job that starts the environment and
  runs `cargo test --workspace --locked -- --ignored`.
- Tests follow Arrange / Act / Assert. Commits follow Conventional Commits. Work happens on branch
  `feat/p0-m1a-catalogs` for issue #16 (`P0/M1a`), with one commit per task, and the PR says `Closes #16`.

## File Structure

| File | Responsibility |
|---|---|
| `dev/iceberg/docker-compose.yml` | Test environment: PostgreSQL, RustFS, REST catalog, Spark, provisioning |
| `dev/iceberg/spark/Dockerfile`, `spark-defaults.conf` | Spark Connect server with the Iceberg runtime and the `rest` catalog |
| `dev/iceberg/spark/provision.py` | Creates the fixture tables; writes Spark's results to `dev/iceberg/expected/fixtures.json` |
| `.github/workflows/ci.yml` | Adds the `integration` job |
| `crates/meta/src/catalog.rs` | `CatalogDefinition`, `OptionValue`, `CatalogStore`, `MetaError` |
| `crates/meta/src/memory.rs` | `InMemoryCatalogStore` |
| `crates/meta/src/postgres.rs`, `migrations/0001_catalogs.sql` | `PgCatalogStore` and the `catalogs` table |
| `crates/sql/src/parser.rs` | `Statement`, `parse()` |
| `crates/iceberg/src/catalog.rs` | `IcebergCatalogProvider`, `IcebergSchemaProvider` |
| `crates/iceberg/src/table.rs` | `IcebergTableProvider` |
| `crates/iceberg/src/scan.rs` | `IcebergScan` (DataFusion `DataSource`) |
| `crates/catalog/src/secrets.rs` | `SecretResolver`, `SecretError` |
| `crates/catalog/src/rest.rs` | Option → REST property mapping, REST catalog connection |
| `crates/catalog/src/registry.rs` | `CatalogRegistry` |
| `crates/catalog/src/error.rs` | `CatalogError` |
| `crates/coordinator/src/engine.rs` | `Engine`: shared runtime and catalogs, session construction |
| `crates/coordinator/src/session.rs` | `Session`, `SessionParams`, `Output`, statement execution |
| `crates/coordinator/src/catalog_list.rs` | `SessionCatalogList`: shared catalogs plus session-local ones |
| `crates/coordinator/src/error.rs` | `QueryError`, `sqlstate` codes |
| `crates/pgwire/src/{handler,server,error}.rs` | Protocol only: the session is built after startup, then statements are executed |
| `crates/common/src/config.rs` | Adds `MetaConfig`, `SecretsConfig` |
| `crates/server/src/lib.rs` | Opens the metadata store, loads catalogs, builds the engine |
| `crates/server/tests/iceberg.rs` | End-to-end tests over pgwire against the environment |

---

### Task 1: Iceberg test environment

**Files:**
- Create: `dev/iceberg/docker-compose.yml`, `dev/iceberg/spark/Dockerfile`, `dev/iceberg/spark/spark-defaults.conf`,
  `dev/iceberg/spark/provision.py`
- Modify: `.gitignore`, `.github/workflows/ci.yml`, `CONTRIBUTING.md` (Development section)

**Interfaces:**
- Consumes: nothing.
- Produces: the running environment on the host ports above, and `dev/iceberg/expected/fixtures.json`. That
  file is a JSON array of `{"sql": "...", "rows": [[<text or null>, ...], ...]}`, with values rendered in the
  engine's PostgreSQL text format. Spark created four tables in catalog `rest`, namespace `fixtures`:
  `positional_deletes` (v2, positional delete file), `deletion_vectors` (v3, PUFFIN deletion vector),
  `schema_evolution` (columns added, renamed, promoted and dropped) and `partition_evolution` (identity
  partitioning replaced by `bucket(4, id)`).

- [ ] **Step 1: Open the issue and branch**

The issue is #16 `P0/M1a: Iceberg catalogs and single-node reads`, a sub-issue of #8 (M1).

```bash
git checkout main && git pull
git checkout -b feat/p0-m1a-catalogs
```

- [ ] **Step 2: Create the Compose file**

`dev/iceberg/docker-compose.yml`:

```yaml
# Integration test environment for Egret Engine.
#
#   docker compose -f dev/iceberg/docker-compose.yml up -d --wait
#
# Starts PostgreSQL (metadata store), an S3-compatible object store, an Iceberg REST catalog and
# Spark, then provisions the fixture tables and writes Spark's results for the fixture queries to
# dev/iceberg/expected/fixtures.json. Host ports use the 2xxxx range to avoid local services.

name: egret-engine-iceberg

services:
  postgres:
    image: postgres:17.6
    environment:
      POSTGRES_USER: egret
      POSTGRES_PASSWORD: egret
      POSTGRES_DB: egret
    ports:
      - "25432:5432"
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "egret", "-d", "egret"]
      interval: 2s
      timeout: 2s
      retries: 30

  object-store:
    image: rustfs/rustfs:1.0.0
    environment:
      RUSTFS_ACCESS_KEY: admin
      RUSTFS_SECRET_KEY: password
    ports:
      - "29000:9000"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/health/ready"]
      interval: 2s
      timeout: 2s
      retries: 30

  create-buckets:
    image: rustfs/rustfs:1.0.0
    depends_on:
      object-store:
        condition: service_healthy
    entrypoint: >
      /bin/sh -c "curl -fsS --aws-sigv4 aws:amz:us-east-1:s3 -u admin:password
      -X PUT http://object-store:9000/warehouse"

  rest:
    image: apache/iceberg-rest-fixture:1.10.1
    environment:
      AWS_ACCESS_KEY_ID: admin
      AWS_SECRET_ACCESS_KEY: password
      AWS_REGION: us-east-1
      CATALOG_CATALOG__IMPL: org.apache.iceberg.jdbc.JdbcCatalog
      CATALOG_URI: jdbc:sqlite:file:/tmp/iceberg_rest.db?journal_mode=WAL
      CATALOG_WAREHOUSE: s3://warehouse/rest
      CATALOG_IO__IMPL: org.apache.iceberg.aws.s3.S3FileIO
      CATALOG_S3_ENDPOINT: http://object-store:9000
      CATALOG_S3_PATH__STYLE__ACCESS: "true"
    depends_on:
      create-buckets:
        condition: service_completed_successfully
    ports:
      - "28181:8181"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8181/v1/config"]
      interval: 2s
      timeout: 2s
      retries: 30
      start_period: 5s

  spark:
    build: ./spark
    environment:
      AWS_ACCESS_KEY_ID: admin
      AWS_SECRET_ACCESS_KEY: password
      AWS_REGION: us-east-1
    depends_on:
      rest:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "sh", "-c", "netstat -an | grep 15002 | grep LISTEN"]
      interval: 5s
      timeout: 5s
      retries: 60
      start_period: 20s

  provision:
    image: ghcr.io/astral-sh/uv:0.9.5-python3.12-bookworm-slim
    depends_on:
      spark:
        condition: service_healthy
    entrypoint:
      - /bin/sh
      - -c
      - rm -f /tmp/provisioned && uv run /opt/provision/provision.py && touch /tmp/provisioned && tail -f /dev/null
    volumes:
      - ./spark/provision.py:/opt/provision/provision.py:ro
      - ./expected:/opt/provision/expected
      - uv-cache:/root/.cache/uv
    healthcheck:
      test: ["CMD-SHELL", "[ -f /tmp/provisioned ]"]
      interval: 2s
      timeout: 2s
      retries: 150
      start_period: 10s

volumes:
  uv-cache:
```

- [ ] **Step 3: Create the Spark image**

`dev/iceberg/spark/Dockerfile`:

```dockerfile
# Spark Connect server with the Iceberg runtime, used only to create fixture tables and to compute
# the expected results of the fixture queries.
FROM apache/spark:4.0.1

ARG ICEBERG_VERSION=1.10.1
ARG MAVEN=https://repo.maven.apache.org/maven2

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl net-tools \
    && rm -rf /var/lib/apt/lists/*

RUN cd "${SPARK_HOME}/jars" \
    && for jar in \
        "org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-4.0_2.13-${ICEBERG_VERSION}.jar" \
        "org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
       do curl -fsSL --retry 3 -O "${MAVEN}/${jar}"; done \
    && chown -R spark:spark "${SPARK_HOME}/jars"

COPY --chown=spark:spark spark-defaults.conf "${SPARK_HOME}/conf/"

USER spark
CMD ["sh", "-c", "SPARK_NO_DAEMONIZE=true ${SPARK_HOME}/sbin/start-connect-server.sh"]
```

`dev/iceberg/spark/spark-defaults.conf`:

```text
spark.sql.extensions                  org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.rest                org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.rest.type           rest
spark.sql.catalog.rest.uri            http://rest:8181
spark.sql.catalog.rest.io-impl        org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.rest.s3.endpoint    http://object-store:9000
spark.sql.catalog.rest.s3.path-style-access true
spark.sql.catalog.rest.cache-enabled  false
spark.sql.defaultCatalog              rest
spark.sql.catalogImplementation       in-memory
spark.sql.session.timeZone            UTC
spark.sql.ansi.enabled                false
spark.ui.enabled                      false
```

- [ ] **Step 4: Create the provisioning script**

`dev/iceberg/spark/provision.py`:

```python
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pyspark[connect]==4.0.1",
# ]
# ///
"""Creates the Iceberg fixture tables and records Spark's results for the fixture queries.

The engine's integration tests run the same queries over pgwire and compare the rows with
`expected/fixtures.json`, so every value is rendered in PostgreSQL text format.
"""

import datetime
import decimal
import json
from pathlib import Path

from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://spark:15002").getOrCreate()

NAMESPACE = "rest.fixtures"
TABLES = ["positional_deletes", "deletion_vectors", "schema_evolution", "partition_evolution"]

# Re-running the provisioning recreates every fixture table.
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")
for table in TABLES:
    spark.sql(f"DROP TABLE IF EXISTS {NAMESPACE}.{table} PURGE")

MERGE_ON_READ = """
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
"""

# Positional delete files (format version 2), written by two separate DELETE commands.
spark.sql(f"""
    CREATE TABLE {NAMESPACE}.positional_deletes (id INT, letter STRING, day DATE)
    USING iceberg
    TBLPROPERTIES ('format-version' = '2', {MERGE_ON_READ})
""")
spark.sql(f"""
    INSERT INTO {NAMESPACE}.positional_deletes
    SELECT id, CHAR(96 + id), DATE_ADD(DATE '2026-01-01', id)
    FROM (SELECT EXPLODE(SEQUENCE(1, 12)) AS id)
""")
spark.sql(f"DELETE FROM {NAMESPACE}.positional_deletes WHERE id = 9")
spark.sql(f"DELETE FROM {NAMESPACE}.positional_deletes WHERE letter = 'f'")

# Deletion vectors (format version 3) on a single data file.
spark.sql(f"""
    CREATE TABLE {NAMESPACE}.deletion_vectors (id BIGINT, amount DECIMAL(10, 2))
    USING iceberg
    TBLPROPERTIES ('format-version' = '3', {MERGE_ON_READ})
""")
spark.sql("""
    SELECT CAST(id AS BIGINT) AS id, CAST(id / 4 AS DECIMAL(10, 2)) AS amount
    FROM (SELECT EXPLODE(SEQUENCE(1, 1000)) AS id)
""").coalesce(1).writeTo(f"{NAMESPACE}.deletion_vectors").append()
spark.sql(f"DELETE FROM {NAMESPACE}.deletion_vectors WHERE id % 7 = 0")
spark.sql(f"DELETE FROM {NAMESPACE}.deletion_vectors WHERE id BETWEEN 100 AND 199")

# Schema evolution: add, rename, promote and drop columns between writes.
spark.sql(f"CREATE TABLE {NAMESPACE}.schema_evolution (id INT, name STRING, legacy STRING) USING iceberg")
spark.sql(f"INSERT INTO {NAMESPACE}.schema_evolution VALUES (1, 'one', 'x'), (2, 'two', 'y')")
spark.sql(f"ALTER TABLE {NAMESPACE}.schema_evolution ADD COLUMN score DOUBLE")
spark.sql(f"ALTER TABLE {NAMESPACE}.schema_evolution RENAME COLUMN name TO label")
spark.sql(f"ALTER TABLE {NAMESPACE}.schema_evolution ALTER COLUMN id TYPE BIGINT")
spark.sql(f"ALTER TABLE {NAMESPACE}.schema_evolution DROP COLUMN legacy")
spark.sql(f"INSERT INTO {NAMESPACE}.schema_evolution VALUES (3, 'three', 1.5), (4000000000, 'big', 2.25)")

# Partition evolution: identity partitioning replaced by a bucket transform.
spark.sql(f"""
    CREATE TABLE {NAMESPACE}.partition_evolution (id BIGINT, category STRING, ts TIMESTAMP_NTZ)
    USING iceberg PARTITIONED BY (category)
""")
spark.sql(f"""
    INSERT INTO {NAMESPACE}.partition_evolution VALUES
        (1, 'a', TIMESTAMP_NTZ '2026-01-01 08:00:00'),
        (2, 'b', TIMESTAMP_NTZ '2026-01-02 09:30:00'),
        (3, 'a', TIMESTAMP_NTZ '2026-01-03 10:45:00')
""")
spark.sql(f"ALTER TABLE {NAMESPACE}.partition_evolution ADD PARTITION FIELD bucket(4, id)")
spark.sql(f"ALTER TABLE {NAMESPACE}.partition_evolution DROP PARTITION FIELD category")
spark.sql(f"""
    INSERT INTO {NAMESPACE}.partition_evolution VALUES
        (4, 'b', TIMESTAMP_NTZ '2026-02-01 00:00:00'),
        (5, 'c', TIMESTAMP_NTZ '2026-02-02 12:00:00')
""")

# Queries whose results the engine must reproduce. Each one has a deterministic order.
QUERIES = [
    f"SELECT * FROM {NAMESPACE}.positional_deletes ORDER BY id",
    f"SELECT count(*), sum(id) FROM {NAMESPACE}.positional_deletes",
    f"SELECT count(*), sum(id), sum(amount) FROM {NAMESPACE}.deletion_vectors",
    f"SELECT * FROM {NAMESPACE}.deletion_vectors WHERE id BETWEEN 95 AND 205 ORDER BY id",
    f"SELECT * FROM {NAMESPACE}.schema_evolution ORDER BY id",
    f"SELECT * FROM {NAMESPACE}.partition_evolution ORDER BY id",
    f"SELECT category, count(*) FROM {NAMESPACE}.partition_evolution GROUP BY category ORDER BY category",
]


def pg_text(value):
    """Renders a Spark value the way the engine's PostgreSQL text output format does."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "t" if value else "f"
    if isinstance(value, float):
        return str(int(value)) if value.is_integer() else repr(value)
    if isinstance(value, datetime.datetime):
        # The engine always prints six fractional digits (PostgreSQL omits a zero fraction);
        # aligning that is tracked with the other PostgreSQL compatibility work (M5).
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")
    if isinstance(value, (datetime.date, decimal.Decimal, int, str)):
        return str(value)
    raise TypeError(f"no PostgreSQL text rendering for {type(value).__name__}")


expected = [
    {"sql": sql, "rows": [[pg_text(v) for v in row] for row in spark.sql(sql).collect()]}
    for sql in QUERIES
]
out = Path(__file__).parent / "expected" / "fixtures.json"
out.parent.mkdir(exist_ok=True)
out.write_text(json.dumps(expected, indent=2) + "\n")
print(f"wrote {len(expected)} expected results to {out}")
```

- [ ] **Step 5: Ignore the generated results and document the environment**

Append to `.gitignore`:

```text
/dev/iceberg/expected/
```

In `CONTRIBUTING.md`, replace the body of the `## Development` section (the "not scaffolded yet" text and its
code block) with:

```diff
diff --git a/CONTRIBUTING.md b/CONTRIBUTING.md
index 253c728..57349bd 100644
--- a/CONTRIBUTING.md
+++ b/CONTRIBUTING.md
@@ -28,7 +28,7 @@ Common types: `feat`, `fix`, `docs`, `refactor`, `test`, `perf`, `build`, `ci`,
 
 ## Development
 
-The Rust workspace is not scaffolded yet. Once it is, the standard checks will be:
+The standard checks, also run by CI:
 
 ```bash
 cargo fmt --all -- --check
@@ -36,6 +36,16 @@ cargo clippy --workspace --all-targets -- -D warnings
 cargo test --workspace
 ```
 
+Integration tests need the Docker test environment in `dev/iceberg` (PostgreSQL, an S3-compatible
+object store, an Iceberg REST catalog and Spark, which creates the fixture tables and records the
+expected query results). They are marked `#[ignore]` and run with `--ignored`:
+
+```bash
+docker compose -f dev/iceberg/docker-compose.yml up -d --wait
+cargo test --workspace -- --ignored
+docker compose -f dev/iceberg/docker-compose.yml down -v
+```
+
 ## Code guidelines
 
 - Write tests for new features and bug fixes (Arrange / Act / Assert).
```

- [ ] **Step 6: Start the environment and check the fixtures**

Run: `docker compose -f dev/iceberg/docker-compose.yml up -d --wait`
Expected: every service reports `Healthy` (`create-buckets` reports `Exited`). The first run builds the Spark
image and takes several minutes.

Run: `python3 -c "import json; print([len(q['rows']) for q in json.load(open('dev/iceberg/expected/fixtures.json'))])"`
Expected: `[10, 1, 1, 9, 4, 5, 3]`.

Run: `grep -c '"772"' dev/iceberg/expected/fixtures.json`
Expected: `1`. The deletion vector removes 228 of 1000 rows.

- [ ] **Step 7: Add the CI job**

Append to `.github/workflows/ci.yml` (under `jobs:`, after `check`):

```yaml
  integration:
    name: integration (Iceberg)
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions-rust-lang/setup-rust-toolchain@v1
      - name: Start the test environment
        run: docker compose -f dev/iceberg/docker-compose.yml up -d --wait
      - name: Integration tests
        run: cargo test --workspace --locked -- --ignored
      - name: Test environment logs
        if: failure()
        run: docker compose -f dev/iceberg/docker-compose.yml logs
```

- [ ] **Step 8: Commit**

```bash
git add dev/iceberg .gitignore .github/workflows/ci.yml CONTRIBUTING.md
git commit -m "test: add Iceberg test environment with Spark-generated fixtures"
```

---

### Task 2: Metadata store (`egret-engine-meta`)

**Files:**
- Modify: `Cargo.toml` (workspace dependencies)
- Create: `crates/meta/Cargo.toml`, `crates/meta/migrations/0001_catalogs.sql`, `crates/meta/src/lib.rs`,
  `crates/meta/src/catalog.rs`, `crates/meta/src/memory.rs`, `crates/meta/src/postgres.rs`,
  `crates/meta/tests/postgres.rs`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `CatalogDefinition { name: String, connector: String, options: BTreeMap<String, OptionValue> }`.
  - `OptionValue::{Literal(String), Secret(String)}`, serialized as JSON `{"literal": ..}` / `{"secret": ..}`.
  - `MetaError::{CatalogExists(String), Database, Migrate, Decode { name, source }}`.
  - `trait CatalogStore: Debug + Send + Sync` with:
    - `async fn list(&self) -> Result<Vec<CatalogDefinition>, MetaError>`, ordered by name.
    - `async fn insert(&self, &CatalogDefinition) -> Result<(), MetaError>`, which fails with `CatalogExists`.
    - `async fn delete(&self, &str) -> Result<bool, MetaError>`.
  - `InMemoryCatalogStore` (`Default`).
  - `PgCatalogStore::connect(url: &str) -> Result<Self, MetaError>`, which runs the migrations.

- [ ] **Step 1: Add workspace dependencies**

Add to `[workspace.dependencies]` in `Cargo.toml`, keeping the alphabetical order (the `egret-engine-*` path
entries form the first group):

```toml
egret-engine-meta = { path = "crates/meta" }
serde_json = "1"
sqlx = { version = "0.9.0", default-features = false, features = ["json", "macros", "migrate", "postgres", "runtime-tokio"] }
```

- [ ] **Step 2: Create the crate with the failing in-memory tests**

`crates/meta/Cargo.toml`:

```toml
[package]
name = "egret-engine-meta"
description = "PostgreSQL metadata store for Egret Engine"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true
repository.workspace = true
publish.workspace = true

[dependencies]
async-trait = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
sqlx = { workspace = true }
thiserror = { workspace = true }
tokio = { workspace = true }

[lints]
workspace = true
```

`crates/meta/src/lib.rs`:

```rust
//! Metadata store for Egret Engine: catalog definitions, persisted in PostgreSQL.

mod catalog;
mod memory;
mod postgres;

pub use catalog::{CatalogDefinition, CatalogStore, MetaError, OptionValue};
pub use memory::InMemoryCatalogStore;
pub use postgres::PgCatalogStore;
```

`crates/meta/src/memory.rs` (test module only for now):

```rust
#[cfg(test)]
mod tests {
    use crate::OptionValue;

    use super::*;

    fn definition(name: &str) -> CatalogDefinition {
        CatalogDefinition {
            name: name.to_owned(),
            connector: "iceberg_rest".to_owned(),
            options: BTreeMap::from([
                (
                    "uri".to_owned(),
                    OptionValue::Literal("http://rest".to_owned()),
                ),
                (
                    "credential".to_owned(),
                    OptionValue::Secret("lake".to_owned()),
                ),
            ]),
        }
    }

    #[tokio::test]
    async fn lists_definitions_by_name() {
        // Arrange
        let store = InMemoryCatalogStore::default();
        store.insert(&definition("b")).await.unwrap();
        store.insert(&definition("a")).await.unwrap();

        // Act
        let names: Vec<String> = store
            .list()
            .await
            .unwrap()
            .into_iter()
            .map(|d| d.name)
            .collect();

        // Assert
        assert_eq!(names, ["a", "b"]);
    }

    #[tokio::test]
    async fn rejects_duplicate_names() {
        // Arrange
        let store = InMemoryCatalogStore::default();
        store.insert(&definition("lake")).await.unwrap();

        // Act
        let err = store.insert(&definition("lake")).await.unwrap_err();

        // Assert
        assert!(matches!(err, MetaError::CatalogExists(name) if name == "lake"));
    }

    #[tokio::test]
    async fn delete_reports_whether_the_catalog_existed() {
        // Arrange
        let store = InMemoryCatalogStore::default();
        store.insert(&definition("lake")).await.unwrap();

        // Act
        let first = store.delete("lake").await.unwrap();
        let second = store.delete("lake").await.unwrap();

        // Assert
        assert!(first);
        assert!(!second);
        assert!(store.list().await.unwrap().is_empty());
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p egret-engine-meta`
Expected: compilation fails. `lib.rs` declares modules `catalog` and `postgres` whose files do not exist yet,
and `InMemoryCatalogStore` is unresolved.

- [ ] **Step 4: Implement the store types and the in-memory store**

`crates/meta/src/catalog.rs`:

```rust
use std::collections::BTreeMap;
use std::fmt::Debug;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// A catalog created with `CREATE CATALOG <name> USING <connector> WITH (<options>)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogDefinition {
    pub name: String,
    pub connector: String,
    pub options: BTreeMap<String, OptionValue>,
}

/// The value of a catalog option.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OptionValue {
    /// A value stored as written, e.g. `uri = 'https://...'`.
    Literal(String),
    /// A reference to a secret, e.g. `credential = secret('lake-client')`. Only the name is
    /// stored; the value is resolved when the catalog is connected.
    Secret(String),
}

/// Errors raised by a metadata store.
#[derive(Debug, thiserror::Error)]
pub enum MetaError {
    #[error("catalog `{0}` already exists")]
    CatalogExists(String),
    #[error("metadata store error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("failed to migrate the metadata store: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    #[error("invalid catalog definition `{name}` in the metadata store: {source}")]
    Decode {
        name: String,
        #[source]
        source: serde_json::Error,
    },
}

/// Persists catalog definitions.
#[async_trait]
pub trait CatalogStore: Debug + Send + Sync {
    /// All catalog definitions, ordered by name.
    async fn list(&self) -> Result<Vec<CatalogDefinition>, MetaError>;

    /// Stores a new definition; fails with [`MetaError::CatalogExists`] if the name is taken.
    async fn insert(&self, definition: &CatalogDefinition) -> Result<(), MetaError>;

    /// Removes a definition. Returns `false` if no catalog has this name.
    async fn delete(&self, name: &str) -> Result<bool, MetaError>;
}
```

`crates/meta/src/memory.rs`: insert above the `#[cfg(test)]` test module:

```rust
use std::collections::BTreeMap;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::{CatalogDefinition, CatalogStore, MetaError};

/// A catalog store that keeps definitions in memory only. Used when no metadata store is
/// configured; definitions are lost when the process stops.
#[derive(Debug, Default)]
pub struct InMemoryCatalogStore {
    catalogs: Mutex<BTreeMap<String, CatalogDefinition>>,
}

#[async_trait]
impl CatalogStore for InMemoryCatalogStore {
    async fn list(&self) -> Result<Vec<CatalogDefinition>, MetaError> {
        Ok(self.catalogs.lock().await.values().cloned().collect())
    }

    async fn insert(&self, definition: &CatalogDefinition) -> Result<(), MetaError> {
        let mut catalogs = self.catalogs.lock().await;
        if catalogs.contains_key(&definition.name) {
            return Err(MetaError::CatalogExists(definition.name.clone()));
        }
        catalogs.insert(definition.name.clone(), definition.clone());
        Ok(())
    }

    async fn delete(&self, name: &str) -> Result<bool, MetaError> {
        Ok(self.catalogs.lock().await.remove(name).is_some())
    }
}
```

- [ ] **Step 5: Implement the PostgreSQL store**

`crates/meta/migrations/0001_catalogs.sql`:

```sql
-- Catalog definitions created with CREATE CATALOG. Secret options are stored as references
-- ({"secret": "<name>"}), never as values.
CREATE TABLE catalogs (
    name       TEXT PRIMARY KEY,
    connector  TEXT NOT NULL,
    options    JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

`crates/meta/src/postgres.rs`:

```rust
use std::collections::BTreeMap;

use async_trait::async_trait;
use sqlx::PgPool;
use sqlx::types::Json;

use crate::{CatalogDefinition, CatalogStore, MetaError, OptionValue};

/// PostgreSQL error code `23505`: unique violation.
const UNIQUE_VIOLATION: &str = "23505";

/// The metadata store in PostgreSQL. Migrations run when it connects.
#[derive(Debug, Clone)]
pub struct PgCatalogStore {
    pool: PgPool,
}

impl PgCatalogStore {
    /// Connects to `url` (e.g. `postgres://user:password@host/db`) and applies pending migrations.
    pub async fn connect(url: &str) -> Result<Self, MetaError> {
        let pool = PgPool::connect(url).await?;
        sqlx::migrate!("./migrations").run(&pool).await?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl CatalogStore for PgCatalogStore {
    async fn list(&self) -> Result<Vec<CatalogDefinition>, MetaError> {
        let rows: Vec<(String, String, serde_json::Value)> =
            sqlx::query_as("SELECT name, connector, options FROM catalogs ORDER BY name")
                .fetch_all(&self.pool)
                .await?;
        rows.into_iter()
            .map(|(name, connector, options)| {
                let options: BTreeMap<String, OptionValue> = serde_json::from_value(options)
                    .map_err(|source| MetaError::Decode {
                        name: name.clone(),
                        source,
                    })?;
                Ok(CatalogDefinition {
                    name,
                    connector,
                    options,
                })
            })
            .collect()
    }

    async fn insert(&self, definition: &CatalogDefinition) -> Result<(), MetaError> {
        sqlx::query("INSERT INTO catalogs (name, connector, options) VALUES ($1, $2, $3)")
            .bind(&definition.name)
            .bind(&definition.connector)
            .bind(Json(&definition.options))
            .execute(&self.pool)
            .await
            .map_err(|error| match &error {
                sqlx::Error::Database(db) if db.code().as_deref() == Some(UNIQUE_VIOLATION) => {
                    MetaError::CatalogExists(definition.name.clone())
                }
                _ => MetaError::Database(error),
            })?;
        Ok(())
    }

    async fn delete(&self, name: &str) -> Result<bool, MetaError> {
        let result = sqlx::query("DELETE FROM catalogs WHERE name = $1")
            .bind(name)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }
}
```

- [ ] **Step 6: Run the unit tests to verify they pass**

Run: `cargo test -p egret-engine-meta`
Expected: unit tests `3 passed`. The `postgres` test file does not exist yet, so nothing is ignored.

- [ ] **Step 7: Add the PostgreSQL tests and run them against the environment**

`crates/meta/tests/postgres.rs`:

```rust
//! Tests against the PostgreSQL of the `dev/iceberg` test environment. Run them with
//! `cargo test -- --ignored` after `docker compose -f dev/iceberg/docker-compose.yml up -d --wait`.

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use egret_engine_meta::{CatalogDefinition, CatalogStore, MetaError, OptionValue, PgCatalogStore};

/// PostgreSQL URL of the test environment, overridable with `EGRET_TEST_POSTGRES_URL`.
fn postgres_url() -> String {
    std::env::var("EGRET_TEST_POSTGRES_URL")
        .unwrap_or_else(|_| "postgres://egret:egret@127.0.0.1:25432/egret".to_owned())
}

/// A catalog name no other test run uses, since the database outlives test runs.
fn unique_name(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}_{nanos}")
}

fn definition(name: &str) -> CatalogDefinition {
    CatalogDefinition {
        name: name.to_owned(),
        connector: "iceberg_rest".to_owned(),
        options: BTreeMap::from([
            (
                "uri".to_owned(),
                OptionValue::Literal("http://rest".to_owned()),
            ),
            (
                "credential".to_owned(),
                OptionValue::Secret("lake".to_owned()),
            ),
        ]),
    }
}

#[tokio::test]
#[ignore = "requires the dev/iceberg test environment"]
async fn definitions_round_trip_and_survive_reconnect() {
    // Arrange
    let name = unique_name("round_trip");
    let store = PgCatalogStore::connect(&postgres_url()).await.unwrap();
    store.insert(&definition(&name)).await.unwrap();

    // Act
    let reconnected = PgCatalogStore::connect(&postgres_url()).await.unwrap();
    let stored = reconnected
        .list()
        .await
        .unwrap()
        .into_iter()
        .find(|d| d.name == name);

    // Assert
    assert_eq!(stored, Some(definition(&name)));
    reconnected.delete(&name).await.unwrap();
}

#[tokio::test]
#[ignore = "requires the dev/iceberg test environment"]
async fn duplicate_names_are_rejected() {
    // Arrange
    let name = unique_name("duplicate");
    let store = PgCatalogStore::connect(&postgres_url()).await.unwrap();
    store.insert(&definition(&name)).await.unwrap();

    // Act
    let err = store.insert(&definition(&name)).await.unwrap_err();

    // Assert
    assert!(
        matches!(err, MetaError::CatalogExists(ref n) if *n == name),
        "{err}"
    );
    store.delete(&name).await.unwrap();
}

#[tokio::test]
#[ignore = "requires the dev/iceberg test environment"]
async fn delete_reports_whether_the_catalog_existed() {
    // Arrange
    let name = unique_name("delete");
    let store = PgCatalogStore::connect(&postgres_url()).await.unwrap();
    store.insert(&definition(&name)).await.unwrap();

    // Act
    let first = store.delete(&name).await.unwrap();
    let second = store.delete(&name).await.unwrap();

    // Assert
    assert!(first);
    assert!(!second);
}
```

Run: `cargo test -p egret-engine-meta -- --ignored`
Expected: `postgres` tests `3 passed`. The environment from Task 1 must be up.

- [ ] **Step 8: Lint and commit**

Run: `cargo fmt --all -- --check && cargo clippy --workspace --all-targets --locked -- -D warnings`
Expected: clean.

```bash
git add Cargo.toml Cargo.lock crates/meta
git commit -m "feat(meta): store catalog definitions in PostgreSQL"
```

---

### Task 3: Engine DDL parser (`egret-engine-sql`)

**Files:**
- Modify: `Cargo.toml` (workspace dependencies)
- Create: `crates/sql/Cargo.toml`, `crates/sql/src/lib.rs`, `crates/sql/src/parser.rs`

**Interfaces:**
- Consumes: `egret_engine_meta::{CatalogDefinition, OptionValue}` (Task 2).
- Produces:
  - `enum Statement { CreateCatalog(CatalogDefinition), DropCatalog { name: String, if_exists: bool },
    ShowCatalogs, DataFusion(datafusion::sql::parser::Statement) }`.
  - `fn parse(sql: &str) -> Result<Vec<Statement>, DataFusionError>`. Syntax errors are
    `DataFusionError::SQL`; unquoted identifiers are lower-cased; option keys are identifiers or string
    literals; values are string literals or `secret('<name>')`; duplicate keys are rejected.

- [ ] **Step 1: Add the workspace dependency**

```toml
egret-engine-sql = { path = "crates/sql" }
```

- [ ] **Step 2: Create the crate with the failing parser tests**

`crates/sql/Cargo.toml`:

```toml
[package]
name = "egret-engine-sql"
description = "SQL parsing for Egret Engine, including engine-specific DDL"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true
repository.workspace = true
publish.workspace = true

[dependencies]
datafusion = { workspace = true }
egret-engine-meta = { workspace = true }

[lints]
workspace = true
```

`crates/sql/src/lib.rs`:

```rust
//! SQL parsing for Egret Engine: engine statements (`CREATE CATALOG`, ...) plus everything
//! DataFusion parses.

mod parser;

pub use parser::{Statement, parse};
```

`crates/sql/src/parser.rs` (test module only for now):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn literal(value: &str) -> OptionValue {
        OptionValue::Literal(value.to_owned())
    }

    #[test]
    fn parses_create_catalog_with_options() {
        // Arrange
        let sql = "CREATE CATALOG Lake USING iceberg_rest WITH (
            uri = 'http://rest:8181', 's3.endpoint' = 'http://s3', credential = secret('lake-client'))";

        // Act
        let statements = parse(sql).unwrap();

        // Assert
        assert_eq!(
            statements,
            vec![Statement::CreateCatalog(CatalogDefinition {
                name: "lake".to_owned(),
                connector: "iceberg_rest".to_owned(),
                options: BTreeMap::from([
                    (
                        "credential".to_owned(),
                        OptionValue::Secret("lake-client".to_owned())
                    ),
                    ("s3.endpoint".to_owned(), literal("http://s3")),
                    ("uri".to_owned(), literal("http://rest:8181")),
                ]),
            })]
        );
    }

    #[test]
    fn quoted_names_keep_their_case() {
        // Arrange
        let sql = r#"CREATE CATALOG "Lake" USING iceberg_rest"#;

        // Act
        let statements = parse(sql).unwrap();

        // Assert
        assert_eq!(
            statements,
            vec![Statement::CreateCatalog(CatalogDefinition {
                name: "Lake".to_owned(),
                connector: "iceberg_rest".to_owned(),
                options: BTreeMap::new(),
            })]
        );
    }

    #[test]
    fn parses_drop_and_show_catalogs() {
        // Arrange
        let sql = "DROP CATALOG lake; DROP CATALOG IF EXISTS other; SHOW CATALOGS";

        // Act
        let statements = parse(sql).unwrap();

        // Assert
        assert_eq!(
            statements,
            vec![
                Statement::DropCatalog {
                    name: "lake".to_owned(),
                    if_exists: false
                },
                Statement::DropCatalog {
                    name: "other".to_owned(),
                    if_exists: true
                },
                Statement::ShowCatalogs,
            ]
        );
    }

    #[test]
    fn other_statements_are_parsed_by_datafusion() {
        // Arrange
        let sql = "SELECT 1; ; SELECT 2";

        // Act
        let statements = parse(sql).unwrap();

        // Assert
        assert_eq!(statements.len(), 2);
        assert!(
            statements
                .iter()
                .all(|s| matches!(s, Statement::DataFusion(_)))
        );
    }

    #[test]
    fn duplicate_options_are_rejected() {
        // Arrange
        let sql = "CREATE CATALOG lake USING iceberg_rest WITH (uri = 'a', URI = 'b')";

        // Act
        let err = parse(sql).unwrap_err();

        // Assert
        assert!(
            err.to_string()
                .contains("option `uri` is specified more than once"),
            "{err}"
        );
    }

    #[test]
    fn syntax_errors_are_sql_errors() {
        // Arrange
        let inputs = [
            "CREATE CATALOG lake",
            "CREATE CATALOG lake USING iceberg_rest WITH (uri 'a')",
            "SELECT 1 SELECT 2",
        ];

        // Act
        let errors: Vec<DataFusionError> =
            inputs.iter().map(|sql| parse(sql).unwrap_err()).collect();

        // Assert
        for error in errors {
            assert!(matches!(error, DataFusionError::SQL(..)), "{error:?}");
        }
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p egret-engine-sql`
Expected: compilation fails (`cannot find function parse`, `cannot find type Statement`).

- [ ] **Step 4: Implement the parser**

`crates/sql/src/parser.rs`: insert above the `#[cfg(test)]` test module:

```rust
use std::collections::BTreeMap;

use datafusion::error::DataFusionError;
use datafusion::sql::parser::{DFParser, DFParserBuilder};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::Token;
use egret_engine_meta::{CatalogDefinition, OptionValue};

/// One parsed statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    /// `CREATE CATALOG <name> USING <connector> [WITH (<key> = <value>, ...)]`
    CreateCatalog(CatalogDefinition),
    /// `DROP CATALOG [IF EXISTS] <name>`
    DropCatalog { name: String, if_exists: bool },
    /// `SHOW CATALOGS`
    ShowCatalogs,
    /// Any statement DataFusion parses.
    DataFusion(datafusion::sql::parser::Statement),
}

/// Parses a semicolon-separated list of statements with the PostgreSQL dialect.
///
/// Unquoted identifiers are folded to lower case, as in PostgreSQL. Option keys may be identifiers
/// or string literals (`'s3.endpoint' = '...'`); values are string literals or `secret('<name>')`.
pub fn parse(sql: &str) -> Result<Vec<Statement>, DataFusionError> {
    let dialect = PostgreSqlDialect {};
    let mut parser = DFParserBuilder::new(sql).with_dialect(&dialect).build()?;
    let mut statements = Vec::new();
    let mut expecting_delimiter = false;
    loop {
        while parser.parser.consume_token(&Token::SemiColon) {
            expecting_delimiter = false;
        }
        if parser.parser.peek_token() == Token::EOF {
            break;
        }
        if expecting_delimiter {
            let found = parser.parser.peek_token();
            return Err(ParserError::ParserError(format!(
                "Expected: end of statement, found: {found}"
            ))
            .into());
        }
        statements.push(parse_statement(&mut parser)?);
        expecting_delimiter = true;
    }
    Ok(statements)
}

fn parse_statement(parser: &mut DFParser<'_>) -> Result<Statement, DataFusionError> {
    let p = &mut parser.parser;
    if p.parse_keywords(&[Keyword::CREATE, Keyword::CATALOG]) {
        return Ok(Statement::CreateCatalog(parse_create_catalog(p)?));
    }
    if p.parse_keywords(&[Keyword::DROP, Keyword::CATALOG]) {
        let if_exists = p.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let name = parse_name(p)?;
        return Ok(Statement::DropCatalog { name, if_exists });
    }
    if p.parse_keywords(&[Keyword::SHOW, Keyword::CATALOGS]) {
        return Ok(Statement::ShowCatalogs);
    }
    Ok(Statement::DataFusion(parser.parse_statement()?))
}

fn parse_create_catalog(p: &mut Parser<'_>) -> Result<CatalogDefinition, ParserError> {
    let name = parse_name(p)?;
    p.expect_keyword(Keyword::USING)?;
    let connector = parse_name(p)?;
    let mut options = BTreeMap::new();
    if p.parse_keyword(Keyword::WITH) {
        p.expect_token(&Token::LParen)?;
        loop {
            let key = parse_option_key(p)?;
            p.expect_token(&Token::Eq)?;
            let value = parse_option_value(p)?;
            if options.insert(key.clone(), value).is_some() {
                return Err(ParserError::ParserError(format!(
                    "option `{key}` is specified more than once"
                )));
            }
            if !p.consume_token(&Token::Comma) {
                break;
            }
        }
        p.expect_token(&Token::RParen)?;
    }
    Ok(CatalogDefinition {
        name,
        connector,
        options,
    })
}

/// An identifier, folded to lower case unless quoted.
fn parse_name(p: &mut Parser<'_>) -> Result<String, ParserError> {
    let ident = p.parse_identifier()?;
    Ok(match ident.quote_style {
        Some(_) => ident.value,
        None => ident.value.to_lowercase(),
    })
}

fn parse_option_key(p: &mut Parser<'_>) -> Result<String, ParserError> {
    match p.peek_token().token {
        Token::SingleQuotedString(_) => p.parse_literal_string(),
        _ => parse_name(p),
    }
}

fn parse_option_value(p: &mut Parser<'_>) -> Result<OptionValue, ParserError> {
    if p.parse_keyword(Keyword::SECRET) {
        p.expect_token(&Token::LParen)?;
        let name = p.parse_literal_string()?;
        p.expect_token(&Token::RParen)?;
        return Ok(OptionValue::Secret(name));
    }
    Ok(OptionValue::Literal(p.parse_literal_string()?))
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p egret-engine-sql`
Expected: `test result: ok. 6 passed`.

- [ ] **Step 6: Lint and commit**

Run: `cargo fmt --all -- --check && cargo clippy --workspace --all-targets --locked -- -D warnings`
Expected: clean.

```bash
git add Cargo.toml Cargo.lock crates/sql
git commit -m "feat(sql): parse CREATE CATALOG, DROP CATALOG and SHOW CATALOGS"
```

---

### Task 4: Iceberg tables in DataFusion (`egret-engine-iceberg`)

**Files:**
- Modify: `Cargo.toml` (workspace dependencies)
- Create: `crates/iceberg/Cargo.toml`, `crates/iceberg/src/lib.rs`, `crates/iceberg/src/catalog.rs`,
  `crates/iceberg/src/table.rs`, `crates/iceberg/src/scan.rs`, `crates/iceberg/tests/rest_catalog.rs`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `IcebergCatalogProvider::try_new(Arc<dyn iceberg::Catalog>) -> iceberg::Result<Self>`:
    - It lists the top-level namespaces, which also checks connectivity, and implements DataFusion's
      `CatalogProvider`.
    - `schema(name)` returns an `IcebergSchemaProvider` for any name.
    - `SchemaProvider::table` loads the table on every call and returns `Ok(None)` for `TableNotFound` /
      `NamespaceNotFound`.
  - `IcebergTableProvider::try_new(Table)`.
  - `IcebergScan::new(Table, SchemaRef)`: a single-partition DataFusion `DataSource`.

Note: DataFusion 55's catalog traits have no `as_any` method; do not add one.

- [ ] **Step 1: Add the workspace dependencies**

```toml
egret-engine-iceberg = { path = "crates/iceberg" }
# iceberg-rust is pinned to the revision datafusion-iceberg uses (DataFusion 55, Arrow 59).
iceberg = { git = "https://github.com/apache/iceberg-rust", rev = "5b82ab99aeaf50bf4f5b3b776ac9cd6380a5f3e3" }
iceberg-catalog-rest = { git = "https://github.com/apache/iceberg-rust", rev = "5b82ab99aeaf50bf4f5b3b776ac9cd6380a5f3e3" }
iceberg-storage-opendal = { git = "https://github.com/apache/iceberg-rust", rev = "5b82ab99aeaf50bf4f5b3b776ac9cd6380a5f3e3", default-features = false, features = ["opendal-s3"] }
```

- [ ] **Step 2: Create the crate and the failing tests**

`crates/iceberg/Cargo.toml`:

```toml
[package]
name = "egret-engine-iceberg"
description = "Iceberg tables as DataFusion catalogs and scans for Egret Engine"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true
repository.workspace = true
publish.workspace = true

[dependencies]
async-trait = { workspace = true }
datafusion = { workspace = true }
futures = { workspace = true }
iceberg = { workspace = true }

[dev-dependencies]
iceberg-catalog-rest = { workspace = true }
iceberg-storage-opendal = { workspace = true }
tokio = { workspace = true }

[lints]
workspace = true
```

`crates/iceberg/src/lib.rs`:

```rust
//! Iceberg integration for Egret Engine: exposes an Iceberg catalog to DataFusion and scans its
//! tables with iceberg-rust's reader, which applies delete files and schema evolution.

mod catalog;
mod scan;
mod table;

use datafusion::error::DataFusionError;

pub use catalog::{IcebergCatalogProvider, IcebergSchemaProvider};
pub use scan::IcebergScan;
pub use table::IcebergTableProvider;

/// Wraps an iceberg-rust error so it can cross DataFusion APIs.
pub(crate) fn to_datafusion_error(error: iceberg::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
```

`crates/iceberg/tests/rest_catalog.rs`:

```rust
//! Tests against the REST catalog of the `dev/iceberg` test environment. Run them with
//! `cargo test -- --ignored` after `docker compose -f dev/iceberg/docker-compose.yml up -d --wait`.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{AsArray, RecordBatch};
use datafusion::arrow::datatypes::Int64Type;
use datafusion::prelude::SessionContext;
use egret_engine_iceberg::IcebergCatalogProvider;
use iceberg::CatalogBuilder;
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_storage_opendal::OpenDalStorageFactory;

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_owned())
}

/// A session with the test environment's REST catalog registered as `rest`.
async fn session() -> SessionContext {
    let properties = HashMap::from([
        (
            "uri".to_owned(),
            env_or("EGRET_TEST_REST_URI", "http://127.0.0.1:28181"),
        ),
        (
            "s3.endpoint".to_owned(),
            env_or("EGRET_TEST_S3_ENDPOINT", "http://127.0.0.1:29000"),
        ),
        ("s3.region".to_owned(), "us-east-1".to_owned()),
        ("s3.path-style-access".to_owned(), "true".to_owned()),
        ("s3.access-key-id".to_owned(), "admin".to_owned()),
        ("s3.secret-access-key".to_owned(), "password".to_owned()),
    ]);
    let catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        }))
        .load("rest", properties)
        .await
        .expect("connect REST catalog");
    let provider = IcebergCatalogProvider::try_new(Arc::new(catalog))
        .await
        .expect("list namespaces");
    let ctx = SessionContext::new();
    ctx.register_catalog("rest", Arc::new(provider));
    ctx
}

async fn count(ctx: &SessionContext, table: &str) -> i64 {
    let batches: Vec<RecordBatch> = ctx
        .sql(&format!("SELECT count(*) FROM {table}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    batches[0].column(0).as_primitive::<Int64Type>().value(0)
}

#[tokio::test]
#[ignore = "requires the dev/iceberg test environment"]
async fn lists_namespaces_as_schemas() {
    // Act
    let ctx = session().await;

    // Assert
    let schemas = ctx.catalog("rest").unwrap().schema_names();
    assert!(schemas.contains(&"fixtures".to_owned()), "{schemas:?}");
}

#[tokio::test]
#[ignore = "requires the dev/iceberg test environment"]
async fn scans_apply_positional_deletes_and_deletion_vectors() {
    // Arrange
    let ctx = session().await;

    // Act
    let positional = count(&ctx, "rest.fixtures.positional_deletes").await;
    let deletion_vectors = count(&ctx, "rest.fixtures.deletion_vectors").await;

    // Assert
    assert_eq!(positional, 10);
    assert_eq!(deletion_vectors, 772);
}

#[tokio::test]
#[ignore = "requires the dev/iceberg test environment"]
async fn missing_tables_are_not_found() {
    // Arrange
    let ctx = session().await;

    // Act
    let missing_table = ctx.sql("SELECT * FROM rest.fixtures.missing").await;
    let missing_namespace = ctx.sql("SELECT * FROM rest.missing.t").await;

    // Assert
    for result in [missing_table, missing_namespace] {
        let message = result.unwrap_err().to_string();
        assert!(message.contains("not found"), "{message}");
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p egret-engine-iceberg -- --ignored`
Expected: compilation fails, because the modules `catalog`, `scan` and `table` do not exist. The first build
fetches iceberg-rust and compiles opendal, which takes several minutes.

- [ ] **Step 4: Implement the catalog and schema providers**

`crates/iceberg/src/catalog.rs`:

```rust
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::error::Result;
use iceberg::{Catalog, ErrorKind, NamespaceIdent, TableIdent};

use crate::table::IcebergTableProvider;
use crate::to_datafusion_error;

/// An Iceberg catalog as a DataFusion catalog: each top-level namespace is a schema.
#[derive(Debug)]
pub struct IcebergCatalogProvider {
    catalog: Arc<dyn Catalog>,
    namespaces: Vec<String>,
}

impl IcebergCatalogProvider {
    /// Lists the catalog's top-level namespaces, which also checks that the catalog is reachable.
    pub async fn try_new(catalog: Arc<dyn Catalog>) -> iceberg::Result<Self> {
        let namespaces = catalog
            .list_namespaces(None)
            .await?
            .iter()
            .map(|namespace| namespace.as_ref().join("."))
            .collect();
        Ok(Self {
            catalog,
            namespaces,
        })
    }
}

impl CatalogProvider for IcebergCatalogProvider {
    /// The namespaces that existed when the catalog was connected.
    fn schema_names(&self) -> Vec<String> {
        self.namespaces.clone()
    }

    /// Any name resolves to a schema; a namespace that does not exist simply has no tables.
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        Some(Arc::new(IcebergSchemaProvider {
            catalog: self.catalog.clone(),
            namespace: NamespaceIdent::new(name.to_owned()),
        }))
    }
}

/// One Iceberg namespace. Tables are loaded from the catalog each time a query resolves them.
#[derive(Debug)]
pub struct IcebergSchemaProvider {
    catalog: Arc<dyn Catalog>,
    namespace: NamespaceIdent,
}

#[async_trait]
impl SchemaProvider for IcebergSchemaProvider {
    /// Tables are resolved by name only; listing them is not supported yet.
    fn table_names(&self) -> Vec<String> {
        Vec::new()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let ident = TableIdent::new(self.namespace.clone(), name.to_owned());
        match self.catalog.load_table(&ident).await {
            Ok(table) => Ok(Some(Arc::new(IcebergTableProvider::try_new(table)?))),
            Err(error)
                if matches!(
                    error.kind(),
                    ErrorKind::TableNotFound | ErrorKind::NamespaceNotFound
                ) =>
            {
                Ok(None)
            }
            Err(error) => Err(to_datafusion_error(error)),
        }
    }

    fn table_exist(&self, _name: &str) -> bool {
        false
    }
}
```

- [ ] **Step 5: Implement the table provider and the scan**

`crates/iceberg/src/table.rs`:

```rust
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::Result;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::table::Table;

use crate::scan::IcebergScan;
use crate::to_datafusion_error;

/// One Iceberg table at the snapshot that was current when it was loaded.
#[derive(Debug)]
pub struct IcebergTableProvider {
    table: Table,
    schema: SchemaRef,
}

impl IcebergTableProvider {
    pub fn try_new(table: Table) -> Result<Self> {
        let schema = schema_to_arrow_schema(table.metadata().current_schema())
            .map_err(to_datafusion_error)?;
        Ok(Self {
            table,
            schema: Arc::new(schema),
        })
    }
}

#[async_trait]
impl TableProvider for IcebergTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = match projection {
            Some(indices) => Arc::new(self.schema.project(indices)?),
            None => self.schema.clone(),
        };
        let scan = IcebergScan::new(self.table.clone(), schema);
        Ok(Arc::new(DataSourceExec::new(Arc::new(scan))))
    }
}
```

`crates/iceberg/src/scan.rs`:

```rust
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Statistics;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::source::DataSource;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayFormatType, Partitioning};
use futures::{StreamExt, TryStreamExt};
use iceberg::table::Table;

use crate::to_datafusion_error;

/// Reads the columns of `schema` from an Iceberg table in a single partition. iceberg-rust plans
/// the files and applies positional deletes, equality deletes and deletion vectors while reading.
#[derive(Debug, Clone)]
pub struct IcebergScan {
    table: Table,
    schema: SchemaRef,
}

impl IcebergScan {
    pub fn new(table: Table, schema: SchemaRef) -> Self {
        Self { table, schema }
    }

    fn column_names(&self) -> Vec<String> {
        self.schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect()
    }
}

impl DataSource for IcebergScan {
    fn open(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let scan = self
            .table
            .scan()
            .select(self.column_names())
            .build()
            .map_err(to_datafusion_error)?;
        let batches = futures::stream::once(async move { scan.to_arrow().await })
            .map_err(to_datafusion_error)
            .map_ok(|stream| stream.map_err(to_datafusion_error))
            .try_flatten()
            .boxed();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            batches,
        )))
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "format=iceberg, table={}, projection=[{}]",
            self.table.identifier(),
            self.column_names().join(", ")
        )
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn eq_properties(&self) -> EquivalenceProperties {
        EquivalenceProperties::new(self.schema.clone())
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Arc<Statistics>> {
        Ok(Arc::new(Statistics::new_unknown(&self.schema)))
    }

    fn with_fetch(&self, _limit: Option<usize>) -> Option<Arc<dyn DataSource>> {
        None
    }

    fn fetch(&self) -> Option<usize> {
        None
    }

    fn try_swapping_with_projection(
        &self,
        _projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn DataSource>>> {
        Ok(None)
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
}
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cargo test -p egret-engine-iceberg -- --ignored`
Expected: `rest_catalog` tests `3 passed` (`lists_namespaces_as_schemas`,
`scans_apply_positional_deletes_and_deletion_vectors`, `missing_tables_are_not_found`).

- [ ] **Step 7: Lint and commit**

Run: `cargo fmt --all -- --check && cargo clippy --workspace --all-targets --locked -- -D warnings`
Expected: clean.

```bash
git add Cargo.toml Cargo.lock crates/iceberg
git commit -m "feat(iceberg): expose Iceberg catalogs and tables to DataFusion"
```

---

### Task 5: Catalog registry and secrets (`egret-engine-catalog`)

**Files:**
- Modify: `Cargo.toml` (workspace dependencies)
- Create: `crates/catalog/Cargo.toml`, `crates/catalog/src/lib.rs`, `crates/catalog/src/error.rs`,
  `crates/catalog/src/secrets.rs`, `crates/catalog/src/rest.rs`, `crates/catalog/src/registry.rs`

**Interfaces:**
- Consumes: `egret_engine_meta::{CatalogDefinition, CatalogStore, MetaError, OptionValue, InMemoryCatalogStore}`
  (Task 2) and `egret_engine_iceberg::IcebergCatalogProvider` (Task 4).
- Produces:
  - `SecretResolver::new(Option<PathBuf>)` (also `Default`) and `resolve(&str) -> Result<String, SecretError>`.
  - `CatalogError`, with variants:
    - `AlreadyExists(String)`, `NotFound(String)` and `Reserved(String)`.
    - `UnknownConnector(String)`, `UnknownOption { catalog, option }` and `MissingOption { catalog, option }`.
    - `Secret { catalog, source }`, `Connect { catalog, source: Box<iceberg::Error> }` and `Store(MetaError)`.
  - `CatalogRegistry`, with these methods:
    - `async load(Arc<dyn CatalogStore>, SecretResolver) -> Result<Self, CatalogError>`.
    - `async create(CatalogDefinition)`, which connects first and then stores the definition.
    - `async remove(&str)`, which returns `NotFound` for an unknown catalog. It is not called `drop`, which
      would clash with `Drop::drop` on `Arc`.
    - `async list()`.
    - `get(&str) -> Option<Arc<dyn CatalogProvider>>` and `names() -> Vec<String>`.
  - `RESERVED_CATALOG = "datafusion"` and `ICEBERG_REST = "iceberg_rest"`.

`CatalogError::Connect` boxes the iceberg error; unboxed, clippy's `result_large_err` fails the build.

- [ ] **Step 1: Add the workspace dependencies**

```toml
egret-engine-catalog = { path = "crates/catalog" }
tempfile = "3"
```

- [ ] **Step 2: Create the crate with the failing tests**

`crates/catalog/Cargo.toml`:

```toml
[package]
name = "egret-engine-catalog"
description = "Catalog registry for Egret Engine: catalog definitions, secrets and connectors"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true
repository.workspace = true
publish.workspace = true

[dependencies]
datafusion = { workspace = true }
egret-engine-iceberg = { workspace = true }
egret-engine-meta = { workspace = true }
iceberg = { workspace = true }
iceberg-catalog-rest = { workspace = true }
iceberg-storage-opendal = { workspace = true }
thiserror = { workspace = true }
tracing = { workspace = true }

[dev-dependencies]
tempfile = { workspace = true }
tokio = { workspace = true }

[lints]
workspace = true
```

`crates/catalog/src/lib.rs`:

```rust
//! Catalog registry for Egret Engine: catalogs created with `CREATE CATALOG`, persisted in the
//! metadata store, with secrets resolved at connect time.

mod error;
mod registry;
mod rest;
mod secrets;

pub use error::CatalogError;
pub use registry::{CatalogRegistry, RESERVED_CATALOG};
pub use rest::ICEBERG_REST;
pub use secrets::{SecretError, SecretResolver};
```

`crates/catalog/src/error.rs`:

```rust
use egret_engine_meta::MetaError;

use crate::SecretError;

/// Errors raised while creating, dropping or connecting catalogs.
#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("catalog `{0}` already exists")]
    AlreadyExists(String),
    #[error("catalog `{0}` does not exist")]
    NotFound(String),
    #[error("catalog name `{0}` is reserved")]
    Reserved(String),
    #[error("unknown connector `{0}`; supported connectors: iceberg_rest")]
    UnknownConnector(String),
    #[error("catalog `{catalog}`: unknown option `{option}`")]
    UnknownOption { catalog: String, option: String },
    #[error("catalog `{catalog}`: missing required option `{option}`")]
    MissingOption {
        catalog: String,
        option: &'static str,
    },
    #[error("catalog `{catalog}`: {source}")]
    Secret {
        catalog: String,
        #[source]
        source: SecretError,
    },
    #[error("failed to connect catalog `{catalog}`: {source}")]
    Connect {
        catalog: String,
        #[source]
        source: Box<iceberg::Error>,
    },
    #[error(transparent)]
    Store(MetaError),
}

impl From<MetaError> for CatalogError {
    fn from(error: MetaError) -> Self {
        match error {
            MetaError::CatalogExists(name) => CatalogError::AlreadyExists(name),
            other => CatalogError::Store(other),
        }
    }
}
```

`crates/catalog/src/secrets.rs` (test module only for now):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn resolver(dir: Option<PathBuf>) -> SecretResolver {
        SecretResolver {
            dir,
            env: |key| (key == "EGRET_SECRET_LAKE_CLIENT").then(|| "from-env".to_owned()),
        }
    }

    #[test]
    fn environment_variable_wins() {
        // Arrange
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("lake-client"), "from-file").unwrap();

        // Act
        let value = resolver(Some(dir.path().to_path_buf()))
            .resolve("lake-client")
            .unwrap();

        // Assert
        assert_eq!(value, "from-env");
    }

    #[test]
    fn falls_back_to_file_without_trailing_newline() {
        // Arrange
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("s3.secret"), "password\n").unwrap();

        // Act
        let value = resolver(Some(dir.path().to_path_buf()))
            .resolve("s3.secret")
            .unwrap();

        // Assert
        assert_eq!(value, "password");
    }

    #[test]
    fn missing_secret_names_where_to_put_it() {
        // Arrange
        let dir = tempfile::tempdir().unwrap();

        // Act
        let err = resolver(Some(dir.path().to_path_buf()))
            .resolve("other")
            .unwrap_err();

        // Assert
        let message = err.to_string();
        assert!(message.contains("EGRET_SECRET_OTHER"), "{message}");
        assert!(message.contains("other"), "{message}");
    }

    #[test]
    fn rejects_names_that_could_escape_the_directory() {
        // Arrange
        let names = ["../etc/passwd", ".hidden", "a/b", ""];

        // Act
        let errors: Vec<SecretError> = names
            .iter()
            .map(|name| resolver(None).resolve(name).unwrap_err())
            .collect();

        // Assert
        assert!(
            errors
                .iter()
                .all(|e| matches!(e, SecretError::InvalidName(_))),
            "{errors:?}"
        );
    }
}
```

`crates/catalog/src/rest.rs` (test module only for now):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn options(entries: &[(&str, OptionValue)]) -> BTreeMap<String, OptionValue> {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_owned(), value.clone()))
            .collect()
    }

    fn literal(value: &str) -> OptionValue {
        OptionValue::Literal(value.to_owned())
    }

    #[test]
    fn maps_options_to_rest_properties() {
        // Arrange
        let options = options(&[
            ("uri", literal("http://rest:8181")),
            ("warehouse", literal("main")),
            ("access_delegation", literal("vended-credentials")),
            ("oauth2_server_uri", literal("http://idp/token")),
            ("s3.endpoint", literal("http://s3:9000")),
        ]);

        // Act
        let properties = rest_properties("lake", &options, &SecretResolver::default()).unwrap();

        // Assert
        assert_eq!(
            properties,
            HashMap::from([
                ("uri".to_owned(), "http://rest:8181".to_owned()),
                ("warehouse".to_owned(), "main".to_owned()),
                (
                    "header.X-Iceberg-Access-Delegation".to_owned(),
                    "vended-credentials".to_owned()
                ),
                (
                    "oauth2-server-uri".to_owned(),
                    "http://idp/token".to_owned()
                ),
                ("s3.endpoint".to_owned(), "http://s3:9000".to_owned()),
            ])
        );
    }

    #[test]
    fn resolves_secret_options() {
        // Arrange
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("lake-client"), "id:secret\n").unwrap();
        let secrets = SecretResolver::new(Some(dir.path().to_path_buf()));
        let options = options(&[
            ("uri", literal("http://rest:8181")),
            ("credential", OptionValue::Secret("lake-client".to_owned())),
        ]);

        // Act
        let properties = rest_properties("lake", &options, &secrets).unwrap();

        // Assert
        assert_eq!(properties["credential"], "id:secret");
    }

    #[test]
    fn rejects_unknown_options() {
        // Arrange
        let options = options(&[("uri", literal("http://rest")), ("urii", literal("x"))]);

        // Act
        let err = rest_properties("lake", &options, &SecretResolver::default()).unwrap_err();

        // Assert
        assert_eq!(err.to_string(), "catalog `lake`: unknown option `urii`");
    }

    #[test]
    fn requires_uri() {
        // Arrange
        let options = options(&[("warehouse", literal("main"))]);

        // Act
        let err = rest_properties("lake", &options, &SecretResolver::default()).unwrap_err();

        // Assert
        assert_eq!(
            err.to_string(),
            "catalog `lake`: missing required option `uri`"
        );
    }
}
```

`crates/catalog/src/registry.rs` (test module only for now):

```rust
#[cfg(test)]
mod tests {
    use egret_engine_meta::{InMemoryCatalogStore, OptionValue};

    use super::*;

    async fn empty_registry() -> (CatalogRegistry, Arc<InMemoryCatalogStore>) {
        let store = Arc::new(InMemoryCatalogStore::default());
        let registry = CatalogRegistry::load(store.clone(), SecretResolver::default())
            .await
            .unwrap();
        (registry, store)
    }

    fn definition(name: &str, connector: &str, uri: &str) -> CatalogDefinition {
        CatalogDefinition {
            name: name.to_owned(),
            connector: connector.to_owned(),
            options: BTreeMap::from([("uri".to_owned(), OptionValue::Literal(uri.to_owned()))]),
        }
    }

    #[tokio::test]
    async fn rejects_the_reserved_name() {
        // Arrange
        let (registry, _) = empty_registry().await;

        // Act
        let err = registry
            .create(definition(RESERVED_CATALOG, ICEBERG_REST, "http://rest"))
            .await
            .unwrap_err();

        // Assert
        assert!(matches!(err, CatalogError::Reserved(_)), "{err}");
    }

    #[tokio::test]
    async fn rejects_unknown_connectors_without_storing_them() {
        // Arrange
        let (registry, store) = empty_registry().await;

        // Act
        let err = registry
            .create(definition("lake", "hive", "thrift://hms"))
            .await
            .unwrap_err();

        // Assert
        assert!(matches!(err, CatalogError::UnknownConnector(_)), "{err}");
        assert!(store.list().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn unreachable_catalog_is_not_stored() {
        // Arrange
        let (registry, store) = empty_registry().await;

        // Act
        let err = registry
            .create(definition("lake", ICEBERG_REST, "http://127.0.0.1:1"))
            .await
            .unwrap_err();

        // Assert
        assert!(matches!(err, CatalogError::Connect { .. }), "{err}");
        assert!(store.list().await.unwrap().is_empty());
        assert!(registry.get("lake").is_none());
    }

    #[tokio::test]
    async fn dropping_a_missing_catalog_fails() {
        // Arrange
        let (registry, _) = empty_registry().await;

        // Act
        let err = registry.remove("lake").await.unwrap_err();

        // Assert
        assert!(matches!(err, CatalogError::NotFound(name) if name == "lake"));
    }

    #[tokio::test]
    async fn stored_catalogs_that_cannot_connect_are_skipped_at_load() {
        // Arrange
        let store = Arc::new(InMemoryCatalogStore::default());
        store
            .insert(&definition("lake", ICEBERG_REST, "http://127.0.0.1:1"))
            .await
            .unwrap();

        // Act
        let registry = CatalogRegistry::load(store, SecretResolver::default())
            .await
            .unwrap();

        // Assert
        assert!(registry.get("lake").is_none());
        assert_eq!(registry.list().await.unwrap().len(), 1);
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p egret-engine-catalog`
Expected: compilation fails (`SecretResolver`, `rest_properties`, `CatalogRegistry` unresolved).

- [ ] **Step 4: Implement secret resolution**

`crates/catalog/src/secrets.rs`: insert above the `#[cfg(test)]` test module:

```rust
use std::path::PathBuf;

/// Prefix of environment variables that hold secret values.
pub const SECRET_ENV_PREFIX: &str = "EGRET_SECRET_";

/// Errors raised while resolving a secret reference.
#[derive(Debug, thiserror::Error)]
pub enum SecretError {
    #[error("invalid secret name `{0}`; use letters, digits, `.`, `_` and `-`")]
    InvalidName(String),
    #[error("secret `{name}` not found: set {env_var}{file_hint}")]
    NotFound {
        name: String,
        env_var: String,
        file_hint: String,
    },
    #[error("failed to read secret `{name}` from {}: {source}", path.display())]
    Read {
        name: String,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

/// Resolves `secret('<name>')` references: first from the environment variable
/// `EGRET_SECRET_<NAME>` (upper case, every character other than a letter or digit replaced by
/// `_`), then from the file `<dir>/<name>`, which is how Kubernetes mounts Secrets.
#[derive(Debug, Clone)]
pub struct SecretResolver {
    dir: Option<PathBuf>,
    env: fn(&str) -> Option<String>,
}

impl Default for SecretResolver {
    fn default() -> Self {
        Self::new(None)
    }
}

impl SecretResolver {
    pub fn new(dir: Option<PathBuf>) -> Self {
        Self {
            dir,
            env: |key| std::env::var(key).ok(),
        }
    }

    pub fn resolve(&self, name: &str) -> Result<String, SecretError> {
        let valid = !name.is_empty()
            && !name.starts_with('.')
            && name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'));
        if !valid {
            return Err(SecretError::InvalidName(name.to_owned()));
        }
        let env_var = env_var_name(name);
        if let Some(value) = (self.env)(&env_var) {
            return Ok(value);
        }
        let Some(dir) = &self.dir else {
            return Err(SecretError::NotFound {
                name: name.to_owned(),
                env_var,
                file_hint: String::new(),
            });
        };
        let path = dir.join(name);
        match std::fs::read_to_string(&path) {
            Ok(value) => Ok(value.trim_end_matches(['\r', '\n']).to_owned()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Err(SecretError::NotFound {
                    name: name.to_owned(),
                    env_var,
                    file_hint: format!(" or create {}", path.display()),
                })
            }
            Err(source) => Err(SecretError::Read {
                name: name.to_owned(),
                path,
                source,
            }),
        }
    }
}

fn env_var_name(name: &str) -> String {
    let suffix: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect();
    format!("{SECRET_ENV_PREFIX}{suffix}")
}
```

- [ ] **Step 5: Implement the REST option mapping and connection**

`crates/catalog/src/rest.rs`: insert above the `#[cfg(test)]` test module:

```rust
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use egret_engine_meta::OptionValue;
use iceberg::{Catalog, CatalogBuilder};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_storage_opendal::OpenDalStorageFactory;

use crate::{CatalogError, SecretResolver};

/// Connector name of Iceberg REST catalogs.
pub const ICEBERG_REST: &str = "iceberg_rest";

/// `CREATE CATALOG` option names and the iceberg-rust REST catalog property each one sets.
/// Options whose name contains a `.` (e.g. `'s3.endpoint'`) are passed through unchanged.
const OPTION_PROPERTIES: &[(&str, &str)] = &[
    ("uri", "uri"),
    ("warehouse", "warehouse"),
    ("auth", "rest.auth.type"),
    ("credential", "credential"),
    ("token", "token"),
    ("scope", "scope"),
    ("oauth2_server_uri", "oauth2-server-uri"),
    ("access_delegation", "header.X-Iceberg-Access-Delegation"),
];

/// Builds the REST catalog properties for a catalog's options, resolving secrets.
pub(crate) fn rest_properties(
    catalog: &str,
    options: &BTreeMap<String, OptionValue>,
    secrets: &SecretResolver,
) -> Result<HashMap<String, String>, CatalogError> {
    if !options.contains_key("uri") {
        return Err(CatalogError::MissingOption {
            catalog: catalog.to_owned(),
            option: "uri",
        });
    }
    options
        .iter()
        .map(|(option, value)| {
            let property = if option.contains('.') {
                option.as_str()
            } else {
                OPTION_PROPERTIES
                    .iter()
                    .find(|(name, _)| name == option)
                    .map(|(_, property)| *property)
                    .ok_or_else(|| CatalogError::UnknownOption {
                        catalog: catalog.to_owned(),
                        option: option.clone(),
                    })?
            };
            let value = match value {
                OptionValue::Literal(value) => value.clone(),
                OptionValue::Secret(name) => {
                    secrets
                        .resolve(name)
                        .map_err(|source| CatalogError::Secret {
                            catalog: catalog.to_owned(),
                            source,
                        })?
                }
            };
            Ok((property.to_owned(), value))
        })
        .collect()
}

/// Connects to an Iceberg REST catalog whose tables live in S3-compatible storage.
pub(crate) async fn connect(
    name: &str,
    properties: HashMap<String, String>,
) -> iceberg::Result<Arc<dyn Catalog>> {
    let catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        }))
        .load(name, properties)
        .await?;
    Ok(Arc::new(catalog))
}
```

- [ ] **Step 6: Implement the registry**

`crates/catalog/src/registry.rs`: insert above the `#[cfg(test)]` test module:

```rust
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use datafusion::catalog::CatalogProvider;
use egret_engine_iceberg::IcebergCatalogProvider;
use egret_engine_meta::{CatalogDefinition, CatalogStore};

use crate::{CatalogError, ICEBERG_REST, SecretResolver, rest};

/// Name of DataFusion's in-memory catalog, which every session has for its own temporary tables.
pub const RESERVED_CATALOG: &str = "datafusion";

/// The catalogs of the engine, shared by all sessions. Definitions live in the metadata store;
/// connected catalogs are kept in memory.
#[derive(Debug)]
pub struct CatalogRegistry {
    store: Arc<dyn CatalogStore>,
    secrets: SecretResolver,
    catalogs: RwLock<BTreeMap<String, Arc<dyn CatalogProvider>>>,
}

impl CatalogRegistry {
    /// Connects every stored catalog. A catalog that cannot be connected is logged and stays
    /// unavailable until the engine restarts or the catalog is dropped and created again.
    pub async fn load(
        store: Arc<dyn CatalogStore>,
        secrets: SecretResolver,
    ) -> Result<Self, CatalogError> {
        let registry = Self {
            store,
            secrets,
            catalogs: RwLock::default(),
        };
        for definition in registry.store.list().await? {
            match registry.connect(&definition).await {
                Ok(provider) => registry.insert(definition.name, provider),
                Err(error) => tracing::warn!(%error, "catalog is unavailable"),
            }
        }
        Ok(registry)
    }

    /// Connects a new catalog, then stores its definition.
    pub async fn create(&self, definition: CatalogDefinition) -> Result<(), CatalogError> {
        if definition.name == RESERVED_CATALOG {
            return Err(CatalogError::Reserved(definition.name));
        }
        if self.get(&definition.name).is_some() {
            return Err(CatalogError::AlreadyExists(definition.name));
        }
        let provider = self.connect(&definition).await?;
        self.store.insert(&definition).await?;
        self.insert(definition.name, provider);
        Ok(())
    }

    /// Removes a catalog's definition and disconnects it.
    pub async fn remove(&self, name: &str) -> Result<(), CatalogError> {
        if !self.store.delete(name).await? {
            return Err(CatalogError::NotFound(name.to_owned()));
        }
        self.catalogs
            .write()
            .expect("catalog registry lock poisoned")
            .remove(name);
        Ok(())
    }

    /// All stored catalog definitions, ordered by name.
    pub async fn list(&self) -> Result<Vec<CatalogDefinition>, CatalogError> {
        Ok(self.store.list().await?)
    }

    /// A connected catalog.
    pub fn get(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        self.catalogs
            .read()
            .expect("catalog registry lock poisoned")
            .get(name)
            .cloned()
    }

    /// Names of the connected catalogs.
    pub fn names(&self) -> Vec<String> {
        self.catalogs
            .read()
            .expect("catalog registry lock poisoned")
            .keys()
            .cloned()
            .collect()
    }

    async fn connect(
        &self,
        definition: &CatalogDefinition,
    ) -> Result<Arc<dyn CatalogProvider>, CatalogError> {
        if definition.connector != ICEBERG_REST {
            return Err(CatalogError::UnknownConnector(definition.connector.clone()));
        }
        let properties =
            rest::rest_properties(&definition.name, &definition.options, &self.secrets)?;
        let connect_error = |source| CatalogError::Connect {
            catalog: definition.name.clone(),
            source: Box::new(source),
        };
        let catalog = rest::connect(&definition.name, properties)
            .await
            .map_err(connect_error)?;
        let provider = IcebergCatalogProvider::try_new(catalog)
            .await
            .map_err(connect_error)?;
        Ok(Arc::new(provider))
    }

    fn insert(&self, name: String, provider: Arc<dyn CatalogProvider>) {
        self.catalogs
            .write()
            .expect("catalog registry lock poisoned")
            .insert(name, provider);
    }
}
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `cargo test -p egret-engine-catalog`
Expected: `test result: ok. 13 passed`. That is 4 secrets, 4 rest and 5 registry tests; the registry tests use
`http://127.0.0.1:1` as an unreachable catalog.

- [ ] **Step 8: Lint and commit**

Run: `cargo fmt --all -- --check && cargo clippy --workspace --all-targets --locked -- -D warnings`
Expected: clean.

```bash
git add Cargo.toml Cargo.lock crates/catalog
git commit -m "feat(catalog): add the catalog registry with secret references"
```

---

### Task 6: Engine and sessions (`egret-engine-coordinator`), protocol-only pgwire

**Files:**
- Modify: `Cargo.toml` (workspace dependencies)
- Create: `crates/coordinator/Cargo.toml`, `crates/coordinator/src/lib.rs`, `crates/coordinator/src/error.rs`,
  `crates/coordinator/src/catalog_list.rs`, `crates/coordinator/src/engine.rs`, `crates/coordinator/src/session.rs`
- Modify (full replacements): `crates/pgwire/Cargo.toml`, `crates/pgwire/src/lib.rs`, `crates/pgwire/src/error.rs`,
  `crates/pgwire/src/handler.rs`, `crates/pgwire/src/server.rs`, `crates/pgwire/tests/simple_query.rs`,
  `crates/server/Cargo.toml`, `crates/server/src/lib.rs`

**Interfaces:**
- Consumes: Tasks 2, 3 and 5 (`InMemoryCatalogStore`, `parse`/`Statement`, `CatalogRegistry`, `CatalogError`,
  `SecretResolver`, `RESERVED_CATALOG`).
- Produces:
  - `Engine`, with these methods:
    - `new(Arc<CatalogRegistry>)` and `async in_memory()`.
    - `catalogs()`.
    - `session(&SessionParams) -> Session`. The default catalog is `database` when it names a registered
      catalog, otherwise `datafusion`; the default schema is `public`.
  - `SessionParams { user: Option<String>, database: Option<String> }`.
  - `Session::execute(Statement) -> Result<Output, QueryError>`.
  - `Output::{Rows(DataFrame), Command(&'static str)}`, where the tag is `CREATE CATALOG`, `DROP CATALOG` or
    `OK`.
  - `QueryError { sqlstate: &'static str, message: String }`, with `From<DataFusionError>`,
    `From<CatalogError>` and `not_supported(&str)`.
  - Module `sqlstate` with the constants from the Global Constraints.
  - pgwire changes:
    - The public API is now `PgWireServer::bind(SocketAddr, Arc<Engine>)` and
      `to_pg_error(QueryError) -> PgWireError`.
    - `SessionFactory`, `sqlstate()` and the SQLSTATE constants are removed from pgwire; the constants move to
      `egret_engine_coordinator::sqlstate`.
    - The `CREATE EXTERNAL TABLE` / `COPY` rejection moves to `Session`.
    - The isolation test now asserts SQLSTATE `42P01`.

The server crate keeps compiling with an in-memory engine. Task 7 wires up the metadata store.

- [ ] **Step 1: Add the workspace dependency**

```toml
egret-engine-coordinator = { path = "crates/coordinator" }
```

- [ ] **Step 2: Create the coordinator crate with the failing tests**

`crates/coordinator/Cargo.toml`:

```toml
[package]
name = "egret-engine-coordinator"
description = "Sessions and statement execution for Egret Engine coordinators"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true
repository.workspace = true
publish.workspace = true

[dependencies]
datafusion = { workspace = true }
egret-engine-catalog = { workspace = true }
egret-engine-meta = { workspace = true }
egret-engine-sql = { workspace = true }
thiserror = { workspace = true }

[dev-dependencies]
tokio = { workspace = true }

[lints]
workspace = true
```

`crates/coordinator/src/lib.rs`:

```rust
//! Coordinator services for Egret Engine: the engine shared by all connections, per-connection
//! sessions, and statement execution.

mod catalog_list;
mod engine;
mod error;
mod session;

pub use engine::Engine;
pub use error::{QueryError, sqlstate};
pub use session::{Output, Session, SessionParams};
```

`crates/coordinator/src/error.rs` (test module only for now):

```rust
#[cfg(test)]
mod tests {
    use datafusion::sql::parser::DFParser;

    use super::*;

    fn parse_error() -> DataFusionError {
        DFParser::parse_sql("SELEC 1").unwrap_err()
    }

    #[test]
    fn syntax_errors_map_to_42601_even_when_wrapped() {
        // Arrange
        let errors = [
            parse_error(),
            DataFusionError::Context("while parsing".to_owned(), Box::new(parse_error())),
        ];

        // Act
        let codes: Vec<&str> = errors
            .into_iter()
            .map(|e| QueryError::from(e).sqlstate)
            .collect();

        // Assert
        assert_eq!(codes, [sqlstate::SYNTAX_ERROR, sqlstate::SYNTAX_ERROR]);
    }

    #[test]
    fn missing_table_maps_to_42p01() {
        // Arrange
        let error = DataFusionError::Plan("table 'lake.ns.t' not found".to_owned());

        // Act
        let query_error = QueryError::from(error);

        // Assert
        assert_eq!(query_error.sqlstate, sqlstate::UNDEFINED_TABLE);
    }

    #[test]
    fn other_errors_map_to_internal_error() {
        // Arrange
        let error = DataFusionError::Execution("boom".to_owned());

        // Act
        let query_error = QueryError::from(error);

        // Assert
        assert_eq!(query_error.sqlstate, sqlstate::INTERNAL_ERROR);
        assert!(
            query_error.message.contains("boom"),
            "{}",
            query_error.message
        );
    }

    #[test]
    fn catalog_errors_map_to_postgres_codes() {
        // Arrange
        let errors = [
            CatalogError::AlreadyExists("lake".to_owned()),
            CatalogError::NotFound("lake".to_owned()),
            CatalogError::Reserved("datafusion".to_owned()),
            CatalogError::UnknownConnector("hive".to_owned()),
        ];

        // Act
        let codes: Vec<&str> = errors
            .into_iter()
            .map(|e| QueryError::from(e).sqlstate)
            .collect();

        // Assert
        assert_eq!(
            codes,
            [
                sqlstate::DUPLICATE_OBJECT,
                sqlstate::UNDEFINED_OBJECT,
                sqlstate::RESERVED_NAME,
                sqlstate::INVALID_PARAMETER_VALUE
            ]
        );
    }

    #[test]
    fn not_supported_names_the_feature() {
        // Act
        let error = QueryError::not_supported("COPY");

        // Assert
        assert_eq!(error.sqlstate, sqlstate::FEATURE_NOT_SUPPORTED);
        assert_eq!(error.message, "COPY is not supported");
    }
}
```

`crates/coordinator/src/session.rs` (test module only for now):

```rust
#[cfg(test)]
mod tests {
    use crate::{Engine, sqlstate};

    use super::*;

    async fn session() -> Session {
        Engine::in_memory().await.session(&SessionParams::default())
    }

    async fn run(session: &Session, sql: &str) -> Result<Vec<Output>, QueryError> {
        let mut outputs = Vec::new();
        for statement in egret_engine_sql::parse(sql)? {
            outputs.push(session.execute(statement).await?);
        }
        Ok(outputs)
    }

    async fn rows(output: Output) -> Vec<RecordBatch> {
        match output {
            Output::Rows(df) => df.collect().await.unwrap(),
            Output::Command(tag) => panic!("expected rows, got command {tag}"),
        }
    }

    #[tokio::test]
    async fn ddl_runs_before_execute_returns() {
        // Arrange
        let session = session().await;

        // Act
        let outputs = run(&session, "CREATE TABLE t AS VALUES (1)").await.unwrap();

        // Assert
        assert!(matches!(outputs[..], [Output::Command("OK")]));
        assert!(session.ctx.table_exist("t").unwrap());
    }

    #[tokio::test]
    async fn show_catalogs_lists_no_catalogs_initially() {
        // Arrange
        let session = session().await;

        // Act
        let mut outputs = run(&session, "SHOW CATALOGS").await.unwrap();
        let batches = rows(outputs.remove(0)).await;

        // Assert
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 0);
        assert_eq!(batches[0].schema().field(0).name(), "name");
    }

    #[tokio::test]
    async fn drop_missing_catalog_fails_unless_if_exists() {
        // Arrange
        let session = session().await;

        // Act
        let missing = run(&session, "DROP CATALOG lake").await.unwrap_err();
        let if_exists = run(&session, "DROP CATALOG IF EXISTS lake").await.unwrap();

        // Assert
        assert_eq!(missing.sqlstate, sqlstate::UNDEFINED_OBJECT);
        assert!(matches!(if_exists[..], [Output::Command("DROP CATALOG")]));
    }

    #[tokio::test]
    async fn missing_table_reports_42p01() {
        // Arrange
        let session = session().await;

        // Act
        let err = run(&session, "SELECT * FROM nope").await.unwrap_err();

        // Assert
        assert_eq!(err.sqlstate, sqlstate::UNDEFINED_TABLE);
    }

    #[tokio::test]
    async fn copy_is_rejected() {
        // Arrange
        let session = session().await;

        // Act
        let err = run(
            &session,
            "COPY (SELECT 1) TO '/tmp/egret-never-written.csv' STORED AS CSV",
        )
        .await
        .unwrap_err();

        // Assert
        assert_eq!(err, QueryError::not_supported("COPY"));
    }

    #[tokio::test]
    async fn database_selects_the_default_catalog_only_if_it_exists() {
        // Arrange
        let engine = Engine::in_memory().await;
        let params = SessionParams {
            user: Some("demo".to_owned()),
            database: Some("demo".to_owned()),
        };

        // Act
        let session = engine.session(&params);

        // Assert
        let options = session.ctx.state().config().options().clone();
        assert_eq!(options.catalog.default_catalog, "datafusion");
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p egret-engine-coordinator`
Expected: compilation fails (`QueryError`, `Engine`, `Session` unresolved; `catalog_list.rs`/`engine.rs`
missing).

- [ ] **Step 4: Implement errors, the session catalog list, the engine and the session**

`crates/coordinator/src/error.rs`: insert above the `#[cfg(test)]` test module:

```rust
use datafusion::error::DataFusionError;
use egret_engine_catalog::CatalogError;

/// SQLSTATE codes reported to clients.
pub mod sqlstate {
    /// Syntax error.
    pub const SYNTAX_ERROR: &str = "42601";
    /// Table missing (or, from M4 on, not visible to the principal).
    pub const UNDEFINED_TABLE: &str = "42P01";
    /// Catalog (or another named object) missing.
    pub const UNDEFINED_OBJECT: &str = "42704";
    /// Catalog (or another named object) already exists.
    pub const DUPLICATE_OBJECT: &str = "42710";
    /// Name reserved by the engine.
    pub const RESERVED_NAME: &str = "42939";
    /// Invalid option or option value.
    pub const INVALID_PARAMETER_VALUE: &str = "22023";
    /// An external system (e.g. a catalog) could not be reached.
    pub const CONNECTION_FAILURE: &str = "08001";
    /// Feature deliberately not supported.
    pub const FEATURE_NOT_SUPPORTED: &str = "0A000";
    /// Internal error.
    pub const INTERNAL_ERROR: &str = "XX000";
}

/// An error reported to the client: a SQLSTATE code and a message.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct QueryError {
    pub sqlstate: &'static str,
    pub message: String,
}

impl QueryError {
    /// A statement Egret Engine deliberately does not support.
    pub fn not_supported(feature: &str) -> Self {
        Self {
            sqlstate: sqlstate::FEATURE_NOT_SUPPORTED,
            message: format!("{feature} is not supported"),
        }
    }
}

impl From<DataFusionError> for QueryError {
    fn from(error: DataFusionError) -> Self {
        let code = match error.find_root() {
            DataFusionError::SQL(..) => sqlstate::SYNTAX_ERROR,
            // DataFusion reports a table it cannot resolve as "table '<name>' not found".
            DataFusionError::Plan(message)
                if message.starts_with("table '") && message.contains("' not found") =>
            {
                sqlstate::UNDEFINED_TABLE
            }
            _ => sqlstate::INTERNAL_ERROR,
        };
        Self {
            sqlstate: code,
            message: error.to_string(),
        }
    }
}

impl From<CatalogError> for QueryError {
    fn from(error: CatalogError) -> Self {
        let code = match &error {
            CatalogError::AlreadyExists(_) => sqlstate::DUPLICATE_OBJECT,
            CatalogError::NotFound(_) => sqlstate::UNDEFINED_OBJECT,
            CatalogError::Reserved(_) => sqlstate::RESERVED_NAME,
            CatalogError::UnknownConnector(_)
            | CatalogError::UnknownOption { .. }
            | CatalogError::MissingOption { .. }
            | CatalogError::Secret { .. } => sqlstate::INVALID_PARAMETER_VALUE,
            CatalogError::Connect { .. } => sqlstate::CONNECTION_FAILURE,
            CatalogError::Store(_) => sqlstate::INTERNAL_ERROR,
        };
        Self {
            sqlstate: code,
            message: error.to_string(),
        }
    }
}
```

`crates/coordinator/src/catalog_list.rs`:

```rust
use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, CatalogProviderList, MemoryCatalogProviderList};
use egret_engine_catalog::CatalogRegistry;

/// The catalogs one session sees: the engine's shared catalogs, plus catalogs that exist only in
/// this session (DataFusion's in-memory default catalog for temporary tables). A shared catalog
/// shadows a session catalog with the same name.
#[derive(Debug)]
pub(crate) struct SessionCatalogList {
    shared: Arc<CatalogRegistry>,
    local: MemoryCatalogProviderList,
}

impl SessionCatalogList {
    pub(crate) fn new(shared: Arc<CatalogRegistry>) -> Self {
        Self {
            shared,
            local: MemoryCatalogProviderList::new(),
        }
    }
}

impl CatalogProviderList for SessionCatalogList {
    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        self.local.register_catalog(name, catalog)
    }

    fn catalog_names(&self) -> Vec<String> {
        let mut names = self.shared.names();
        names.extend(self.local.catalog_names());
        names.sort();
        names.dedup();
        names
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        self.shared.get(name).or_else(|| self.local.catalog(name))
    }
}
```

`crates/coordinator/src/engine.rs`:

```rust
use std::sync::Arc;

use datafusion::execution::SessionStateBuilder;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::{SessionConfig, SessionContext};
use egret_engine_catalog::{CatalogRegistry, RESERVED_CATALOG, SecretResolver};
use egret_engine_meta::InMemoryCatalogStore;

use crate::catalog_list::SessionCatalogList;
use crate::session::{Session, SessionParams};

/// Schema that unqualified table names resolve in.
const DEFAULT_SCHEMA: &str = "public";

/// State shared by every session of a coordinator: the runtime (memory pool, object stores) and
/// the catalogs.
#[derive(Debug)]
pub struct Engine {
    runtime: Arc<RuntimeEnv>,
    catalogs: Arc<CatalogRegistry>,
}

impl Engine {
    pub fn new(catalogs: Arc<CatalogRegistry>) -> Self {
        Self {
            runtime: Arc::new(RuntimeEnv::default()),
            catalogs,
        }
    }

    /// An engine without a metadata store: catalogs are kept in memory only.
    pub async fn in_memory() -> Self {
        let store = Arc::new(InMemoryCatalogStore::default());
        let catalogs = CatalogRegistry::load(store, SecretResolver::default())
            .await
            .expect("an empty in-memory store loads");
        Self::new(Arc::new(catalogs))
    }

    pub fn catalogs(&self) -> &Arc<CatalogRegistry> {
        &self.catalogs
    }

    /// Starts a session. The startup `database` parameter selects the default catalog when it
    /// names a catalog of the engine; otherwise the session's in-memory catalog is the default.
    pub fn session(&self, params: &SessionParams) -> Session {
        let default_catalog = params
            .database
            .as_deref()
            .filter(|database| self.catalogs.get(database).is_some())
            .unwrap_or(RESERVED_CATALOG);
        let config =
            SessionConfig::new().with_default_catalog_and_schema(default_catalog, DEFAULT_SCHEMA);
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(self.runtime.clone())
            .with_catalog_list(Arc::new(SessionCatalogList::new(self.catalogs.clone())))
            .with_default_features()
            .build();
        Session::new(SessionContext::new_with_state(state), self.catalogs.clone())
    }
}
```

`crates/coordinator/src/session.rs`: insert above the `#[cfg(test)]` test module:

```rust
use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::{DdlStatement, LogicalPlan};
use datafusion::prelude::SessionContext;
use egret_engine_catalog::{CatalogError, CatalogRegistry};
use egret_engine_sql::Statement;

use crate::QueryError;

/// Startup parameters of a client connection.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionParams {
    pub user: Option<String>,
    pub database: Option<String>,
}

/// The result of one statement.
#[derive(Debug)]
pub enum Output {
    /// Rows, produced lazily as the client reads them.
    Rows(DataFrame),
    /// A statement without rows, with the command tag to report.
    Command(&'static str),
}

/// One client's session. Settings and temporary tables are never shared between sessions.
pub struct Session {
    ctx: SessionContext,
    catalogs: Arc<CatalogRegistry>,
}

impl Session {
    pub(crate) fn new(ctx: SessionContext, catalogs: Arc<CatalogRegistry>) -> Self {
        Self { ctx, catalogs }
    }

    /// Executes one statement. DDL and engine commands have taken effect when this returns;
    /// queries and DML run as their rows are read.
    pub async fn execute(&self, statement: Statement) -> Result<Output, QueryError> {
        match statement {
            Statement::CreateCatalog(definition) => {
                self.catalogs.create(definition).await?;
                Ok(Output::Command("CREATE CATALOG"))
            }
            Statement::DropCatalog { name, if_exists } => match self.catalogs.remove(&name).await {
                Ok(()) => Ok(Output::Command("DROP CATALOG")),
                Err(CatalogError::NotFound(_)) if if_exists => Ok(Output::Command("DROP CATALOG")),
                Err(error) => Err(error.into()),
            },
            Statement::ShowCatalogs => self.show_catalogs().await,
            Statement::DataFusion(statement) => {
                let plan = self.ctx.state().statement_to_plan(statement).await?;
                reject_unsupported(&plan)?;
                let df = self.ctx.execute_logical_plan(plan).await?;
                // Statements without output columns (DDL, SET) have already run inside
                // `execute_logical_plan`.
                if df.schema().fields().is_empty() {
                    Ok(Output::Command("OK"))
                } else {
                    Ok(Output::Rows(df))
                }
            }
        }
    }

    async fn show_catalogs(&self) -> Result<Output, QueryError> {
        let definitions = self.catalogs.list().await?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("connector", DataType::Utf8, false),
        ]));
        let names = StringArray::from_iter_values(definitions.iter().map(|d| d.name.as_str()));
        let connectors =
            StringArray::from_iter_values(definitions.iter().map(|d| d.connector.as_str()));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(names), Arc::new(connectors)])
            .map_err(datafusion::error::DataFusionError::from)?;
        Ok(Output::Rows(self.ctx.read_batch(batch)?))
    }
}

/// Rejects plans that read or write arbitrary local files, anywhere in the plan (including under
/// `EXPLAIN ANALYZE`, which executes its input). Tables come from Iceberg catalogs instead.
fn reject_unsupported(plan: &LogicalPlan) -> Result<(), QueryError> {
    let mut unsupported = None;
    plan.apply(|node| {
        unsupported = match node {
            LogicalPlan::Ddl(DdlStatement::CreateExternalTable(_)) => Some("CREATE EXTERNAL TABLE"),
            LogicalPlan::Copy(_) => Some("COPY"),
            _ => None,
        };
        Ok(if unsupported.is_some() {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    match unsupported {
        Some(feature) => Err(QueryError::not_supported(feature)),
        None => Ok(()),
    }
}
```

- [ ] **Step 5: Run the coordinator tests to verify they pass**

Run: `cargo test -p egret-engine-coordinator`
Expected: `test result: ok. 11 passed`.

- [ ] **Step 6: Make pgwire protocol-only**

Replace these files completely:

`crates/pgwire/Cargo.toml`:

```toml
[package]
name = "egret-engine-pgwire"
description = "PostgreSQL wire protocol frontend for Egret Engine"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true
repository.workspace = true
publish.workspace = true

[dependencies]
arrow-pg = { workspace = true }
async-trait = { workspace = true }
egret-engine-coordinator = { workspace = true }
egret-engine-sql = { workspace = true }
futures = { workspace = true }
pgwire = { workspace = true }
tokio = { workspace = true }
tracing = { workspace = true }

[dev-dependencies]
tokio-postgres = { workspace = true }

[lints]
workspace = true
```

`crates/pgwire/src/lib.rs`:

```rust
//! PostgreSQL wire protocol frontend for Egret Engine.

mod error;
mod handler;
mod server;

pub use error::to_pg_error;
pub use server::PgWireServer;
```

`crates/pgwire/src/error.rs`:

```rust
use egret_engine_coordinator::QueryError;
use pgwire::error::{ErrorInfo, PgWireError};

/// Converts a query error into an `ERROR` response for the client.
pub fn to_pg_error(error: QueryError) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        error.sqlstate.to_owned(),
        error.message,
    )))
}

#[cfg(test)]
mod tests {
    use egret_engine_coordinator::sqlstate;

    use super::*;

    #[test]
    fn builds_user_error_with_sqlstate_and_message() {
        // Arrange
        let error = QueryError::not_supported("COPY");

        // Act
        let pg_error = to_pg_error(error);

        // Assert
        match pg_error {
            PgWireError::UserError(info) => {
                assert_eq!(info.severity, "ERROR");
                assert_eq!(info.code, sqlstate::FEATURE_NOT_SUPPORTED);
                assert_eq!(info.message, "COPY is not supported");
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }
}
```

`crates/pgwire/src/handler.rs`:

```rust
use std::fmt::Debug;
use std::sync::{Arc, OnceLock};

use arrow_pg::datatypes::df::encode_dataframe;
use async_trait::async_trait;
use egret_engine_coordinator::{Engine, Output, QueryError, Session, SessionParams, sqlstate};
use egret_engine_sql::Statement;
use futures::Sink;
use pgwire::api::auth::StartupHandler;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::portal::Format;
use pgwire::api::query::{SimpleQueryHandler, send_execution_response, send_query_response};
use pgwire::api::results::{Response, Tag};
use pgwire::api::store::PortalStore;
use pgwire::api::{
    ClientInfo, ClientPortalStore, METADATA_DATABASE, METADATA_USER, PgWireServerHandlers,
};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};

use crate::error::to_pg_error;

/// Serves one client connection. Its session is created once the startup handshake has provided
/// the user and database, and is never shared with other connections.
pub(crate) struct ConnectionHandler {
    engine: Arc<Engine>,
    session: OnceLock<Session>,
}

impl ConnectionHandler {
    pub(crate) fn new(engine: Arc<Engine>) -> Self {
        Self {
            engine,
            session: OnceLock::new(),
        }
    }

    fn session(&self) -> PgWireResult<&Session> {
        self.session.get().ok_or_else(|| {
            to_pg_error(QueryError {
                sqlstate: sqlstate::INTERNAL_ERROR,
                message: "session used before startup completed".to_owned(),
            })
        })
    }

    /// Runs one statement to completion and sends its result to the client, so the next statement
    /// of the query observes its effects.
    async fn execute<C>(&self, client: &mut C, statement: Statement) -> PgWireResult<()>
    where
        C: Sink<PgWireBackendMessage> + Unpin,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match self
            .session()?
            .execute(statement)
            .await
            .map_err(to_pg_error)?
        {
            Output::Command(tag) => send_execution_response(client, Tag::new(tag)).await,
            Output::Rows(df) => {
                // Sending drains the DataFrame's stream, which is what executes DML and queries.
                let response = encode_dataframe(df, &Format::UnifiedText, None).await?;
                send_query_response(client, response, true).await
            }
        }
    }
}

// Authentication is added in M4; until then every startup is accepted.
#[async_trait]
impl NoopStartupHandler for ConnectionHandler {
    async fn post_startup<C>(
        &self,
        client: &mut C,
        _message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let metadata = client.metadata();
        let params = SessionParams {
            user: metadata.get(METADATA_USER).cloned(),
            database: metadata.get(METADATA_DATABASE).cloned(),
        };
        // Startup runs once per connection, so the session cannot already be set.
        let _ = self.session.set(self.engine.session(&params));
        Ok(())
    }
}

#[async_trait]
impl SimpleQueryHandler for ConnectionHandler {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let statements =
            egret_engine_sql::parse(query).map_err(|e| to_pg_error(QueryError::from(e)))?;
        if statements.is_empty() {
            return Ok(vec![Response::EmptyQuery]);
        }
        // Each statement's result is sent before the next statement is planned. On error the
        // results already sent stay with the client, pgwire follows them with the ErrorResponse
        // and ReadyForQuery, and the remaining statements are skipped, as in PostgreSQL.
        for statement in statements {
            self.execute(client, statement).await?;
        }
        Ok(Vec::new())
    }
}

/// The pgwire handler set for one connection.
pub(crate) struct Handlers {
    connection: Arc<ConnectionHandler>,
}

impl Handlers {
    pub(crate) fn new(connection: ConnectionHandler) -> Self {
        Self {
            connection: Arc::new(connection),
        }
    }
}

impl PgWireServerHandlers for Handlers {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.connection.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        self.connection.clone()
    }
}
```

`crates/pgwire/src/server.rs`:

```rust
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use egret_engine_coordinator::Engine;
use tokio::net::TcpListener;

use crate::handler::{ConnectionHandler, Handlers};

/// Back-off after a failed `accept`, e.g. when the process is out of file descriptors.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);

/// Accepts PostgreSQL wire protocol connections and serves each one with its own session.
pub struct PgWireServer {
    listener: TcpListener,
    engine: Arc<Engine>,
}

impl PgWireServer {
    /// Binds the listener. Use port `0` to let the OS pick a free port.
    pub async fn bind(addr: SocketAddr, engine: Arc<Engine>) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self { listener, engine })
    }

    /// The address the listener is bound to.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Accepts connections until `shutdown` completes. Connections already open keep running
    /// on their own tasks.
    pub async fn serve(self, shutdown: impl Future<Output = ()>) {
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                () = &mut shutdown => break,
                accepted = self.listener.accept() => match accepted {
                    Ok((socket, peer)) => {
                        let handlers = Arc::new(Handlers::new(ConnectionHandler::new(
                            self.engine.clone(),
                        )));
                        tokio::spawn(async move {
                            if let Err(error) =
                                pgwire::tokio::process_socket(socket, None, handlers).await
                            {
                                tracing::warn!(%peer, %error, "connection closed with error");
                            }
                        });
                    }
                    Err(error) => {
                        tracing::warn!(%error, "failed to accept connection");
                        tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
                    }
                },
            }
        }
    }
}
```

In `crates/pgwire/tests/simple_query.rs`, change the imports and `start_server`, and make the isolation test
assert the SQLSTATE:

```diff
diff --git a/crates/pgwire/tests/simple_query.rs b/crates/pgwire/tests/simple_query.rs
index 04a9870..f22701f 100644
--- a/crates/pgwire/tests/simple_query.rs
+++ b/crates/pgwire/tests/simple_query.rs
@@ -1,7 +1,8 @@
 use std::sync::Arc;
 
-use datafusion::prelude::SessionContext;
-use egret_engine_pgwire::{FEATURE_NOT_SUPPORTED, PgWireServer, SYNTAX_ERROR, SessionFactory};
+use egret_engine_coordinator::Engine;
+use egret_engine_coordinator::sqlstate::{FEATURE_NOT_SUPPORTED, SYNTAX_ERROR, UNDEFINED_TABLE};
+use egret_engine_pgwire::PgWireServer;
 use futures::StreamExt;
 use tokio::sync::oneshot;
 use tokio_postgres::{Client, NoTls, SimpleQueryMessage};
@@ -13,8 +14,8 @@ struct TestServer {
 }
 
 async fn start_server() -> TestServer {
-    let sessions: SessionFactory = Arc::new(SessionContext::new);
-    let server = PgWireServer::bind("127.0.0.1:0".parse().unwrap(), sessions)
+    let engine = Arc::new(Engine::in_memory().await);
+    let server = PgWireServer::bind("127.0.0.1:0".parse().unwrap(), engine)
         .await
         .expect("bind");
     let port = server.local_addr().expect("local addr").port();
@@ -169,12 +170,9 @@ async fn sessions_are_isolated_per_connection() {
     // Assert
     assert_eq!(rows(&own), vec![vec![s("2")]]);
     let error = other.unwrap_err();
-    assert!(
-        error
-            .as_db_error()
-            .expect("db error")
-            .message()
-            .contains("not found"),
+    assert_eq!(
+        error.code().expect("sqlstate").code(),
+        UNDEFINED_TABLE,
         "{error}"
     );
 }
```

- [ ] **Step 7: Keep the server compiling with an in-memory engine**

```diff
diff --git a/crates/server/Cargo.toml b/crates/server/Cargo.toml
index cd0d488..82431fb 100644
--- a/crates/server/Cargo.toml
+++ b/crates/server/Cargo.toml
@@ -14,8 +14,8 @@ path = "src/main.rs"
 
 [dependencies]
 clap = { workspace = true }
-datafusion = { workspace = true }
 egret-engine-common = { workspace = true }
+egret-engine-coordinator = { workspace = true }
 egret-engine-pgwire = { workspace = true }
 thiserror = { workspace = true }
 tokio = { workspace = true }
```

`crates/server/src/lib.rs`:

```rust
//! Wires configuration to the engine's services for the `egret-engine` binary.

use std::net::SocketAddr;
use std::sync::Arc;

use egret_engine_common::{Config, Role};
use egret_engine_coordinator::Engine;
use egret_engine_pgwire::PgWireServer;

/// Errors raised while starting the server.
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("role `{0}` is not supported yet")]
    UnsupportedRole(Role),
    #[error("failed to bind pgwire listener on {addr}: {source}")]
    Bind {
        addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },
}

/// A started (bound) engine process.
pub struct Server {
    pgwire: PgWireServer,
}

impl Server {
    /// Binds the listeners required by the configured role.
    pub async fn bind(config: &Config) -> Result<Self, ServerError> {
        match config.server.role {
            Role::Coordinator | Role::All => {}
            // Workers arrive with distributed execution (P0 milestone M2).
            Role::Worker => return Err(ServerError::UnsupportedRole(Role::Worker)),
        }
        let engine = Arc::new(Engine::in_memory().await);
        let addr = config.server.pgwire_listen;
        let pgwire = PgWireServer::bind(addr, engine)
            .await
            .map_err(|source| ServerError::Bind { addr, source })?;
        Ok(Self { pgwire })
    }

    /// The address the pgwire listener is bound to.
    pub fn pgwire_addr(&self) -> std::io::Result<SocketAddr> {
        self.pgwire.local_addr()
    }

    /// Serves until `shutdown` completes.
    pub async fn run(self, shutdown: impl Future<Output = ()>) {
        self.pgwire.serve(shutdown).await;
    }
}
```

- [ ] **Step 8: Run all tests**

Run: `cargo test --workspace`
Expected: everything passes. That is pgwire `1` unit + `11` `simple_query`, coordinator `11`, and the existing
server and cli tests `3` + `3`, plus the earlier crates.

- [ ] **Step 9: Lint and commit**

Run: `cargo fmt --all -- --check && cargo clippy --workspace --all-targets --locked -- -D warnings`
Expected: clean.

```bash
git add Cargo.toml Cargo.lock crates/coordinator crates/pgwire crates/server
git commit -m "feat(coordinator): add the engine and per-connection sessions; build sessions after startup"
```

---

### Task 7: Metadata store and catalogs in the server, end-to-end tests, docs

**Files:**
- Modify: `crates/common/src/config.rs`, `crates/common/src/lib.rs`, `crates/server/Cargo.toml`,
  `crates/server/src/lib.rs`, `README.md`
- Create: `crates/server/tests/iceberg.rs`

**Interfaces:**
- Consumes: everything above.
- Produces:
  - `Config { server, meta: MetaConfig, secrets: SecretsConfig }` (both new sections `#[serde(default)]`).
  - `MetaConfig { url: Option<String> }` and `SecretsConfig { dir: Option<PathBuf> }`.
  - `ServerError` gains `Meta(MetaError)` and `Catalogs(CatalogError)`.
  - `Server::bind` uses PostgreSQL when `meta.url` is set. Otherwise it uses the in-memory store and logs a
    warning.

- [ ] **Step 1: Write the failing configuration test**

Add to the `tests` module of `crates/common/src/config.rs`, before `missing_file_is_an_error`:

```rust
    #[test]
    fn env_sets_meta_url_and_secrets_dir() {
        Jail::expect_with(|jail| {
            // Arrange
            jail.set_env("EGRET_ENGINE__META__URL", "postgres://egret@db/egret");
            jail.set_env("EGRET_ENGINE__SECRETS__DIR", "/etc/egret/secrets");

            // Act
            let config = Config::load(None).expect("env loads");

            // Assert
            assert_eq!(
                config.meta.url.as_deref(),
                Some("postgres://egret@db/egret")
            );
            assert_eq!(
                config.secrets.dir.as_deref(),
                Some(Path::new("/etc/egret/secrets"))
            );
            Ok(())
        });
    }
```

Run: `cargo test -p egret-engine-common`
Expected: compilation fails (`no field meta on type Config`).

- [ ] **Step 2: Add the configuration sections**

```diff
diff --git a/crates/common/src/config.rs b/crates/common/src/config.rs
index 53431ae..d6dc4f2 100644
--- a/crates/common/src/config.rs
+++ b/crates/common/src/config.rs
@@ -16,6 +16,10 @@ pub const ENV_PREFIX: &str = "EGRET_ENGINE__";
 #[serde(deny_unknown_fields)]
 pub struct Config {
     pub server: ServerConfig,
+    #[serde(default)]
+    pub meta: MetaConfig,
+    #[serde(default)]
+    pub secrets: SecretsConfig,
 }
 
 /// Process-level settings.
@@ -37,6 +41,23 @@ impl Default for ServerConfig {
     }
 }
 
+/// Metadata store settings.
+#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
+#[serde(deny_unknown_fields)]
+pub struct MetaConfig {
+    /// PostgreSQL URL of the metadata store, e.g. `postgres://user:password@host:5432/egret`.
+    /// Without it, catalog definitions are kept in memory and lost when the process stops.
+    pub url: Option<String>,
+}
+
+/// Where `secret('<name>')` references are resolved, besides `EGRET_SECRET_<NAME>` variables.
+#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
+#[serde(deny_unknown_fields)]
+pub struct SecretsConfig {
+    /// Directory holding one file per secret, e.g. a mounted Kubernetes Secret.
+    pub dir: Option<PathBuf>,
+}
+
 /// Errors raised while loading configuration.
 #[derive(Debug, thiserror::Error)]
 pub enum ConfigError {
@@ -127,6 +148,29 @@ mod tests {
         });
     }
 
+    #[test]
+    fn env_sets_meta_url_and_secrets_dir() {
+        Jail::expect_with(|jail| {
+            // Arrange
+            jail.set_env("EGRET_ENGINE__META__URL", "postgres://egret@db/egret");
+            jail.set_env("EGRET_ENGINE__SECRETS__DIR", "/etc/egret/secrets");
+
+            // Act
+            let config = Config::load(None).expect("env loads");
+
+            // Assert
+            assert_eq!(
+                config.meta.url.as_deref(),
+                Some("postgres://egret@db/egret")
+            );
+            assert_eq!(
+                config.secrets.dir.as_deref(),
+                Some(Path::new("/etc/egret/secrets"))
+            );
+            Ok(())
+        });
+    }
+
     #[test]
     fn missing_file_is_an_error() {
         Jail::expect_with(|_jail| {
```

```diff
diff --git a/crates/common/src/lib.rs b/crates/common/src/lib.rs
index b51bf78..bf75fc3 100644
--- a/crates/common/src/lib.rs
+++ b/crates/common/src/lib.rs
@@ -3,5 +3,5 @@
 mod config;
 mod role;
 
-pub use config::{Config, ConfigError, ENV_PREFIX, ServerConfig};
+pub use config::{Config, ConfigError, ENV_PREFIX, MetaConfig, SecretsConfig, ServerConfig};
 pub use role::{Role, UnknownRole};
```

Run: `cargo test -p egret-engine-common`
Expected: `test result: ok. 10 passed`.

- [ ] **Step 3: Write the failing end-to-end tests**

`crates/server/tests/iceberg.rs`:

```rust
//! End-to-end tests against the `dev/iceberg` test environment. Run them with
//! `cargo test -- --ignored` after `docker compose -f dev/iceberg/docker-compose.yml up -d --wait`.

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use egret_engine_common::{Config, Role};
use egret_engine_coordinator::sqlstate::UNDEFINED_TABLE;
use egret_engine_server::Server;
use serde::Deserialize;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_owned())
}

fn postgres_url() -> String {
    env_or(
        "EGRET_TEST_POSTGRES_URL",
        "postgres://egret:egret@127.0.0.1:25432/egret",
    )
}

/// `CREATE CATALOG` for the test environment's REST catalog. The S3 secret key comes from the
/// `dev-s3` secret.
fn create_catalog(name: &str) -> String {
    let rest = env_or("EGRET_TEST_REST_URI", "http://127.0.0.1:28181");
    let s3 = env_or("EGRET_TEST_S3_ENDPOINT", "http://127.0.0.1:29000");
    format!(
        "CREATE CATALOG {name} USING iceberg_rest WITH (
            uri = '{rest}',
            's3.endpoint' = '{s3}',
            's3.region' = 'us-east-1',
            's3.path-style-access' = 'true',
            's3.access-key-id' = 'admin',
            's3.secret-access-key' = secret('dev-s3'))"
    )
}

/// A secrets directory holding the test environment's S3 secret key.
fn secrets_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("dev-s3"), "password\n").unwrap();
    dir
}

fn config(meta_url: Option<String>, secrets: &Path) -> Config {
    let mut config = Config::default();
    config.server.role = Role::All;
    config.server.pgwire_listen = "127.0.0.1:0".parse().unwrap();
    config.meta.url = meta_url;
    config.secrets.dir = Some(secrets.to_path_buf());
    config
}

/// An in-process server; `stop` shuts it down and waits for it.
struct Running {
    port: u16,
    stop: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

impl Running {
    async fn start(config: &Config) -> Self {
        let server = Server::bind(config).await.expect("bind");
        let port = server.pgwire_addr().expect("addr").port();
        let (stop, stopped) = oneshot::channel::<()>();
        let task = tokio::spawn(server.run(async {
            let _ = stopped.await;
        }));
        Self { port, stop, task }
    }

    async fn connect(&self, database: &str) -> Client {
        let (client, connection) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user=test dbname={database}",
                self.port
            ),
            NoTls,
        )
        .await
        .expect("connect");
        tokio::spawn(connection);
        client
    }

    async fn stop(self) {
        self.stop.send(()).expect("server still running");
        self.task.await.expect("server task");
    }
}

/// Text values of every data row, in order.
async fn query(client: &Client, sql: &str) -> Vec<Vec<Option<String>>> {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|i| row.get(i).map(str::to_owned))
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

/// A fixture query and the rows Spark returned for it.
#[derive(Debug, Deserialize)]
struct Expected {
    sql: String,
    rows: Vec<Vec<Option<String>>>,
}

fn expected_results() -> Vec<Expected> {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../dev/iceberg/expected/fixtures.json");
    let json = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("{}: {e}; is the test environment up?", path.display()));
    serde_json::from_str(&json).expect("valid fixtures.json")
}

/// A catalog name no other test run uses, since the metadata store outlives test runs.
fn unique_name(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}_{nanos}")
}

#[tokio::test]
#[ignore = "requires the dev/iceberg test environment"]
async fn fixture_queries_match_spark() {
    // Arrange
    let secrets = secrets_dir();
    let server = Running::start(&config(None, secrets.path())).await;
    let client = server.connect("test").await;
    client
        .simple_query(&create_catalog("rest"))
        .await
        .expect("create catalog");
    let expected = expected_results();

    // Act
    let mut actual = Vec::new();
    for case in &expected {
        actual.push(query(&client, &case.sql).await);
    }

    // Assert
    for (case, rows) in expected.iter().zip(&actual) {
        assert_eq!(rows, &case.rows, "{}", case.sql);
    }
    server.stop().await;
}

#[tokio::test]
#[ignore = "requires the dev/iceberg test environment"]
async fn database_parameter_selects_the_default_catalog() {
    // Arrange
    let secrets = secrets_dir();
    let server = Running::start(&config(None, secrets.path())).await;
    server
        .connect("test")
        .await
        .simple_query(&create_catalog("lake"))
        .await
        .expect("create catalog");
    let client = server.connect("lake").await;

    // Act
    let rows = query(&client, "SELECT count(*) FROM fixtures.positional_deletes").await;

    // Assert
    assert_eq!(rows, vec![vec![Some("10".to_owned())]]);
    server.stop().await;
}

#[tokio::test]
#[ignore = "requires the dev/iceberg test environment"]
async fn missing_iceberg_table_reports_42p01() {
    // Arrange
    let secrets = secrets_dir();
    let server = Running::start(&config(None, secrets.path())).await;
    let client = server.connect("test").await;
    client
        .simple_query(&create_catalog("rest"))
        .await
        .expect("create catalog");

    // Act
    let error = client
        .simple_query("SELECT * FROM rest.fixtures.missing")
        .await
        .unwrap_err();

    // Assert
    assert_eq!(
        error.code().expect("sqlstate").code(),
        UNDEFINED_TABLE,
        "{error}"
    );
    server.stop().await;
}

#[tokio::test]
#[ignore = "requires the dev/iceberg test environment"]
async fn catalogs_persist_across_restarts() {
    // Arrange
    let secrets = secrets_dir();
    let config = config(Some(postgres_url()), secrets.path());
    let name = unique_name("persisted");
    let first = Running::start(&config).await;
    first
        .connect("test")
        .await
        .simple_query(&create_catalog(&name))
        .await
        .expect("create catalog");
    first.stop().await;

    // Act
    let second = Running::start(&config).await;
    let client = second.connect("test").await;
    let catalogs = query(&client, "SHOW CATALOGS").await;
    let count = query(
        &client,
        &format!("SELECT count(*) FROM {name}.fixtures.positional_deletes"),
    )
    .await;

    // Assert
    assert!(
        catalogs.contains(&vec![Some(name.clone()), Some("iceberg_rest".to_owned())]),
        "{catalogs:?}"
    );
    assert_eq!(count, vec![vec![Some("10".to_owned())]]);
    client
        .simple_query(&format!("DROP CATALOG {name}"))
        .await
        .expect("drop catalog");
    second.stop().await;
}
```

Add the dev-dependencies, and the runtime dependencies that Step 4 needs, to `crates/server/Cargo.toml`:

```diff
diff --git a/crates/server/Cargo.toml b/crates/server/Cargo.toml
index 82431fb..b7af821 100644
--- a/crates/server/Cargo.toml
+++ b/crates/server/Cargo.toml
@@ -14,8 +14,10 @@ path = "src/main.rs"
 
 [dependencies]
 clap = { workspace = true }
+egret-engine-catalog = { workspace = true }
 egret-engine-common = { workspace = true }
 egret-engine-coordinator = { workspace = true }
+egret-engine-meta = { workspace = true }
 egret-engine-pgwire = { workspace = true }
 thiserror = { workspace = true }
 tokio = { workspace = true }
@@ -23,6 +25,9 @@ tracing = { workspace = true }
 tracing-subscriber = { workspace = true }
 
 [dev-dependencies]
+serde = { workspace = true }
+serde_json = { workspace = true }
+tempfile = { workspace = true }
 tokio-postgres = { workspace = true }
 
 [lints]
```

Run: `cargo test -p egret-engine-server --test iceberg -- --ignored`
Expected: the tests compile, but `fixture_queries_match_spark`, `database_parameter_selects_the_default_catalog`
and `catalogs_persist_across_restarts` fail. `CREATE CATALOG` reports `secret `dev-s3` not found` (SQLSTATE
`22023`), because `Server::bind` still uses `Engine::in_memory()` and ignores `secrets.dir` and `meta.url`.
`missing_iceberg_table_reports_42p01` also fails, while creating its catalog.

- [ ] **Step 4: Wire the metadata store and catalogs into the server**

`crates/server/src/lib.rs`:

```rust
//! Wires configuration to the engine's services for the `egret-engine` binary.

use std::net::SocketAddr;
use std::sync::Arc;

use egret_engine_catalog::{CatalogError, CatalogRegistry, SecretResolver};
use egret_engine_common::{Config, Role};
use egret_engine_coordinator::Engine;
use egret_engine_meta::{CatalogStore, InMemoryCatalogStore, MetaError, PgCatalogStore};
use egret_engine_pgwire::PgWireServer;

/// Errors raised while starting the server.
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("role `{0}` is not supported yet")]
    UnsupportedRole(Role),
    #[error("failed to open the metadata store: {0}")]
    Meta(#[from] MetaError),
    #[error("failed to load catalogs: {0}")]
    Catalogs(#[from] CatalogError),
    #[error("failed to bind pgwire listener on {addr}: {source}")]
    Bind {
        addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },
}

/// A started (bound) engine process.
pub struct Server {
    pgwire: PgWireServer,
}

impl Server {
    /// Opens the metadata store, connects the stored catalogs and binds the listeners required by
    /// the configured role.
    pub async fn bind(config: &Config) -> Result<Self, ServerError> {
        match config.server.role {
            Role::Coordinator | Role::All => {}
            // Workers arrive with distributed execution (P0 milestone M2).
            Role::Worker => return Err(ServerError::UnsupportedRole(Role::Worker)),
        }
        let store: Arc<dyn CatalogStore> = match &config.meta.url {
            Some(url) => Arc::new(PgCatalogStore::connect(url).await?),
            None => {
                tracing::warn!(
                    "meta.url is not set; catalog definitions are kept in memory and lost on restart"
                );
                Arc::new(InMemoryCatalogStore::default())
            }
        };
        let secrets = SecretResolver::new(config.secrets.dir.clone());
        let catalogs = CatalogRegistry::load(store, secrets).await?;
        let engine = Arc::new(Engine::new(Arc::new(catalogs)));
        let addr = config.server.pgwire_listen;
        let pgwire = PgWireServer::bind(addr, engine)
            .await
            .map_err(|source| ServerError::Bind { addr, source })?;
        Ok(Self { pgwire })
    }

    /// The address the pgwire listener is bound to.
    pub fn pgwire_addr(&self) -> std::io::Result<SocketAddr> {
        self.pgwire.local_addr()
    }

    /// Serves until `shutdown` completes.
    pub async fn run(self, shutdown: impl Future<Output = ()>) {
        self.pgwire.serve(shutdown).await;
    }
}
```

- [ ] **Step 5: Run the end-to-end tests to verify they pass**

Run: `cargo test -p egret-engine-server -- --include-ignored`
Expected: `iceberg` tests `4 passed`, `server` `3 passed`, `cli` `3 passed`.

- [ ] **Step 6: Document Iceberg catalogs**

In `README.md`, replace the paragraph after the `EGRET_ENGINE__SERVER__PGWIRE_LISTEN` example ("M0 has no
authentication: ...") with:

```diff
diff --git a/README.md b/README.md
index 39e14be..d6bab53 100644
--- a/README.md
+++ b/README.md
@@ -37,8 +37,45 @@ pgwire_listen = "127.0.0.1:5432"
 EGRET_ENGINE__SERVER__PGWIRE_LISTEN=127.0.0.1:15432 cargo run -p egret-engine-server
 ```
 
-M0 has no authentication: every client that can reach the pgwire port gets full access, so keep it
-bound to localhost and do not expose it beyond the local machine.
+There is no authentication yet: every client that can reach the pgwire port gets full access, so
+keep it bound to localhost and do not expose it beyond the local machine.
+
+### Querying Iceberg tables
+
+Register an Iceberg REST catalog, then query its tables as `<catalog>.<namespace>.<table>`:
+
+```sql
+CREATE CATALOG lake USING iceberg_rest WITH (
+  uri = 'http://127.0.0.1:28181',
+  's3.endpoint' = 'http://127.0.0.1:29000',
+  's3.region' = 'us-east-1',
+  's3.path-style-access' = 'true',
+  's3.access-key-id' = 'admin',
+  's3.secret-access-key' = secret('lake-s3'));
+SHOW CATALOGS;
+SELECT count(*) FROM lake.fixtures.positional_deletes;
+DROP CATALOG lake;
+```
+
+- Options: `uri` (required), `warehouse`, `auth` (`none` or `oauth2`), `credential`, `token`, `scope`,
+  `oauth2_server_uri` and `access_delegation` (e.g. `'vended-credentials'`). Options whose name
+  contains a `.` are passed to the catalog client unchanged (e.g. `'s3.endpoint'`).
+- `secret('<name>')` reads the environment variable `EGRET_SECRET_<NAME>` (upper case, other
+  characters replaced by `_`), then the file `<secrets.dir>/<name>`. Only the reference is stored.
+- Catalog definitions are stored in PostgreSQL when `meta.url` is set, and kept in memory otherwise.
+- Connecting with a database name that matches a catalog (`psql -d lake`) makes it the default
+  catalog, so `SELECT * FROM fixtures.positional_deletes` works.
+
+```toml
+[meta]
+url = "postgres://egret:egret@127.0.0.1:25432/egret"
+
+[secrets]
+dir = "/etc/egret/secrets"
+```
+
+The example uses the test environment from [CONTRIBUTING.md](CONTRIBUTING.md#development); with it
+running, `EGRET_SECRET_LAKE_S3=password` provides the secret.
 
 ## Design
```

- [ ] **Step 7: Verify the M1a exit criterion manually**

With the environment up, run in one terminal:
`EGRET_SECRET_LAKE_S3=password EGRET_ENGINE__SERVER__PGWIRE_LISTEN=127.0.0.1:15432 cargo run -p egret-engine-server`.

Run in another terminal the `CREATE CATALOG lake ...` statement from the README via
`psql -h 127.0.0.1 -p 15432 -U demo`, then `SELECT count(*) FROM lake.fixtures.deletion_vectors;`.
Expected: `772`.

- [ ] **Step 8: Run all CI gates, including the integration tests**

Run: `cargo fmt --all -- --check && cargo clippy --workspace --all-targets --locked -- -D warnings && cargo test --workspace --locked -- --include-ignored`
Expected: clean, with 71 tests passing in total. Of these, 61 run without the environment: common 10, meta 3,
sql 6, catalog 13, coordinator 11, pgwire 1 + 11, server 3, cli 3. The other 10 need the environment: meta
postgres 3, iceberg rest_catalog 3, server iceberg 4.

- [ ] **Step 9: Commit and open the pull request**

```bash
git add crates/common crates/server README.md Cargo.lock
git commit -m "feat(server): persist catalogs in the metadata store and query Iceberg tables end to end"
git push -u origin feat/p0-m1a-catalogs
gh pr create -R egret-data/engine --base main --title "feat: P0/M1a Iceberg catalogs and single-node reads" \
  --body "Implements milestone M1a (metadata store, CREATE/DROP/SHOW CATALOG, Iceberg reads with delete files, test environment). Closes #16"
```

Merge with squash once both CI jobs are green.
