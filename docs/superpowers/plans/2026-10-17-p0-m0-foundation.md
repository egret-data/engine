# P0 / M0 Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Cargo workspace with CI and an `egret-engine` binary that serves SQL over the PostgreSQL wire protocol
(simple query protocol) from an in-memory DataFusion session per connection, so `psql -c "SELECT 1"` works.

**Architecture:** Three crates. `egret-engine-common` holds configuration (defaults → TOML file → `EGRET_ENGINE__*`
environment variables, via figment) and the `Role` enum. `egret-engine-pgwire` implements pgwire handlers: each
accepted TCP connection gets its own `SessionContext`; statements are parsed with DataFusion's `PostgreSqlDialect`,
planned and executed by DataFusion, and Arrow results are encoded to PostgreSQL text format with `arrow-pg`.
`egret-engine-server` binds listeners for the configured role and provides the `egret-engine` binary with graceful
shutdown on Ctrl-C/SIGTERM.

**Tech Stack:** Rust 1.99.0 (edition 2024), DataFusion 55.2, pgwire 0.41.1 (`server-api-ring`), arrow-pg (git rev
`94f7f4c4d8ca7a6ba2cea3f83420802827d11852`), tokio, figment 0.10, clap 4.6, thiserror 2, tracing; tests use
tokio-postgres 0.7.

**Spec:** [`docs/superpowers/specs/2026-10-17-egret-engine-p0-core-design.md`](../specs/2026-10-17-egret-engine-p0-core-design.md)
(§1.1, §1.3, §1.4, §4.1 simple query, §4.4 `42601`/`XX000`). **Roadmap:** [`2026-10-17-p0-roadmap.md`](2026-10-17-p0-roadmap.md).

## Global Constraints

- Package names `egret-engine-<x>` in `crates/<x>`; binary name `egret-engine`.
- Toolchain pinned in `rust-toolchain.toml` to `1.99.0`; edition `2024`; `rust-version = "1.99"`.
- License `Apache-2.0`; every crate sets `publish = false` (via `publish.workspace = true`).
- DataFusion `55.2.0`, pgwire `0.41.1` with `default-features = false, features = ["server-api-ring"]`.
- `arrow-pg` from git revision `94f7f4c4d8ca7a6ba2cea3f83420802827d11852` (crates.io 0.15.0 targets DataFusion 54).
- Environment override prefix `EGRET_ENGINE__`, nested keys separated by `__`.
- Defaults: `server.role = "all"`, `server.pgwire_listen = "127.0.0.1:5432"`.
- SQLSTATE: parse errors `42601`, everything else `XX000` (finer mapping arrives in later milestones).
- CI gates: `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`,
  `cargo test --workspace --locked`.
- Tests follow Arrange / Act / Assert. Commit messages follow Conventional Commits.
- Work happens on branch `feat/p0-m0-foundation` for issue `P0/M0: Foundation`; one commit per task; the PR uses
  `Closes #<issue>`.

## File Structure

| File | Responsibility |
|---|---|
| `Cargo.toml` | Workspace members, shared package metadata, pinned dependency versions, lints |
| `rust-toolchain.toml` | Pinned Rust toolchain |
| `.github/workflows/ci.yml` | fmt + clippy + test on pushes to `main` and on pull requests |
| `crates/common/src/lib.rs` | Public re-exports of the common crate |
| `crates/common/src/role.rs` | `Role` enum, `FromStr`/`Display`, `UnknownRole` |
| `crates/common/src/config.rs` | `Config`, `ServerConfig`, `ConfigError`, `Config::load` |
| `crates/pgwire/src/lib.rs` | Public API of the pgwire frontend |
| `crates/pgwire/src/error.rs` | DataFusion error → SQLSTATE / pgwire error |
| `crates/pgwire/src/handler.rs` | Per-connection handler: parse, plan, execute, encode |
| `crates/pgwire/src/server.rs` | `PgWireServer`: accept loop, per-connection session, shutdown |
| `crates/pgwire/tests/simple_query.rs` | End-to-end protocol tests with tokio-postgres |
| `crates/server/src/lib.rs` | `Server::bind` / `run` for the configured role |
| `crates/server/src/main.rs` | CLI (`--config`, `--role`), logging, signal handling |
| `crates/server/tests/server.rs` | In-process server lifecycle tests |
| `crates/server/tests/cli.rs` | Tests that run the compiled binary |
| `README.md` | "Getting started" section |

---

### Task 1: Workspace, toolchain, CI and `Role`

**Files:**
- Create: `Cargo.toml`, `rust-toolchain.toml`, `.github/workflows/ci.yml`
- Create: `crates/common/Cargo.toml`, `crates/common/src/lib.rs`, `crates/common/src/role.rs`
- Generated: `Cargo.lock` (commit it)

**Interfaces:**
- Consumes: nothing.
- Produces: `egret_engine_common::Role` (`Coordinator | Worker | All`; serde lowercase; `as_str()`, `Display`,
  `FromStr<Err = UnknownRole>`), `egret_engine_common::UnknownRole(pub String)`; workspace dependency table used by
  every later task.

- [ ] **Step 1: Open the issue and branch**

```bash
gh issue create -R egret-data/engine --title "P0/M0: Foundation" --label enhancement \
  --body "Implements milestone M0 of docs/superpowers/plans/2026-10-17-p0-roadmap.md following docs/superpowers/plans/2026-10-17-p0-m0-foundation.md."
git checkout main && git pull
git checkout -b feat/p0-m0-foundation
```

- [ ] **Step 2: Create the workspace manifest and toolchain file**

`Cargo.toml`:

```toml
[workspace]
resolver = "3"
members = ["crates/*"]

[workspace.package]
version = "0.1.0"
edition = "2024"
rust-version = "1.99"
license = "Apache-2.0"
repository = "https://github.com/egret-data/engine"
publish = false

[workspace.dependencies]
egret-engine-common = { path = "crates/common" }
egret-engine-pgwire = { path = "crates/pgwire" }

# arrow-pg is pinned to a git revision because the crates.io release (0.15.0)
# still targets DataFusion 54 / pgwire 0.40.
arrow-pg = { git = "https://github.com/datafusion-contrib/datafusion-postgres", rev = "94f7f4c4d8ca7a6ba2cea3f83420802827d11852", default-features = false, features = ["arrow", "datafusion"] }
async-trait = "0.1"
clap = { version = "4.6", features = ["derive"] }
datafusion = "55.2.0"
figment = { version = "0.10.19", features = ["toml", "env"] }
pgwire = { version = "0.41.1", default-features = false, features = ["server-api-ring"] }
serde = { version = "1", features = ["derive"] }
thiserror = "2"
tokio = { version = "1", features = ["macros", "net", "rt-multi-thread", "signal", "sync", "time"] }
tokio-postgres = "0.7.18"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }

[workspace.lints.rust]
unsafe_code = "forbid"

[workspace.lints.clippy]
all = { level = "warn", priority = -1 }
```

`rust-toolchain.toml`:

```toml
[toolchain]
channel = "1.99.0"
components = ["rustfmt", "clippy"]
profile = "minimal"
```

- [ ] **Step 3: Create the common crate with the failing `Role` tests**

`crates/common/Cargo.toml`:

```toml
[package]
name = "egret-engine-common"
description = "Shared configuration and types for Egret Engine"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true
repository.workspace = true
publish.workspace = true

[dependencies]
figment = { workspace = true }
serde = { workspace = true }
thiserror = { workspace = true }

[dev-dependencies]
figment = { workspace = true, features = ["test"] }

[lints]
workspace = true
```

`crates/common/src/lib.rs` (temporary — Task 2 adds the config module):

```rust
//! Shared configuration and types for Egret Engine.

mod role;

pub use role::{Role, UnknownRole};
```

`crates/common/src/role.rs` (tests only for now):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_known_roles() {
        // Arrange
        let inputs = ["coordinator", "worker", "all"];

        // Act
        let parsed: Vec<Role> = inputs.iter().map(|s| s.parse().unwrap()).collect();

        // Assert
        assert_eq!(parsed, vec![Role::Coordinator, Role::Worker, Role::All]);
    }

    #[test]
    fn display_round_trips_through_from_str() {
        // Arrange
        let roles = [Role::Coordinator, Role::Worker, Role::All];

        // Act
        let round_tripped: Vec<Role> = roles
            .iter()
            .map(|r| r.to_string().parse().unwrap())
            .collect();

        // Assert
        assert_eq!(round_tripped, roles);
    }

    #[test]
    fn rejects_unknown_role() {
        // Arrange
        let input = "leader";

        // Act
        let err = input.parse::<Role>().unwrap_err();

        // Assert
        assert_eq!(err, UnknownRole("leader".to_owned()));
        assert_eq!(
            err.to_string(),
            "unknown role `leader`; expected one of: coordinator, worker, all"
        );
    }
}
```

- [ ] **Step 4: Run the tests to verify they fail**

Run: `cargo test -p egret-engine-common`
Expected: compilation fails with `cannot find type `Role` in this scope` (and `UnknownRole`).

- [ ] **Step 5: Implement `Role`**

Replace `crates/common/src/role.rs` with the full file:

```rust
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// The role a process plays in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    /// Accepts client connections, plans queries and coordinates workers.
    Coordinator,
    /// Executes query stages on behalf of coordinators.
    Worker,
    /// Coordinator and worker in one process (development, single node).
    All,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Coordinator => "coordinator",
            Role::Worker => "worker",
            Role::All => "all",
        }
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Returned when parsing a string that is not a known [`Role`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown role `{0}`; expected one of: coordinator, worker, all")]
pub struct UnknownRole(pub String);

impl FromStr for Role {
    type Err = UnknownRole;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "coordinator" => Ok(Role::Coordinator),
            "worker" => Ok(Role::Worker),
            "all" => Ok(Role::All),
            other => Err(UnknownRole(other.to_owned())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_known_roles() {
        // Arrange
        let inputs = ["coordinator", "worker", "all"];

        // Act
        let parsed: Vec<Role> = inputs.iter().map(|s| s.parse().unwrap()).collect();

        // Assert
        assert_eq!(parsed, vec![Role::Coordinator, Role::Worker, Role::All]);
    }

    #[test]
    fn display_round_trips_through_from_str() {
        // Arrange
        let roles = [Role::Coordinator, Role::Worker, Role::All];

        // Act
        let round_tripped: Vec<Role> = roles
            .iter()
            .map(|r| r.to_string().parse().unwrap())
            .collect();

        // Assert
        assert_eq!(round_tripped, roles);
    }

    #[test]
    fn rejects_unknown_role() {
        // Arrange
        let input = "leader";

        // Act
        let err = input.parse::<Role>().unwrap_err();

        // Assert
        assert_eq!(err, UnknownRole("leader".to_owned()));
        assert_eq!(
            err.to_string(),
            "unknown role `leader`; expected one of: coordinator, worker, all"
        );
    }
}
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cargo test -p egret-engine-common`
Expected: `test result: ok. 3 passed`.

- [ ] **Step 7: Add CI**

`.github/workflows/ci.yml`:

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:

concurrency:
  group: ci-${{ github.ref }}
  cancel-in-progress: true

env:
  CARGO_TERM_COLOR: always

jobs:
  check:
    name: fmt, clippy, test
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions-rust-lang/setup-rust-toolchain@v1
        with:
          components: rustfmt, clippy
      - name: Format
        run: cargo fmt --all -- --check
      - name: Clippy
        run: cargo clippy --workspace --all-targets --locked -- -D warnings
      - name: Test
        run: cargo test --workspace --locked
```

- [ ] **Step 8: Run the CI gates locally**

Run: `cargo fmt --all -- --check && cargo clippy --workspace --all-targets --locked -- -D warnings && cargo test --workspace --locked`
Expected: no formatting diff, no clippy warnings, all tests pass. (`--locked` needs `Cargo.lock`; run
`cargo generate-lockfile` first if it does not exist yet.)

- [ ] **Step 9: Commit**

```bash
git add Cargo.toml Cargo.lock rust-toolchain.toml .github/workflows/ci.yml crates/common
git commit -m "build: scaffold cargo workspace, CI and common Role type"
```

---

### Task 2: Configuration loading

**Files:**
- Create: `crates/common/src/config.rs`
- Modify: `crates/common/src/lib.rs`

**Interfaces:**
- Consumes: `Role` (Task 1).
- Produces: `egret_engine_common::{Config, ServerConfig, ConfigError, ENV_PREFIX}`.
  - `pub struct Config { pub server: ServerConfig }` (`Default`, `Clone`, `PartialEq`, serde, `deny_unknown_fields`)
  - `pub struct ServerConfig { pub role: Role, pub pgwire_listen: std::net::SocketAddr }`
  - `pub fn Config::load(path: Option<&std::path::Path>) -> Result<Config, ConfigError>`
  - `pub enum ConfigError { FileNotFound(PathBuf), Invalid(Box<figment::Error>) }`
  - `pub const ENV_PREFIX: &str = "EGRET_ENGINE__"`

- [ ] **Step 1: Write the failing tests**

Update `crates/common/src/lib.rs`:

```rust
//! Shared configuration and types for Egret Engine.

mod config;
mod role;

pub use config::{Config, ConfigError, ENV_PREFIX, ServerConfig};
pub use role::{Role, UnknownRole};
```

Create `crates/common/src/config.rs` containing only the test module for now:

```rust
use std::path::Path;

use crate::Role;

// `figment::Jail` closures must return `figment::Result`, whose error type is large.
#[cfg(test)]
#[allow(clippy::result_large_err)]
mod tests {
    use figment::Jail;

    use super::*;

    #[test]
    fn defaults_when_no_file_and_no_env() {
        Jail::expect_with(|_jail| {
            // Act
            let config = Config::load(None).expect("defaults load");

            // Assert
            assert_eq!(config, Config::default());
            assert_eq!(config.server.role, Role::All);
            assert_eq!(
                config.server.pgwire_listen,
                "127.0.0.1:5432".parse().unwrap()
            );
            Ok(())
        });
    }

    #[test]
    fn file_overrides_defaults() {
        Jail::expect_with(|jail| {
            // Arrange
            jail.create_file(
                "engine.toml",
                "[server]\nrole = \"coordinator\"\npgwire_listen = \"0.0.0.0:15432\"\n",
            )?;

            // Act
            let config = Config::load(Some(Path::new("engine.toml"))).expect("file loads");

            // Assert
            assert_eq!(config.server.role, Role::Coordinator);
            assert_eq!(
                config.server.pgwire_listen,
                "0.0.0.0:15432".parse().unwrap()
            );
            Ok(())
        });
    }

    #[test]
    fn env_overrides_file() {
        Jail::expect_with(|jail| {
            // Arrange
            jail.create_file("engine.toml", "[server]\nrole = \"coordinator\"\n")?;
            jail.set_env("EGRET_ENGINE__SERVER__ROLE", "worker");

            // Act
            let config = Config::load(Some(Path::new("engine.toml"))).expect("env loads");

            // Assert
            assert_eq!(config.server.role, Role::Worker);
            Ok(())
        });
    }

    #[test]
    fn missing_file_is_an_error() {
        Jail::expect_with(|_jail| {
            // Act
            let err = Config::load(Some(Path::new("missing.toml"))).unwrap_err();

            // Assert
            assert!(matches!(err, ConfigError::FileNotFound(p) if p == Path::new("missing.toml")));
            Ok(())
        });
    }

    #[test]
    fn unknown_key_is_an_error() {
        Jail::expect_with(|jail| {
            // Arrange
            jail.create_file("engine.toml", "[server]\nrole = \"all\"\nport = 1\n")?;

            // Act
            let err = Config::load(Some(Path::new("engine.toml"))).unwrap_err();

            // Assert
            assert!(
                err.to_string().contains("unknown field: found `port`"),
                "{err}"
            );
            Ok(())
        });
    }

    #[test]
    fn invalid_role_is_an_error() {
        Jail::expect_with(|jail| {
            // Arrange
            jail.set_env("EGRET_ENGINE__SERVER__ROLE", "leader");

            // Act
            let err = Config::load(None).unwrap_err();

            // Assert
            assert!(err.to_string().contains("unknown variant"), "{err}");
            Ok(())
        });
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p egret-engine-common`
Expected: compilation fails (`cannot find type `Config``, unresolved imports `ConfigError`, `ServerConfig`,
`ENV_PREFIX`).

- [ ] **Step 3: Implement configuration loading**

Replace `crates/common/src/config.rs` with the full file:

```rust
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};

use figment::Figment;
use figment::providers::{Env, Format, Serialized, Toml};
use serde::{Deserialize, Serialize};

use crate::Role;

/// Prefix of environment variables that override configuration values.
/// Nested keys are separated by `__`, e.g. `EGRET_ENGINE__SERVER__PGWIRE_LISTEN`.
pub const ENV_PREFIX: &str = "EGRET_ENGINE__";

/// Top-level engine configuration.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub server: ServerConfig,
}

/// Process-level settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    /// Role of this process.
    pub role: Role,
    /// Address the PostgreSQL wire protocol listener binds to.
    pub pgwire_listen: SocketAddr,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            role: Role::All,
            pgwire_listen: SocketAddr::from((Ipv4Addr::LOCALHOST, 5432)),
        }
    }
}

/// Errors raised while loading configuration.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("configuration file not found: {}", .0.display())]
    FileNotFound(PathBuf),
    #[error("invalid configuration: {0}")]
    Invalid(Box<figment::Error>),
}

impl Config {
    /// Loads configuration with increasing precedence: built-in defaults, the optional TOML file,
    /// then `EGRET_ENGINE__*` environment variables.
    pub fn load(path: Option<&Path>) -> Result<Self, ConfigError> {
        let mut figment = Figment::from(Serialized::defaults(Config::default()));
        if let Some(path) = path {
            if !path.is_file() {
                return Err(ConfigError::FileNotFound(path.to_path_buf()));
            }
            figment = figment.merge(Toml::file_exact(path));
        }
        figment
            .merge(Env::prefixed(ENV_PREFIX).split("__"))
            .extract()
            .map_err(|e| ConfigError::Invalid(Box::new(e)))
    }
}

// `figment::Jail` closures must return `figment::Result`, whose error type is large.
#[cfg(test)]
#[allow(clippy::result_large_err)]
mod tests {
    use figment::Jail;

    use super::*;

    #[test]
    fn defaults_when_no_file_and_no_env() {
        Jail::expect_with(|_jail| {
            // Act
            let config = Config::load(None).expect("defaults load");

            // Assert
            assert_eq!(config, Config::default());
            assert_eq!(config.server.role, Role::All);
            assert_eq!(
                config.server.pgwire_listen,
                "127.0.0.1:5432".parse().unwrap()
            );
            Ok(())
        });
    }

    #[test]
    fn file_overrides_defaults() {
        Jail::expect_with(|jail| {
            // Arrange
            jail.create_file(
                "engine.toml",
                "[server]\nrole = \"coordinator\"\npgwire_listen = \"0.0.0.0:15432\"\n",
            )?;

            // Act
            let config = Config::load(Some(Path::new("engine.toml"))).expect("file loads");

            // Assert
            assert_eq!(config.server.role, Role::Coordinator);
            assert_eq!(
                config.server.pgwire_listen,
                "0.0.0.0:15432".parse().unwrap()
            );
            Ok(())
        });
    }

    #[test]
    fn env_overrides_file() {
        Jail::expect_with(|jail| {
            // Arrange
            jail.create_file("engine.toml", "[server]\nrole = \"coordinator\"\n")?;
            jail.set_env("EGRET_ENGINE__SERVER__ROLE", "worker");

            // Act
            let config = Config::load(Some(Path::new("engine.toml"))).expect("env loads");

            // Assert
            assert_eq!(config.server.role, Role::Worker);
            Ok(())
        });
    }

    #[test]
    fn missing_file_is_an_error() {
        Jail::expect_with(|_jail| {
            // Act
            let err = Config::load(Some(Path::new("missing.toml"))).unwrap_err();

            // Assert
            assert!(matches!(err, ConfigError::FileNotFound(p) if p == Path::new("missing.toml")));
            Ok(())
        });
    }

    #[test]
    fn unknown_key_is_an_error() {
        Jail::expect_with(|jail| {
            // Arrange
            jail.create_file("engine.toml", "[server]\nrole = \"all\"\nport = 1\n")?;

            // Act
            let err = Config::load(Some(Path::new("engine.toml"))).unwrap_err();

            // Assert
            assert!(
                err.to_string().contains("unknown field: found `port`"),
                "{err}"
            );
            Ok(())
        });
    }

    #[test]
    fn invalid_role_is_an_error() {
        Jail::expect_with(|jail| {
            // Arrange
            jail.set_env("EGRET_ENGINE__SERVER__ROLE", "leader");

            // Act
            let err = Config::load(None).unwrap_err();

            // Assert
            assert!(err.to_string().contains("unknown variant"), "{err}");
            Ok(())
        });
    }
}
```

Notes: `Toml::file_exact` is used together with the explicit `is_file()` check because `Toml::file` silently ignores
a missing file. `deny_unknown_fields` turns typos in configuration into startup errors.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p egret-engine-common`
Expected: `test result: ok. 9 passed`.

- [ ] **Step 5: Lint and commit**

Run: `cargo fmt --all -- --check && cargo clippy --workspace --all-targets --locked -- -D warnings`
Expected: clean.

```bash
git add crates/common
git commit -m "feat(common): load configuration from defaults, TOML file and environment"
```

---

### Task 3: pgwire frontend with per-connection DataFusion sessions

**Files:**
- Create: `crates/pgwire/Cargo.toml`, `crates/pgwire/src/lib.rs`, `crates/pgwire/src/error.rs`,
  `crates/pgwire/src/handler.rs`, `crates/pgwire/src/server.rs`
- Test: `crates/pgwire/tests/simple_query.rs`

**Interfaces:**
- Consumes: nothing from earlier tasks (independent of configuration).
- Produces:
  - `pub type SessionFactory = Arc<dyn Fn() -> datafusion::prelude::SessionContext + Send + Sync>`
  - `pub async fn PgWireServer::bind(addr: SocketAddr, sessions: SessionFactory) -> std::io::Result<PgWireServer>`
  - `pub fn PgWireServer::local_addr(&self) -> std::io::Result<SocketAddr>`
  - `pub async fn PgWireServer::serve(self, shutdown: impl Future<Output = ()>)`
  - `pub fn sqlstate(&DataFusionError) -> &'static str`, `pub fn to_pg_error(DataFusionError) -> PgWireError`,
    `pub const SYNTAX_ERROR: &str = "42601"`, `pub const INTERNAL_ERROR: &str = "XX000"`

- [ ] **Step 1: Create the crate and the failing error-mapping tests**

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
datafusion = { workspace = true }
pgwire = { workspace = true }
tokio = { workspace = true }
tracing = { workspace = true }

[dev-dependencies]
tokio-postgres = { workspace = true }

[lints]
workspace = true
```

`crates/pgwire/src/lib.rs` (temporary — Step 5 adds the handler and server):

```rust
//! PostgreSQL wire protocol frontend for Egret Engine.

mod error;

pub use error::{INTERNAL_ERROR, SYNTAX_ERROR, sqlstate, to_pg_error};
```

`crates/pgwire/src/error.rs` (imports + tests only for now):

```rust
use datafusion::error::DataFusionError;
use pgwire::error::PgWireError;

#[cfg(test)]
mod tests {
    use datafusion::sql::parser::DFParser;

    use super::*;

    fn parse_error() -> DataFusionError {
        DFParser::parse_sql("SELEC 1").unwrap_err()
    }

    #[test]
    fn syntax_error_maps_to_42601() {
        // Arrange
        let error = parse_error();

        // Act
        let code = sqlstate(&error);

        // Assert
        assert_eq!(code, SYNTAX_ERROR);
    }

    #[test]
    fn wrapped_syntax_error_maps_to_42601() {
        // Arrange
        let error = DataFusionError::Context("while parsing".to_owned(), Box::new(parse_error()));

        // Act
        let code = sqlstate(&error);

        // Assert
        assert_eq!(code, SYNTAX_ERROR);
    }

    #[test]
    fn other_errors_map_to_internal_error() {
        // Arrange
        let error = DataFusionError::Plan("table 't' not found".to_owned());

        // Act
        let code = sqlstate(&error);

        // Assert
        assert_eq!(code, INTERNAL_ERROR);
    }

    #[test]
    fn to_pg_error_builds_user_error() {
        // Arrange
        let error = DataFusionError::Execution("boom".to_owned());

        // Act
        let pg_error = to_pg_error(error);

        // Assert
        match pg_error {
            PgWireError::UserError(info) => {
                assert_eq!(info.severity, "ERROR");
                assert_eq!(info.code, INTERNAL_ERROR);
                assert!(info.message.contains("boom"), "{}", info.message);
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p egret-engine-pgwire --lib`
Expected: compilation fails (`cannot find function `sqlstate``, `SYNTAX_ERROR`, `to_pg_error`). The first build
compiles DataFusion and takes several minutes.

- [ ] **Step 3: Implement the error mapping**

Replace `crates/pgwire/src/error.rs` with:

```rust
use datafusion::error::DataFusionError;
use pgwire::error::{ErrorInfo, PgWireError};

/// SQLSTATE `42601`: syntax error.
pub const SYNTAX_ERROR: &str = "42601";
/// SQLSTATE `XX000`: internal error.
pub const INTERNAL_ERROR: &str = "XX000";

/// Maps a DataFusion error to the SQLSTATE reported to the client.
pub fn sqlstate(error: &DataFusionError) -> &'static str {
    match error.find_root() {
        DataFusionError::SQL(..) => SYNTAX_ERROR,
        _ => INTERNAL_ERROR,
    }
}

/// Converts a DataFusion error into an `ERROR` response for the client.
pub fn to_pg_error(error: DataFusionError) -> PgWireError {
    let code = sqlstate(&error);
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        error.to_string(),
    )))
}

#[cfg(test)]
mod tests {
    use datafusion::sql::parser::DFParser;

    use super::*;

    fn parse_error() -> DataFusionError {
        DFParser::parse_sql("SELEC 1").unwrap_err()
    }

    #[test]
    fn syntax_error_maps_to_42601() {
        // Arrange
        let error = parse_error();

        // Act
        let code = sqlstate(&error);

        // Assert
        assert_eq!(code, SYNTAX_ERROR);
    }

    #[test]
    fn wrapped_syntax_error_maps_to_42601() {
        // Arrange
        let error = DataFusionError::Context("while parsing".to_owned(), Box::new(parse_error()));

        // Act
        let code = sqlstate(&error);

        // Assert
        assert_eq!(code, SYNTAX_ERROR);
    }

    #[test]
    fn other_errors_map_to_internal_error() {
        // Arrange
        let error = DataFusionError::Plan("table 't' not found".to_owned());

        // Act
        let code = sqlstate(&error);

        // Assert
        assert_eq!(code, INTERNAL_ERROR);
    }

    #[test]
    fn to_pg_error_builds_user_error() {
        // Arrange
        let error = DataFusionError::Execution("boom".to_owned());

        // Act
        let pg_error = to_pg_error(error);

        // Assert
        match pg_error {
            PgWireError::UserError(info) => {
                assert_eq!(info.severity, "ERROR");
                assert_eq!(info.code, INTERNAL_ERROR);
                assert!(info.message.contains("boom"), "{}", info.message);
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }
}
```

- [ ] **Step 4: Run the unit tests to verify they pass**

Run: `cargo test -p egret-engine-pgwire --lib`
Expected: `test result: ok. 4 passed`.

- [ ] **Step 5: Write the failing protocol tests**

Update `crates/pgwire/src/lib.rs` to its final form:

```rust
//! PostgreSQL wire protocol frontend for Egret Engine.

mod error;
mod handler;
mod server;

pub use error::{INTERNAL_ERROR, SYNTAX_ERROR, sqlstate, to_pg_error};
pub use server::{PgWireServer, SessionFactory};
```

Create `crates/pgwire/tests/simple_query.rs`:

```rust
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use egret_engine_pgwire::{PgWireServer, SYNTAX_ERROR, SessionFactory};
use tokio::sync::oneshot;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

/// A server on a random local port that stops when dropped.
struct TestServer {
    port: u16,
    _shutdown: oneshot::Sender<()>,
}

async fn start_server() -> TestServer {
    let sessions: SessionFactory = Arc::new(SessionContext::new);
    let server = PgWireServer::bind("127.0.0.1:0".parse().unwrap(), sessions)
        .await
        .expect("bind");
    let port = server.local_addr().expect("local addr").port();
    let (tx, rx) = oneshot::channel::<()>();
    tokio::spawn(server.serve(async {
        let _ = rx.await;
    }));
    TestServer {
        port,
        _shutdown: tx,
    }
}

async fn connect(server: &TestServer) -> Client {
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=test", server.port),
        NoTls,
    )
    .await
    .expect("connect");
    tokio::spawn(connection);
    client
}

/// Text values of every data row, in order.
fn rows(messages: &[SimpleQueryMessage]) -> Vec<Vec<Option<String>>> {
    messages
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

fn s(value: &str) -> Option<String> {
    Some(value.to_owned())
}

#[tokio::test]
async fn select_returns_rows() {
    // Arrange
    let server = start_server().await;
    let client = connect(&server).await;

    // Act
    let messages = client
        .simple_query("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name) ORDER BY id")
        .await
        .expect("query");

    // Assert
    assert_eq!(
        rows(&messages),
        vec![vec![s("1"), s("a")], vec![s("2"), s("b")]]
    );
}

#[tokio::test]
async fn multiple_statements_run_in_order() {
    // Arrange
    let server = start_server().await;
    let client = connect(&server).await;

    // Act
    let messages = client
        .simple_query("SELECT 1 AS one; SELECT 'two' AS two")
        .await
        .expect("query");

    // Assert
    assert_eq!(rows(&messages), vec![vec![s("1")], vec![s("two")]]);
}

#[tokio::test]
async fn empty_query_succeeds() {
    // Arrange
    let server = start_server().await;
    let client = connect(&server).await;

    // Act
    let messages = client.simple_query("").await.expect("query");

    // Assert
    assert!(rows(&messages).is_empty());
}

#[tokio::test]
async fn values_use_postgres_text_format() {
    // Arrange
    let server = start_server().await;
    let client = connect(&server).await;

    // Act
    let messages = client
        .simple_query(
            "SELECT CAST(1.5 AS DECIMAL(10,2)), DATE '2026-01-02', true, CAST(NULL AS INT), \
             TIMESTAMP '2026-01-02 03:04:05', CAST(42 AS BIGINT)",
        )
        .await
        .expect("query");

    // Assert
    assert_eq!(
        rows(&messages),
        vec![vec![
            s("1.50"),
            s("2026-01-02"),
            s("t"),
            None,
            s("2026-01-02 03:04:05.000000"),
            s("42"),
        ]]
    );
}

#[tokio::test]
async fn syntax_error_reports_42601() {
    // Arrange
    let server = start_server().await;
    let client = connect(&server).await;

    // Act
    let error = client.simple_query("SELEC 1").await.unwrap_err();

    // Assert
    assert_eq!(error.code().expect("sqlstate").code(), SYNTAX_ERROR);
}

#[tokio::test]
async fn sessions_are_isolated_per_connection() {
    // Arrange
    let server = start_server().await;
    let first = connect(&server).await;
    let second = connect(&server).await;
    first
        .simple_query("CREATE TABLE t AS VALUES (1), (2)")
        .await
        .expect("create table");

    // Act
    let own = first
        .simple_query("SELECT count(*) FROM t")
        .await
        .expect("own table");
    let other = second.simple_query("SELECT count(*) FROM t").await;

    // Assert
    assert_eq!(rows(&own), vec![vec![s("2")]]);
    let error = other.unwrap_err();
    assert!(
        error
            .as_db_error()
            .expect("db error")
            .message()
            .contains("not found"),
        "{error}"
    );
}
```

- [ ] **Step 6: Run the tests to verify they fail**

Run: `cargo test -p egret-engine-pgwire --test simple_query`
Expected: compilation fails (`file not found for module `handler`` / `server`).

- [ ] **Step 7: Implement the connection handler**

`crates/pgwire/src/handler.rs`:

```rust
use std::sync::Arc;

use arrow_pg::datatypes::df::encode_dataframe;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::{DFParser, Statement};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use pgwire::api::auth::StartupHandler;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::portal::Format;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{Response, Tag};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, PgWireServerHandlers};
use pgwire::error::PgWireResult;

use crate::error::to_pg_error;

/// Serves one client connection. Owns that connection's DataFusion session, so session state
/// (settings, temporary tables) is never shared between connections.
pub(crate) struct ConnectionHandler {
    ctx: SessionContext,
}

impl ConnectionHandler {
    pub(crate) fn new(ctx: SessionContext) -> Self {
        Self { ctx }
    }

    async fn execute(&self, statement: Statement) -> PgWireResult<Response> {
        let plan = self
            .ctx
            .state()
            .statement_to_plan(statement)
            .await
            .map_err(to_pg_error)?;
        let df = self
            .ctx
            .execute_logical_plan(plan)
            .await
            .map_err(to_pg_error)?;
        // Statements without output columns (DDL, SET) have already run inside
        // `execute_logical_plan`.
        if df.schema().fields().is_empty() {
            return Ok(Response::Execution(Tag::new("OK")));
        }
        let response = encode_dataframe(df, &Format::UnifiedText, None).await?;
        Ok(Response::Query(response))
    }
}

// Authentication is added in M4; until then every startup is accepted.
impl NoopStartupHandler for ConnectionHandler {}

#[async_trait]
impl SimpleQueryHandler for ConnectionHandler {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
    {
        let statements =
            DFParser::parse_sql_with_dialect(query, &PostgreSqlDialect {}).map_err(to_pg_error)?;
        if statements.is_empty() {
            return Ok(vec![Response::EmptyQuery]);
        }
        let mut responses = Vec::with_capacity(statements.len());
        for statement in statements {
            responses.push(self.execute(statement).await?);
        }
        Ok(responses)
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

Notes: `DFParser::parse_sql_with_dialect` already returns `DataFusionError` (a `DataFusionError::SQL` for syntax
errors), so `to_pg_error` maps it to `42601`. `encode_dataframe` streams batches and encodes rows lazily; it requires
the `datafusion` feature of `arrow-pg`.

- [ ] **Step 8: Implement the server**

`crates/pgwire/src/server.rs`:

```rust
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use datafusion::prelude::SessionContext;
use tokio::net::TcpListener;

use crate::handler::{ConnectionHandler, Handlers};

/// Creates a fresh DataFusion session for each new client connection.
pub type SessionFactory = Arc<dyn Fn() -> SessionContext + Send + Sync>;

/// Back-off after a failed `accept`, e.g. when the process is out of file descriptors.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);

/// Accepts PostgreSQL wire protocol connections and serves each one with its own session.
pub struct PgWireServer {
    listener: TcpListener,
    sessions: SessionFactory,
}

impl PgWireServer {
    /// Binds the listener. Use port `0` to let the OS pick a free port.
    pub async fn bind(addr: SocketAddr, sessions: SessionFactory) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self { listener, sessions })
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
                            (self.sessions)(),
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

- [ ] **Step 9: Run all pgwire tests to verify they pass**

Run: `cargo test -p egret-engine-pgwire`
Expected: unit tests `4 passed`; `simple_query` tests `6 passed` (`select_returns_rows`,
`multiple_statements_run_in_order`, `empty_query_succeeds`, `values_use_postgres_text_format`,
`syntax_error_reports_42601`, `sessions_are_isolated_per_connection`).

- [ ] **Step 10: Lint and commit**

Run: `cargo fmt --all -- --check && cargo clippy --workspace --all-targets --locked -- -D warnings`
Expected: clean.

```bash
git add Cargo.lock crates/pgwire
git commit -m "feat(pgwire): serve simple queries over pgwire with per-connection DataFusion sessions"
```

---

### Task 4: `egret-engine` binary

**Files:**
- Create: `crates/server/Cargo.toml`, `crates/server/src/lib.rs`, `crates/server/src/main.rs`
- Test: `crates/server/tests/server.rs`, `crates/server/tests/cli.rs`
- Modify: `README.md`

**Interfaces:**
- Consumes: `Config`, `Role` (Tasks 1–2); `PgWireServer`, `SessionFactory` (Task 3).
- Produces:
  - `pub async fn Server::bind(config: &Config) -> Result<Server, ServerError>`
  - `pub fn Server::pgwire_addr(&self) -> std::io::Result<SocketAddr>`
  - `pub async fn Server::run(self, shutdown: impl Future<Output = ()>)`
  - `pub enum ServerError { UnsupportedRole(Role), Bind { addr: SocketAddr, source: std::io::Error } }`
  - Binary `egret-engine [--config <path>] [--role coordinator|worker|all]`

- [ ] **Step 1: Create the crate with the failing in-process tests**

`crates/server/Cargo.toml`:

```toml
[package]
name = "egret-engine-server"
description = "The egret-engine binary"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true
repository.workspace = true
publish.workspace = true

[[bin]]
name = "egret-engine"
path = "src/main.rs"

[dependencies]
clap = { workspace = true }
datafusion = { workspace = true }
egret-engine-common = { workspace = true }
egret-engine-pgwire = { workspace = true }
thiserror = { workspace = true }
tokio = { workspace = true }
tracing = { workspace = true }
tracing-subscriber = { workspace = true }

[dev-dependencies]
tokio-postgres = { workspace = true }

[lints]
workspace = true
```

`crates/server/tests/server.rs`:

```rust
use egret_engine_common::{Config, Role};
use egret_engine_server::{Server, ServerError};
use tokio::sync::oneshot;
use tokio_postgres::{NoTls, SimpleQueryMessage};

fn config(role: Role) -> Config {
    let mut config = Config::default();
    config.server.role = role;
    config.server.pgwire_listen = "127.0.0.1:0".parse().unwrap();
    config
}

#[tokio::test]
async fn all_role_serves_pgwire_until_shutdown() {
    // Arrange
    let server = Server::bind(&config(Role::All)).await.expect("bind");
    let port = server.pgwire_addr().expect("addr").port();
    let (stop, stopped) = oneshot::channel::<()>();
    let running = tokio::spawn(server.run(async {
        let _ = stopped.await;
    }));
    let (client, connection) =
        tokio_postgres::connect(&format!("host=127.0.0.1 port={port} user=test"), NoTls)
            .await
            .expect("connect");
    tokio::spawn(connection);

    // Act
    let messages = client.simple_query("SELECT 1").await.expect("query");
    stop.send(()).expect("server still running");
    running.await.expect("server task");

    // Assert
    assert!(
        messages
            .iter()
            .any(|m| matches!(m, SimpleQueryMessage::Row(row) if row.get(0) == Some("1")))
    );
}

#[tokio::test]
async fn worker_role_is_not_supported_yet() {
    // Act
    let result = Server::bind(&config(Role::Worker)).await;

    // Assert
    assert!(matches!(
        result,
        Err(ServerError::UnsupportedRole(Role::Worker))
    ));
}

#[tokio::test]
async fn bind_error_names_the_address() {
    // Arrange
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").expect("occupy port");
    let mut config = config(Role::All);
    config.server.pgwire_listen = occupied.local_addr().unwrap();

    // Act
    let result = Server::bind(&config).await;

    // Assert
    let error = result.err().expect("bind must fail");
    assert!(matches!(error, ServerError::Bind { .. }));
    assert!(
        error
            .to_string()
            .contains(&config.server.pgwire_listen.to_string())
    );
}
```

Create a placeholder binary so the package compiles — `crates/server/src/main.rs`:

```rust
fn main() {}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p egret-engine-server --test server`
Expected: compilation fails (no library target `egret_engine_server`, `Server`/`ServerError` unresolved).

- [ ] **Step 3: Implement `Server`**

`crates/server/src/lib.rs`:

```rust
//! Wires configuration to the engine's services for the `egret-engine` binary.

use std::net::SocketAddr;
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use egret_engine_common::{Config, Role};
use egret_engine_pgwire::{PgWireServer, SessionFactory};

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
        let sessions: SessionFactory = Arc::new(SessionContext::new);
        let addr = config.server.pgwire_listen;
        let pgwire = PgWireServer::bind(addr, sessions)
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

`Role::Worker` is rejected on purpose: worker processes arrive with distributed execution in milestone M2.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p egret-engine-server --test server`
Expected: `test result: ok. 3 passed`.

- [ ] **Step 5: Write the failing binary tests**

`crates/server/tests/cli.rs`:

```rust
use std::process::{Command, Stdio};
use std::time::Duration;

use tokio_postgres::{NoTls, SimpleQueryMessage};

const BINARY: &str = env!("CARGO_BIN_EXE_egret-engine");

#[test]
fn help_lists_options() {
    // Act
    let output = Command::new(BINARY).arg("--help").output().expect("run");

    // Assert
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("--config"), "{stdout}");
    assert!(stdout.contains("--role"), "{stdout}");
}

#[test]
fn missing_config_file_fails() {
    // Act
    let output = Command::new(BINARY)
        .args(["--config", "does-not-exist.toml"])
        .output()
        .expect("run");

    // Assert
    assert!(!output.status.success());
}

#[cfg(unix)]
#[tokio::test]
async fn binary_serves_queries_and_stops_on_sigterm() {
    // Arrange: reserve a free port, then hand it to the binary.
    let port = {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        probe.local_addr().unwrap().port()
    };
    let mut child = Command::new(BINARY)
        .env(
            "EGRET_ENGINE__SERVER__PGWIRE_LISTEN",
            format!("127.0.0.1:{port}"),
        )
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn");
    let dsn = format!("host=127.0.0.1 port={port} user=test");
    let mut attempt = 0;
    let client = loop {
        match tokio_postgres::connect(&dsn, NoTls).await {
            Ok((client, connection)) => {
                tokio::spawn(connection);
                break client;
            }
            Err(_) if attempt < 100 => {
                attempt += 1;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => panic!("server did not start: {error}"),
        }
    };

    // Act
    let messages = client.simple_query("SELECT 41 + 1").await.expect("query");
    let killed = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .expect("send SIGTERM");
    let status = child.wait().expect("wait");

    // Assert
    assert!(
        messages
            .iter()
            .any(|m| matches!(m, SimpleQueryMessage::Row(row) if row.get(0) == Some("42")))
    );
    assert!(killed.success());
    assert!(status.success(), "exit status: {status}");
}
```

- [ ] **Step 6: Run the tests to verify they fail**

Run: `cargo test -p egret-engine-server --test cli`
Expected: `help_lists_options` and `missing_config_file_fails` fail (the placeholder binary prints nothing and
exits successfully) and `binary_serves_queries_and_stops_on_sigterm` panics with `server did not start` after about
10 seconds.

- [ ] **Step 7: Implement the CLI**

Replace `crates/server/src/main.rs` with:

```rust
use std::error::Error;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use egret_engine_common::{Config, Role};
use egret_engine_server::Server;
use tracing_subscriber::EnvFilter;

/// Egret Engine: a distributed SQL engine for the lakehouse.
#[derive(Debug, Parser)]
#[command(name = "egret-engine", version, about)]
struct Cli {
    /// Path to a TOML configuration file.
    #[arg(long, short)]
    config: Option<PathBuf>,
    /// Override `server.role` (coordinator, worker, all).
    #[arg(long)]
    role: Option<Role>,
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    match run(Cli::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!("{error}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<(), Box<dyn Error>> {
    let mut config = Config::load(cli.config.as_deref())?;
    if let Some(role) = cli.role {
        config.server.role = role;
    }
    let server = Server::bind(&config).await?;
    tracing::info!(role = %config.server.role, pgwire = %server.pgwire_addr()?, "egret-engine started");
    server.run(shutdown_signal()).await;
    tracing::info!("egret-engine stopped");
    Ok(())
}

/// Completes on Ctrl-C or, on Unix, SIGTERM.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut sigterm) => {
                sigterm.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
}
```

- [ ] **Step 8: Run the tests to verify they pass**

Run: `cargo test -p egret-engine-server`
Expected: `server` tests `3 passed`, `cli` tests `3 passed` (the SIGTERM test only runs on Unix).

- [ ] **Step 9: Document how to run it**

In `README.md`, replace the line

```markdown
> **Status:** early design phase. Nothing is usable yet. See the design documents below.
```

with

````markdown
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
EGRET_ENGINE__SERVER__PGWIRE_LISTEN=0.0.0.0:15432 cargo run -p egret-engine-server
```
````

- [ ] **Step 10: Verify the M0 exit criterion manually**

Run in one terminal: `cargo run -p egret-engine-server -- --role all`
Run in another: `psql -h 127.0.0.1 -p 5432 -U demo -c "SELECT 1 AS one, 'hello' AS greeting"`
Expected psql output:

```text
 one | greeting
-----+----------
   1 | hello
(1 row)
```

Stop the server with Ctrl-C; the log ends with `egret-engine stopped`.

- [ ] **Step 11: Run the CI gates**

Run: `cargo fmt --all -- --check && cargo clippy --workspace --all-targets --locked -- -D warnings && cargo test --workspace --locked`
Expected: clean; 25 tests pass in total (9 common, 4 pgwire unit, 6 pgwire protocol, 3 server, 3 cli).

- [ ] **Step 12: Commit and open the pull request**

```bash
git add Cargo.lock crates/server README.md
git commit -m "feat(server): add egret-engine binary with config, role selection and graceful shutdown"
git push -u origin feat/p0-m0-foundation
gh pr create -R egret-data/engine --base main --title "feat: P0/M0 foundation" \
  --body "Implements milestone M0 (workspace, CI, configuration, pgwire simple-query frontend, egret-engine binary). Closes #<issue>"
```

Merge with squash once CI is green.

