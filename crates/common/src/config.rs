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
