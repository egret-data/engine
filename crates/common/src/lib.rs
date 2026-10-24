//! Shared configuration and types for Egret Engine.

mod config;
mod role;

pub use config::{Config, ConfigError, ENV_PREFIX, ServerConfig};
pub use role::{Role, UnknownRole};
