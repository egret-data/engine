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
