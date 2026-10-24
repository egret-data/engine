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
