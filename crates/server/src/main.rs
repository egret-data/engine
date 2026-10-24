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
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::warn!(%error, "cannot listen for Ctrl-C");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut sigterm) => {
                sigterm.recv().await;
            }
            Err(error) => {
                tracing::warn!(%error, "cannot listen for SIGTERM");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
}
