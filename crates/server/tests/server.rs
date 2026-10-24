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
