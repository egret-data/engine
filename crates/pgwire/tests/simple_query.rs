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
