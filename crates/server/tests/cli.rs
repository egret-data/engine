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
