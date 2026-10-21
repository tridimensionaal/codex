use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use codex_app_server_protocol::JSONRPCMessage;
use futures::SinkExt;
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use tokio::task::JoinHandle;
use tokio::time::timeout;

use crate::Daemon;
use crate::LifecycleCommand;
use crate::LifecycleStatus;
use crate::settings::DaemonSettings;

async fn test_daemon(server_command: &str) -> Result<(TempDir, Daemon)> {
    let home = TempDir::new()?;
    let state = home.path().join("app-server-daemon");
    codex_uds::prepare_private_socket_directory(&state).await?;
    let managed_codex_bin = home
        .path()
        .join("packages/app-server-daemon/current/bin/codex");
    std::fs::create_dir_all(managed_codex_bin.parent().context("bin parent")?)?;
    codex_utils_cargo_bin::write_executable(
        &managed_codex_bin,
        &format!(
            "#!/bin/sh\n\
             if [ \"$1\" = --version ]; then echo 'codex-cli 1.2.3'; exit 0; fi\n\
             if [ \"$2\" != --listen ]; then exit 0; fi\n\
             {server_command}\n"
        ),
    )?;
    let daemon = Daemon {
        log_diagnostics: false,
        socket_path: home.path().join("server.sock"),
        pid_file: state.join("daemon.pid"),
        update_pid_file: state.join("daemon-updater.pid"),
        operation_lock_file: state.join("daemon.lock"),
        settings_file: state.join("settings.json"),
        managed_codex_bin,
    };
    DaemonSettings {
        auto_update_enabled: false,
        shutdown_grace_seconds: 0,
        ..Default::default()
    }
    .save(&daemon.settings_file)
    .await?;
    Ok((home, daemon))
}

fn spawn_control_server(socket_path: PathBuf, delay: Duration) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        tokio::time::sleep(delay).await;
        let mut listener = codex_uds::UnixListener::bind(&socket_path).await?;
        let connection = listener.accept().await?;
        let mut websocket = tokio_tungstenite::accept_async(connection).await?;
        let JSONRPCMessage::Request(request) = crate::client::read_message(&mut websocket).await?
        else {
            panic!("expected initialize request");
        };
        assert_eq!(request.method, "initialize");
        websocket
            .send(tokio_tungstenite::tungstenite::Message::Text(
                serde_json::json!({"id": request.id, "result": {
                    "userAgent": "codex_app_server_daemon/1.2.3",
                    "codexHome": socket_path.parent().context("Codex home")?,
                    "platformFamily": "unix",
                    "platformOs": std::env::consts::OS,
                }})
                .to_string()
                .into(),
            ))
            .await?;
        let JSONRPCMessage::Notification(notification) =
            crate::client::read_message(&mut websocket).await?
        else {
            panic!("expected initialized notification");
        };
        assert_eq!(notification.method, "initialized");
        Ok(())
    })
}

#[tokio::test]
async fn start_waits_for_slow_daemon() {
    let (_home, daemon) = test_daemon("exec sleep 60").await.expect("fixture");
    let server = spawn_control_server(daemon.socket_path.clone(), Duration::from_secs(15));

    let result = daemon.run(LifecycleCommand::Start).await;

    // Clean up the detached fixture even when an assertion below fails.
    daemon
        .run(LifecycleCommand::Stop)
        .await
        .expect("stop fixture");
    server.abort();
    match server.await {
        Ok(result) => result.expect("control server"),
        Err(error) => assert!(error.is_cancelled(), "control server failed: {error}"),
    }

    let output = result.expect("first start waits for slow readiness");
    assert_eq!(output.status, LifecycleStatus::Started);
    assert_eq!(output.app_server_version.as_deref(), Some("1.2.3"));
}

#[tokio::test]
async fn readiness_fails_promptly_when_daemon_exits() {
    let (_home, daemon) = test_daemon("echo 'fixture startup failed' >&2; sleep 0.1; exit 7")
        .await
        .expect("fixture");

    let result = timeout(Duration::from_secs(5), daemon.run(LifecycleCommand::Start)).await;
    daemon
        .run(LifecycleCommand::Stop)
        .await
        .expect("stop fixture");

    let error = result
        .expect("process exit should not exhaust the readiness timeout")
        .expect_err("exited daemon cannot become ready");
    let diagnostic = format!("{error:#}");
    assert!(diagnostic.contains("app server did not become ready"));
    assert!(diagnostic.contains("fixture startup failed"));
}
