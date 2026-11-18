use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::time::timeout;

use punch::cert;
use punch::client::{Options, PortMap};
use punch::config::{PoolCfg, ServerConfig, TcpTune, WebCfg};
use punch::server;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tunnel_moves_bytes_and_reports_listen_failure() {
    timeout(Duration::from_secs(40), body())
        .await
        .expect("test timed out")
        .unwrap();
}

async fn body() -> anyhow::Result<()> {
    let dir = std::env::temp_dir().join(format!(
        "punch-it-{}-{}",
        std::process::id(),
        punch::util::now_ms()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;
    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");
    cert::write_self_signed(&cert_path, &key_path)?;

    let echo = echo_server().await;
    let closed = closed_port().await;
    let cfg = ServerConfig {
        control_listen: "127.0.0.1:0".into(),
        data_listen: "127.0.0.1:0".into(),
        api_key: "test-key-123456".into(),
        web: WebCfg {
            listen: "127.0.0.1:0".into(),
            cert: cert_path.display().to_string(),
            key: key_path.display().to_string(),
        },
        tcp: TcpTune::default(),
        pool: PoolCfg {
            idle: 4,
            max_idle: 8,
        },
        config_path: {
            let path = dir.join("punch-server.yaml");
            std::fs::write(&path, "control_listen: \"127.0.0.1:1\"\napi_key: \"test-key-123456\"\n")?;
            path
        },
    };
    let bound = server::bind(cfg)?;
    let control = bound.control;
    let web = bound.web;
    let (stop_tx, stop_rx) = watch::channel(false);
    let server_task = tokio::spawn(async move { bound.serve(stop_rx).await });

    let index = https(web, &cert_path, "GET", "/", None, None).await?;
    assert!(index.status == 200, "{index:?}");
    assert!(index.body.contains("PUNCH"));
    assert!(index.body.contains("/dl/punch-macos"));
    assert!(index.body.contains("/dl/punch-linux"));
    assert!(index.body.contains("/dl/punch-windows.exe"));
    assert!(index.body.contains("--key <api-key>"), "{index:?}");
    assert!(index.body.contains("设置"), "{index:?}");
    assert!(!index.body.contains("test-key-123456"), "{index:?}");
    let missing = https(web, &cert_path, "GET", "/dl/not-a-client", None, None).await?;
    assert_eq!(missing.status, 404, "{missing:?}");

    let health = https(web, &cert_path, "GET", "/healthz", None, None).await?;
    assert!(health.body.contains("ok"), "{health:?}");

    let denied = https(web, &cert_path, "GET", "/api/clients", None, None).await?;
    assert_eq!(denied.status, 401, "{denied:?}");

    let client = Options {
        server: control,
        api_key: "test-key-123456".into(),
        name: "edge".into(),
        maps: vec![PortMap {
            bind_ip: "127.0.0.1".into(),
            bind_port: 0,
            target_host: echo.ip().to_string(),
            target_port: echo.port(),
        }],
        bench: None,
        pool: Some(2),
    };
    let client_task = tokio::spawn(punch::client::run(client, stop_tx.subscribe()));

    let public = wait_map(web, &cert_path).await?;
    let payload = patterned(512 * 1024);
    copy_through(public, &payload).await?;

    let mut joins = Vec::new();
    for n in 0..8u8 {
        let addr = public;
        joins.push(tokio::spawn(async move {
            let payload = patterned(32 * 1024 + n as usize);
            copy_through(addr, &payload).await
        }));
    }
    for join in joins {
        join.await??;
    }

    many_on_one_connection(public, &patterned(8 * 1024), 120).await?;

    let before = overview_bytes(web, &cert_path).await?;
    {
        let mut sock = timeout(Duration::from_secs(5), tokio::net::TcpStream::connect(public)).await??;
        let abrupt = patterned(64 * 1024);
        sock.write_all(&abrupt).await?;
        drop(sock);
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            let now = overview_bytes(web, &cert_path).await?;
            if now.0 >= before.0 + abrupt.len() as u64 {
                break;
            }
            if std::time::Instant::now() > deadline {
                anyhow::bail!(
                    "abrupt close dropped bytes: before {} after {}",
                    before.0,
                    now.0
                );
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    let created = https(
        web,
        &cert_path,
        "POST",
        "/api/mappings",
        Some("test-key-123456"),
        Some(&format!(
            r#"{{"client_name":"edge","bind":"127.0.0.1:0","target":"127.0.0.1:{closed}"}}"#
        )),
    )
    .await?;
    assert_eq!(created.status, 201, "{created:?}");
    assert!(
        created.body.contains("listen failed"),
        "expected listen failure, got {}",
        created.body
    );

    let listed = https(
        web,
        &cert_path,
        "GET",
        "/api/mappings",
        Some("test-key-123456"),
        None,
    )
    .await?;
    assert!(listed.body.contains("listen failed"), "{listed:?}");

    let events = https(
        web,
        &cert_path,
        "GET",
        "/api/events",
        Some("test-key-123456"),
        None,
    )
    .await?;
    assert!(events.body.contains("listen_fail"), "{events:?}");

    let rotated = https(
        web,
        &cert_path,
        "POST",
        "/api/key",
        Some("test-key-123456"),
        Some(r#"{"api_key":"rotated-key-99"}"#),
    )
    .await?;
    assert_eq!(rotated.status, 200, "{rotated:?}");
    assert!(!rotated.body.contains("rotated-key-99"), "{rotated:?}");
    let stale = https(web, &cert_path, "GET", "/api/ping", Some("test-key-123456"), None).await?;
    assert_eq!(stale.status, 401, "{stale:?}");
    let fresh = https(web, &cert_path, "GET", "/api/ping", Some("rotated-key-99"), None).await?;
    assert_eq!(fresh.status, 200, "{fresh:?}");
    let saved = std::fs::read_to_string(dir.join("punch-server.yaml"))?;
    assert!(saved.contains("api_key: \"rotated-key-99\""));
    assert!(saved.contains("control_listen: \"127.0.0.1:1\""));
    let short = https(
        web,
        &cert_path,
        "POST",
        "/api/key",
        Some("rotated-key-99"),
        Some(r#"{"api_key":"short"}"#),
    )
    .await?;
    assert_eq!(short.status, 400, "{short:?}");

    let _ = stop_tx.send(true);
    let _ = timeout(Duration::from_secs(3), server_task).await;
    let _ = timeout(Duration::from_secs(3), client_task).await;
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

async fn echo_server() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((sock, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let (mut r, mut w) = sock.into_split();
                let _ = tokio::io::copy(&mut r, &mut w).await;
            });
        }
    });
    addr
}

async fn closed_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

fn patterned(n: usize) -> Vec<u8> {
    let mut buf = vec![0u8; n];
    for (i, b) in buf.iter_mut().enumerate() {
        *b = (i % 251) as u8;
    }
    buf
}

async fn many_on_one_connection(
    addr: std::net::SocketAddr,
    payload: &[u8],
    times: usize,
) -> anyhow::Result<()> {
    let mut sock = timeout(Duration::from_secs(5), tokio::net::TcpStream::connect(addr)).await??;
    for _ in 0..times {
        sock.write_all(payload).await?;
    }
    sock.shutdown().await?;
    let mut got = vec![0u8; payload.len() * times];
    timeout(Duration::from_secs(8), sock.read_exact(&mut got)).await??;
    for (i, chunk) in got.chunks(payload.len()).enumerate() {
        if chunk != payload {
            anyhow::bail!("chunk {i} mismatch");
        }
    }
    Ok(())
}

async fn overview_bytes(
    web: std::net::SocketAddr,
    cert_path: &std::path::Path,
) -> anyhow::Result<(u64, u64)> {
    let res = https(web, cert_path, "GET", "/api/overview", Some("test-key-123456"), None).await?;
    let inn = json_u64(&res.body, "bytes_in").ok_or_else(|| anyhow::anyhow!("no bytes_in {}", res.body))?;
    let out = json_u64(&res.body, "bytes_out").ok_or_else(|| anyhow::anyhow!("no bytes_out {}", res.body))?;
    Ok((inn, out))
}

fn json_u64(body: &str, key: &str) -> Option<u64> {
    let pat = format!("\"{key}\":");
    let rest = body.split(&pat).nth(1)?;
    let end = rest.find(|c: char| !c.is_ascii_digit()).unwrap_or(rest.len());
    rest[..end].parse().ok()
}

async fn copy_through(addr: std::net::SocketAddr, payload: &[u8]) -> anyhow::Result<()> {
    let mut sock = timeout(Duration::from_secs(5), tokio::net::TcpStream::connect(addr))
        .await??;
    sock.write_all(payload).await?;
    sock.shutdown().await?;
    let mut got = Vec::new();
    sock.read_to_end(&mut got).await?;
    if got != payload {
        anyhow::bail!("payload mismatch: sent {} got {}", payload.len(), got.len());
    }
    Ok(())
}

async fn wait_map(
    web: std::net::SocketAddr,
    cert_path: &std::path::Path,
) -> anyhow::Result<std::net::SocketAddr> {
    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    loop {
        let res = https(
            web,
            cert_path,
            "GET",
            "/api/mappings",
            Some("test-key-123456"),
            None,
        )
        .await?;
        if let Some(port) = bind_port(&res.body) {
            return Ok(std::net::SocketAddr::from(([127, 0, 0, 1], port)));
        }
        if std::time::Instant::now() > deadline {
            anyhow::bail!("mapping did not appear: {}", res.body);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn bind_port(body: &str) -> Option<u16> {
    let key = "\"bind\":\"127.0.0.1:";
    let idx = body.find(key)?;
    let rest = &body[idx + key.len()..];
    let end = rest.find('"')?;
    rest[..end].parse().ok()
}

struct HttpRes {
    status: u16,
    body: String,
}

impl std::fmt::Debug for HttpRes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HTTP {} {}", self.status, self.body)
    }
}

async fn https(
    addr: std::net::SocketAddr,
    cert_path: &std::path::Path,
    method: &str,
    path: &str,
    key: Option<&str>,
    body: Option<&str>,
) -> anyhow::Result<HttpRes> {
    let body = body.unwrap_or("");
    let mut req = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    if let Some(key) = key {
        req.push_str(&format!("X-Api-Key: {key}\r\n"));
    }
    if !body.is_empty() {
        req.push_str("Content-Type: application/json\r\n");
    }
    req.push_str("\r\n");
    req.push_str(body);
    let cert_owned = cert_path.to_path_buf();
    let raw = tokio::task::spawn_blocking(move || cert::https_exchange(addr, &cert_owned, req.as_bytes()))
        .await
        .map_err(|err| anyhow::anyhow!("https task: {err}"))??;
    let text = String::from_utf8_lossy(&raw).to_string();
    let status = text
        .split_whitespace()
        .nth(1)
        .unwrap_or("0")
        .parse()
        .unwrap_or(0);
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    Ok(HttpRes { status, body })
}
