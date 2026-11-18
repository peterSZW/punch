//! 压测端点的正确性门槛。GET 的正文必须是请求的长度，并且每个字节都是 0xA5。
//! 改成 sendfile 之后，少一个字节或拆包错位都会在这里失败。

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::time::timeout;

#[tokio::test]
async fn bench_get_and_post_match_the_requested_bytes() {
    timeout(Duration::from_secs(10), body())
        .await
        .expect("bench test timed out")
        .unwrap();
}

async fn body() -> anyhow::Result<()> {
    let (_tx, rx) = watch::channel(false);
    let addr = punch::bench::serve("127.0.0.1:0".parse().unwrap(), rx).await?;

    for size in [0usize, 1, 4096, 256 * 1024, 256 * 1024 + 13] {
        let (status, headers, payload) = get_close(addr, size).await?;
        assert_eq!(status, 200, "size {size}");
        assert!(headers.contains("X-Punch-Bench: 1"), "{headers}");
        assert_eq!(payload.len(), size, "size {size}");
        assert!(payload.iter().all(|b| *b == 0xA5), "size {size} body byte");
    }

    let mut sock = TcpStream::connect(addr).await?;
    sock.set_nodelay(true)?;
    let first = get_keep(&mut sock, 100).await?;
    let second = get_keep(&mut sock, 50).await?;
    assert_eq!(first.len(), 100);
    assert_eq!(second.len(), 50);
    assert!(first.iter().chain(second.iter()).all(|b| *b == 0xA5));

    let posted = post_close(addr, b"hello").await?;
    assert_eq!(posted, "{\"bytes\":5}\n");
    Ok(())
}

async fn get_close(addr: std::net::SocketAddr, size: usize) -> anyhow::Result<(u16, String, Vec<u8>)> {
    let mut sock = TcpStream::connect(addr).await?;
    let req = format!(
        "GET /bench?size={size} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    );
    sock.write_all(req.as_bytes()).await?;
    let mut buf = Vec::new();
    timeout(Duration::from_secs(3), sock.read_to_end(&mut buf)).await??;
    split_body(&buf)
}

async fn get_keep(sock: &mut TcpStream, size: usize) -> anyhow::Result<Vec<u8>> {
    let req = format!(
        "GET /bench?size={size} HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n"
    );
    sock.write_all(req.as_bytes()).await?;
    let (_status, _headers, body) = read_one(sock).await?;
    assert_eq!(body.len(), size);
    Ok(body)
}

async fn post_close(addr: std::net::SocketAddr, body: &[u8]) -> anyhow::Result<String> {
    let mut sock = TcpStream::connect(addr).await?;
    let req = format!(
        "POST /bench HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    sock.write_all(req.as_bytes()).await?;
    sock.write_all(body).await?;
    let mut buf = Vec::new();
    timeout(Duration::from_secs(3), sock.read_to_end(&mut buf)).await??;
    let (_status, _headers, payload) = split_body(&buf)?;
    Ok(String::from_utf8(payload)?)
}

async fn read_one(sock: &mut TcpStream) -> anyhow::Result<(u16, String, Vec<u8>)> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 8192];
    let (status, headers, header_len, clen) = loop {
        if let Some(parsed) = parse_head(&buf) {
            break parsed;
        }
        let n = timeout(Duration::from_secs(3), sock.read(&mut tmp)).await??;
        if n == 0 {
            anyhow::bail!("eof before headers");
        }
        buf.extend_from_slice(&tmp[..n]);
    };
    let mut body = buf.split_off(header_len);
    while body.len() < clen {
        let n = timeout(Duration::from_secs(3), sock.read(&mut tmp)).await??;
        if n == 0 {
            anyhow::bail!("eof in body, have {} want {clen}", body.len());
        }
        body.extend_from_slice(&tmp[..n]);
    }
    if body.len() > clen {
        anyhow::bail!("read past content-length");
    }
    Ok((status, headers, body))
}

fn split_body(buf: &[u8]) -> anyhow::Result<(u16, String, Vec<u8>)> {
    let (status, headers, header_len, clen) = parse_head(buf).ok_or_else(|| anyhow::anyhow!("no header"))?;
    let body = buf[header_len..].to_vec();
    if body.len() != clen {
        anyhow::bail!("body {} != content-length {clen}", body.len());
    }
    Ok((status, headers, body))
}

fn parse_head(buf: &[u8]) -> Option<(u16, String, usize, usize)> {
    let end = buf.windows(4).position(|w| w == b"\r\n\r\n")?;
    let text = std::str::from_utf8(&buf[..end]).ok()?;
    let status = text.split_whitespace().nth(1)?.parse().ok()?;
    let mut clen = None;
    for line in text.split("\r\n").skip(1) {
        let (name, value) = line.split_once(':')?;
        if name.eq_ignore_ascii_case("content-length") {
            clen = value.trim().parse().ok();
        }
    }
    Some((status, text.to_string(), end + 4, clen?))
}
