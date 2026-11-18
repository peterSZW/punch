use std::io;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    Get,
    Post,
    Delete,
    Put,
    Options,
    Other,
}

impl Method {
    pub fn parse(s: &str) -> Method {
        match s {
            "GET" => Method::Get,
            "POST" => Method::Post,
            "DELETE" => Method::Delete,
            "PUT" => Method::Put,
            "OPTIONS" => Method::Options,
            _ => Method::Other,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Method::Get => "GET",
            Method::Post => "POST",
            Method::Delete => "DELETE",
            Method::Put => "PUT",
            Method::Options => "OPTIONS",
            Method::Other => "OTHER",
        }
    }
}

#[derive(Debug)]
pub struct RawRequest {
    pub method: Method,
    pub path: String,
    pub query: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub keep_alive: bool,
}

impl RawRequest {
    pub fn header(&self, name: &str) -> Option<&str> {
        let name = name.to_ascii_lowercase();
        self.headers
            .iter()
            .find(|(k, _)| k == &name)
            .map(|(_, v)| v.as_str())
    }
}

pub fn query_get<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        match pair.split_once('=') {
            Some((k, v)) if k == key => return Some(v),
            None if pair == key => return Some(""),
            _ => {}
        }
    }
    None
}

pub struct HttpConn<S> {
    pub sock: S,
    buf: Vec<u8>,
}

impl<S> HttpConn<S> {
    pub fn new(sock: S) -> Self {
        Self {
            sock,
            buf: Vec::with_capacity(2048),
        }
    }

    pub async fn next_request(&mut self, max_body: usize) -> io::Result<Option<RawRequest>>
    where
        S: AsyncRead + Unpin,
    {
        loop {
            if let Some(req) = try_parse(&mut self.buf, max_body)? {
                return Ok(Some(req));
            }
            if self.buf.len() > max_body + 16 * 1024 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "request too large",
                ));
            }
            let mut tmp = [0u8; 8192];
            let n = self.sock.read(&mut tmp).await?;
            if n == 0 {
                return if self.buf.is_empty() {
                    Ok(None)
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "eof during request",
                    ))
                };
            }
            self.buf.extend_from_slice(&tmp[..n]);
        }
    }

    pub async fn write_all(&mut self, bytes: &[u8]) -> io::Result<()>
    where
        S: AsyncWrite + Unpin,
    {
        self.sock.write_all(bytes).await
    }
}

impl<S: io::Read> HttpConn<S> {
    pub fn next_request_blocking(&mut self, max_body: usize) -> io::Result<Option<RawRequest>> {
        loop {
            if let Some(req) = try_parse(&mut self.buf, max_body)? {
                return Ok(Some(req));
            }
            if self.buf.len() > max_body + 16 * 1024 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "request too large",
                ));
            }
            let mut tmp = [0u8; 8192];
            let n = self.sock.read(&mut tmp)?;
            if n == 0 {
                return if self.buf.is_empty() {
                    Ok(None)
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "eof during request",
                    ))
                };
            }
            self.buf.extend_from_slice(&tmp[..n]);
        }
    }
}

impl<S: io::Write> HttpConn<S> {
    pub fn write_all_blocking(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.sock.write_all(bytes)
    }
}

fn try_parse(buf: &mut Vec<u8>, max_body: usize) -> io::Result<Option<RawRequest>> {
    let header_end = match find_header_end(buf) {
        Some(p) => p,
        None => {
            if buf.len() > 16 * 1024 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "headers too large",
                ));
            }
            return Ok(None);
        }
    };
    let head = std::str::from_utf8(&buf[..header_end]).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidData, "headers are not utf-8")
    })?;
    let mut lines = head.split("\r\n");
    let reqline = lines.next().unwrap_or("");
    let mut parts = reqline.split(' ');
    let method = Method::parse(parts.next().unwrap_or(""));
    let target = parts.next().unwrap_or("/");
    let version = parts.next().unwrap_or("HTTP/1.1");
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (target.to_string(), String::new()),
    };

    let mut headers = Vec::new();
    let mut content_length = 0usize;
    let mut connection = String::new();
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            let name = name.trim().to_ascii_lowercase();
            let value_owned = value.trim().to_string();
            if name == "content-length" {
                content_length = value_owned.parse().unwrap_or(usize::MAX);
            } else if name == "connection" {
                connection = value_owned.to_ascii_lowercase();
            } else if name == "transfer-encoding" && value_owned.to_ascii_lowercase().contains("chunked")
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "chunked request bodies are not supported",
                ));
            }
            headers.push((name, value_owned));
        }
    }
    if content_length > max_body {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "body too large",
        ));
    }
    let body_start = header_end + 4;
    let total = body_start + content_length;
    if buf.len() < total {
        return Ok(None);
    }
    let keep_alive = if connection.contains("close") {
        false
    } else if connection.contains("keep-alive") {
        true
    } else {
        version == "HTTP/1.1"
    };
    let body = buf[body_start..total].to_vec();
    buf.drain(..total);
    Ok(Some(RawRequest {
        method,
        path,
        query,
        headers,
        body,
        keep_alive,
    }))
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}
