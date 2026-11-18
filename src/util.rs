use std::fs::File;
use std::io::Read;
use std::net::SocketAddr;
use std::sync::{Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

pub fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub fn random_bytes(n: usize) -> Vec<u8> {
    let mut buf = vec![0u8; n];
    let mut file = File::open("/dev/urandom").expect("open /dev/urandom");
    file.read_exact(&mut buf).expect("read /dev/urandom");
    buf
}

pub fn random_hex(nbytes: usize) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let bytes = random_bytes(nbytes);
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

pub fn random_u64() -> u64 {
    let b = random_bytes(8);
    let mut a = [0u8; 8];
    a.copy_from_slice(&b);
    u64::from_le_bytes(a)
}

pub fn random_token() -> [u8; 16] {
    let b = random_bytes(16);
    let mut t = [0u8; 16];
    t.copy_from_slice(&b);
    t
}

pub fn keys_equal(a: &str, b: &str) -> bool {
    let a = a.as_bytes();
    let b = b.as_bytes();
    let n = a.len().max(b.len());
    let mut diff = (a.len() ^ b.len()) as u8;
    let mut i = 0;
    while i < n {
        let x = if i < a.len() { a[i] } else { 0 };
        let y = if i < b.len() { b[i] } else { 0 };
        diff |= x ^ y;
        i += 1;
    }
    diff == 0
}

pub fn token_eq(a: &[u8; 16], b: &[u8; 16]) -> bool {
    let mut diff = 0u8;
    for i in 0..16 {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

/// `host:port` or `[ipv6]:port`.
pub fn split_host_port(input: &str) -> Option<(String, u16)> {
    let s = input.trim();
    if let Some(rest) = s.strip_prefix('[') {
        let (host, port) = rest.split_once("]:")?;
        if host.is_empty() {
            return None;
        }
        let port = port.parse().ok()?;
        return Some((host.to_string(), port));
    }
    let (host, port) = s.rsplit_once(':')?;
    if host.is_empty() {
        return None;
    }
    let port = port.parse().ok()?;
    Some((host.to_string(), port))
}

/// Bind address must be an IP, so the server can listen on it.
pub fn parse_bind(input: &str) -> Result<(String, u16), String> {
    let addr: SocketAddr = input.parse().map_err(|_| {
        format!("bind 必须是 ip:port，例如 0.0.0.0:8080 或 127.0.0.1:8080，收到 {input}")
    })?;
    Ok((addr.ip().to_string(), addr.port()))
}

pub fn parse_target(input: &str) -> Result<(String, u16), String> {
    let (host, port) = split_host_port(input)
        .ok_or_else(|| format!("target 必须是 host:port，收到 {input}"))?;
    if port == 0 {
        return Err("target 端口不能是 0".into());
    }
    if host.len() > 255 {
        return Err("target 主机名过长".into());
    }
    Ok((host, port))
}

pub fn hostname() -> String {
    #[cfg(unix)]
    {
        let mut buf = [0u8; 256];
        let rc = unsafe { libc::gethostname(buf.as_mut_ptr() as *mut libc::c_char, buf.len()) };
        if rc != 0 {
            return "client".to_string();
        }
        let end = buf.iter().position(|b| *b == 0).unwrap_or(buf.len());
        let name = String::from_utf8_lossy(&buf[..end]).trim().to_string();
        if name.is_empty() {
            return "client".to_string();
        }
        return name;
    }
    #[cfg(not(unix))]
    {
        std::env::var("COMPUTERNAME")
            .ok()
            .map(|name| name.trim().to_string())
            .filter(|name| !name.is_empty())
            .unwrap_or_else(|| "client".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_hosts() {
        assert_eq!(
            split_host_port("10.1.2.3:80").unwrap(),
            ("10.1.2.3".into(), 80)
        );
        assert_eq!(
            split_host_port("[::1]:443").unwrap(),
            ("::1".into(), 443)
        );
        assert!(split_host_port("noport").is_none());
    }

    #[test]
    fn bind_rejects_hostname() {
        assert!(parse_bind("localhost:80").is_err());
        assert_eq!(
            parse_bind("127.0.0.1:8080").unwrap(),
            ("127.0.0.1".into(), 8080)
        );
        assert_eq!(parse_bind("0.0.0.0:9").unwrap(), ("0.0.0.0".into(), 9));
    }

    #[test]
    fn key_compare() {
        assert!(keys_equal("abc", "abc"));
        assert!(!keys_equal("abc", "abd"));
        assert!(!keys_equal("abc", "abcd"));
    }
}
