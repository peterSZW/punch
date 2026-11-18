use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use std::collections::HashMap;

use anyhow::{bail, Context, Result};

#[cfg(unix)]
use crate::cert;
use crate::util::random_hex;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub control_listen: String,
    pub data_listen: String,
    pub api_key: String,
    pub config_path: PathBuf,
    pub web: WebCfg,
    pub tcp: TcpTune,
    pub pool: PoolCfg,
}

#[derive(Debug, Clone)]
pub struct WebCfg {
    pub listen: String,
    pub cert: String,
    pub key: String,
}

#[derive(Debug, Clone)]
pub struct TcpTune {
    pub nodelay: bool,
    pub recv_buffer: usize,
    pub send_buffer: usize,
    pub pipe_size: usize,
    pub backlog: i32,
}

#[derive(Debug, Clone)]
pub struct PoolCfg {
    pub idle: u16,
    pub max_idle: u16,
}

fn default_true() -> bool {
    true
}
fn default_buf() -> usize {
    4 * 1024 * 1024
}
fn default_pipe() -> usize {
    256 * 1024
}
fn default_backlog() -> i32 {
    4096
}
fn default_idle() -> u16 {
    16
}
fn default_max_idle() -> u16 {
    64
}

impl Default for TcpTune {
    fn default() -> Self {
        Self {
            nodelay: default_true(),
            recv_buffer: default_buf(),
            send_buffer: default_buf(),
            pipe_size: default_pipe(),
            backlog: default_backlog(),
        }
    }
}

impl Default for PoolCfg {
    fn default() -> Self {
        Self {
            idle: default_idle(),
            max_idle: default_max_idle(),
        }
    }
}

impl ServerConfig {
    /// 只解析这个程序自己写出的两层 yaml：顶层键、一层缩进、`#` 注释、引号字符串。
    pub fn parse_yaml(text: &str) -> Result<ServerConfig> {
        let map = parse_flat(text)?;
        let mut cfg = ServerConfig {
            control_listen: req(&map, "control_listen")?,
            data_listen: req(&map, "data_listen")?,
            api_key: req(&map, "api_key")?,
            config_path: PathBuf::new(),
            web: WebCfg {
                listen: req(&map, "web.listen")?,
                cert: req(&map, "web.cert")?,
                key: req(&map, "web.key")?,
            },
            tcp: TcpTune {
                nodelay: opt_bool(&map, "tcp.nodelay")?.unwrap_or(true),
                recv_buffer: opt_usize(&map, "tcp.recv_buffer")?.unwrap_or(default_buf()),
                send_buffer: opt_usize(&map, "tcp.send_buffer")?.unwrap_or(default_buf()),
                pipe_size: opt_usize(&map, "tcp.pipe_size")?.unwrap_or(default_pipe()),
                backlog: opt_i32(&map, "tcp.backlog")?.unwrap_or(default_backlog()),
            },
            pool: PoolCfg {
                idle: opt_u16(&map, "pool.idle")?.unwrap_or(default_idle()),
                max_idle: opt_u16(&map, "pool.max_idle")?.unwrap_or(default_max_idle()),
            },
        };
        cfg.normalize()?;
        Ok(cfg)
    }

    pub fn normalize(&mut self) -> Result<()> {
        if self.api_key.trim().len() < 8 {
            bail!("api_key 至少 8 个字符");
        }
        self.api_key = self.api_key.trim().to_string();
        if self.pool.idle == 0 {
            self.pool.idle = 1;
        }
        if self.pool.max_idle < self.pool.idle {
            self.pool.max_idle = self.pool.idle;
        }
        self.tcp.pipe_size = self.tcp.pipe_size.clamp(64 * 1024, 4 * 1024 * 1024);
        self.tcp.backlog = self.tcp.backlog.clamp(128, 65535);
        Ok(())
    }

    pub fn control_addr(&self) -> Result<SocketAddr> {
        parse_listen(&self.control_listen, "control_listen")
    }
    pub fn data_addr(&self) -> Result<SocketAddr> {
        parse_listen(&self.data_listen, "data_listen")
    }
    pub fn web_addr(&self) -> Result<SocketAddr> {
        parse_listen(&self.web.listen, "web.listen")
    }
}

fn parse_listen(value: &str, field: &str) -> Result<SocketAddr> {
    value.parse().with_context(|| {
        format!("{field} 必须是 ip:port，例如 0.0.0.0:8024 或 127.0.0.1:8024，当前是 {value}")
    })
}

/// 没有配置文件时在本地生成 yaml 和一份 nginx 能直接用的自签名 PEM。
pub fn load_or_create(path: &Path) -> Result<(ServerConfig, bool)> {
    if path.exists() {
        let text = fs::read_to_string(path)
            .with_context(|| format!("读取配置 {}", path.display()))?;
        let mut cfg = ServerConfig::parse_yaml(&text)?;
        remember_path(&mut cfg, path);
        resolve_paths(&mut cfg, path)?;
        ensure_cert_files(&cfg)?;
        return Ok((cfg, false));
    }
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .with_context(|| format!("创建目录 {}", parent.display()))?;
        }
    }
    let cfg = default_config();
    let text = render_yaml(&cfg);
    fs::write(path, &text).with_context(|| format!("写入 {}", path.display()))?;
    let mut loaded = ServerConfig::parse_yaml(&text)?;
    remember_path(&mut loaded, path);
    resolve_paths(&mut loaded, path)?;
    ensure_cert_files(&loaded)?;
    Ok((loaded, true))
}

fn remember_path(cfg: &mut ServerConfig, path: &Path) {
    cfg.config_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    };
}

/// 只改配置文件里的 `api_key` 这一行，其余内容保持原样。
pub fn write_api_key(path: &Path, key: &str) -> Result<()> {
    if key.len() < 8 || key != key.trim() {
        bail!("api key 至少 8 个字符");
    }
    if key.len() > 128 || key.chars().any(|c| c.is_control() || c == '"' || c == '\\') {
        bail!("api key 不能包含引号、反斜杠或控制字符");
    }
    let text = fs::read_to_string(path).with_context(|| format!("读取配置 {}", path.display()))?;
    let mut found = false;
    let mut out = String::new();
    for line in text.split_inclusive('\n') {
        if !found && is_api_key_line(line) {
            let ending = if line.ends_with("\r\n") {
                "\r\n"
            } else if line.ends_with('\n') {
                "\n"
            } else {
                ""
            };
            out.push_str("api_key: \"");
            out.push_str(key);
            out.push('"');
            out.push_str(ending);
            found = true;
        } else {
            out.push_str(line);
        }
    }
    if !found {
        bail!("配置文件里没有 api_key");
    }
    let name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("punch-server.yaml");
    let tmp = path.with_file_name(format!(".{name}.tmp"));
    fs::write(&tmp, &out).with_context(|| format!("写入 {}", tmp.display()))?;
    #[cfg(unix)]
    if let Ok(meta) = fs::metadata(path) {
        let _ = fs::set_permissions(&tmp, meta.permissions());
    }
    fs::rename(&tmp, path).with_context(|| format!("替换 {}", path.display()))?;
    Ok(())
}

fn is_api_key_line(line: &str) -> bool {
    let trimmed = line.trim_start();
    if trimmed.starts_with('#') {
        return false;
    }
    let trimmed = trimmed.trim_end_matches(['\n', '\r']);
    trimmed.starts_with("api_key:")
}

fn default_config() -> ServerConfig {
    ServerConfig {
        control_listen: "0.0.0.0:8024".into(),
        data_listen: "0.0.0.0:8025".into(),
        api_key: random_hex(24),
        config_path: PathBuf::new(),
        web: WebCfg {
            listen: "0.0.0.0:8443".into(),
            cert: "certs/cert.pem".into(),
            key: "certs/key.pem".into(),
        },
        tcp: TcpTune::default(),
        pool: PoolCfg::default(),
    }
}

fn render_yaml(cfg: &ServerConfig) -> String {
    format!(
        r#"# punch 服务端配置。删掉这个文件后再次启动会重新生成。
# 控制通道、数据通道、管理页都可以绑 0.0.0.0（对外）或 127.0.0.1（仅本机）。

control_listen: "{control}"
data_listen: "{data}"

# 客户端和 HTTPS 管理接口共用
api_key: "{key}"

web:
  listen: "{web}"
  # nginx 兼容 PEM，对应 ssl_certificate / ssl_certificate_key
  # 支持 PRIVATE KEY、RSA PRIVATE KEY、EC PRIVATE KEY
  cert: "{cert}"
  key: "{key_file}"

tcp:
  nodelay: {nodelay}
  # 想把吞吐打满时，把系统的 net.core.rmem_max / net.core.wmem_max 调到不小于这里
  recv_buffer: {recv}
  send_buffer: {send}
  # Linux splice 管道容量。单连接越大越快，占用的内核内存也越高
  pipe_size: {pipe}
  backlog: {backlog}

pool:
  idle: {idle}
  max_idle: {max_idle}
"#,
        control = cfg.control_listen,
        data = cfg.data_listen,
        key = cfg.api_key,
        web = cfg.web.listen,
        cert = cfg.web.cert,
        key_file = cfg.web.key,
        recv = cfg.tcp.recv_buffer,
        send = cfg.tcp.send_buffer,
        nodelay = if cfg.tcp.nodelay { "true" } else { "false" },
        pipe = cfg.tcp.pipe_size,
        backlog = cfg.tcp.backlog,
        idle = cfg.pool.idle,
        max_idle = cfg.pool.max_idle,
    )
}

fn resolve_paths(cfg: &mut ServerConfig, config_path: &Path) -> Result<()> {
    let base = config_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    cfg.web.cert = absolutize(base, &cfg.web.cert);
    cfg.web.key = absolutize(base, &cfg.web.key);
    Ok(())
}

fn parse_flat(text: &str) -> Result<HashMap<String, String>> {
    let mut map = HashMap::new();
    let mut section = String::new();
    for (idx, raw) in text.lines().enumerate() {
        let line_no = idx + 1;
        let raw = raw.trim_end_matches('\r');
        let line = strip_comment(raw);
        if line.trim().is_empty() {
            continue;
        }
        let indent = line.len() - line.trim_start().len();
        let trimmed = line.trim();
        let Some((key, value)) = trimmed.split_once(':') else {
            bail!("第 {line_no} 行不是 key: value");
        };
        let key = key.trim();
        if key.is_empty() {
            bail!("第 {line_no} 行缺少键名");
        }
        let value = unquote(value.trim());
        if indent == 0 {
            if value.is_empty() {
                section = key.to_string();
            } else {
                section.clear();
                map.insert(key.to_string(), value);
            }
        } else if section.is_empty() {
            bail!("第 {line_no} 行缩进了，但上面没有段落");
        } else {
            map.insert(format!("{section}.{key}"), value);
        }
    }
    Ok(map)
}

fn strip_comment(line: &str) -> String {
    let mut out = String::new();
    let mut quoted = false;
    for ch in line.chars() {
        if ch == '"' {
            quoted = !quoted;
        }
        if ch == '#' && !quoted {
            break;
        }
        out.push(ch);
    }
    out
}

fn unquote(value: &str) -> String {
    if value.len() >= 2 && value.starts_with('"') && value.ends_with('"') {
        return value[1..value.len() - 1].to_string();
    }
    if value.len() >= 2 && value.starts_with('\'') && value.ends_with('\'') {
        return value[1..value.len() - 1].to_string();
    }
    value.to_string()
}

fn req(map: &HashMap<String, String>, key: &str) -> Result<String> {
    map.get(key)
        .filter(|v| !v.is_empty())
        .cloned()
        .with_context(|| format!("配置缺少 {key}"))
}

fn opt_bool(map: &HashMap<String, String>, key: &str) -> Result<Option<bool>> {
    match map.get(key).map(|v| v.as_str()) {
        None => Ok(None),
        Some("true" | "yes" | "on") => Ok(Some(true)),
        Some("false" | "no" | "off") => Ok(Some(false)),
        Some(other) => bail!("{key} 必须是 true 或 false，收到 {other}"),
    }
}

fn opt_usize(map: &HashMap<String, String>, key: &str) -> Result<Option<usize>> {
    match map.get(key) {
        None => Ok(None),
        Some(value) => value
            .parse()
            .map(Some)
            .with_context(|| format!("{key} 必须是整数，收到 {value}")),
    }
}

fn opt_u16(map: &HashMap<String, String>, key: &str) -> Result<Option<u16>> {
    match map.get(key) {
        None => Ok(None),
        Some(value) => value
            .parse()
            .map(Some)
            .with_context(|| format!("{key} 必须是整数，收到 {value}")),
    }
}

fn opt_i32(map: &HashMap<String, String>, key: &str) -> Result<Option<i32>> {
    match map.get(key) {
        None => Ok(None),
        Some(value) => value
            .parse()
            .map(Some)
            .with_context(|| format!("{key} 必须是整数，收到 {value}")),
    }
}

fn absolutize(base: &Path, value: &str) -> String {
    let path = PathBuf::from(value);
    let full = if path.is_absolute() {
        path
    } else {
        base.join(path)
    };
    full.to_string_lossy().to_string()
}

fn ensure_cert_files(cfg: &ServerConfig) -> Result<()> {
    let cert = Path::new(&cfg.web.cert);
    let key = Path::new(&cfg.web.key);
    if cert.exists() && key.exists() {
        return Ok(());
    }
    if cert.exists() ^ key.exists() {
        bail!(
            "证书和私钥必须同时存在：{} 与 {}",
            cert.display(),
            key.display()
        );
    }
    if let Some(parent) = cert.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    #[cfg(unix)]
    {
        cert::write_self_signed(cert, key).context("生成本地自签名证书")?;
    }
    #[cfg(not(unix))]
    {
        let _ = (cert, key);
        bail!("这个 Windows 程序只包含客户端");
    }
    #[cfg(unix)]
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn yaml_roundtrip_keeps_fields() {
        let cfg = default_config();
        let text = render_yaml(&cfg);
        let parsed = ServerConfig::parse_yaml(&text).unwrap();
        assert_eq!(parsed.control_listen, "0.0.0.0:8024");
        assert_eq!(parsed.data_listen, "0.0.0.0:8025");
        assert_eq!(parsed.api_key, cfg.api_key);
        assert_eq!(parsed.web.listen, "0.0.0.0:8443");
        assert_eq!(parsed.tcp.pipe_size, 256 * 1024);
        assert_eq!(parsed.pool.idle, 16);
    }

    #[test]
    fn replacing_api_key_keeps_the_other_lines() {
        let dir = std::env::temp_dir().join(format!("punch-key-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("punch-server.yaml");
        fs::write(
            &path,
            "control_listen: \"127.0.0.1:6100\"\napi_key: \"old-key-1234\"\nweb:\n  listen: \"127.0.0.1:6102\"\n",
        )
        .unwrap();
        write_api_key(&path, "new-key-5678").unwrap();
        let text = fs::read_to_string(&path).unwrap();
        assert!(text.contains("api_key: \"new-key-5678\""));
        assert!(text.contains("control_listen: \"127.0.0.1:6100\""));
        assert!(text.contains("listen: \"127.0.0.1:6102\""));
        assert!(write_api_key(&path, "short").is_err());
        let _ = fs::remove_dir_all(&dir);
    }
}
