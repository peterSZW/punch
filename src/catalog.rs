//! 客户端 secret 和端口映射的持久化。文件在配置旁边，叫 punch-clients.json。
//! 管理页的 api key 不在这里，客户端也不能拿那把 key 登录。

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::util::random_hex;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClientRecord {
    pub name: String,
    pub secret: String,
    pub allow_maps: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SavedMap {
    pub id: u32,
    pub client_name: String,
    pub bind_ip: String,
    pub bind_port: u16,
    pub target_host: String,
    pub target_port: u16,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct FileBody {
    clients: Vec<ClientRecord>,
    maps: Vec<SavedMap>,
}

pub fn path_for(config_path: &Path) -> PathBuf {
    let dir = config_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    dir.join("punch-clients.json")
}

pub fn load(path: &Path) -> Result<(Vec<ClientRecord>, Vec<SavedMap>)> {
    if !path.exists() {
        return Ok((Vec::new(), Vec::new()));
    }
    let text = fs::read_to_string(path).with_context(|| format!("读取 {}", path.display()))?;
    let body: FileBody = serde_json::from_str(&text)
        .with_context(|| format!("解析 {}", path.display()))?;
    Ok((body.clients, body.maps))
}

pub fn save(path: &Path, clients: &[ClientRecord], maps: &[SavedMap]) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .with_context(|| format!("创建目录 {}", parent.display()))?;
        }
    }
    let body = FileBody {
        clients: clients.to_vec(),
        maps: maps.to_vec(),
    };
    let text = serde_json::to_string_pretty(&body).context("编码客户端配置")?;
    let name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("punch-clients.json");
    let tmp = path.with_file_name(format!(".{name}.tmp"));
    fs::write(&tmp, text).with_context(|| format!("写入 {}", tmp.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(&tmp, fs::Permissions::from_mode(0o600));
    }
    fs::rename(&tmp, path).with_context(|| format!("替换 {}", path.display()))?;
    Ok(())
}

pub fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-')
}

pub fn valid_secret(secret: &str) -> Result<()> {
    if secret.len() < 8 || secret.len() > 128 {
        bail!("secret 需要 8 到 128 个字符");
    }
    if secret.chars().any(|c| c.is_control() || c.is_whitespace() || c == '"' || c == '\\') {
        bail!("secret 不能包含空白、引号、反斜杠或控制字符");
    }
    Ok(())
}

pub fn new_secret() -> String {
    random_hex(24)
}
