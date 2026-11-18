use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, watch};

use crate::config::TcpTune;
use crate::proto::CtrlMsg;
use crate::util::{self, lock};
use crate::{info, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    Cli,
    Api,
}

#[derive(Debug)]
pub struct MapAck {
    pub status: u8,
    pub message: String,
}

pub struct Client {
    pub id: u64,
    pub name: String,
    pub addr: String,
    pub connected_ms: u64,
    pub token: [u8; 16],
    pub ctrl: mpsc::UnboundedSender<CtrlMsg>,
    pub idle: Mutex<VecDeque<TcpStream>>,
    pub pending_streams: Mutex<HashMap<u32, oneshot::Sender<Result<TcpStream, String>>>>,
    pub pending_acks: Mutex<HashMap<u32, oneshot::Sender<MapAck>>>,
    pub cancel: watch::Sender<bool>,
}

pub struct MapState {
    pub id: u32,
    pub client_name: String,
    pub origin: Origin,
    pub bind_ip: String,
    pub requested_port: u16,
    pub bind_port: AtomicU16,
    pub target_host: Mutex<String>,
    pub target_port: AtomicU16,
    pub stop: watch::Sender<bool>,
    pub active: AtomicU64,
    pub bytes_in: AtomicU64,
    pub bytes_out: AtomicU64,
    pub accepts: AtomicU64,
    pub fail_count: AtomicU64,
    pub last_error: Mutex<String>,
    pub last_error_ms: AtomicU64,
    pub last_event_ms: AtomicU64,
    pub stale: AtomicBool,
}

impl MapState {
    pub fn bind_text(&self) -> String {
        format!(
            "{}:{}",
            self.bind_ip,
            self.bind_port.load(Ordering::Relaxed)
        )
    }

    pub fn target_text(&self) -> String {
        format!(
            "{}:{}",
            lock(&self.target_host),
            self.target_port.load(Ordering::Relaxed)
        )
    }
}

#[derive(Clone, Debug)]
pub struct Event {
    pub ts_ms: u64,
    pub kind: String,
    pub client_name: String,
    pub map_id: u32,
    pub message: String,
}

pub struct OpenReq {
    pub client_name: String,
    pub bind_ip: String,
    pub bind_port: u16,
    pub target_host: String,
    pub target_port: u16,
    pub origin: Origin,
}

pub struct App {
    pub api_key: Mutex<String>,
    pub config_path: PathBuf,
    pub tcp: TcpTune,
    pub pipe_size: usize,
    pub max_idle: usize,
    pub pool_idle: u16,
    pub pool_max: u16,
    pub control_addr: SocketAddr,
    pub data_addr: SocketAddr,
    pub web_addr: SocketAddr,
    pub clients: Mutex<HashMap<u64, Arc<Client>>>,
    pub by_name: Mutex<HashMap<String, u64>>,
    pub maps: Mutex<HashMap<u32, Arc<MapState>>>,
    pub next_map: AtomicU32,
    pub next_stream: AtomicU32,
    pub events: Mutex<VecDeque<Event>>,
    pub bytes_in: AtomicU64,
    pub bytes_out: AtomicU64,
    pub active: AtomicU64,
    pub started_ms: u64,
}

impl App {
    pub fn api_key(&self) -> String {
        lock(&self.api_key).clone()
    }

    pub fn client_by_name(&self, name: &str) -> Option<Arc<Client>> {
        let id = *lock(&self.by_name).get(name)?;
        lock(&self.clients).get(&id).cloned()
    }

    pub fn client_by_id(&self, id: u64) -> Option<Arc<Client>> {
        lock(&self.clients).get(&id).cloned()
    }

    pub fn get_map(&self, id: u32) -> Option<Arc<MapState>> {
        lock(&self.maps).get(&id).cloned()
    }

    pub fn push_event(&self, event: Event) {
        let mut q = lock(&self.events);
        q.push_front(event);
        while q.len() > 200 {
            q.pop_back();
        }
    }

    pub fn note_map_error(&self, map: &MapState, msg: &str) {
        if msg.is_empty() {
            return;
        }
        map.fail_count.fetch_add(1, Ordering::Relaxed);
        let mut cur = lock(&map.last_error);
        let changed = cur.as_str() != msg;
        if changed {
            *cur = msg.to_string();
        }
        drop(cur);
        let now = util::now_ms();
        map.last_error_ms.store(now, Ordering::Relaxed);
        let prev = map.last_event_ms.load(Ordering::Relaxed);
        if changed || now.saturating_sub(prev) > 5_000 {
            map.last_event_ms.store(now, Ordering::Relaxed);
            let kind = if msg.starts_with("listen failed") {
                "listen_fail"
            } else {
                "target_error"
            };
            self.push_event(Event {
                ts_ms: now,
                kind: kind.to_string(),
                client_name: map.client_name.clone(),
                map_id: map.id,
                message: msg.to_string(),
            });
            warn!("map error client={} map={}: {msg}", map.client_name, map.id);
        }
    }

    pub fn clear_map_error(&self, map: &MapState) {
        *lock(&map.last_error) = String::new();
        map.last_error_ms.store(0, Ordering::Relaxed);
    }

    pub fn mark_cli_stale(&self, name: &str) {
        for map in lock(&self.maps).values() {
            if map.origin == Origin::Cli && map.client_name == name {
                map.stale.store(true, Ordering::Relaxed);
            }
        }
    }

    pub fn stale_cli_ids(&self, name: &str) -> Vec<u32> {
        lock(&self.maps)
            .values()
            .filter(|m| m.origin == Origin::Cli && m.client_name == name && m.stale.load(Ordering::Relaxed))
            .map(|m| m.id)
            .collect()
    }

    pub fn find_cli_reuse(&self, req: &OpenReq) -> Option<Arc<MapState>> {
        if req.origin != Origin::Cli {
            return None;
        }
        lock(&self.maps).values().find(|m| {
            m.origin == Origin::Cli
                && m.client_name == req.client_name
                && m.bind_ip == req.bind_ip
                && m.requested_port == req.bind_port
        }).cloned()
    }

    pub fn find_listener(&self, ip: &str, port: u16) -> Option<Arc<MapState>> {
        lock(&self.maps).values().find(|m| {
            m.bind_ip == ip && m.bind_port.load(Ordering::Relaxed) == port
        }).cloned()
    }

    pub fn insert_client(&self, client: Arc<Client>) {
        let mut names = lock(&self.by_name);
        if let Some(old_id) = names.insert(client.name.clone(), client.id) {
            if let Some(old) = lock(&self.clients).remove(&old_id) {
                let _ = old.cancel.send(true);
                fail_client_waiters(&old);
                info!("replaced previous session client={} id={}", old.name, old.id);
            }
        }
        lock(&self.clients).insert(client.id, client);
    }

    pub fn disconnect_client(&self, id: u64, name: &str) {
        {
            let mut names = lock(&self.by_name);
            if names.get(name).copied() == Some(id) {
                names.remove(name);
            }
        }
        if let Some(client) = lock(&self.clients).remove(&id) {
            let _ = client.cancel.send(true);
            fail_client_waiters(&client);
        }
    }
}

pub fn fail_client_waiters(client: &Client) {
    for (_, tx) in lock(&client.pending_streams).drain() {
        let _ = tx.send(Err("client disconnected".into()));
    }
    for (_, tx) in lock(&client.pending_acks).drain() {
        let _ = tx.send(MapAck {
            status: 1,
            message: "client disconnected".into(),
        });
    }
    lock(&client.idle).clear();
}
