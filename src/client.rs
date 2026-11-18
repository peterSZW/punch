use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, watch, Notify};
use tokio::time::timeout;

use crate::bench;
use crate::{debug, error, info, warn};
use crate::config::TcpTune;
use crate::ioframe::{read_assign, read_msg, write_msg};
use crate::netutil::{classify_dial, connect_tuned, dial};
use crate::proto::{encode_data_hello, CtrlMsg, DataHello, DATA_BOUND, DATA_IDLE, PROTO_VERSION};
use crate::relay::relay;
use crate::util::lock;

#[derive(Clone, Debug)]
pub struct PortMap {
    pub bind_ip: String,
    pub bind_port: u16,
    pub target_host: String,
    pub target_port: u16,
}

#[derive(Clone, Debug)]
pub struct Options {
    pub server: SocketAddr,
    pub secret: String,
    pub expose: Option<PortMap>,
    pub bench: Option<SocketAddr>,
    pub pool: Option<u16>,
    pub local_dir: Option<PathBuf>,
}

#[derive(Debug)]
struct StopRetry(String);

impl std::fmt::Display for StopRetry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for StopRetry {}

pub async fn run(opts: Options, mut shutdown: watch::Receiver<bool>) -> Result<()> {
    if let Some(addr) = opts.bench {
        let bound = bench::serve(addr, shutdown.clone()).await?;
        info!("bench http listening {bound}");
        println!("bench http  http://{bound}/bench?size=1048576");
    }
    let mut backoff = Duration::from_secs(1);
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        match session(&opts, shutdown.clone()).await {
            Ok(()) => return Ok(()),
            Err(err) => {
                if err.downcast_ref::<StopRetry>().is_some() {
                    return Err(err);
                }
                warn!("control disconnected, retrying: {err:#}");
                tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            return Ok(());
                        }
                    }
                    _ = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(Duration::from_secs(15));
            }
        }
    }
}

struct KnownMap {
    bind: String,
    target: String,
}

struct Session {
    ctrl: mpsc::UnboundedSender<CtrlMsg>,
    hello: Mutex<Option<oneshot::Sender<Result<(), String>>>>,
    map_wait: Mutex<HashMap<u32, oneshot::Sender<CtrlMsg>>>,
    maps: Mutex<HashMap<u32, KnownMap>>,
    client_id: AtomicU64,
    token: Mutex<[u8; 16]>,
    data_addr: Mutex<Option<SocketAddr>>,
    name: Mutex<String>,
    allow_maps: AtomicBool,
    ready: AtomicBool,
    replaced: AtomicBool,
    halt: Mutex<String>,
    pool_idle: AtomicU16,
    pool_max: AtomicU16,
    tune: TcpTune,
    server_ip: IpAddr,
    next_req: AtomicU32,
}

struct CancelOnDrop(watch::Sender<bool>);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        let _ = self.0.send(true);
    }
}

async fn session(opts: &Options, mut shutdown: watch::Receiver<bool>) -> Result<()> {
    let sock = timeout(
        Duration::from_secs(10),
        connect_tuned(opts.server, &TcpTune::default()),
    )
    .await
    .context("连接服务端超时")?
    .context("连接服务端失败")?;
    let (mut rd, mut wr) = sock.into_split();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let _cancel = CancelOnDrop(cancel_tx.clone());
    let session = Arc::new(Session {
        ctrl: tx,
        hello: Mutex::new(None),
        map_wait: Mutex::new(HashMap::new()),
        maps: Mutex::new(HashMap::new()),
        client_id: AtomicU64::new(0),
        token: Mutex::new([0u8; 16]),
        data_addr: Mutex::new(None),
        name: Mutex::new(String::new()),
        allow_maps: AtomicBool::new(false),
        ready: AtomicBool::new(false),
        replaced: AtomicBool::new(false),
        halt: Mutex::new(String::new()),
        pool_idle: AtomicU16::new(0),
        pool_max: AtomicU16::new(0),
        tune: TcpTune::default(),
        server_ip: opts.server.ip(),
        next_req: AtomicU32::new(1),
    });

    let mut writer_stop = cancel_rx.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                changed = writer_stop.changed() => {
                    if changed.is_err() || *writer_stop.borrow() {
                        break;
                    }
                }
                msg = rx.recv() => {
                    match msg {
                        Some(msg) => {
                            if write_msg(&mut wr, &msg).await.is_err() {
                                let _ = writer_stop.borrow();
                                break;
                            }
                        }
                        None => break,
                    }
                }
            }
        }
        let _ = cancel_tx.send(true);
    });

    let reader = {
        let session = session.clone();
        let mut stop = cancel_rx.clone();
        tokio::spawn(async move { read_loop(&session, &mut rd, &mut stop).await })
    };

    let (hello_tx, hello_rx) = oneshot::channel();
    *lock(&session.hello) = Some(hello_tx);
    session
        .ctrl
        .send(CtrlMsg::Hello {
            version: PROTO_VERSION,
            secret: opts.secret.clone(),
        })
        .map_err(|_| anyhow!("control writer closed"))?;
    timeout(Duration::from_secs(10), hello_rx)
        .await
        .context("等待服务端握手超时")?
        .map_err(|_| anyhow!("handshake cancelled"))?
        .map_err(|err| anyhow!(err))?;

    let advertised = session.pool_idle.load(Ordering::Relaxed).max(1);
    let max = session.pool_max.load(Ordering::Relaxed).max(advertised);
    let mut target = opts.pool.unwrap_or(advertised);
    if target > max {
        target = max;
    }
    if target == 0 {
        target = 1;
    }
    let client_name = lock(&session.name).clone();
    info!(
        "control connected client={client_name} data={:?} pool={target}",
        lock(&session.data_addr)
    );
    let local = serve_local(session.clone()).await?;
    let addr_path = write_addr_file(opts, &client_name, local)?;
    let _addr_file = AddrFile(addr_path);
    println!("本地命令  {local}    punch map list");
    let pool_stop = cancel_rx;
    let pool_session = session.clone();
    tokio::spawn(async move {
        run_pool(pool_session, target as usize, pool_stop).await;
    });

    if let Some(map) = &opts.expose {
        if let Err(err) = register_map(&session, map).await {
            warn!("expose bench: {err}");
            println!("expose bench 失败  {err}");
        }
    }

    tokio::select! {
        changed = shutdown.changed() => {
            if changed.is_err() || *shutdown.borrow() {
                return Ok(());
            }
        }
        res = reader => {
            if session.replaced.load(Ordering::Relaxed) {
                let message = lock(&session.halt).clone();
                let message = if message.is_empty() {
                    "这个 secret 已经不能继续用在这台机器上".into()
                } else {
                    message
                };
                return Err(StopRetry(message).into());
            }
            if *shutdown.borrow() {
                return Ok(());
            }
            return match res {
                Ok(Ok(())) => Err(anyhow!("control closed")),
                Ok(Err(err)) => Err(err.into()),
                Err(err) => Err(err.into()),
            };
        }
    }
    Ok(())
}

async fn read_loop(
    session: &Arc<Session>,
    rd: &mut tokio::net::tcp::OwnedReadHalf,
    stop: &mut watch::Receiver<bool>,
) -> io::Result<()> {
    let mut last = std::time::Instant::now();
    let mut ticker = tokio::time::interval(Duration::from_secs(15));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    return Ok(());
                }
            }
            _ = ticker.tick() => {
                if last.elapsed() > Duration::from_secs(45) {
                    return Err(io::Error::new(io::ErrorKind::TimedOut, "control heartbeat timeout"));
                }
            }
            msg = read_msg(rd) => {
                let msg = msg?;
                last = std::time::Instant::now();
                if !handle_server(session, msg) {
                    return Ok(());
                }
            }
        }
    }
}

fn handle_server(session: &Arc<Session>, msg: CtrlMsg) -> bool {
    let forward = match &msg {
        CtrlMsg::MapAddResult { req_id, .. }
        | CtrlMsg::MapUpdateResult { req_id, .. }
        | CtrlMsg::MapDelResult { req_id, .. } => Some(*req_id),
        _ => None,
    };
    if let Some(req_id) = forward {
        if let Some(tx) = lock(&session.map_wait).remove(&req_id) {
            let _ = tx.send(msg);
        }
        return true;
    }
    match msg {
        CtrlMsg::HelloOk {
            client_id,
            data_port,
            token,
            pool_idle,
            pool_max,
            name,
            allow_maps,
        } => {
            session.client_id.store(client_id, Ordering::Relaxed);
            *lock(&session.token) = token;
            *lock(&session.data_addr) = Some(SocketAddr::new(session.server_ip, data_port));
            *lock(&session.name) = name;
            session.allow_maps.store(allow_maps, Ordering::Relaxed);
            session.pool_idle.store(pool_idle, Ordering::Relaxed);
            session.pool_max.store(pool_max, Ordering::Relaxed);
            if let Some(tx) = lock(&session.hello).take() {
                let _ = tx.send(Ok(()));
            }
        }
        CtrlMsg::HelloErr { message } => {
            if let Some(tx) = lock(&session.hello).take() {
                let _ = tx.send(Err(message));
            }
        }
        CtrlMsg::Superseded { message } => {
            *lock(&session.halt) = message;
            session.replaced.store(true, Ordering::Relaxed);
            return false;
        }
        CtrlMsg::Ping { ts_ms } => {
            let _ = session.ctrl.send(CtrlMsg::Pong { ts_ms });
        }
        CtrlMsg::Pong { .. } | CtrlMsg::SyncDone => {}
        CtrlMsg::Allow { allow } => {
            session.allow_maps.store(allow, Ordering::Relaxed);
        }
        CtrlMsg::Snapshot => {
            session.ready.store(true, Ordering::Relaxed);
        }
        CtrlMsg::MapDel { map_id } => {
            lock(&session.maps).remove(&map_id);
        }
        CtrlMsg::MapOpen {
            map_id,
            bind_ip,
            bind_port,
            target_host,
            target_port,
        } => {
            lock(&session.maps).insert(
                map_id,
                KnownMap {
                    bind: format!("{bind_ip}:{bind_port}"),
                    target: format!("{target_host}:{target_port}"),
                },
            );
            let ctrl = session.ctrl.clone();
            tokio::spawn(async move {
                let (status, message) = probe(&target_host, target_port).await;
                let _ = ctrl.send(CtrlMsg::MapOpenAck {
                    map_id,
                    status,
                    message,
                });
            });
        }
        CtrlMsg::OpenStream {
            stream_id,
            map_id,
            target_host,
            target_port,
        } => {
            let session = session.clone();
            tokio::spawn(async move {
                if let Err(message) =
                    open_bound(&session, stream_id, &target_host, target_port).await
                {
                    let _ = session.ctrl.send(CtrlMsg::StreamFail {
                        stream_id,
                        map_id,
                        message,
                    });
                }
            });
        }
        other => {
            debug!("ignore server message {other:?}");
        }
    }
    true
}

async fn register_map(session: &Session, map: &PortMap) -> std::result::Result<String, String> {
    let req_id = session.next_req.fetch_add(1, Ordering::Relaxed);
    let (tx, rx) = oneshot::channel();
    lock(&session.map_wait).insert(req_id, tx);
    if session
        .ctrl
        .send(CtrlMsg::MapAdd {
            req_id,
            bind_ip: map.bind_ip.clone(),
            bind_port: map.bind_port,
            target_host: map.target_host.clone(),
            target_port: map.target_port,
        })
        .is_err()
    {
        return Err("控制连接已断开".into());
    }
    let result = timeout(Duration::from_secs(5), rx).await;
    let msg = match result {
        Ok(Ok(msg)) => msg,
        _ => {
            lock(&session.map_wait).remove(&req_id);
            return Err(format!(
                "服务端没有确认这条映射 {}:{}",
                map.bind_ip, map.bind_port
            ));
        }
    };
    if let CtrlMsg::MapAddResult {
        ok,
        map_id,
        message,
        ..
    } = msg
    {
        if ok {
            info!("map {map_id} {message}");
            println!(
                "map {map_id}  {bind} -> {target}  {message}",
                bind = format!("{}:{}", map.bind_ip, map.bind_port),
                target = format!("{}:{}", map.target_host, map.target_port),
            );
            let ctrl = session.ctrl.clone();
            let host = map.target_host.clone();
            let port = map.target_port;
            tokio::spawn(async move {
                let (status, message) = probe(&host, port).await;
                if status != 0 {
                    warn!("map {map_id} {message}");
                    let _ = ctrl.send(CtrlMsg::ListenFail { map_id, message });
                }
            });
            return Ok(format!("ok id={map_id} {message}"));
        }
        error!(
            "map 失败 {}:{} {message}",
            map.bind_ip, map.bind_port
        );
        println!(
            "map 失败  {}:{}  {message}",
            map.bind_ip, map.bind_port
        );
        return Err(message);
    }
    Err("服务端返回了意外的结果".into())
}

struct AddrFile(PathBuf);

impl Drop for AddrFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn write_addr_file(opts: &Options, name: &str, local: SocketAddr) -> Result<PathBuf> {
    let dir = match &opts.local_dir {
        Some(dir) => dir.clone(),
        None => std::env::current_dir().context("读取当前目录")?,
    };
    let path = dir.join(format!("punch.{name}.client.addr"));
    std::fs::write(&path, format!("{local}\n")).with_context(|| format!("写入 {}", path.display()))?;
    Ok(path)
}

async fn serve_local(session: Arc<Session>) -> Result<SocketAddr> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .context("监听本地命令端口")?;
    let addr = listener.local_addr().context("读取本地命令端口")?;
    tokio::spawn(async move {
        loop {
            let Ok((sock, peer)) = listener.accept().await else {
                break;
            };
            if !peer.ip().is_loopback() {
                continue;
            }
            let session = session.clone();
            tokio::spawn(async move {
                let _ = local_conn(session, sock).await;
            });
        }
    });
    Ok(addr)
}

async fn local_conn(session: Arc<Session>, mut sock: TcpStream) -> io::Result<()> {
    let mut buf = Vec::new();
    let _ = timeout(Duration::from_secs(3), sock.read_to_end(&mut buf)).await;
    if buf.len() > 2048 {
        buf.truncate(2048);
    }
    let line = String::from_utf8_lossy(&buf).trim().to_string();
    let reply = match local_command(&session, &line).await {
        Ok(text) => text,
        Err(err) => format!("error {err}"),
    };
    let mut out = reply;
    if !out.ends_with('\n') {
        out.push('\n');
    }
    sock.write_all(out.as_bytes()).await
}

async fn local_command(session: &Session, line: &str) -> std::result::Result<String, String> {
    if !session.allow_maps.load(Ordering::Relaxed) {
        return Err("服务端没有允许这个客户端自己改映射".into());
    }
    let mut parts = line.split_whitespace();
    let cmd = parts.next().unwrap_or("");
    match cmd {
        "LIST" => {
            if !session.ready.load(Ordering::Relaxed) {
                return Err("映射还在同步".into());
            }
            let maps = lock(&session.maps);
            if maps.is_empty() {
                return Ok("ok".into());
            }
            let mut ids: Vec<u32> = maps.keys().copied().collect();
            ids.sort_unstable();
            let mut out = String::from("ok");
            for id in ids {
                if let Some(map) = maps.get(&id) {
                    out.push('\n');
                    out.push_str(&format!("{id} {} {}", map.bind, map.target));
                }
            }
            Ok(out)
        }
        "ADD" => {
            let bind = parts.next().ok_or_else(|| "用法: ADD bind target".to_string())?;
            let target = parts.next().ok_or_else(|| "用法: ADD bind target".to_string())?;
            let map = parse_port_map(bind, target)?;
            register_map(session, &map).await
        }
        "UPDATE" => {
            let id: u32 = parts
                .next()
                .ok_or_else(|| "用法: UPDATE id bind target".to_string())?
                .parse()
                .map_err(|_| "id 必须是数字".to_string())?;
            let bind = parts.next().ok_or_else(|| "用法: UPDATE id bind target".to_string())?;
            let target = parts.next().ok_or_else(|| "用法: UPDATE id bind target".to_string())?;
            let map = parse_port_map(bind, target)?;
            update_map(session, id, &map).await
        }
        "DEL" => {
            let id: u32 = parts
                .next()
                .ok_or_else(|| "用法: DEL id".to_string())?
                .parse()
                .map_err(|_| "id 必须是数字".to_string())?;
            delete_map(session, id).await
        }
        _ => Err("命令是 LIST、ADD、UPDATE、DEL".into()),
    }
}

fn parse_port_map(bind: &str, target: &str) -> std::result::Result<PortMap, String> {
    let (bind_ip, bind_port) = crate::util::parse_bind(bind)?;
    let (target_host, target_port) = crate::util::parse_target(target)?;
    Ok(PortMap {
        bind_ip,
        bind_port,
        target_host,
        target_port,
    })
}

async fn update_map(session: &Session, id: u32, map: &PortMap) -> std::result::Result<String, String> {
    let req_id = session.next_req.fetch_add(1, Ordering::Relaxed);
    let (tx, rx) = oneshot::channel();
    lock(&session.map_wait).insert(req_id, tx);
    if session
        .ctrl
        .send(CtrlMsg::MapUpdate {
            req_id,
            map_id: id,
            bind_ip: map.bind_ip.clone(),
            bind_port: map.bind_port,
            target_host: map.target_host.clone(),
            target_port: map.target_port,
        })
        .is_err()
    {
        return Err("控制连接已断开".into());
    }
    let msg = match timeout(Duration::from_secs(5), rx).await {
        Ok(Ok(msg)) => msg,
        _ => {
            lock(&session.map_wait).remove(&req_id);
            return Err("服务端没有确认修改".into());
        }
    };
    if let CtrlMsg::MapUpdateResult { ok, message, map_id, .. } = msg {
        if ok {
            return Ok(format!("ok id={map_id} {message}"));
        }
        return Err(message);
    }
    Err("服务端返回了意外的结果".into())
}

async fn delete_map(session: &Session, id: u32) -> std::result::Result<String, String> {
    let req_id = session.next_req.fetch_add(1, Ordering::Relaxed);
    let (tx, rx) = oneshot::channel();
    lock(&session.map_wait).insert(req_id, tx);
    if session
        .ctrl
        .send(CtrlMsg::MapDelReq { req_id, map_id: id })
        .is_err()
    {
        return Err("控制连接已断开".into());
    }
    let msg = match timeout(Duration::from_secs(5), rx).await {
        Ok(Ok(msg)) => msg,
        _ => {
            lock(&session.map_wait).remove(&req_id);
            return Err("服务端没有确认删除".into());
        }
    };
    if let CtrlMsg::MapDelResult { ok, message, .. } = msg {
        if ok {
            return Ok(format!("ok id={id}"));
        }
        return Err(message);
    }
    Err("服务端返回了意外的结果".into())
}

async fn probe(host: &str, port: u16) -> (u8, String) {
    match timeout(Duration::from_millis(500), TcpStream::connect((host, port))).await {
        Ok(Ok(sock)) => {
            drop(sock);
            (0, String::new())
        }
        Ok(Err(err)) => (1, classify_dial(&err)),
        Err(_) => (
            1,
            format!("target unreachable: dial {host}:{port} timed out"),
        ),
    }
}

async fn open_bound(
    session: &Session,
    stream_id: u32,
    host: &str,
    port: u16,
) -> Result<(), String> {
    let target = dial_target(&session.tune, host, port).await?;
    let addr = lock(&session.data_addr).ok_or_else(|| "data channel unknown".to_string())?;
    let mut data = connect_tuned(addr, &session.tune)
        .await
        .map_err(|err| format!("target unreachable: data channel: {err}"))?;
    let hello = DataHello {
        kind: DATA_BOUND,
        client_id: session.client_id.load(Ordering::Relaxed),
        token: *lock(&session.token),
        stream_id,
    };
    data.write_all(&encode_data_hello(&hello))
        .await
        .map_err(|err| format!("target unreachable: {err}"))?;
    let _ = relay(data, target, session.tune.pipe_size).await;
    Ok(())
}

async fn dial_target(tune: &TcpTune, host: &str, port: u16) -> Result<TcpStream, String> {
    match timeout(Duration::from_secs(10), dial(host, port, tune)).await {
        Ok(Ok(sock)) => Ok(sock),
        Ok(Err(err)) => Err(classify_dial(&err)),
        Err(_) => Err(format!("target unreachable: dial {host}:{port} timed out")),
    }
}

struct Slot {
    idle: Arc<AtomicUsize>,
    notify: Arc<Notify>,
    armed: bool,
}

impl Slot {
    fn release(&mut self) {
        if self.armed {
            self.armed = false;
            self.idle.fetch_sub(1, Ordering::Relaxed);
            self.notify.notify_one();
        }
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        self.release();
    }
}

enum IdleOutcome {
    Assigned(TcpStream, u32, u32, u16, String),
    Failed,
    Cancelled,
}

async fn run_pool(session: Arc<Session>, target: usize, mut cancel: watch::Receiver<bool>) {
    let idle = Arc::new(AtomicUsize::new(0));
    let notify = Arc::new(Notify::new());
    loop {
        if *cancel.borrow() {
            break;
        }
        if idle.load(Ordering::Relaxed) >= target {
            tokio::select! {
                changed = cancel.changed() => {
                    if changed.is_err() || *cancel.borrow() {
                        break;
                    }
                }
                _ = notify.notified() => {}
            }
            continue;
        }
        idle.fetch_add(1, Ordering::Relaxed);
        let slot = Slot {
            idle: idle.clone(),
            notify: notify.clone(),
            armed: true,
        };
        let session = session.clone();
        let mut stop = cancel.clone();
        tokio::spawn(async move {
            hold_slot(session, slot, &mut stop).await;
        });
    }
}

async fn hold_slot(session: Arc<Session>, mut slot: Slot, cancel: &mut watch::Receiver<bool>) {
    match connect_idle(&session, cancel).await {
        IdleOutcome::Assigned(sock, _stream_id, map_id, port, host) => {
            slot.release();
            if let Err(message) = bridge_idle(&session, sock, &host, port).await {
                warn!("map {map_id} {message}");
                let _ = session.ctrl.send(CtrlMsg::ListenFail { map_id, message });
            }
        }
        IdleOutcome::Failed => {
            if !*cancel.borrow() {
                tokio::select! {
                    _ = cancel.changed() => {}
                    _ = tokio::time::sleep(Duration::from_millis(300)) => {}
                }
            }
        }
        IdleOutcome::Cancelled => {}
    }
}

async fn connect_idle(session: &Session, cancel: &mut watch::Receiver<bool>) -> IdleOutcome {
    if *cancel.borrow() {
        return IdleOutcome::Cancelled;
    }
    let Some(addr) = *lock(&session.data_addr) else {
        return IdleOutcome::Failed;
    };
    let mut sock = match connect_tuned(addr, &session.tune).await {
        Ok(sock) => sock,
        Err(_) => return IdleOutcome::Failed,
    };
    let hello = DataHello {
        kind: DATA_IDLE,
        client_id: session.client_id.load(Ordering::Relaxed),
        token: *lock(&session.token),
        stream_id: 0,
    };
    if sock.write_all(&encode_data_hello(&hello)).await.is_err() {
        return IdleOutcome::Failed;
    }
    tokio::select! {
        changed = cancel.changed() => {
            if changed.is_err() || *cancel.borrow() {
                IdleOutcome::Cancelled
            } else {
                IdleOutcome::Failed
            }
        }
        res = read_assign(&mut sock) => {
            match res {
                Ok((stream_id, map_id, port, host)) => {
                    IdleOutcome::Assigned(sock, stream_id, map_id, port, host)
                }
                Err(_) => IdleOutcome::Failed,
            }
        }
    }
}

async fn bridge_idle(session: &Session, sock: TcpStream, host: &str, port: u16) -> Result<(), String> {
    let target = dial_target(&session.tune, host, port).await?;
    let _ = relay(sock, target, session.tune.pipe_size).await;
    Ok(())
}
