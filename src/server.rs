use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{timeout, MissedTickBehavior};

use crate::{debug, error, info, warn};
use crate::admin;
use crate::cert;
use crate::config::ServerConfig;
use crate::ioframe::{read_data_hello, read_msg, write_msg};
use crate::netutil::{self, tune_tokio};
use crate::proto::{encode_assign, CtrlMsg, DATA_BOUND, DATA_IDLE};
use crate::relay::relay;
use crate::state::{App, Client, Event, MapAck, MapState, OpenReq, Origin};
use crate::util::{self, lock, token_eq};

pub struct BoundServer {
    pub control: SocketAddr,
    pub data: SocketAddr,
    pub web: SocketAddr,
    app: Arc<App>,
    control_l: TcpListener,
    data_l: TcpListener,
    web_l: TcpListener,
    tls: Arc<cert::ServerCtx>,
}

pub fn bind(cfg: ServerConfig) -> Result<BoundServer> {
    let control_addr = cfg.control_addr()?;
    let data_addr = cfg.data_addr()?;
    let web_addr = cfg.web_addr()?;
    let control_l = netutil::bind_tcp(control_addr, &cfg.tcp)
        .with_context(|| format!("绑定控制通道 {control_addr}"))?;
    let data_l = netutil::bind_tcp(data_addr, &cfg.tcp)
        .with_context(|| format!("绑定数据通道 {data_addr}"))?;
    let web_l = netutil::bind_tcp(web_addr, &cfg.tcp)
        .with_context(|| format!("绑定管理页 {web_addr}"))?;
    let control = control_l.local_addr()?;
    let data = data_l.local_addr()?;
    let web = web_l.local_addr()?;
    let tls = cert::load_server_config(std::path::Path::new(&cfg.web.cert), std::path::Path::new(&cfg.web.key))?;
    let app = Arc::new(App {
        api_key: Mutex::new(cfg.api_key),
        config_path: cfg.config_path,
        tcp: cfg.tcp.clone(),
        pipe_size: cfg.tcp.pipe_size,
        max_idle: cfg.pool.max_idle as usize,
        pool_idle: cfg.pool.idle,
        pool_max: cfg.pool.max_idle,
        control_addr: control,
        data_addr: data,
        web_addr: web,
        clients: Default::default(),
        by_name: Default::default(),
        maps: Default::default(),
        next_map: std::sync::atomic::AtomicU32::new(1),
        next_stream: std::sync::atomic::AtomicU32::new(1),
        events: Default::default(),
        bytes_in: Default::default(),
        bytes_out: Default::default(),
        active: Default::default(),
        started_ms: util::now_ms(),
    });
    Ok(BoundServer {
        control,
        data,
        web,
        app,
        control_l,
        data_l,
        web_l,
        tls,
    })
}

impl BoundServer {
    pub async fn serve(self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let app = self.app.clone();
        let mut control = tokio::spawn(accept_control(
            self.control_l,
            app.clone(),
            shutdown.clone(),
        ));
        let mut data = tokio::spawn(accept_data(self.data_l, app.clone(), shutdown.clone()));
        let mut web = tokio::spawn(admin::serve(self.web_l, self.tls, app, shutdown.clone()));
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    info!("shutting down");
                }
            }
            res = &mut control => {
                error!("control accept loop ended: {res:?}");
            }
            res = &mut data => {
                error!("data accept loop ended: {res:?}");
            }
            res = &mut web => {
                error!("web accept loop ended: {res:?}");
            }
        }
        control.abort();
        data.abort();
        web.abort();
        let _ = control.await;
        let _ = data.await;
        let _ = web.await;
        Ok(())
    }
}

async fn accept_control(listener: TcpListener, app: Arc<App>, mut stop: watch::Receiver<bool>) {
    loop {
        if *stop.borrow() {
            break;
        }
        tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    break;
                }
            }
            acc = listener.accept() => {
                match acc {
                    Ok((sock, peer)) => {
                        tune_tokio(&sock, &app.tcp);
                        let app = app.clone();
                        tokio::spawn(async move {
                            if let Err(err) = handle_control(app, sock, peer).await {
                                debug!("control session ended: {err}");
                            }
                        });
                    }
                    Err(err) => {
                        warn!("control accept: {err}");
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                }
            }
        }
    }
}

async fn accept_data(listener: TcpListener, app: Arc<App>, mut stop: watch::Receiver<bool>) {
    loop {
        if *stop.borrow() {
            break;
        }
        tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    break;
                }
            }
            acc = listener.accept() => {
                match acc {
                    Ok((sock, _)) => {
                        tune_tokio(&sock, &app.tcp);
                        let app = app.clone();
                        tokio::spawn(async move {
                            handle_data(app, sock).await;
                        });
                    }
                    Err(err) => {
                        warn!("data accept: {err}");
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                }
            }
        }
    }
}

async fn handle_data(app: Arc<App>, mut sock: TcpStream) {
    let hello = match timeout(Duration::from_secs(10), read_data_hello(&mut sock)).await {
        Ok(Ok(hello)) => hello,
        _ => return,
    };
    let Some(client) = app.client_by_id(hello.client_id) else {
        return;
    };
    if !token_eq(&client.token, &hello.token) {
        return;
    }
    if hello.kind == DATA_IDLE {
        let mut queue = lock(&client.idle);
        if queue.len() >= app.max_idle {
            return;
        }
        queue.push_back(sock);
    } else if hello.kind == DATA_BOUND {
        if let Some(tx) = lock(&client.pending_streams).remove(&hello.stream_id) {
            let _ = tx.send(Ok(sock));
        }
    }
}

async fn handle_control(app: Arc<App>, sock: TcpStream, peer: SocketAddr) -> std::io::Result<()> {
    let (mut rd, mut wr) = sock.into_split();
    let hello = match timeout(Duration::from_secs(10), read_msg(&mut rd)).await {
        Ok(Ok(CtrlMsg::Hello {
            version,
            name,
            api_key,
        })) => (version, name, api_key),
        Ok(Ok(_)) => {
            let _ = write_msg(
                &mut wr,
                &CtrlMsg::HelloErr {
                    message: "expected hello".into(),
                },
            )
            .await;
            return Ok(());
        }
        _ => return Ok(()),
    };
    let (version, name, api_key) = hello;
    if version != crate::proto::PROTO_VERSION {
        let _ = write_msg(
            &mut wr,
            &CtrlMsg::HelloErr {
                message: format!("unsupported version {version}"),
            },
        )
        .await;
        return Ok(());
    }
    if name.is_empty() || name.len() > 64 {
        let _ = write_msg(
            &mut wr,
            &CtrlMsg::HelloErr {
                message: "client name must be 1..=64 bytes".into(),
            },
        )
        .await;
        return Ok(());
    }
    if !util::keys_equal(&api_key, &app.api_key()) {
        let _ = write_msg(
            &mut wr,
            &CtrlMsg::HelloErr {
                message: "invalid api key".into(),
            },
        )
        .await;
        warn!("rejected client, bad api key name={name} peer={peer}");
        return Ok(());
    }

    let (tx, mut rx) = mpsc::unbounded_channel();
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let client = Arc::new(Client {
        id: util::random_u64().max(1),
        name: name.clone(),
        addr: peer.to_string(),
        connected_ms: util::now_ms(),
        token: util::random_token(),
        ctrl: tx,
        idle: Default::default(),
        pending_streams: Default::default(),
        pending_acks: Default::default(),
        cancel: cancel_tx,
    });
    app.insert_client(client.clone());
    app.mark_cli_stale(&client.name);
    app.push_event(Event {
        ts_ms: util::now_ms(),
        kind: "client_online".into(),
        client_name: client.name.clone(),
        map_id: 0,
        message: format!("connected from {peer}"),
    });
    info!("client connected name={} id={} peer={peer}", client.name, client.id);

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
                                break;
                            }
                        }
                        None => break,
                    }
                }
            }
        }
    });

    let hello_ok = CtrlMsg::HelloOk {
        client_id: client.id,
        data_port: app.data_addr.port(),
        token: client.token,
        pool_idle: app.pool_idle,
        pool_max: app.pool_max,
    };
    if client.ctrl.send(hello_ok).is_err() {
        app.disconnect_client(client.id, &client.name);
        return Ok(());
    }
    replay_api_maps(&app, &client);

    let mut reader_stop = cancel_rx;
    let mut ticker = tokio::time::interval(Duration::from_secs(15));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut last = Instant::now();
    loop {
        tokio::select! {
            changed = reader_stop.changed() => {
                if changed.is_err() || *reader_stop.borrow() {
                    break;
                }
            }
            _ = ticker.tick() => {
                if last.elapsed() > Duration::from_secs(45) {
                    info!("control heartbeat timeout client={}", client.name);
                    break;
                }
                let _ = client.ctrl.send(CtrlMsg::Ping { ts_ms: util::now_ms() });
            }
            msg = read_msg(&mut rd) => {
                let msg = match msg {
                    Ok(msg) => msg,
                    Err(_) => break,
                };
                last = Instant::now();
                if !dispatch_control(&app, &client, msg) {
                    break;
                }
            }
        }
    }
    info!("client disconnected name={} id={}", client.name, client.id);
    app.push_event(Event {
        ts_ms: util::now_ms(),
        kind: "client_offline".into(),
        client_name: client.name.clone(),
        map_id: 0,
        message: "control connection closed".into(),
    });
    app.disconnect_client(client.id, &client.name);
    Ok(())
}

fn replay_api_maps(app: &App, client: &Client) {
    let maps: Vec<Arc<MapState>> = lock(&app.maps)
        .values()
        .filter(|m| m.origin == Origin::Api && m.client_name == client.name)
        .cloned()
        .collect();
    for map in maps {
        let _ = client.ctrl.send(map_open_msg(&map));
    }
}

fn map_open_msg(map: &MapState) -> CtrlMsg {
    CtrlMsg::MapOpen {
        map_id: map.id,
        bind_ip: map.bind_ip.clone(),
        bind_port: map.bind_port.load(Ordering::Relaxed),
        target_host: lock(&map.target_host).clone(),
        target_port: map.target_port.load(Ordering::Relaxed),
    }
}

fn dispatch_control(app: &Arc<App>, client: &Client, msg: CtrlMsg) -> bool {
    match msg {
        CtrlMsg::Ping { ts_ms } => {
            let _ = client.ctrl.send(CtrlMsg::Pong { ts_ms });
        }
        CtrlMsg::Pong { .. } => {}
        CtrlMsg::MapAdd {
            req_id,
            bind_ip,
            bind_port,
            target_host,
            target_port,
        } => {
            let req = OpenReq {
                client_name: client.name.clone(),
                bind_ip,
                bind_port,
                target_host,
                target_port,
                origin: Origin::Cli,
            };
            let result = match app.open_map(req) {
                Ok(map) => CtrlMsg::MapAddResult {
                    req_id,
                    map_id: map.id,
                    ok: true,
                    message: format!("listening on {}", map.bind_text()),
                },
                Err(err) => {
                    app.push_event(Event {
                        ts_ms: util::now_ms(),
                        kind: "bind_fail".into(),
                        client_name: client.name.clone(),
                        map_id: 0,
                        message: err.clone(),
                    });
                    CtrlMsg::MapAddResult {
                        req_id,
                        map_id: 0,
                        ok: false,
                        message: err,
                    }
                }
            };
            let _ = client.ctrl.send(result);
        }
        CtrlMsg::SyncDone => {
            for id in app.stale_cli_ids(&client.name) {
                let _ = app.close_map(id, false);
            }
        }
        CtrlMsg::MapOpenAck {
            map_id,
            status,
            message,
        } => {
            if let Some(map) = app.get_map(map_id) {
                if status == 0 {
                    app.clear_map_error(&map);
                } else {
                    app.note_map_error(&map, &message);
                }
            }
            if let Some(tx) = lock(&client.pending_acks).remove(&map_id) {
                let _ = tx.send(MapAck { status, message });
            }
        }
        CtrlMsg::ListenFail { map_id, message } => {
            if let Some(map) = app.get_map(map_id) {
                app.note_map_error(&map, &message);
            }
        }
        CtrlMsg::StreamFail {
            stream_id,
            map_id,
            message,
        } => {
            if let Some(map) = app.get_map(map_id) {
                app.note_map_error(&map, &message);
            }
            if let Some(tx) = lock(&client.pending_streams).remove(&stream_id) {
                let _ = tx.send(Err(message));
            }
        }
        other => {
            debug!("ignore control message client={} msg={other:?}", client.name);
        }
    }
    true
}

impl App {
    pub fn open_map(self: &Arc<Self>, req: OpenReq) -> Result<Arc<MapState>, String> {
        if req.client_name.is_empty() {
            return Err("client_name 不能为空".into());
        }
        if req.target_host.is_empty() || req.target_port == 0 {
            return Err("target 无效".into());
        }
        if req.target_host.len() > 255 {
            return Err("target 主机名过长".into());
        }
        let ip: IpAddr = req
            .bind_ip
            .parse()
            .map_err(|_| format!("bind ip 无效: {}", req.bind_ip))?;
        if let Some(existing) = self.find_cli_reuse(&req) {
            *lock(&existing.target_host) = req.target_host;
            existing.target_port.store(req.target_port, Ordering::Relaxed);
            existing.stale.store(false, Ordering::Relaxed);
            self.clear_map_error(&existing);
            return Ok(existing);
        }
        if req.bind_port != 0 {
            if self.find_listener(&req.bind_ip, req.bind_port).is_some() {
                return Err(format!("{}:{} 已经被映射占用", req.bind_ip, req.bind_port));
            }
        }
        let addr = SocketAddr::new(ip, req.bind_port);
        let listener = netutil::bind_tcp(addr, &self.tcp)
            .map_err(|err| format!("listen {addr} 失败: {err}"))?;
        let actual = listener
            .local_addr()
            .map_err(|err| format!("读取监听地址失败: {err}"))?;
        let id = self.next_map.fetch_add(1, Ordering::Relaxed);
        let (stop, stop_rx) = watch::channel(false);
        let map = Arc::new(MapState {
            id,
            client_name: req.client_name,
            origin: req.origin,
            bind_ip: req.bind_ip,
            requested_port: req.bind_port,
            bind_port: std::sync::atomic::AtomicU16::new(actual.port()),
            target_host: std::sync::Mutex::new(req.target_host),
            target_port: std::sync::atomic::AtomicU16::new(req.target_port),
            stop,
            active: Default::default(),
            bytes_in: Default::default(),
            bytes_out: Default::default(),
            accepts: Default::default(),
            fail_count: Default::default(),
            last_error: std::sync::Mutex::new(String::new()),
            last_error_ms: Default::default(),
            last_event_ms: Default::default(),
            stale: Default::default(),
        });
        lock(&self.maps).insert(id, map.clone());
        let app = Arc::clone(self);
        let accepted = map.clone();
        tokio::spawn(async move {
            accept_public(listener, accepted, app, stop_rx).await;
        });
        info!(
            "map listening id={} client={} bind={} target={}",
            map.id,
            map.client_name,
            map.bind_text(),
            map.target_text()
        );
        Ok(map)
    }

    pub fn close_map(&self, id: u32, notify: bool) -> Result<(), String> {
        let Some(map) = lock(&self.maps).remove(&id) else {
            return Err("mapping 不存在".into());
        };
        let _ = map.stop.send(true);
        if notify {
            if let Some(client) = self.client_by_name(&map.client_name) {
                let _ = client.ctrl.send(CtrlMsg::MapDel { map_id: id });
            }
        }
        info!("map closed id={id} bind={}", map.bind_text());
        Ok(())
    }

    pub async fn push_map(&self, map: &MapState) -> Result<MapAck, String> {
        let Some(client) = self.client_by_name(&map.client_name) else {
            return Err("client offline".into());
        };
        let (tx, rx) = oneshot::channel();
        lock(&client.pending_acks).insert(map.id, tx);
        if client.ctrl.send(map_open_msg(map)).is_err() {
            lock(&client.pending_acks).remove(&map.id);
            return Err("client disconnected".into());
        }
        match timeout(Duration::from_secs(2), rx).await {
            Ok(Ok(ack)) => Ok(ack),
            _ => {
                lock(&client.pending_acks).remove(&map.id);
                Err("client ack timeout".into())
            }
        }
    }
}

async fn accept_public(
    listener: TcpListener,
    map: Arc<MapState>,
    app: Arc<App>,
    mut stop: watch::Receiver<bool>,
) {
    loop {
        if *stop.borrow() {
            break;
        }
        tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    break;
                }
            }
            acc = listener.accept() => {
                match acc {
                    Ok((sock, _)) => {
                        tune_tokio(&sock, &app.tcp);
                        map.accepts.fetch_add(1, Ordering::Relaxed);
                        let map = map.clone();
                        let app = app.clone();
                        tokio::spawn(async move {
                            drive_public(app, map, sock).await;
                        });
                    }
                    Err(err) => {
                        warn!("public accept map={}: {err}", map.id);
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                }
            }
        }
    }
}

struct ActiveGuard {
    map: Arc<MapState>,
    app: Arc<App>,
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.map.active.fetch_sub(1, Ordering::Relaxed);
        self.app.active.fetch_sub(1, Ordering::Relaxed);
    }
}

async fn drive_public(app: Arc<App>, map: Arc<MapState>, public: TcpStream) {
    map.active.fetch_add(1, Ordering::Relaxed);
    app.active.fetch_add(1, Ordering::Relaxed);
    let _guard = ActiveGuard {
        map: map.clone(),
        app: app.clone(),
    };
    let Some(client) = app.client_by_name(&map.client_name) else {
        app.note_map_error(&map, "client offline");
        return;
    };
    let tunnel = if let Some(sock) = pop_idle(&client) {
        match assign_idle(sock, &app, &map).await {
            Ok(sock) => sock,
            Err(err) => {
                debug!("idle assign failed map={}: {err}", map.id);
                return;
            }
        }
    } else {
        match open_stream(&app, &client, &map).await {
            Ok(sock) => sock,
            Err(err) => {
                if err == "stream open timeout" || err == "client disconnected" {
                    app.note_map_error(&map, &err);
                }
                return;
            }
        }
    };
    let end = relay(public, tunnel, app.pipe_size).await;
    map.bytes_in.fetch_add(end.a_to_b, Ordering::Relaxed);
    map.bytes_out.fetch_add(end.b_to_a, Ordering::Relaxed);
    app.bytes_in.fetch_add(end.a_to_b, Ordering::Relaxed);
    app.bytes_out.fetch_add(end.b_to_a, Ordering::Relaxed);
    if let Some(err) = end.err {
        debug!("relay ended map={}: {err}", map.id);
    }
}

fn pop_idle(client: &Client) -> Option<TcpStream> {
    lock(&client.idle).pop_front()
}

async fn assign_idle(
    mut sock: TcpStream,
    app: &App,
    map: &MapState,
) -> std::io::Result<TcpStream> {
    let stream_id = app.next_stream.fetch_add(1, Ordering::Relaxed);
    let host = lock(&map.target_host).clone();
    let port = map.target_port.load(Ordering::Relaxed);
    let bytes = encode_assign(stream_id, map.id, port, &host);
    sock.write_all(&bytes).await?;
    Ok(sock)
}

async fn open_stream(app: &App, client: &Client, map: &MapState) -> Result<TcpStream, String> {
    let stream_id = app.next_stream.fetch_add(1, Ordering::Relaxed);
    let (tx, rx) = oneshot::channel();
    lock(&client.pending_streams).insert(stream_id, tx);
    let msg = CtrlMsg::OpenStream {
        stream_id,
        map_id: map.id,
        target_host: lock(&map.target_host).clone(),
        target_port: map.target_port.load(Ordering::Relaxed),
    };
    if client.ctrl.send(msg).is_err() {
        lock(&client.pending_streams).remove(&stream_id);
        return Err("client disconnected".into());
    }
    match timeout(Duration::from_secs(10), rx).await {
        Ok(Ok(Ok(sock))) => Ok(sock),
        Ok(Ok(Err(message))) => Err(message),
        Ok(Err(_)) => Err("client disconnected".into()),
        Err(_) => {
            lock(&client.pending_streams).remove(&stream_id);
            Err("stream open timeout".into())
        }
    }
}

