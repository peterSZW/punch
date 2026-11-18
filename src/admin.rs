//! HTTPS 管理接口。页面编译进二进制。
//!
//! 增加一个接口：
//! 1. 写 `async fn my_api(app: Arc<App>, req: Request) -> Response`
//! 2. 在 `routes()` 里加一行，例如 `Route::get("/api/my-api", my_api)`
//!
//! `/api/...` 会校验 `X-Api-Key` 或 `Authorization: Bearer <key>`。
//! 路径里的 `:id` 用 `req.param("id")` 读取。

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::sync::watch;

use crate::cert::ServerCtx;
use crate::minihttp::{HttpConn, Method, RawRequest};
use crate::{debug, warn};
use crate::relay::io_mode;
use crate::catalog::{self, ClientRecord};
use crate::proto::CtrlMsg;
use crate::state::{App, OpenReq};
use crate::util::{self, lock};

pub const INDEX_HTML: &str = include_str!("web.html");

pub async fn serve(
    listener: TcpListener,
    tls: Arc<ServerCtx>,
    app: Arc<App>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        if *shutdown.borrow() {
            break;
        }
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            acc = listener.accept() => {
                let Ok((sock, _)) = acc else {
                    continue;
                };
                let tls = tls.clone();
                let app = app.clone();
                let handle = tokio::runtime::Handle::current();
                tokio::task::spawn_blocking(move || {
                    let _enter = handle.enter();
                    let std_sock = match sock.into_std() {
                        Ok(sock) => sock,
                        Err(err) => {
                            warn!("web socket: {err}");
                            return;
                        }
                    };
                    if let Err(err) = std_sock.set_nonblocking(false) {
                        warn!("web socket: {err}");
                        return;
                    }
                    let mut stream = match tls.accept(std_sock) {
                        Ok(stream) => stream,
                        Err(err) => {
                            debug!("tls handshake: {err}");
                            return;
                        }
                    };
                    blocking_http(&mut stream, app);
                });
            }
        }
    }
}

fn blocking_http(sock: &mut crate::cert::SslStream, app: Arc<App>) {
    let mut conn = HttpConn::new(sock);
    loop {
        let raw = match conn.next_request_blocking(1024 * 1024) {
            Ok(Some(raw)) => raw,
            Ok(None) => break,
            Err(err) => {
                debug!("web read: {err}");
                break;
            }
        };
        let keep = raw.keep_alive;
        let req = Request::from_raw(raw);
        let resp = run::drive(app.clone(), req);
        let bytes = resp.to_bytes(keep);
        if let Err(err) = conn.write_all_blocking(&bytes) {
            debug!("web write: {err}");
            break;
        }
        if !keep {
            break;
        }
    }
}

mod run {
    use super::{dispatch, Request, Response};
    use crate::state::App;
    use std::sync::Arc;

    pub fn drive(app: Arc<App>, req: Request) -> Response {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        tokio::spawn(async move {
            let _ = tx.send(dispatch(app, req).await);
        });
        rx.recv()
            .unwrap_or_else(|_| Response::err(500, "runtime shutting down"))
    }
}

pub struct Request {
    pub method: Method,
    pub path: String,
    pub query: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    params: Vec<(&'static str, String)>,
}

impl Request {
    fn from_raw(raw: RawRequest) -> Self {
        Self {
            method: raw.method,
            path: raw.path,
            query: raw.query,
            headers: raw.headers,
            body: raw.body,
            params: Vec::new(),
        }
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        let name = name.to_ascii_lowercase();
        self.headers
            .iter()
            .find(|(k, _)| k == &name)
            .map(|(_, v)| v.as_str())
    }

    pub fn param(&self, name: &str) -> Option<&str> {
        self.params
            .iter()
            .find(|(k, _)| *k == name)
            .map(|(_, v)| v.as_str())
    }

    pub fn json<T: for<'de> Deserialize<'de>>(&self) -> Result<T, String> {
        serde_json::from_slice(&self.body).map_err(|err| format!("json: {err}"))
    }
}

pub struct Response {
    status: u16,
    reason: &'static str,
    content_type: &'static str,
    extra: &'static str,
    body: Vec<u8>,
}

impl Response {
    pub fn json(status: u16, value: &impl Serialize) -> Self {
        let body = serde_json::to_vec(value).unwrap_or_else(|_| b"{}".to_vec());
        Self {
            status,
            reason: reason(status),
            content_type: "application/json; charset=utf-8",
            extra: "",
            body,
        }
    }

    pub fn err(status: u16, message: impl Into<String>) -> Self {
        Self::json(status, &serde_json::json!({ "error": message.into() }))
    }

    pub fn html(body: &'static str) -> Self {
        Self::html_bytes(body.as_bytes().to_vec())
    }

    pub fn html_bytes(body: Vec<u8>) -> Self {
        Self {
            status: 200,
            reason: "OK",
            content_type: "text/html; charset=utf-8",
            extra: "",
            body,
        }
    }

    pub fn download(filename: &'static str, body: Vec<u8>) -> Self {
        let extra = match filename {
            "punch-macos" => "Content-Disposition: attachment; filename=\"punch-macos\"\r\n",
            "punch-linux" => "Content-Disposition: attachment; filename=\"punch-linux\"\r\n",
            "punch-windows.exe" => {
                "Content-Disposition: attachment; filename=\"punch-windows.exe\"\r\n"
            }
            _ => "",
        };
        Self {
            status: 200,
            reason: "OK",
            content_type: "application/octet-stream",
            extra,
            body,
        }
    }

    pub fn empty(status: u16) -> Self {
        Self {
            status,
            reason: reason(status),
            content_type: "text/plain",
            extra: "",
            body: Vec::new(),
        }
    }

    fn to_bytes(&self, keep: bool) -> Vec<u8> {
        let conn = if keep { "keep-alive" } else { "close" };
        let head = format!(
            "HTTP/1.1 {} {}\r\nContent-Type: {}\r\n{}Content-Length: {}\r\nConnection: {}\r\nCache-Control: no-store\r\n\r\n",
            self.status,
            self.reason,
            self.content_type,
            self.extra,
            self.body.len(),
            conn
        );
        let mut out = Vec::with_capacity(head.len() + self.body.len());
        out.extend_from_slice(head.as_bytes());
        out.extend_from_slice(&self.body);
        out
    }
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        409 => "Conflict",
        _ => "Error",
    }
}

enum Pattern {
    Exact(&'static str),
    Param { prefix: &'static str, name: &'static str },
}

impl Pattern {
    fn parse(path: &'static str) -> Self {
        if let Some(idx) = path.find("/:") {
            Pattern::Param {
                prefix: &path[..=idx],
                name: &path[idx + 2..],
            }
        } else {
            Pattern::Exact(path)
        }
    }

    fn captures(&self, path: &str) -> Option<Vec<(&'static str, String)>> {
        match self {
            Pattern::Exact(expected) if *expected == path => Some(Vec::new()),
            Pattern::Param { prefix, name }
                if path.starts_with(prefix) && !path[prefix.len()..].contains('/') =>
            {
                let rest = &path[prefix.len()..];
                if rest.is_empty() {
                    None
                } else {
                    Some(vec![(*name, rest.to_string())])
                }
            }
            _ => None,
        }
    }
}

struct Route {
    method: Method,
    pattern: Pattern,
    handler: Box<
        dyn Fn(Arc<App>, Request) -> Pin<Box<dyn Future<Output = Response> + Send>> + Send + Sync,
    >,
}

impl Route {
    fn get<F, Fut>(path: &'static str, f: F) -> Self
    where
        F: Fn(Arc<App>, Request) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Response> + Send + 'static,
    {
        Self::make(Method::Get, path, f)
    }

    fn post<F, Fut>(path: &'static str, f: F) -> Self
    where
        F: Fn(Arc<App>, Request) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Response> + Send + 'static,
    {
        Self::make(Method::Post, path, f)
    }

    fn put<F, Fut>(path: &'static str, f: F) -> Self
    where
        F: Fn(Arc<App>, Request) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Response> + Send + 'static,
    {
        Self::make(Method::Put, path, f)
    }

    fn delete<F, Fut>(path: &'static str, f: F) -> Self
    where
        F: Fn(Arc<App>, Request) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Response> + Send + 'static,
    {
        Self::make(Method::Delete, path, f)
    }

    fn make<F, Fut>(method: Method, path: &'static str, f: F) -> Self
    where
        F: Fn(Arc<App>, Request) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Response> + Send + 'static,
    {
        Self {
            method,
            pattern: Pattern::parse(path),
            handler: Box::new(move |app, req| Box::pin(f(app, req))),
        }
    }
}

fn index_page(app: &App) -> Response {
    let html = INDEX_HTML.replace("__PUNCH_PORT__", &app.control_addr.port().to_string());
    Response::html_bytes(html.into_bytes())
}

fn dist_download(name: &str) -> Response {
    let filename = match name {
        "punch-macos" => "punch-macos",
        "punch-linux" => "punch-linux",
        "punch-windows.exe" => "punch-windows.exe",
        _ => return Response::err(404, "not found"),
    };
    let path = PathBuf::from("dist").join(filename);
    match std::fs::read(&path) {
        Ok(body) => Response::download(filename, body),
        Err(_) => Response::err(404, "客户端二进制还没有放到 dist/"),
    }
}

fn routes() -> Vec<Route> {
    vec![
        Route::get("/api/ping", ping),
        Route::get("/api/overview", overview),
        Route::get("/api/clients", clients),
        Route::post("/api/clients", create_client),
        Route::put("/api/clients/:name", update_client),
        Route::delete("/api/clients/:name", delete_client),
        Route::get("/api/mappings", mappings),
        Route::post("/api/mappings", create_mapping),
        Route::put("/api/mappings/:id", update_mapping),
        Route::delete("/api/mappings/:id", delete_mapping),
        Route::get("/api/events", events),
        Route::post("/api/key", set_api_key),
    ]
}

pub async fn dispatch(app: Arc<App>, mut req: Request) -> Response {
    if req.path == "/favicon.ico" {
        return Response::empty(204);
    }
    if req.path == "/healthz" {
        return Response {
            status: 200,
            reason: "OK",
            content_type: "text/plain; charset=utf-8",
            extra: "",
            body: b"ok\n".to_vec(),
        };
    }
    if req.path == "/" {
        return index_page(&app);
    }
    if let Some(name) = req.path.strip_prefix("/dl/") {
        return dist_download(name);
    }
    if req.path.starts_with("/api/") && !authorized(&app, &req) {
        return Response::err(401, "invalid api key");
    }
    for route in routes() {
        if route.method != req.method {
            continue;
        }
        if let Some(params) = route.pattern.captures(&req.path) {
            req.params = params;
            return (route.handler)(app, req).await;
        }
    }
    Response::err(404, "not found")
}

fn authorized(app: &App, req: &Request) -> bool {
    let presented = if let Some(value) = req.header("x-api-key") {
        value.trim().to_string()
    } else if let Some(value) = req.header("authorization") {
        let value = value.trim();
        if let Some(rest) = value.strip_prefix("Bearer ") {
            rest.trim().to_string()
        } else if let Some(rest) = value.strip_prefix("bearer ") {
            rest.trim().to_string()
        } else {
            return false;
        }
    } else {
        return false;
    };
    util::keys_equal(&presented, &app.api_key())
}

#[derive(Deserialize)]
struct KeyBody {
    api_key: String,
}

async fn set_api_key(app: Arc<App>, req: Request) -> Response {
    let body: KeyBody = match req.json() {
        Ok(body) => body,
        Err(err) => return Response::err(400, err),
    };
    let key = body.api_key.trim();
    if key.len() < 8 {
        return Response::err(400, "api key 至少 8 个字符");
    }
    if key.len() > 128 || key.chars().any(|c| c.is_control() || c == '"' || c == '\\') {
        return Response::err(400, "api key 不能包含引号、反斜杠或控制字符");
    }
    if app.config_path.as_os_str().is_empty() {
        return Response::err(500, "没有配置文件，不能保存");
    }
    let key = key.to_string();
    let mut current = lock(&app.api_key);
    if let Err(err) = crate::config::write_api_key(&app.config_path, &key) {
        return Response::err(500, err.to_string());
    }
    *current = key;
    Response::json(200, &serde_json::json!({ "ok": true }))
}

async fn ping(_app: Arc<App>, _req: Request) -> Response {
    Response::json(200, &serde_json::json!({ "pong": true, "io": io_mode() }))
}

async fn overview(app: Arc<App>, _req: Request) -> Response {
    let clients = lock(&app.by_name).len();
    let mappings = lock(&app.maps).len();
    Response::json(
        200,
        &serde_json::json!({
            "clients": clients,
            "mappings": mappings,
            "active": app.active.load(Ordering::Relaxed),
            "bytes_in": app.bytes_in.load(Ordering::Relaxed),
            "bytes_out": app.bytes_out.load(Ordering::Relaxed),
            "control": app.control_addr.to_string(),
            "data": app.data_addr.to_string(),
            "web": app.web_addr.to_string(),
            "io": io_mode(),
            "uptime_ms": util::now_ms().saturating_sub(app.started_ms),
        }),
    )
}

async fn clients(app: Arc<App>, _req: Request) -> Response {
    let records = lock(&app.records).clone();
    let list: Vec<_> = records
        .iter()
        .map(|r| {
            let online = app.client_by_name(&r.name);
            serde_json::json!({
                "name": r.name,
                "secret": r.secret,
                "allow_maps": r.allow_maps,
                "online": online.is_some(),
                "addr": online.as_ref().map(|c| c.addr.clone()).unwrap_or_default(),
                "connected_ms": online.as_ref().map(|c| c.connected_ms).unwrap_or(0),
                "pool_idle": online.as_ref().map(|c| lock(&c.idle).len()).unwrap_or(0),
            })
        })
        .collect();
    Response::json(200, &serde_json::json!({ "clients": list }))
}

#[derive(Deserialize)]
struct ClientBody {
    name: String,
    secret: Option<String>,
    allow_maps: Option<bool>,
}

#[derive(Deserialize)]
struct ClientPatch {
    secret: Option<String>,
    allow_maps: Option<bool>,
}

async fn create_client(app: Arc<App>, req: Request) -> Response {
    let body: ClientBody = match req.json() {
        Ok(body) => body,
        Err(err) => return Response::err(400, err),
    };
    let name = body.name.trim();
    if !catalog::valid_name(name) {
        return Response::err(400, "名字只能是 1 到 64 位字母、数字、点、下划线或横线");
    }
    let secret = match body.secret {
        Some(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => catalog::new_secret(),
    };
    if let Err(err) = catalog::valid_secret(&secret) {
        return Response::err(400, err.to_string());
    }
    if app.find_record(name).is_some() {
        return Response::err(409, "已经有这个名字");
    }
    if app.find_secret(&secret).is_some() {
        return Response::err(409, "这把 secret 已经给了别的客户端");
    }
    let record = ClientRecord {
        name: name.to_string(),
        secret,
        allow_maps: body.allow_maps.unwrap_or(false),
    };
    lock(&app.records).push(record.clone());
    if let Err(err) = app.save_catalog() {
        lock(&app.records).retain(|r| r.name != record.name);
        return Response::err(500, err);
    }
    Response::json(
        201,
        &serde_json::json!({
            "name": record.name,
            "secret": record.secret,
            "allow_maps": record.allow_maps,
        }),
    )
}

async fn update_client(app: Arc<App>, req: Request) -> Response {
    let name = match req.param("name") {
        Some(name) => name.to_string(),
        None => return Response::err(400, "缺少客户端名字"),
    };
    let body: ClientPatch = match req.json() {
        Ok(body) => body,
        Err(err) => return Response::err(400, err),
    };
    let Some(mut record) = app.find_record(&name) else {
        return Response::err(404, "客户端不存在");
    };
    let mut secret_changed = false;
    if let Some(secret) = body.secret {
        let secret = secret.trim().to_string();
        if let Err(err) = catalog::valid_secret(&secret) {
            return Response::err(400, err.to_string());
        }
        if secret != record.secret {
            if app
                .find_secret(&secret)
                .map(|r| r.name != record.name)
                .unwrap_or(false)
            {
                return Response::err(409, "这把 secret 已经给了别的客户端");
            }
            record.secret = secret;
            secret_changed = true;
        }
    }
    if let Some(allow) = body.allow_maps {
        record.allow_maps = allow;
    }
    {
        let mut records = lock(&app.records);
        let Some(slot) = records.iter_mut().find(|r| r.name == name) else {
            return Response::err(404, "客户端不存在");
        };
        *slot = record.clone();
    }
    if let Err(err) = app.save_catalog() {
        return Response::err(500, err);
    }
    if let Some(client) = app.client_by_name(&name) {
        client
            .allow_maps
            .store(record.allow_maps, std::sync::atomic::Ordering::Relaxed);
        if secret_changed {
            if client
                .ctrl
                .send(CtrlMsg::Superseded {
                    message: "管理员更换了 secret，请用新的 secret 重新登录".into(),
                })
                .is_err()
            {
                app.disconnect_client(client.id, &client.name);
            }
        } else {
            let _ = client.ctrl.send(CtrlMsg::Allow {
                allow: record.allow_maps,
            });
        }
    }
    Response::json(
        200,
        &serde_json::json!({
            "name": record.name,
            "secret": record.secret,
            "allow_maps": record.allow_maps,
        }),
    )
}

async fn delete_client(app: Arc<App>, req: Request) -> Response {
    let name = match req.param("name") {
        Some(name) => name.to_string(),
        None => return Response::err(400, "缺少客户端名字"),
    };
    if app.find_record(&name).is_none() {
        return Response::err(404, "客户端不存在");
    }
    let ids: Vec<u32> = lock(&app.maps)
        .values()
        .filter(|m| m.client_name == name)
        .map(|m| m.id)
        .collect();
    for id in ids {
        let _ = app.close_map(id, true);
    }
    if let Some(client) = app.client_by_name(&name) {
        if client
            .ctrl
            .send(CtrlMsg::Superseded {
                message: "管理员删除了这个客户端".into(),
            })
            .is_err()
        {
            app.disconnect_client(client.id, &client.name);
        }
    }
    lock(&app.records).retain(|r| r.name != name);
    if let Err(err) = app.save_catalog() {
        return Response::err(500, err);
    }
    Response::json(200, &serde_json::json!({ "deleted": name }))
}

async fn mappings(app: Arc<App>, _req: Request) -> Response {
    let mut rows: Vec<Arc<crate::state::MapState>> = lock(&app.maps).values().cloned().collect();
    rows.sort_by_key(|m| m.id);
    let list: Vec<_> = rows
        .iter()
        .map(|m| {
            serde_json::json!({
                "id": m.id,
                "client_name": m.client_name,
                "client_online": app.client_by_name(&m.client_name).is_some(),
                "bind": m.bind_text(),
                "target": m.target_text(),
                "active": m.active.load(Ordering::Relaxed),
                "accepts": m.accepts.load(Ordering::Relaxed),
                "bytes_in": m.bytes_in.load(Ordering::Relaxed),
                "bytes_out": m.bytes_out.load(Ordering::Relaxed),
                "fail_count": m.fail_count.load(Ordering::Relaxed),
                "last_error": lock(&m.last_error).clone(),
                "last_error_ms": m.last_error_ms.load(Ordering::Relaxed),
            })
        })
        .collect();
    Response::json(200, &serde_json::json!({ "mappings": list }))
}

#[derive(Deserialize)]
struct CreateBody {
    client_name: String,
    bind: String,
    target: String,
}

async fn create_mapping(app: Arc<App>, req: Request) -> Response {
    let body: CreateBody = match req.json() {
        Ok(body) => body,
        Err(err) => return Response::err(400, err),
    };
    let (bind_ip, bind_port) = match util::parse_bind(body.bind.trim()) {
        Ok(v) => v,
        Err(err) => return Response::err(400, err),
    };
    let (target_host, target_port) = match util::parse_target(body.target.trim()) {
        Ok(v) => v,
        Err(err) => return Response::err(400, err),
    };
    let opened = match app.open_map(OpenReq {
        client_name: body.client_name.trim().to_string(),
        bind_ip,
        bind_port,
        target_host,
        target_port,
        fixed_id: None,
        persist: true,
    }) {
        Ok(map) => map,
        Err(err) => {
            app.push_event(crate::state::Event {
                ts_ms: util::now_ms(),
                kind: "bind_fail".into(),
                client_name: body.client_name,
                map_id: 0,
                message: err.clone(),
            });
            return Response::err(400, err);
        }
    };
    let warning = match app.push_map(&opened).await {
        Ok(ack) if ack.status == 0 => None,
        Ok(ack) => Some(ack.message),
        Err(err) => Some(err),
    };
    Response::json(
        201,
        &serde_json::json!({
            "id": opened.id,
            "bind": opened.bind_text(),
            "target": opened.target_text(),
            "client_name": opened.client_name,
            "warning": warning,
        }),
    )
}

#[derive(Deserialize)]
struct UpdateBody {
    bind: String,
    target: String,
}

async fn update_mapping(app: Arc<App>, req: Request) -> Response {
    let id = match req.param("id").and_then(|s| s.parse::<u32>().ok()) {
        Some(id) => id,
        None => return Response::err(400, "mapping id 无效"),
    };
    let body: UpdateBody = match req.json() {
        Ok(body) => body,
        Err(err) => return Response::err(400, err),
    };
    let (bind_ip, bind_port) = match util::parse_bind(body.bind.trim()) {
        Ok(v) => v,
        Err(err) => return Response::err(400, err),
    };
    let (target_host, target_port) = match util::parse_target(body.target.trim()) {
        Ok(v) => v,
        Err(err) => return Response::err(400, err),
    };
    let opened = match app.update_map(id, bind_ip, bind_port, target_host, target_port) {
        Ok(map) => map,
        Err(err) => return Response::err(400, err),
    };
    let warning = match app.push_map(&opened).await {
        Ok(ack) if ack.status == 0 => None,
        Ok(ack) => Some(ack.message),
        Err(err) => Some(err),
    };
    Response::json(
        200,
        &serde_json::json!({
            "id": opened.id,
            "bind": opened.bind_text(),
            "target": opened.target_text(),
            "client_name": opened.client_name,
            "warning": warning,
        }),
    )
}

async fn delete_mapping(app: Arc<App>, req: Request) -> Response {
    let id = match req.param("id").and_then(|s| s.parse::<u32>().ok()) {
        Some(id) => id,
        None => return Response::err(400, "mapping id 无效"),
    };
    match app.close_map(id, true) {
        Ok(()) => Response::json(200, &serde_json::json!({ "deleted": id })),
        Err(err) => Response::err(404, err),
    }
}

async fn events(app: Arc<App>, _req: Request) -> Response {
    let list: Vec<_> = lock(&app.events)
        .iter()
        .map(|e| {
            serde_json::json!({
                "ts_ms": e.ts_ms,
                "kind": e.kind,
                "client_name": e.client_name,
                "map_id": e.map_id,
                "message": e.message,
            })
        })
        .collect();
    Response::json(200, &serde_json::json!({ "events": list }))
}
