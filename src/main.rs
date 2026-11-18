use std::net::ToSocketAddrs;
#[cfg(unix)]
use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::{anyhow, bail, Context, Result};
use tokio::sync::watch;

use punch::client::{Options, PortMap};
#[cfg(unix)]
use punch::config;
use punch::netutil;
#[cfg(unix)]
use punch::relay::io_mode;
#[cfg(unix)]
use punch::server;
use punch::util;

fn main() -> ExitCode {
    punch::log::init();
    match real() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err:#}");
            ExitCode::from(1)
        }
    }
}

fn real() -> Result<()> {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() || args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
        print_help();
        return Ok(());
    }
    let cmd = args.remove(0);
    match cmd.as_str() {
        "server" => {
            if wants_help(&args) {
                print_server_help();
                return Ok(());
            }
            let rt = runtime()?;
            rt.block_on(cmd_server(args))
        }
        "client" => {
            if wants_help(&args) {
                print_client_help();
                return Ok(());
            }
            let rt = runtime()?;
            rt.block_on(cmd_client(args))
        }
        "map" => cmd_map(args),
        other => bail!("未知命令 {other}\n\n{}", short_help()),
    }
}

fn wants_help(args: &[String]) -> bool {
    args.iter().any(|a| a == "-h" || a == "--help")
}

fn runtime() -> Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("创建 tokio runtime")
}

#[cfg(not(unix))]
async fn cmd_server(_args: Vec<String>) -> Result<()> {
    bail!("这个 Windows 程序只包含客户端");
}

#[cfg(unix)]
async fn cmd_server(args: Vec<String>) -> Result<()> {
    netutil::raise_nofile();
    let mut config_path = PathBuf::from("punch-server.yaml");
    let mut i = 0;
    while i < args.len() {
        let (flag, inline) = split_flag(&args[i]);
        i += 1;
        match flag.as_str() {
            "-c" | "--config" => {
                config_path = PathBuf::from(take_val(&args, &mut i, &flag, inline)?);
            }
            other => bail!("未知参数 {other}"),
        }
    }
    let (cfg, created) = config::load_or_create(&config_path)?;
    if created {
        println!("已生成配置 {}", config_path.display());
        println!("api key: {}", cfg.api_key);
        println!("证书 {} （自签名，可换成 nginx 的 fullchain.pem / privkey.pem）", cfg.web.cert);
    } else {
        println!("配置 {}", config_path.display());
    }
    let bound = server::bind(cfg)?;
    println!("io        {mode}（Linux 上是 epoll + splice 零拷贝）", mode = io_mode());
    println!("控制通道  {}", bound.control);
    println!("数据通道  {}", bound.data);
    println!("管理页    https://{}  （证书域名 localhost）", bound.web);
    let (tx, rx) = watch::channel(false);
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = tx.send(true);
    });
    bound.serve(rx).await
}

async fn cmd_client(args: Vec<String>) -> Result<()> {
    netutil::raise_nofile();
    let opts = parse_client(args)?;
    println!("连接 {}", opts.server);
    let (tx, rx) = watch::channel(false);
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = tx.send(true);
    });
    punch::client::run(opts, rx).await
}

fn parse_client(args: Vec<String>) -> Result<Options> {
    let mut server = None;
    let mut secret = None;
    let mut bench = None;
    let mut expose = None;
    let mut pool = None;
    let mut i = 0;
    while i < args.len() {
        let (flag, inline) = split_flag(&args[i]);
        i += 1;
        match flag.as_str() {
            "-s" | "--server" => server = Some(take_val(&args, &mut i, &flag, inline)?),
            "--secret" => secret = Some(take_val(&args, &mut i, &flag, inline)?),
            "-k" | "--key" => {
                bail!("客户端登录用 --secret。管理页的 api key 不能拿来连接")
            }
            "-n" | "--name" => bail!("客户端名字由服务端分配，不要传 --name"),
            "-m" | "--map" => bail!("连接时不能带 -m。连上之后，被允许的客户端用 punch map add"),
            "--bench" => bench = Some(parse_addr(&take_val(&args, &mut i, &flag, inline)?)?),
            "--expose-bench" => expose = Some(take_val(&args, &mut i, &flag, inline)?),
            "--pool" => {
                let v = take_val(&args, &mut i, &flag, inline)?;
                pool = Some(v.parse::<u16>().context("--pool 必须是数字")?);
            }
            other => bail!("未知参数 {other}"),
        }
    }
    let server = parse_addr(&server.ok_or_else(|| anyhow!("需要 --server host:port"))?)?;
    let secret = secret.ok_or_else(|| anyhow!("需要 --secret"))?;
    let expose = if let Some(expose) = expose {
        let bench = bench.context("--expose-bench 需要同时指定 --bench")?;
        let (bind_ip, bind_port) = util::parse_bind(&expose).map_err(anyhow::Error::msg)?;
        let target_host = if bench.ip().is_unspecified() {
            if bench.is_ipv4() {
                "127.0.0.1".to_string()
            } else {
                "::1".to_string()
            }
        } else {
            bench.ip().to_string()
        };
        Some(PortMap {
            bind_ip,
            bind_port,
            target_host,
            target_port: bench.port(),
        })
    } else {
        None
    };
    Ok(Options {
        server,
        secret,
        expose,
        bench,
        pool,
        local_dir: None,
    })
}

fn cmd_map(args: Vec<String>) -> Result<()> {
    let mut addr = None;
    let mut rest = Vec::new();
    let mut i = 0;
    while i < args.len() {
        let (flag, inline) = split_flag(&args[i]);
        i += 1;
        if flag == "--addr" {
            addr = Some(take_val(&args, &mut i, &flag, inline)?);
        } else {
            rest.push(if let Some(inline) = inline {
                format!("{flag}={inline}")
            } else {
                flag
            });
        }
    }
    if rest.is_empty() || rest[0] == "-h" || rest[0] == "--help" {
        print_map_help();
        return Ok(());
    }
    let addr = match addr {
        Some(value) => parse_addr(&value)?,
        None => find_client_addr()?,
    };
    let line = match rest[0].as_str() {
        "list" => "LIST".to_string(),
        "add" => {
            let spec = rest.get(1).context("用法: punch map add BIND=TARGET")?;
            let (bind, target) = spec.split_once('=').context("映射格式是 BIND=TARGET")?;
            format!("ADD {} {}", bind.trim(), target.trim())
        }
        "update" => {
            let id = rest.get(1).context("用法: punch map update ID BIND=TARGET")?;
            let spec = rest.get(2).context("用法: punch map update ID BIND=TARGET")?;
            let (bind, target) = spec.split_once('=').context("映射格式是 BIND=TARGET")?;
            format!("UPDATE {id} {} {}", bind.trim(), target.trim())
        }
        "del" => {
            let id = rest.get(1).context("用法: punch map del ID")?;
            format!("DEL {id}")
        }
        other => bail!("未知的映射命令 {other}"),
    };
    let mut sock = std::net::TcpStream::connect_timeout(&addr, std::time::Duration::from_secs(3))
        .with_context(|| format!("连不上本地客户端 {addr}，先运行 punch client"))?;
    use std::io::{Read, Write};
    sock.write_all(line.as_bytes())?;
    sock.shutdown(std::net::Shutdown::Write)?;
    let mut text = String::new();
    sock.read_to_string(&mut text)?;
    let text = text.trim();
    if let Some(err) = text.strip_prefix("error ") {
        bail!("{err}");
    }
    if text.is_empty() || text == "ok" {
        if rest[0] == "list" {
            println!("（没有映射）");
        } else {
            println!("{text}");
        }
        return Ok(());
    }
    if rest[0] == "list" {
        for line in text.lines().skip(1) {
            println!("{line}");
        }
        return Ok(());
    }
    println!("{text}");
    Ok(())
}

fn find_client_addr() -> Result<std::net::SocketAddr> {
    let dir = std::env::current_dir().context("读取当前目录")?;
    let mut found = Vec::new();
    for entry in std::fs::read_dir(&dir).with_context(|| format!("读取 {}", dir.display()))? {
        let Ok(entry) = entry else { continue };
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("punch.") && name.ends_with(".client.addr") {
            found.push(entry.path());
        }
    }
    if found.is_empty() {
        bail!("当前目录没有正在运行的客户端。先运行 punch client，或用 --addr 指定");
    }
    if found.len() > 1 {
        bail!("当前目录有多个客户端，用 punch map --addr ip:port 指定一个");
    }
    let text = std::fs::read_to_string(&found[0])?;
    parse_addr(text.trim())
}

fn parse_addr(value: &str) -> Result<std::net::SocketAddr> {
    if let Ok(addr) = value.parse() {
        return Ok(addr);
    }
    let (host, port) = util::split_host_port(value)
        .with_context(|| format!("地址必须是 host:port，收到 {value}"))?;
    (host.as_str(), port)
        .to_socket_addrs()
        .with_context(|| format!("解析 {value}"))?
        .next()
        .with_context(|| format!("dns 没有结果: {value}"))
}

fn split_flag(arg: &str) -> (String, Option<String>) {
    if let Some((key, value)) = arg.split_once('=') {
        if key.starts_with("--") {
            return (key.to_string(), Some(value.to_string()));
        }
    }
    (arg.to_string(), None)
}

fn take_val(args: &[String], i: &mut usize, flag: &str, inline: Option<String>) -> Result<String> {
    if let Some(value) = inline {
        return Ok(value);
    }
    let value = args
        .get(*i)
        .cloned()
        .with_context(|| format!("{flag} 需要参数"))?;
    *i += 1;
    Ok(value)
}

fn short_help() -> &'static str {
    "用法: punch server [-c punch-server.yaml] | punch client --server HOST:PORT --secret SECRET | punch map list"
}

fn print_help() {
    println!(
        r#"punch — 单二进制的高性能反向 TCP 隧道

{short}

  punch server [-c FILE]     启动服务端。没有配置文件时会在当前目录生成
  punch client [选项]        用 secret 连到服务端
  punch map ...              连上之后，被允许的客户端可以改映射

服务端监听 0.0.0.0 或 127.0.0.1，把进来的 TCP 转到客户端能访问的任意 IP 和端口。
Linux 上用 epoll 接入，数据面用 splice 零拷贝。管理页打进这个二进制，HTTPS 证书用 nginx 的 PEM。

环境变量 RUST_LOG=debug 可以看连接日志。
"#,
        short = short_help()
    );
}

fn print_server_help() {
    println!(
        r#"punch server [-c FILE]

  -c, --config FILE   配置文件，默认 ./punch-server.yaml
                      文件不存在时自动生成，并写一份自签名证书

生成的 yaml 里：
  control_listen / data_listen / web.listen
      写成 0.0.0.0:端口 对外，或 127.0.0.1:端口 只本机
  api_key
      只给管理页。客户端登录用管理员分配的 secret
  web.cert / web.key
      nginx 的 ssl_certificate 和 ssl_certificate_key，PEM
      支持 PRIVATE KEY、RSA PRIVATE KEY、EC PRIVATE KEY

管理页 https://<主机>:8443/  自签名证书的域名是 localhost
增加映射也可以：
  curl -k -H "X-Api-Key: KEY" -H "Content-Type: application/json" \
    -d '{{"client_name":"edge","bind":"0.0.0.0:18080","target":"10.0.0.8:80"}}' \
    https://127.0.0.1:8443/api/mappings
"#
    );
}

fn print_client_help() {
    println!(
        r#"  punch client --server HOST:PORT --secret SECRET [选项]

  -s, --server HOST:PORT     服务端控制端口
      --secret SECRET        管理页给这个客户端分配的 secret
  --bench ADDR               在客户端开一个 HTTP，用来压测隧道
                             GET /bench?size=1048576
                             POST /bench
  --expose-bench BIND        把 --bench 映射到服务端的 BIND。这个客户端必须被允许自己改映射
  --pool N                   预连接的数据通道数量，默认用服务端配置

连上之后，如果服务端允许这个客户端自己改映射：
  punch map list
  punch map add 0.0.0.0:8080=10.0.0.8:80
  punch map update 3 0.0.0.0:8080=10.0.0.8:81
  punch map del 3

同一把 secret 只能有一台机器连着。另一台用它登录时，这台会退出并且不再重连。
连接时不能带 -m。映射由管理页或 punch map 修改，客户端会自动用最新的。
"#
    );
}

fn print_map_help() {
    println!(
        r#"punch map list|add|update|del

  先在这台机器上运行 punch client。命令发给正在运行的客户端，由它交给服务端。
  服务端没有允许这个客户端自己改映射时，这些命令会失败。

  punch map list
  punch map add BIND=TARGET
  punch map update ID BIND=TARGET
  punch map del ID
  punch map --addr 127.0.0.1:端口 list
"#
    );
}

#[cfg(test)]
mod tests {
    use super::parse_client;

    #[test]
    fn client_rejects_map_flag() {
        let err = parse_client(vec![
            "--server".into(),
            "127.0.0.1:8024".into(),
            "--secret".into(),
            "secret-0123456789".into(),
            "-m".into(),
            "0.0.0.0:1=127.0.0.1:2".into(),
        ])
        .unwrap_err();
        let text = err.to_string();
        assert!(text.contains("不能带"), "{text}");
    }
}
