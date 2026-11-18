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
use punch::util::{self, hostname};

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
    println!(
        "连接 {}  name={}  映射 {} 条",
        opts.server,
        opts.name,
        opts.maps.len()
    );
    let (tx, rx) = watch::channel(false);
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = tx.send(true);
    });
    punch::client::run(opts, rx).await
}

fn parse_client(args: Vec<String>) -> Result<Options> {
    let mut server = None;
    let mut key = None;
    let mut name = None;
    let mut maps = Vec::new();
    let mut bench = None;
    let mut expose = None;
    let mut pool = None;
    let mut i = 0;
    while i < args.len() {
        let (flag, inline) = split_flag(&args[i]);
        i += 1;
        match flag.as_str() {
            "-s" | "--server" => server = Some(take_val(&args, &mut i, &flag, inline)?),
            "-k" | "--key" => key = Some(take_val(&args, &mut i, &flag, inline)?),
            "-n" | "--name" => name = Some(take_val(&args, &mut i, &flag, inline)?),
            "-m" | "--map" => maps.push(parse_map(&take_val(&args, &mut i, &flag, inline)?)?),
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
    let api_key = key.ok_or_else(|| anyhow!("需要 --key"))?;
    if let Some(expose) = expose {
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
        maps.push(PortMap {
            bind_ip,
            bind_port,
            target_host,
            target_port: bench.port(),
        });
    }
    Ok(Options {
        server,
        api_key,
        name: name.unwrap_or_else(hostname),
        maps,
        bench,
        pool,
    })
}

fn parse_map(spec: &str) -> Result<PortMap> {
    let (bind, target) = spec
        .split_once('=')
        .with_context(|| format!("映射格式是 BIND=TARGET，例如 0.0.0.0:8080=10.0.0.8:80，收到 {spec}"))?;
    let (bind_ip, bind_port) = util::parse_bind(bind.trim()).map_err(anyhow::Error::msg)?;
    let (target_host, target_port) = util::parse_target(target.trim()).map_err(anyhow::Error::msg)?;
    Ok(PortMap {
        bind_ip,
        bind_port,
        target_host,
        target_port,
    })
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
    "用法: punch server [-c punch-server.yaml] | punch client --server HOST:PORT --key KEY --map BIND=TARGET"
}

fn print_help() {
    println!(
        r#"punch — 单二进制的高性能反向 TCP 隧道

{short}

  punch server [-c FILE]     启动服务端。没有配置文件时会在当前目录生成
  punch client [选项]        连到服务端，并按 --map 注册端口

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
      客户端 --key 和管理页共用
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
        r#"punch client --server HOST:PORT --key KEY [选项]

  -s, --server HOST:PORT     服务端控制端口
  -k, --key KEY              配置里的 api_key
  -n, --name NAME            客户端名字，默认主机名。服务端按这个名字下发映射
  -m, --map BIND=TARGET      可重复。BIND 是服务端监听的 ip:port
                             TARGET 是客户端能访问的 host:port，不限本机
      例: --map 0.0.0.0:8080=10.0.0.8:80
          --map 127.0.0.1:2222=192.168.1.9:22
  --bench ADDR               在客户端开一个 HTTP，用来压测隧道
                             GET /bench?size=1048576
                             POST /bench
  --expose-bench BIND        把 --bench 映射到服务端的 BIND
  --pool N                   预连接的数据通道数量，默认用服务端配置

压测示例：
  punch client --server 10.0.0.1:8024 --key KEY \
    --bench 127.0.0.1:19090 --expose-bench 0.0.0.0:18080
  wrk -t4 -c256 -d30s http://10.0.0.1:18080/bench?size=1048576

客户端拨 TARGET 失败（连接被拒绝，对端没在 listen）会立刻报告给服务端，
管理页的映射行和 /api/events 里能看到 listen failed。
"#
    );
}
