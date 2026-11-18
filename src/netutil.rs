use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use socket2::{Domain, Protocol, Socket, TcpKeepalive, Type};
use tokio::net::{TcpListener, TcpStream};

use crate::config::TcpTune;

pub fn raise_nofile() {
    #[cfg(unix)]
    unsafe {
        let mut lim = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) != 0 {
            return;
        }
        let hard = if lim.rlim_max == libc::RLIM_INFINITY {
            1_048_576
        } else {
            lim.rlim_max as u64
        };
        let target = hard.min(1_048_576);
        if (lim.rlim_cur as u64) < target {
            lim.rlim_cur = target as libc::rlim_t;
            let _ = libc::setrlimit(libc::RLIMIT_NOFILE, &lim);
        }
    }
}

pub fn bind_tcp(addr: SocketAddr, tune: &TcpTune) -> io::Result<TcpListener> {
    let domain = if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;
    if addr.is_ipv6() {
        let _ = socket.set_only_v6(false);
    }
    socket.bind(&addr.into())?;
    socket.listen(tune.backlog)?;
    let listener: std::net::TcpListener = socket.into();
    TcpListener::from_std(listener)
}

pub fn tune_tokio(stream: &TcpStream, tune: &TcpTune) {
    let _ = stream.set_nodelay(tune.nodelay);
    let sock = socket2::SockRef::from(stream);
    if tune.recv_buffer > 0 {
        let _ = sock.set_recv_buffer_size(tune.recv_buffer);
    }
    if tune.send_buffer > 0 {
        let _ = sock.set_send_buffer_size(tune.send_buffer);
    }
    let _ = sock.set_keepalive(true);
    let keepalive = TcpKeepalive::new()
        .with_time(Duration::from_secs(30))
        .with_interval(Duration::from_secs(10));
    let _ = sock.set_tcp_keepalive(&keepalive);
}

pub fn classify_dial(err: &io::Error) -> String {
    if err.kind() == io::ErrorKind::ConnectionRefused {
        format!("listen failed: {err}")
    } else {
        format!("target unreachable: {err}")
    }
}

pub async fn connect_tuned(addr: SocketAddr, tune: &TcpTune) -> io::Result<TcpStream> {
    let stream = TcpStream::connect(addr).await?;
    tune_tokio(&stream, tune);
    Ok(stream)
}

pub async fn dial(host: &str, port: u16, tune: &TcpTune) -> io::Result<TcpStream> {
    let addr = tokio::net::lookup_host((host, port))
        .await?
        .next()
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("dns 没有结果: {host}"),
            )
        })?;
    let stream = TcpStream::connect(addr).await?;
    tune_tokio(&stream, tune);
    Ok(stream)
}
