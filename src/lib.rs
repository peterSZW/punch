//! punch：单二进制的高性能反向 TCP 隧道。
//!
//! 模型与 nps 相同。客户端主动连出，服务端在 `0.0.0.0` 或 `127.0.0.1`
//! 上接受连接，再让客户端去拨它能访问的任意地址。
//! Linux 上运行时用 epoll（tokio/mio）接连接，数据面用 splice 零拷贝。

#[macro_use]
pub mod log;
#[cfg(unix)]
pub mod admin;
pub mod bench;
pub mod catalog;
#[cfg(unix)]
pub mod cert;
pub mod client;
pub mod config;
pub mod ioframe;
pub mod minihttp;
pub mod netutil;
pub mod proto;
pub mod relay;
#[cfg(unix)]
pub mod server;
pub mod state;
pub mod util;

pub use client::Options as ClientOptions;
pub use config::ServerConfig;
#[cfg(unix)]
pub use server::BoundServer;
