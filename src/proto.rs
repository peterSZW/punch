//! 控制通道的小端二进制帧。数据面在握手之后不再走这个编码。

pub const PROTO_VERSION: u8 = 2;
pub const DATA_MAGIC: u32 = 0x504E_4348;
pub const DATA_HELLO_LEN: usize = 34;

const HELLO: u8 = 1;
const HELLO_OK: u8 = 2;
const HELLO_ERR: u8 = 3;
const PING: u8 = 4;
const PONG: u8 = 5;
const MAP_ADD: u8 = 6;
const MAP_ADD_RESULT: u8 = 7;
const MAP_OPEN: u8 = 8;
const MAP_OPEN_ACK: u8 = 9;
const MAP_DEL: u8 = 10;
const LISTEN_FAIL: u8 = 11;
const OPEN_STREAM: u8 = 12;
const STREAM_FAIL: u8 = 13;
const SYNC_DONE: u8 = 14;
const SUPERSEDED: u8 = 15;
const MAP_UPDATE: u8 = 16;
const MAP_UPDATE_RESULT: u8 = 17;
const MAP_DEL_REQ: u8 = 18;
const MAP_DEL_RESULT: u8 = 19;
const SNAPSHOT: u8 = 20;
const ALLOW: u8 = 21;

pub const DATA_IDLE: u8 = 0;
pub const DATA_BOUND: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CtrlMsg {
    Hello {
        version: u8,
        secret: String,
    },
    HelloOk {
        client_id: u64,
        data_port: u16,
        token: [u8; 16],
        pool_idle: u16,
        pool_max: u16,
        name: String,
        allow_maps: bool,
    },
    /// 这把 secret 已经有别的机器连上，或者管理员换了 secret。收到后不要重连。
    Superseded {
        message: String,
    },
    HelloErr {
        message: String,
    },
    Ping {
        ts_ms: u64,
    },
    Pong {
        ts_ms: u64,
    },
    MapAdd {
        req_id: u32,
        bind_ip: String,
        bind_port: u16,
        target_host: String,
        target_port: u16,
    },
    MapAddResult {
        req_id: u32,
        map_id: u32,
        ok: bool,
        message: String,
    },
    MapOpen {
        map_id: u32,
        bind_ip: String,
        bind_port: u16,
        target_host: String,
        target_port: u16,
    },
    /// status 0: 目标可连。1: 映射保留，但客户端拨号失败（通常是对端没在 listen）。
    MapOpenAck {
        map_id: u32,
        status: u8,
        message: String,
    },
    MapDel {
        map_id: u32,
    },
    MapUpdate {
        req_id: u32,
        map_id: u32,
        bind_ip: String,
        bind_port: u16,
        target_host: String,
        target_port: u16,
    },
    MapUpdateResult {
        req_id: u32,
        map_id: u32,
        ok: bool,
        message: String,
    },
    MapDelReq {
        req_id: u32,
        map_id: u32,
    },
    MapDelResult {
        req_id: u32,
        map_id: u32,
        ok: bool,
        message: String,
    },
    /// 服务端把这个客户端的映射全部推送完。
    Snapshot,
    Allow {
        allow: bool,
    },
    ListenFail {
        map_id: u32,
        message: String,
    },
    OpenStream {
        stream_id: u32,
        map_id: u32,
        target_host: String,
        target_port: u16,
    },
    StreamFail {
        stream_id: u32,
        map_id: u32,
        message: String,
    },
    SyncDone,
}

impl CtrlMsg {
    pub fn encode(&self) -> Vec<u8> {
        let mut body = Vec::with_capacity(64);
        body.push(self.tag());
        match self {
            CtrlMsg::Hello { version, secret } => {
                body.push(*version);
                put_str(&mut body, secret);
            }
            CtrlMsg::HelloOk {
                client_id,
                data_port,
                token,
                pool_idle,
                pool_max,
                name,
                allow_maps,
            } => {
                body.extend_from_slice(&client_id.to_le_bytes());
                body.extend_from_slice(&data_port.to_le_bytes());
                body.extend_from_slice(token);
                body.extend_from_slice(&pool_idle.to_le_bytes());
                body.extend_from_slice(&pool_max.to_le_bytes());
                put_str(&mut body, name);
                body.push(u8::from(*allow_maps));
            }
            CtrlMsg::HelloErr { message } | CtrlMsg::Superseded { message } => {
                put_str(&mut body, message);
            }
            CtrlMsg::Ping { ts_ms } | CtrlMsg::Pong { ts_ms } => {
                body.extend_from_slice(&ts_ms.to_le_bytes());
            }
            CtrlMsg::MapAdd {
                req_id,
                bind_ip,
                bind_port,
                target_host,
                target_port,
            } => {
                body.extend_from_slice(&req_id.to_le_bytes());
                put_str(&mut body, bind_ip);
                body.extend_from_slice(&bind_port.to_le_bytes());
                put_str(&mut body, target_host);
                body.extend_from_slice(&target_port.to_le_bytes());
            }
            CtrlMsg::MapAddResult {
                req_id,
                map_id,
                ok,
                message,
            } => {
                body.extend_from_slice(&req_id.to_le_bytes());
                body.extend_from_slice(&map_id.to_le_bytes());
                body.push(u8::from(*ok));
                put_str(&mut body, message);
            }
            CtrlMsg::MapOpen {
                map_id,
                bind_ip,
                bind_port,
                target_host,
                target_port,
            } => {
                body.extend_from_slice(&map_id.to_le_bytes());
                put_str(&mut body, bind_ip);
                body.extend_from_slice(&bind_port.to_le_bytes());
                put_str(&mut body, target_host);
                body.extend_from_slice(&target_port.to_le_bytes());
            }
            CtrlMsg::MapOpenAck {
                map_id,
                status,
                message,
            } => {
                body.extend_from_slice(&map_id.to_le_bytes());
                body.push(*status);
                put_str(&mut body, message);
            }
            CtrlMsg::MapDel { map_id } => body.extend_from_slice(&map_id.to_le_bytes()),
            CtrlMsg::MapUpdate {
                req_id,
                map_id,
                bind_ip,
                bind_port,
                target_host,
                target_port,
            } => {
                body.extend_from_slice(&req_id.to_le_bytes());
                body.extend_from_slice(&map_id.to_le_bytes());
                put_str(&mut body, bind_ip);
                body.extend_from_slice(&bind_port.to_le_bytes());
                put_str(&mut body, target_host);
                body.extend_from_slice(&target_port.to_le_bytes());
            }
            CtrlMsg::MapUpdateResult {
                req_id,
                map_id,
                ok,
                message,
            }
            | CtrlMsg::MapDelResult {
                req_id,
                map_id,
                ok,
                message,
            } => {
                body.extend_from_slice(&req_id.to_le_bytes());
                body.extend_from_slice(&map_id.to_le_bytes());
                body.push(u8::from(*ok));
                put_str(&mut body, message);
            }
            CtrlMsg::MapDelReq { req_id, map_id } => {
                body.extend_from_slice(&req_id.to_le_bytes());
                body.extend_from_slice(&map_id.to_le_bytes());
            }
            CtrlMsg::Snapshot => {}
            CtrlMsg::Allow { allow } => body.push(u8::from(*allow)),
            CtrlMsg::ListenFail { map_id, message } => {
                body.extend_from_slice(&map_id.to_le_bytes());
                put_str(&mut body, message);
            }
            CtrlMsg::OpenStream {
                stream_id,
                map_id,
                target_host,
                target_port,
            } => {
                body.extend_from_slice(&stream_id.to_le_bytes());
                body.extend_from_slice(&map_id.to_le_bytes());
                put_str(&mut body, target_host);
                body.extend_from_slice(&target_port.to_le_bytes());
            }
            CtrlMsg::StreamFail {
                stream_id,
                map_id,
                message,
            } => {
                body.extend_from_slice(&stream_id.to_le_bytes());
                body.extend_from_slice(&map_id.to_le_bytes());
                put_str(&mut body, message);
            }
            CtrlMsg::SyncDone => {}
        }
        let mut out = Vec::with_capacity(4 + body.len());
        out.extend_from_slice(&(body.len() as u32).to_le_bytes());
        out.extend(body);
        out
    }

    pub fn decode(body: &[u8]) -> Result<CtrlMsg, String> {
        if body.is_empty() {
            return Err("empty frame".into());
        }
        let mut r = R { buf: body, i: 1 };
        let msg = match body[0] {
            HELLO => CtrlMsg::Hello {
                version: r.u8()?,
                secret: r.str()?,
            },
            HELLO_OK => {
                let client_id = r.u64()?;
                let data_port = r.u16()?;
                let token = r.arr()?;
                let pool_idle = r.u16()?;
                let pool_max = r.u16()?;
                let name = r.str()?;
                let allow_maps = r.u8()? == 1;
                CtrlMsg::HelloOk {
                    client_id,
                    data_port,
                    token,
                    pool_idle,
                    pool_max,
                    name,
                    allow_maps,
                }
            }
            HELLO_ERR => CtrlMsg::HelloErr { message: r.str()? },
            SUPERSEDED => CtrlMsg::Superseded { message: r.str()? },
            PING => CtrlMsg::Ping { ts_ms: r.u64()? },
            PONG => CtrlMsg::Pong { ts_ms: r.u64()? },
            MAP_ADD => CtrlMsg::MapAdd {
                req_id: r.u32()?,
                bind_ip: r.str()?,
                bind_port: r.u16()?,
                target_host: r.str()?,
                target_port: r.u16()?,
            },
            MAP_ADD_RESULT => CtrlMsg::MapAddResult {
                req_id: r.u32()?,
                map_id: r.u32()?,
                ok: r.u8()? == 1,
                message: r.str()?,
            },
            MAP_OPEN => CtrlMsg::MapOpen {
                map_id: r.u32()?,
                bind_ip: r.str()?,
                bind_port: r.u16()?,
                target_host: r.str()?,
                target_port: r.u16()?,
            },
            MAP_OPEN_ACK => CtrlMsg::MapOpenAck {
                map_id: r.u32()?,
                status: r.u8()?,
                message: r.str()?,
            },
            MAP_DEL => CtrlMsg::MapDel { map_id: r.u32()? },
            MAP_UPDATE => CtrlMsg::MapUpdate {
                req_id: r.u32()?,
                map_id: r.u32()?,
                bind_ip: r.str()?,
                bind_port: r.u16()?,
                target_host: r.str()?,
                target_port: r.u16()?,
            },
            MAP_UPDATE_RESULT => CtrlMsg::MapUpdateResult {
                req_id: r.u32()?,
                map_id: r.u32()?,
                ok: r.u8()? == 1,
                message: r.str()?,
            },
            MAP_DEL_REQ => CtrlMsg::MapDelReq {
                req_id: r.u32()?,
                map_id: r.u32()?,
            },
            MAP_DEL_RESULT => CtrlMsg::MapDelResult {
                req_id: r.u32()?,
                map_id: r.u32()?,
                ok: r.u8()? == 1,
                message: r.str()?,
            },
            SNAPSHOT => CtrlMsg::Snapshot,
            ALLOW => CtrlMsg::Allow { allow: r.u8()? == 1 },
            LISTEN_FAIL => CtrlMsg::ListenFail {
                map_id: r.u32()?,
                message: r.str()?,
            },
            OPEN_STREAM => CtrlMsg::OpenStream {
                stream_id: r.u32()?,
                map_id: r.u32()?,
                target_host: r.str()?,
                target_port: r.u16()?,
            },
            STREAM_FAIL => CtrlMsg::StreamFail {
                stream_id: r.u32()?,
                map_id: r.u32()?,
                message: r.str()?,
            },
            SYNC_DONE => CtrlMsg::SyncDone,
            other => return Err(format!("unknown tag {other}")),
        };
        if r.i != r.buf.len() {
            return Err("trailing bytes".into());
        }
        Ok(msg)
    }

    fn tag(&self) -> u8 {
        match self {
            CtrlMsg::Hello { .. } => HELLO,
            CtrlMsg::HelloOk { .. } => HELLO_OK,
            CtrlMsg::HelloErr { .. } => HELLO_ERR,
            CtrlMsg::Ping { .. } => PING,
            CtrlMsg::Pong { .. } => PONG,
            CtrlMsg::MapAdd { .. } => MAP_ADD,
            CtrlMsg::MapAddResult { .. } => MAP_ADD_RESULT,
            CtrlMsg::MapOpen { .. } => MAP_OPEN,
            CtrlMsg::MapOpenAck { .. } => MAP_OPEN_ACK,
            CtrlMsg::MapDel { .. } => MAP_DEL,
            CtrlMsg::MapUpdate { .. } => MAP_UPDATE,
            CtrlMsg::MapUpdateResult { .. } => MAP_UPDATE_RESULT,
            CtrlMsg::MapDelReq { .. } => MAP_DEL_REQ,
            CtrlMsg::MapDelResult { .. } => MAP_DEL_RESULT,
            CtrlMsg::Snapshot => SNAPSHOT,
            CtrlMsg::Allow { .. } => ALLOW,
            CtrlMsg::Superseded { .. } => SUPERSEDED,
            CtrlMsg::ListenFail { .. } => LISTEN_FAIL,
            CtrlMsg::OpenStream { .. } => OPEN_STREAM,
            CtrlMsg::StreamFail { .. } => STREAM_FAIL,
            CtrlMsg::SyncDone => SYNC_DONE,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataHello {
    pub kind: u8,
    pub client_id: u64,
    pub token: [u8; 16],
    pub stream_id: u32,
}

pub fn encode_data_hello(hello: &DataHello) -> [u8; DATA_HELLO_LEN] {
    let mut out = [0u8; DATA_HELLO_LEN];
    out[0..4].copy_from_slice(&DATA_MAGIC.to_le_bytes());
    out[4] = PROTO_VERSION;
    out[5] = hello.kind;
    out[6..14].copy_from_slice(&hello.client_id.to_le_bytes());
    out[14..30].copy_from_slice(&hello.token);
    out[30..34].copy_from_slice(&hello.stream_id.to_le_bytes());
    out
}

pub fn decode_data_hello(buf: &[u8]) -> Result<DataHello, String> {
    if buf.len() < DATA_HELLO_LEN {
        return Err("short data hello".into());
    }
    let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    if magic != DATA_MAGIC {
        return Err("bad data magic".into());
    }
    if buf[4] != PROTO_VERSION {
        return Err("bad data version".into());
    }
    let mut token = [0u8; 16];
    token.copy_from_slice(&buf[14..30]);
    Ok(DataHello {
        kind: buf[5],
        client_id: u64::from_le_bytes(buf[6..14].try_into().unwrap()),
        token,
        stream_id: u32::from_le_bytes(buf[30..34].try_into().unwrap()),
    })
}

pub const ASSIGN_PREFIX: usize = 18;

pub fn encode_assign(stream_id: u32, map_id: u32, port: u16, host: &str) -> Vec<u8> {
    let host_b = host.as_bytes();
    let mut out = Vec::with_capacity(ASSIGN_PREFIX + host_b.len());
    out.extend_from_slice(&DATA_MAGIC.to_le_bytes());
    out.push(PROTO_VERSION);
    out.push(1);
    out.extend_from_slice(&stream_id.to_le_bytes());
    out.extend_from_slice(&map_id.to_le_bytes());
    out.extend_from_slice(&port.to_le_bytes());
    out.extend_from_slice(&(host_b.len() as u16).to_le_bytes());
    out.extend_from_slice(host_b);
    out
}

pub fn decode_assign_prefix(buf: &[u8]) -> Result<(u32, u32, u16, u16), String> {
    if buf.len() < ASSIGN_PREFIX {
        return Err("short assign".into());
    }
    let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    if magic != DATA_MAGIC || buf[4] != PROTO_VERSION || buf[5] != 1 {
        return Err("bad assign header".into());
    }
    let stream_id = u32::from_le_bytes(buf[6..10].try_into().unwrap());
    let map_id = u32::from_le_bytes(buf[10..14].try_into().unwrap());
    let port = u16::from_le_bytes(buf[14..16].try_into().unwrap());
    let host_len = u16::from_le_bytes(buf[16..18].try_into().unwrap());
    if host_len as usize > 255 {
        return Err("assign host too long".into());
    }
    Ok((stream_id, map_id, port, host_len))
}

fn put_str(out: &mut Vec<u8>, s: &str) {
    let b = s.as_bytes();
    let n = b.len().min(4096);
    out.extend_from_slice(&(n as u16).to_le_bytes());
    out.extend_from_slice(&b[..n]);
}

struct R<'a> {
    buf: &'a [u8],
    i: usize,
}

impl<'a> R<'a> {
    fn u8(&mut self) -> Result<u8, String> {
        if self.i >= self.buf.len() {
            return Err("truncated".into());
        }
        let v = self.buf[self.i];
        self.i += 1;
        Ok(v)
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        if self.i + n > self.buf.len() {
            return Err("truncated".into());
        }
        let s = &self.buf[self.i..self.i + n];
        self.i += n;
        Ok(s)
    }
    fn u16(&mut self) -> Result<u16, String> {
        let b = self.take(2)?;
        Ok(u16::from_le_bytes(b.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32, String> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes(b.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, String> {
        let b = self.take(8)?;
        Ok(u64::from_le_bytes(b.try_into().unwrap()))
    }
    fn arr(&mut self) -> Result<[u8; 16], String> {
        let b = self.take(16)?;
        let mut a = [0u8; 16];
        a.copy_from_slice(b);
        Ok(a)
    }
    fn str(&mut self) -> Result<String, String> {
        let n = self.u16()? as usize;
        if n > 4096 {
            return Err("string too long".into());
        }
        let b = self.take(n)?;
        String::from_utf8(b.to_vec()).map_err(|_| "bad utf8".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(msg: CtrlMsg) {
        let bytes = msg.encode();
        let len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        assert_eq!(len + 4, bytes.len());
        let decoded = CtrlMsg::decode(&bytes[4..]).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn all_messages_roundtrip() {
        let mut token = [0u8; 16];
        token[0] = 7;
        token[15] = 9;
        let samples = vec![
            CtrlMsg::Hello {
                version: PROTO_VERSION,
                secret: "secret-value".into(),
            },
            CtrlMsg::HelloOk {
                client_id: 99,
                data_port: 8025,
                token,
                pool_idle: 16,
                pool_max: 64,
                name: "edge".into(),
                allow_maps: true,
            },
            CtrlMsg::Superseded {
                message: "replaced".into(),
            },
            CtrlMsg::HelloErr {
                message: "bad key".into(),
            },
            CtrlMsg::Ping { ts_ms: 5 },
            CtrlMsg::Pong { ts_ms: 5 },
            CtrlMsg::MapAdd {
                req_id: 3,
                bind_ip: "0.0.0.0".into(),
                bind_port: 8080,
                target_host: "10.0.0.8".into(),
                target_port: 80,
            },
            CtrlMsg::MapAddResult {
                req_id: 3,
                map_id: 8,
                ok: true,
                message: "listening".into(),
            },
            CtrlMsg::MapOpen {
                map_id: 8,
                bind_ip: "127.0.0.1".into(),
                bind_port: 1,
                target_host: "example.com".into(),
                target_port: 443,
            },
            CtrlMsg::MapOpenAck {
                map_id: 8,
                status: 1,
                message: "listen failed: connection refused".into(),
            },
            CtrlMsg::MapDel { map_id: 8 },
            CtrlMsg::ListenFail {
                map_id: 8,
                message: "listen failed: connection refused".into(),
            },
            CtrlMsg::OpenStream {
                stream_id: 4,
                map_id: 8,
                target_host: "10.1.1.1".into(),
                target_port: 22,
            },
            CtrlMsg::StreamFail {
                stream_id: 4,
                map_id: 8,
                message: "listen failed: connection refused".into(),
            },
            CtrlMsg::MapUpdate {
                req_id: 4,
                map_id: 8,
                bind_ip: "127.0.0.1".into(),
                bind_port: 9,
                target_host: "10.0.0.9".into(),
                target_port: 81,
            },
            CtrlMsg::MapUpdateResult {
                req_id: 4,
                map_id: 8,
                ok: true,
                message: "updated".into(),
            },
            CtrlMsg::MapDelReq { req_id: 5, map_id: 8 },
            CtrlMsg::MapDelResult {
                req_id: 5,
                map_id: 8,
                ok: false,
                message: "missing".into(),
            },
            CtrlMsg::Snapshot,
            CtrlMsg::Allow { allow: false },
            CtrlMsg::SyncDone,
        ];
        for msg in samples {
            roundtrip(msg);
        }
    }

    #[test]
    fn data_headers_roundtrip() {
        let hello = DataHello {
            kind: DATA_IDLE,
            client_id: 42,
            token: [3u8; 16],
            stream_id: 0,
        };
        let bytes = encode_data_hello(&hello);
        assert_eq!(decode_data_hello(&bytes).unwrap(), hello);
        let assign = encode_assign(9, 2, 80, "192.168.1.9");
        let (sid, mid, port, hlen) = decode_assign_prefix(&assign).unwrap();
        assert_eq!((sid, mid, port, hlen), (9, 2, 80, 11));
        assert_eq!(&assign[ASSIGN_PREFIX..], b"192.168.1.9");
    }
}
