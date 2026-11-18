//! 数据面的正确性门槛。性能改动先过这里：单字节必须及时到达，
//! 两个方向的字节必须和送进去的一致，计数必须对得上。

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

#[tokio::test]
async fn relay_flushes_one_byte_then_matches_both_directions() {
    timeout(Duration::from_secs(10), body())
        .await
        .expect("relay test timed out")
        .unwrap();
}

async fn body() -> anyhow::Result<()> {
    let (mut left, left_peer) = connected().await;
    let (mut right, right_peer) = connected().await;
    for sock in [&left, &right, &left_peer, &right_peer] {
        sock.set_nodelay(true)?;
    }
    let relayed = tokio::spawn(async move {
        punch::relay::relay(left_peer, right_peer, 256 * 1024).await
    });

    left.write_all(&[0x11]).await?;
    let mut one = [0u8; 1];
    timeout(Duration::from_millis(400), right.read_exact(&mut one))
        .await
        .expect("a single byte did not flush")?;
    assert_eq!(one[0], 0x11);

    let ab = patterned(200_000);
    let ba = patterned(80_000);
    left.write_all(&ab).await?;
    left.shutdown().await?;
    let mut got_ab = vec![0u8; ab.len()];
    timeout(Duration::from_secs(3), right.read_exact(&mut got_ab)).await??;
    assert_eq!(got_ab, ab);
    let mut extra = [0u8; 1];
    let n = timeout(Duration::from_secs(2), right.read(&mut extra)).await??;
    assert_eq!(n, 0, "left shutdown did not close the right read");

    right.write_all(&ba).await?;
    right.shutdown().await?;
    let mut got_ba = vec![0u8; ba.len()];
    timeout(Duration::from_secs(3), left.read_exact(&mut got_ba)).await??;
    assert_eq!(got_ba, ba);
    let n = timeout(Duration::from_secs(2), left.read(&mut extra)).await??;
    assert_eq!(n, 0, "right shutdown did not close the left read");

    let end = timeout(Duration::from_secs(3), relayed).await??;
    assert!(end.err.is_none(), "relay error: {:?}", end.err);
    assert_eq!(end.a_to_b, 1 + ab.len() as u64);
    assert_eq!(end.b_to_a, ba.len() as u64);
    Ok(())
}

async fn connected() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let dial = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });
    let (accepted, _) = listener.accept().await.unwrap();
    (dial.await.unwrap(), accepted)
}

fn patterned(n: usize) -> Vec<u8> {
    let mut buf = vec![0u8; n];
    for (i, byte) in buf.iter_mut().enumerate() {
        *byte = (i % 251) as u8;
    }
    buf
}
