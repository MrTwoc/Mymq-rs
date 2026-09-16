use mymq_rs::broker::Broker;
use mymq_rs::proto::mq;
use quinn::{Endpoint, ServerConfig, TransportConfig};
use std::sync::Arc;
use tokio::sync::Mutex;

fn load_cert() -> anyhow::Result<(
    Vec<quinn::rustls::pki_types::CertificateDer<'static>>,
    quinn::rustls::pki_types::PrivateKeyDer<'static>,
)> {
    use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    let cert = CertificateDer::from(std::fs::read("cert.der")?);
    let key = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(std::fs::read("key.der")?));
    Ok((vec![cert], key))
}

async fn handle_stream(
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
    broker: Arc<Mutex<Broker>>,
) -> anyhow::Result<()> {
    let data = recv.read_to_end(64 * 1024).await?;
    let cmd: mq::Command = prost::Message::decode(&*data)?;

    let resp = broker.lock().await.handle_command(cmd);

    let buf = prost::Message::encode_to_vec(&resp);
    send.write_all(&buf).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (cert, key) = load_cert()?; // 读 cert.der / key.der
    let mut server_config = quinn::ServerConfig::with_single_cert(cert, key)?;
    let transport = Arc::new(TransportConfig::default());
    server_config.transport = transport;

    let endpoint = Endpoint::server(server_config, "127.0.0.1:8443".parse()?)?;
    let broker = Arc::new(Mutex::new(Broker::new()));
    println!("服务端已启动，监听 127.0.0.1:8443");

    while let Some(incoming) = endpoint.accept().await {
        let broker = Arc::clone(&broker);
        tokio::spawn(async move {
            let conn = incoming.await?; // 握手完成
            while let Ok((send, recv)) = conn.accept_bi().await {
                let broker = Arc::clone(&broker);
                tokio::spawn(handle_stream(send, recv, broker));
            }
            Ok::<(), anyhow::Error>(())
        });
    }
    Ok(())
}
