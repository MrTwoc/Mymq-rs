use std::sync::Arc;

use anyhow::Ok;
use mymq_rs::{broker::Topic, proto::mq};
use quinn::{ClientConfig, VarInt}; // Endpoint 到步骤五再加

const SERVER_ADDR: &str = "127.0.0.1:8443";
const SERVER_NAME: &str = "localhost";

const USAGE: &str = "\
用法：
  client subscribe <topic> <subscriber>
  client publish  <topic> <body...>
  client dequeue  <topic> <subscriber>
  client ack      <topic> <subscriber> <msg_id>
  client nack     <topic> <subscriber> <msg_id>
";

fn need(args: &[String], n: usize) -> anyhow::Result<()> {
    if args.len() < n {
        anyhow::bail!("参数不足\n{USAGE}");
    }
    Ok(())
}

fn client_config() -> anyhow::Result<quinn::ClientConfig> {
    use quinn::rustls::RootCertStore;
    use quinn::rustls::pki_types::CertificateDer;
    let mut roots = RootCertStore::empty();
    roots.add(CertificateDer::from(std::fs::read("cert.der")?))?;
    let cfg = ClientConfig::with_root_certificates(Arc::new(roots))?;
    Ok(cfg)
}

async fn send_command(conn: &quinn::Connection, cmd: mq::Command) -> anyhow::Result<mq::Response> {
    let (mut send, mut recv) = conn.open_bi().await?;
    let buf = prost::Message::encode_to_vec(&cmd);
    send.write_all(&buf).await?;
    send.finish()?;

    let data = recv.read_to_end(64 * 1024).await?;
    Ok(prost::Message::decode(&*data)?)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        println!("{USAGE}");
        return Ok(());
    }
    let verb = args[1].to_lowercase();
    let rest = &args[2..];

    // 解析命令
    let cmd = match verb.as_str() {
        "subscribe" => {
            need(rest, 2)?;
            mq::Command {
                cmd: Some(mq::command::Cmd::Subscribe(mq::command::Subscribe {
                    topic: rest[0].clone(),
                    subscriber: rest[1].clone(),
                })),
            }
        }
        "publish" => {
            need(rest, 2)?;
            mq::Command {
                cmd: Some(mq::command::Cmd::Publish(mq::command::Publish {
                    topic: rest[0].clone(),
                    body: rest[1..].join(" "),
                })),
            }
        }
        "dequeue" => {
            need(rest, 2)?;
            mq::Command {
                cmd: Some(mq::command::Cmd::Dequeue(mq::command::Dequeue {
                    topic: rest[0].clone(),
                    subscriber: rest[1].clone(),
                })),
            }
        }
        "ack" | "nack" => {
            need(rest, 3)?;
            let msg_id: u64 = rest[2].parse()?;
            let (topic, subscriber) = (rest[0].clone(), rest[1].clone());
            if verb == "ack" {
                mq::Command {
                    cmd: Some(mq::command::Cmd::Ack(mq::command::Ack {
                        topic,
                        subscriber,
                        msg_id,
                    })),
                }
            } else {
                mq::Command {
                    cmd: Some(mq::command::Cmd::Nack(mq::command::Nack {
                        topic,
                        subscriber,
                        msg_id,
                    })),
                }
            }
        }
        // 教程里这里的_ 写为了 other => 这个other和_ 是一个意思吗
        // other => anyhow::bail!("未知命令 {other}\n{USAGE}"),
        other => anyhow::bail!("未知命令\n{USAGE}"),
    };
    let mut endpoint = quinn::Endpoint::client("0.0.0.0:0".parse()?)?;
    let conn = endpoint.connect(SERVER_ADDR.parse()?, SERVER_NAME)?.await?;

    let resp = send_command(&conn, cmd).await?;
    conn.close(VarInt::from_u32(0), b"bye");

    if !resp.ok {
        println!("FAIL {}", resp.error);
    } else if let Some(m) = resp.message {
        println!("MSG {} {} ", m.id, m.body);
    } else {
        println!("OK msg_id={}", resp.msg_id);
    }
    Ok(())
}
