use mymq_rs::broker::Broker;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

async fn handle_conn(stream: TcpStream, broker: Arc<Mutex<Broker>>) -> anyhow::Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();

    loop {
        line.clear();

        if reader.read_line(&mut line).await? == 0 {
            break;
        }
        let parts: Vec<&str> = line.trim().split_whitespace().collect();
        if parts.len() < 3 {
            write_half.write_all(b"ERR need more args\n").await?;
            continue;
        }
        let mut b = broker.lock().await;
        let resp = match parts[0] {
            "SUBSCRIBE" => {
                b.subscribe(parts[1], parts[2]);
                "OK\n".to_string()
            }
            "PUBLISH" => {
                let id = b.publish(parts[1], parts[2..].join(" "));
                format!("OK {id}\n")
            }
            "DEQUEUE" => match b.dequeue(parts[1], parts[2]) {
                Some(msg) => format!("MSG {} {} \n", msg.id, msg.body),
                None => "EMPTY\n".to_string(),
            },
            "ACK" | "NACK" => {
                let ok = if parts[0] == "ACK" {
                    b.ack(parts[1], parts[2], parts[3].parse().unwrap_or(0))
                } else {
                    b.nack(parts[1], parts[2], parts[3].parse().unwrap_or(0))
                };
                if ok {
                    "OK\n".to_string()
                } else {
                    "FAIL\n".to_string()
                }
            }
            _ => "ERR unknown cmd\n".to_string(),
        };
        write_half.write_all(resp.as_bytes()).await?;
    }

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let broker = Arc::new(Mutex::new(Broker::new()));
    let listener = TcpListener::bind("127.0.0.1:7777").await?;
    println!("TCP 服务端已启动，监听 127.0.0.1:7777");

    loop {
        let (stream, _) = listener.accept().await?;
        let broker = Arc::clone(&broker);
        tokio::spawn(async move {
            if let Err(e) = handle_conn(stream, broker).await {
                println!("处理连接错误: {:?}", e);
            }
        });
    }
}
