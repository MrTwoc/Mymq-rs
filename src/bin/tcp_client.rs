use std::env::args;

use anyhow::Ok;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = args().collect();
    let cmd = args[1..].join(" ");
    let mut stream = TcpStream::connect("127.0.0.1:7777").await?;
    stream.write_all((cmd + "\n").as_bytes()).await?;
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line).await?;
    println!("{}", line);
    Ok(())
}
