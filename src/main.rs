use std::collections::HashMap;
use tokio::sync::mpsc;

struct Broker {
    queues: HashMap<String, mpsc::Sender<String>>,
}

impl Broker {
    fn new() -> Self {
        Broker {
            queues: HashMap::new(),
        }
    }

    fn create_queue(&mut self, name: &str) -> mpsc::Receiver<String> {
        let (tx, rx) = mpsc::channel(1024);
        self.queues.insert(name.to_string(), tx);
        rx
    }

    async fn publish(&self, queue: &str, msg: String) {
        if let Some(tx) = self.queues.get(queue) {
            let _ = tx.send(msg).await;
        }
    }
}
#[tokio::main]
async fn main() {
    let mut broker = Broker::new();
    let mut rx = broker.create_queue("orders");

    broker.publish("orders", "订单：001；宫保鸡丁".into()).await;

    while let Some(msg) = rx.recv().await {
        println!("处理订单：{}", msg);

        break;
    }
}
