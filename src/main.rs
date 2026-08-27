use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Instant,
};
use tokio::sync::mpsc;

/// 这里可以尝试使用 Arc 来共享消息，避免克隆消息
#[derive(Clone)]
struct Message {
    id: u64,
    body: String,
}

struct Subscriber_State {
    pending: VecDeque<Message>,
    inflight: HashMap<u64, (Message, Instant)>,
}

impl Subscriber_State {
    fn new() -> Self {
        Subscriber_State {
            pending: VecDeque::new(),
            inflight: HashMap::new(),
        }
    }
}

struct Topic {
    subscribers: HashMap<String, Subscriber_State>,
}
impl Topic {
    fn new() -> Self {
        Topic {
            subscribers: HashMap::new(),
        }
    }
}
struct Broker {
    // queues: HashMap<String, mpsc::Sender<String>>,
    topics: HashMap<String, Topic>,
    next_id: u64,
    notifier: Arc<tokio::sync::Notify>,
}

impl Broker {
    fn new() -> Self {
        Broker {
            topics: HashMap::new(),
            next_id: 1,
            notifier: Arc::new(tokio::sync::Notify::new()),
        }
    }

    // fn create_queue(&mut self, name: &str) -> mpsc::Receiver<String> {
    //     let (tx, rx) = mpsc::channel(1024);
    //     self.queues.insert(name.to_string(), tx);
    //     rx
    // }

    fn publish(&mut self, topic: &str, body: String) -> u64 {
        let id = self.next_message_id();
        let msg = Message { id, body };
        if let Some(t) = self.topics.get_mut(topic) {
            for sub in t.subscribers.values_mut() {
                sub.pending.push_back(msg.clone());
            }
            self.notifier.notify_waiters();
        }
        id
    }

    fn subscribe(&mut self, topic: &str, subscriber: &str) -> &mut Subscriber_State {
        self.topics
            .entry(topic.to_string())
            .or_insert_with(Topic::new)
            .subscribers
            .entry(subscriber.to_string())
            .or_insert(Subscriber_State::new())
    }

    fn next_message_id(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }
}
#[tokio::main]
async fn main() {
    let mut broker = Broker::new();

    broker.subscribe("topic1", "sub1");
    broker.subscribe("topic1", "sub2");

    let id = broker.publish("topic1", "hello".to_string());

    // publish 结束，&mut 借用已释放，可以重新拿引用读取 len
    let s1 = broker
        .topics
        .get("topic1")
        .unwrap()
        .subscribers
        .get("sub1")
        .unwrap();
    let s2 = broker
        .topics
        .get("topic1")
        .unwrap()
        .subscribers
        .get("sub2")
        .unwrap();

    println!(
        "发布了消息 id = {}, sub1 pending len = {}, sub2 pending len = {}",
        id,
        s1.pending.len(),
        s2.pending.len()
    );
}
