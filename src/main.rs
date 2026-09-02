use core::time;
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    task,
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, mpsc};

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

    fn dequeue(&mut self, topic: &str, sub: &str) -> Option<Message> {
        let state = self.topics.get_mut(topic)?.subscribers.get_mut(sub)?;
        let msg = state.pending.pop_front()?;
        // 附带时间戳，记录消息发送时间
        state.inflight.insert(msg.id, (msg.clone(), Instant::now()));
        Some(msg)
    }

    fn ack(&mut self, topic: &str, sub: &str, msg_id: u64) -> bool {
        match self
            .topics
            .get_mut(topic)
            .and_then(|t| t.subscribers.get_mut(sub))
        {
            // 这里真的删除了消息，后续可以改为逻辑删除
            Some(s) => s.inflight.remove(&msg_id).is_some(),
            None => false,
        }
    }

    fn nack(&mut self, topic: &str, sub: &str, msg_id: u64) -> bool {
        if let Some(s) = self
            .topics
            .get_mut(topic)
            .and_then(|t| t.subscribers.get_mut(sub))
        {
            if let Some((msg, _)) = s.inflight.remove(&msg_id) {
                s.pending.push_back(msg);
                return true;
            }
        }
        false
    }

    /// 重投超时未确认的消息（对每个订阅者自己的 inflight 巡检）
    /// 当前没有重试次数上限，会导致一直循环
    fn redeliver_timeout(&mut self, timeout: Duration) {
        for topic in self.topics.values_mut() {
            for sub in topic.subscribers.values_mut() {
                let expired: Vec<u64> = sub
                    .inflight
                    .iter()
                    .filter(|(_, (_, at))| at.elapsed() > timeout)
                    .map(|(id, _)| *id)
                    .collect();
                for id in expired {
                    if let Some((msg, _)) = sub.inflight.remove(&id) {
                        sub.pending.push_back(msg);
                    }
                }
            }
        }
    }

    fn stats(&self) -> Vec<(String, String, usize, usize)> {
        let mut out = Vec::new();
        for (topic_name, topic) in &self.topics {
            for (sub_name, sub_state) in &topic.subscribers {
                out.push((
                    topic_name.clone(),
                    sub_name.clone(),
                    sub_state.pending.len(),
                    sub_state.inflight.len(),
                ));
            }
        }
        out
    }
}

/// 等待消息
async fn wati_for_message(broker: &Arc<Mutex<Broker>>, topic: &str, sub: &str) {
    let notify = broker.lock().await.notifier.clone();
    loop {
        let notified = notify.notified();
        {
            let b = broker.lock().await;
            let ready = b
                .topics
                .get(topic)
                .and_then(|t| t.subscribers.get(sub))
                .map(|s| !s.pending.is_empty())
                .unwrap_or(false);
            if ready {
                return;
            }
        }
        notified.await;
    }
}
/// 订阅者
async fn subscriber(broker: Arc<Mutex<Broker>>, topic: &str, name: &str, fail_rate: u32) {
    loop {
        wati_for_message(&broker, topic, name).await;
        let msg = {
            let mut b = broker.lock().await;
            b.dequeue(topic, name)
        };
        if let Some(msg) = msg {
            tokio::time::sleep(Duration::from_millis(80)).await;
            // let success = (msg.id as u32) % 100 >= fail_rate;
            let success = rand::random::<u32>() % 100 >= fail_rate;
            let mut b = broker.lock().await;
            if success {
                b.ack(topic, name, msg.id);
                println!("[{}] ✅ 处理成功: [{}] {}", name, msg.id, msg.body);
            } else {
                b.nack(topic, name, msg.id);
                println!("[{}] 🔁 处理失败重投: [{}] {}", name, msg.id, msg.body);
            }
        }
    }
}

/// 重投超时未确认的消息（对每个订阅者自己的 inflight 巡检）
async fn redelivery_worker(broker: Arc<Mutex<Broker>>, timeout: Duration) {
    loop {
        tokio::time::sleep(timeout).await;
        broker.lock().await.redeliver_timeout(timeout);
    }
}

#[tokio::main]
async fn main() {
    let broker = Arc::new(tokio::sync::Mutex::new(Broker::new()));

    {
        let mut b = broker.lock().await;
        b.subscribe("orders", "订单推送");
        b.subscribe("orders", "库存扣减");
        b.subscribe("orders", "报表分析");
    }

    {
        let mut b = broker.lock().await;
        b.publish("orders", "事件：新订单 001".into());
        b.publish("orders", "事件：新订单 002".into());
        b.publish("orders", "事件：新订单 003".into());
        b.publish("orders", "事件：新订单 004".into());
    }
    println!("=== 已发布 4 条事件，广播给 3 个订阅者 ===");

    let mut tasks = Vec::new();
    for name in ["订单推送", "库存扣减", "报表分析"] {
        let b = Arc::clone(&broker);
        tasks.push(tokio::spawn(subscriber(b, "orders", name, 30)));
    }

    let b = Arc::clone(&broker);
    tasks.push(tokio::spawn(redelivery_worker(b, Duration::from_secs(2))));

    for _ in 0..5 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let b = broker.lock().await;
        println!("\n=== 当前订阅者状态 ===");
        for (t, s, pending, inflight) in b.stats() {
            println!(
                "topic[{}] 订阅者[{}]: 待投递={} 待确认={}",
                t, s, pending, inflight
            );
        }
    }

    tokio::time::sleep(Duration::from_secs(5)).await;
    for t in tasks {
        t.abort();
    }

    println!("\n=== 演示结束 ===");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ack_nack_isolation() {
        let mut b = Broker::new();
        b.subscribe("orders", "A");
        b.subscribe("orders", "B");
        b.publish("orders", "测试消息".into());

        // A 消费消息，NACK
        let msg_a = b.dequeue("orders", "A").unwrap();
        assert!(b.nack("orders", "A", msg_a.id));

        // A 的消息被重投回 A 的 pending
        let state_a = b
            .topics
            .get("orders")
            .unwrap()
            .subscribers
            .get("A")
            .unwrap();
        assert_eq!(state_a.pending.len(), 1);
        assert!(state_a.inflight.is_empty());

        // B 的 pending 仍是 1——那是广播复制给 B 自己的那份，A 的操作碰不到它
        let state_b = b
            .topics
            .get("orders")
            .unwrap()
            .subscribers
            .get("B")
            .unwrap();
        assert_eq!(state_b.pending.len(), 1);
        assert!(state_b.inflight.is_empty());
    }

    #[tokio::test]
    async fn redeliver_expired_message() {
        let mut b = Broker::new();
        b.subscribe("orders", "A");
        b.publish("orders", "超时消息".into());

        let msg = b.dequeue("orders", "A").unwrap();
        let s0 = b
            .topics
            .get("orders")
            .unwrap()
            .subscribers
            .get("A")
            .unwrap();
        assert_eq!(s0.pending.len(), 0);

        let state = b
            .topics
            .get_mut("orders")
            .unwrap()
            .subscribers
            .get_mut("A")
            .unwrap();
        if let Some((_, at)) = state.inflight.get_mut(&msg.id) {
            *at = Instant::now() - Duration::from_secs(60);
        }

        b.redeliver_timeout(Duration::from_secs(1));

        let state = b
            .topics
            .get("orders")
            .unwrap()
            .subscribers
            .get("A")
            .unwrap();
        assert_eq!(state.pending.len(), 1);
        assert!(state.inflight.is_empty());
    }
}
