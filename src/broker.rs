use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

/// 这里可以尝试使用 Arc 来共享消息，避免克隆消息
#[derive(Clone)]
pub struct Message {
    // 消息ID
    pub id: u64,
    // 消息体
    pub body: String,
}

/// 订阅者状态
pub struct SubscriberState {
    // 待处理消息队列，为什么是VecDeque？因为消息是按顺序到达的，所以需要从队头出队
    // 保序用VecDeque
    pub pending: VecDeque<Message>,
    // 发送中的消息队列，为什么是HashMap？ack是乱序到达，所以需要根据消息ID来查找
    pub inflight: HashMap<u64, (Message, Instant)>,
}

impl SubscriberState {
    pub fn new() -> Self {
        SubscriberState {
            pending: VecDeque::new(),
            inflight: HashMap::new(),
        }
    }
}

/// 主题
pub struct Topic {
    pub subscribers: HashMap<String, SubscriberState>,
}
impl Topic {
    pub fn new() -> Self {
        Topic {
            subscribers: HashMap::new(),
        }
    }
}
/// 消息代理
pub struct Broker {
    pub topics: HashMap<String, Topic>,
    pub next_id: u64,
    pub notifier: Arc<tokio::sync::Notify>,
}

impl Broker {
    pub fn new() -> Self {
        Broker {
            topics: HashMap::new(),
            next_id: 0,
            notifier: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// 发布消息
    pub fn publish(&mut self, topic: &str, body: String) -> u64 {
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

    /// 订阅主题
    pub fn subscribe(&mut self, topic: &str, subscriber: &str) -> &mut SubscriberState {
        self.topics
            .entry(topic.to_string())
            .or_insert_with(Topic::new)
            .subscribers
            .entry(subscriber.to_string())
            .or_insert(SubscriberState::new())
    }

    /// 先自增、再返回 ⇒ `next_id` 初值为 0 时，首条消息 id = 1。
    /// 永远不会发出 id = 0，避免与 proto3 中 `uint64` 的默认值（0）撞车。
    fn next_message_id(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }

    /// 出队消息
    pub fn dequeue(&mut self, topic: &str, sub: &str) -> Option<Message> {
        let state = self.topics.get_mut(topic)?.subscribers.get_mut(sub)?;
        let msg = state.pending.pop_front()?;
        // 附带时间戳，记录消息发送时间
        state.inflight.insert(msg.id, (msg.clone(), Instant::now()));
        Some(msg)
    }

    /// 确认消息
    pub fn ack(&mut self, topic: &str, sub: &str, msg_id: u64) -> bool {
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

    /// 拒绝消息
    pub fn nack(&mut self, topic: &str, sub: &str, msg_id: u64) -> bool {
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
    pub fn redeliver_timeout(&mut self, timeout: Duration) {
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

    /// 获取代理状态
    pub fn stats(&self) -> Vec<(String, String, usize, usize)> {
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
