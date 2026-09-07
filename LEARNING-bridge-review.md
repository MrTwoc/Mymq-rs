# 衔接审查报告：LEARNING.md → LEARNING-2-quic.md

> 审查日期：2026-09-02
> 审查对象：`LEARNING.md`（阶段 1）与 `LEARNING-2-quic.md`（阶段 2）之间的内容衔接，以及你的 `src/main.rs` 是否达到阶段 2 文档第 0 章声明的前置条件。
> 审查方法：逐项比对两份文档的「结构 / 方法 / 概念 / 工程约定」对接点；并将文档前置条件与你当前代码逐条核对。

---

## 结论摘要

- **文档对文档的衔接整体良好**：`LEARNING-2-quic.md` 在结构、方法签名、概念心智模型上和第 1 阶段**刻意严格对齐**，可以无缝启动。
- **但存在 3 处"未言明的衔接缺口"**：阶段 1 的「超时重投」和「等待唤醒（Notify）」两大机制**没有接入阶段 2 的网络架构**；步骤一的模块抽取指引有隐藏可见性坑。
- **你的代码与文档有两处小偏差**（id 从 2 开始、`subscribe` 返回引用），不影响大局但会让文档示例输出对不上。

综合衔接度：**结构/方法层 ≈ 95%**，**功能特性完整延续 ≈ 70%**。

---

## 一、逐项衔接核对表（文档 ↔ 文档）

| # | 核对项 | LEARNING.md 侧依据 | LEARNING-2-quic.md 侧依据 | 判定 |
|---|--------|--------------------|---------------------------|------|
| 1 | 前置依赖声明 | 第 0 章 Cargo.toml：tokio 需 `rt-multi-thread/macros/sync/time` | 步骤一：需补 `net`/`io-util`，并说明"`time` 在 LEARNING.md 步骤七就已需要" | ✅ 完全对接 |
| 2 | 数据结构清单 | 第 2 章蓝图 + 最终核对清单：`Message`/`SubscriberState`/`Topic`/`Broker` | 第 0 章前置条件逐条列出相同四结构 | ✅ 完全一致 |
| 3 | Broker 方法清单 | 最终核对清单：`new`/`next_message_id`/`subscribe`/`publish`/`dequeue`/`ack`/`nack`/`redeliver_timeout`/`stats` | 第 0 章前置条件列出相同 7 个核心方法（不含 `new`） | ✅ 完全一致 |
| 4 | 方法签名对齐 | 步骤四~六：`publish(&mut self, topic, body) -> u64`、`dequeue -> Option<Message>`、`ack/nack -> bool`、`redeliver_timeout(&mut self, Duration)` | 步骤二 tcp_server 与步骤五 `handle_command` 按相同签名调用 | ✅ 逐一匹配 |
| 5 | pub 化指引 | 阶段 1 全部为私有项（单 main.rs 场景） | 步骤一明确：类型/方法加 `pub`，`Message.id/body` 必须 `pub`（网络层要读） | ✅ 指引到位 |
| 6 | 概念心智模型 | 广播/订阅、每订阅者独立 ack、帧边界预告（第 3 章表格"粘包/拆包"） | 第 1 章概念 + 步骤二正式处理帧边界（`\n` 划界）并回顾问题 | ✅ 无缝续接 |
| 7 | 路线图变化自解释 | 第 3 章把阶段 2 描述为"TCP 服务化" | 第 0 章明确声明"TCP 服务化升级为 QUIC+protobuf，保留 TCP 过渡步骤" | ✅ 已自我解释 |
| 8 | `stats()` 复用 | 步骤七让你实现 `stats() -> Vec<(String,String,usize,usize)>` | 后续阶段（HTTP 管理接口）明说复用 `stats()` | ✅ |
| 9 | 旧测试迁移 | main.rs 底部 2 个 `#[tokio::test]`（访问 `b.topics` 私有字段） | 步骤一只说"demo 函数可保留"，**未指示把测试一并搬进 `broker.rs`** | ⚠️ 未言明（见缺口 3） |
| 10 | demo 函数保留可行性 | `wait_for_message`/`subscriber`/`redelivery_worker` 直接访问 `topics`/`notifier` 私有字段 | 步骤一："demo 函数可以保留" | ⚠️ 有坑（见缺口 3） |
| 11 | 超时重投的网络接线 | 阶段 1 核心能力（`redelivery_worker` 后台巡检） | 协议 oneof 只有 5 种命令；`server.rs` 主流程只 spawn 连接处理，**无任何巡检 worker** | ❌ 缺口（见缺口 1） |
| 12 | Notify 等待机制的映射 | `publish` 里 `notify_waiters()` 唤醒等待订阅者 | 网络版 DEQUEUE 为客户端主动拉取，服务端**无通知/推送路径** | ⚠️ 未言明的设计取舍（见缺口 2） |
| 13 | proto 引入时机 | — | 步骤一~三不依赖 proto，步骤五才在 broker 引入 `crate::proto::mq` | ✅ 顺序自洽，不会提前编译失败 |
| 14 | `[lib] name = "mymq"` | 单 main.rs，无 lib | 步骤一明确要求设 `[lib] name`（package 名 `Mymq-rs` 含连字符，必须显式改名才能 `use mymq::...`） | ✅ 指引到位 |

**小结**：14 项中 ✅ 10 项、⚠️ 3 项、❌ 1 项。纯文档层的结构/方法/工程约定对接是严丝合缝的，问题集中在「第 1 阶段特性的完整延续」和「模块抽取时的操作细节」。

---

## 二、你的代码 vs 第 2 阶段前置条件（文档 ↔ 代码）

对照 `LEARNING-2-quic.md` 第 0 章前置条件与你的 `src/main.rs`：

| 前置条件 | 你的现状 | 判定 |
|---------|---------|------|
| `Arc<tokio::sync::Mutex<Broker>>` 共享状态 | ✅ main.rs 第 213 行 | ✅ |
| `Broker` 具备 subscribe/publish/dequeue/ack/nack/redeliver_timeout/stats | ✅ 全部实现（第 63–156 行） | ✅ |
| 结构 `Message { id, body }` / pending+inflight / Topic / Broker | ✅ 全部存在 | ✅ |
| 两个 `#[tokio::test]` | ✅ main.rs 第 260–337 行 | ✅（但迁移方式见缺口 3） |
| tokio features 齐全 | ⚠️ 目前缺 `net`/`io-util`（阶段 2 步骤一会补） | ✅ 文档已覆盖 |
| 类型/字段可见性 | ⚠️ 全部私有，需按步骤一 pub 化 | ✅ 文档已覆盖 |

**与你代码相关的两处偏差（不影响衔接，但会让文档示例对不上）：**

1. **消息 id 从 2 开始**：文档（LEARNING.md 步骤一）要求"返回当前值并自增"，即第 1 条消息 id = 1；你的 `next_message_id()` 是先 `self.next_id += 1` 再返回，而 `next_id` 初始为 1 → 第 1 条消息 id = **2**。
   → 后果：LEARNING-2 步骤二验证示例预期 `DEQUEUE` 打印 `MSG 1 order-001`，你的输出会是 `MSG 2 order-001`，第一次对不上文档时会困惑。
   → 修复（任选其一）：`next_id` 初始化为 0；或改成 `let id = self.next_id; self.next_id += 1; id`。
2. **`subscribe` 返回 `&mut Subscriber_State`**：文档版返回 `()`。当前无实际影响（调用方都忽略返回值），但 `handle_command` 场景里若将来有人想"subscribe 后立即判断是否新建"，语义会不同。建议与文档对齐返回 `()`，把状态查改留给专门方法。

> 另注：你多加了 `rand` 依赖用于 demo 随机失败率（文档用 `msg.id % 100`），对阶段 2 无冲突，保留即可。

---

## 三、需要留意的 3 处衔接缺口（详解）

### 🔴 缺口 1：超时重投没有接入网络层（阶段 1 核心特性"悄悄消失"）

- **现象**：`LEARNING-2-quic.md` 的协议 `Command.oneof` 只有 `subscribe / publish / dequeue / ack / nack` 五种；`server.rs`（步骤六）的 main 只做 `accept → 每连接 task → accept_bi → 每流 task`，**没有任何周期性巡检**。阶段 1 里 `redelivery_worker`（每 N 秒调 `redeliver_timeout`）在阶段 2 的代码中没有对应物。
- **后果**：网络版完成后，消费者 `dequeue` 后如果进程崩溃/卡死，消息将**永远躺在 inflight 里**——这正是阶段 1 教程开篇列出的"三个核心缺陷之二（无重投机制）"的复活。文档全程未说明这一取舍。
- **修复建议（约 2 行）**：在步骤六 `server.rs` 的 main 里补上巡检 task，与内存版同构：
  ```rust
  let b = Arc::clone(&broker);
  tokio::spawn(async move {
      loop {
          tokio::time::sleep(Duration::from_secs(2)).await;
          b.lock().await.redeliver_timeout(Duration::from_secs(2));
      }
  });
  ```
  建议你把这条补进你的实现，并在文档对应位置加一行说明（这是文档遗漏点）。

### 🟡 缺口 2：Notify"等待唤醒"机制没有网络映射（广播变成纯轮询）

- **现象**：阶段 1 用 `Notify` 让订阅者无消息时睡眠、发布时被 `notify_waiters()` 唤醒。网络版协议是"客户端发 DEQUEUE 命令"，**服务端没有推送通道**——订阅者拿到 `ok:true + 空消息` 只能自己再发一次 DEQUEUE。
- **后果**：步骤八的演示脚本能跑通，是因为**人肉反复敲 dequeue**，掩盖了"订阅者如何得知新消息到达"这个问题。真正做常驻订阅者时，会变成忙轮询 DEQUEUE（阶段 1 特意避免的浪费）或需要新增长轮询/服务端推送协议。
- **说明**：这不是错误，是 pull 模型的正常代价，但文档**未言明**，容易让学习者在做"常驻订阅者"时卡住。建议心里有数：这是阶段 2 结束后值得自己思考的延伸题（如何加一条 `wait` 命令用 Notify + 超时返回，让订阅者低开销等待）。

### 🟡 缺口 3：步骤一"demo 函数可保留"有隐藏可见性坑 + 旧测试迁移未指示

- **现象 A**：`wait_for_message`/`subscriber` 直接访问 `b.topics`、`b.notifier`（私有字段）。一旦 Broker 搬进 lib 而 `main.rs` 变成独立的 bin（只能访问 lib 的 `pub` 项），这些函数**编译不过**。文档步骤一却说"demo 函数可以保留"、"cargo run 能跑内存 demo"，这条验证很可能失败。
- **现象 B**：你的两个测试在 main.rs 底部、直接访问 `b.topics` 私有字段。搬模块后若留在 bin 里同样编译不过；必须**整体移进 `broker.rs` 的 `#[cfg(test)] mod tests`**（lib 模块内可访问私有字段，天然成立）。文档步骤一只字未提搬测试。
- **建议处理**（任选，最简单是方案 1）：
  1. 抽模块时**直接把 demo 函数和内存版 main 一起删掉**（它们已无教学价值），只保留 Broker + 两个测试（测试搬进 `broker.rs`）。这样零可见性改动。
  2. 若想保留内存 demo 作为对照：给 `Broker` 增加一个 pub 查询方法（如 `fn has_pending(&self, topic, sub) -> bool`），让 `wait_for_message` 走方法而非裸字段。

---

## 四、衔接度量化小结

| 维度 | 评估 | 说明 |
|------|------|------|
| 结构/方法/签名对接 | ≈ 95% | 文档侧几乎 100%；你的代码侧有 2 处无害偏差（id 起点、subscribe 返回类型） |
| 概念与教学法衔接 | ✅ 优秀 | 广播/独立 ack/帧边界的心智模型一致，文档第 0 章还主动解释了路线变化 |
| 特性完整延续 | ≈ 70% | ack/nack/广播/订阅都接线；**超时重投未接线（缺口 1）**、**等待唤醒未映射（缺口 2）** |
| 步骤一可执行性 | ⚠️ 有卡点 | 可见性坑 + 测试迁移未说明（缺口 3），照抄"demo 可保留"会编译失败 |

---

## 五、建议操作顺序

1. **开工前**（可选但推荐）：修 `next_message_id` 让 id 从 1 开始；`subscribe` 返回类型对齐 `()`。
2. **步骤一执行时**：demo 函数与内存版 main 建议直接删除（或按缺口 3 方案 2 处理）；把 main.rs 底部的 2 个测试**整体搬进 `broker.rs`**。
3. **步骤六 server.rs 里**：补上 `redelivery_worker`（缺口 1 的 2 行代码），让超时重投在网络版继续生效。
4. **步骤八做完后**：思考缺口 2——订阅者如何低开销等待新消息（可尝试给协议加 `wait` 命令，用 Notify + 1s 超时返回），这是阶段 2 最好的延伸练习。
5. 对照本报告第 1~2 节表格，逐条确认没有其他遗漏后再开始 `cargo add`。
