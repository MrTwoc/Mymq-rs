# Mymq-rs

一个用 Rust + tokio 从零手写的**消息队列（MQ）**学习项目。目标是：不依赖任何现成 MQ 框架，一步一步实现一个支持**广播/订阅 + 消息确认（ack）+ 超时重投 + 网络化（QUIC）**的消息中间件，借此深入理解 Rust 异步编程与分布式系统基础。
<br>教程文档由 Deepseek-V4-Flash 生成，代码纯手写；文档与代码同步演进，可能仍有滞后之处。

## 项目现状

- `src/broker.rs`：**阶段 1 已完成**——`Broker` 以「topic → 订阅者 → 各自的 pending/inflight」组织状态，支持广播发布、拉取、ack/nack、超时重投，并配有单元测试。
- `src/main.rs`：阶段 1 的并发演示（3 个订阅者 + 后台重投巡检 + 状态打印）。
- `src/bin/tcp_server.rs` / `tcp_client.rs`：阶段 2 的 **TCP 文本协议过渡版**，用于在引入 QUIC 前先跑通「网络 = 包一层 broker 方法」。
- `proto/mq.proto` + `build.rs` + `src/proto.rs`：阶段 2 的 **protobuf 协议定义与代码生成**已就绪。
- `src/lib.rs`：库入口（`pub mod broker; pub mod proto;`），供 `main` 与各 `bin` 复用。

**下一步**：完成阶段 2 的 QUIC 服务端/客户端，并按 `LEARNING-2.5-reliability.md` 补上重试上限与死信队列。

**技术栈**：Rust（edition 2024）+ [tokio](https://tokio.rs)（异步运行时）

```toml
[dependencies]
tokio = { version = "1.53.1", features = ["rt-multi-thread", "macros", "sync", "time", "net", "io-util"] }
rand = "0.10.2"
quinn = "0.11.11"      # 阶段2：QUIC 传输
prost = "0.14.4"       # 阶段2：protobuf 编解码
rcgen = "0.14.10"      # 阶段2：自签名证书
anyhow = "1.0.104"
bytes = "1.12.1"

[build-dependencies]
prost-build = "0.14.4"
```

## 核心特性

| 特性 | 说明 | 状态 |
|------|------|------|
| 广播/订阅（Pub-Sub） | 一条消息广播给所有订阅者，各自独立处理 | ✅ 已完成（阶段 1） |
| 消息确认（ack/nack） | 消费者处理成功才确认，失败可重投 | ✅ 已完成（阶段 1） |
| 超时重投 | 拉取后超时未确认的消息自动重投 | ✅ 已完成（阶段 1） |
| 每订阅者独立进度 | 每个订阅者拥有独立 pending/inflight 状态 | ✅ 已完成（阶段 1） |
| 重试上限 + 死信队列（DLQ） | 失败消息超过重试上限后隔离归档，可查看与重放 | 📘 教程已就绪（阶段 2.5） |
| QUIC 网络化 | 用 quinn + protobuf 提供跨网络服务 | 🚧 进行中（阶段 2） |
| 持久化 | 消息落盘，重启不丢 | 🔮 后续阶段 |
| topic 路由 | 类似 RabbitMQ 的 exchange 路由 | 🔮 后续阶段 |
| 管理台 | salvo + REST API + Web 页面 | 🔮 后续阶段 |

## 规划路线

项目采用**分阶段**推进，每阶段都独立可运行、可验证：

```mermaid
flowchart LR
    S1[✅ 阶段1<br/>内存版广播订阅] --> S2[🚧 阶段2<br/>QUIC+protobuf 网络化]
    S2 --> S25[📘 阶段2.5<br/>重试上限+死信队列]
    S25 --> S3[阶段3<br/>持久化]
    S3 --> S4[阶段4<br/>topic 路由]
    S4 --> S5[阶段5<br/>salvo 管理台]
```

### 学习文档

- [`LEARNING.md`](./LEARNING.md) —— **阶段 1**：内存版广播/订阅 + 每订阅者独立 ack + 超时重投（引导式分步教程）
- [`LEARNING-2-quic.md`](./LEARNING-2-quic.md) —— **阶段 2**：用 quinn（QUIC）+ protobuf 把 broker 改造成跨网络服务（引导式分步教程）
- [`LEARNING-2.5-reliability.md`](./LEARNING-2.5-reliability.md) —— **阶段 2.5**：给重投加上限 + 死信队列（DLQ），补掉「无限重投」缺陷（引导式分步教程）

> 文档采用「分步引导 + 手动补齐 + 运行验证」形式，核心逻辑留给你亲手实现，适合学习练手。

## 快速开始

```bash
# 运行当前内存版 demo
cargo run
```

## 设计目标

- **学习优先**：代码结构清晰、循序渐进，每一步都能独立编译与观察效果
- **工程化演进**：从内存单进程 → QUIC 客户端/服务端 → 持久化，逐步贴近生产 MQ 架构
- **技术深度**：覆盖 tokio 异步并发、并发数据结构（Arc/Mutex/Notify）、网络协议（QUIC/protobuf）、文件 I/O（持久化）

## 目录结构（规划）

```
Mymq-rs/
├── src/
│   ├── main.rs            # 入口（当前为内存版 demo）
│   ├── broker.rs          # 阶段1：broker 核心逻辑
│   ├── proto.rs           # 阶段2：protobuf 生成代码入口
│   └── bin/
│       ├── tcp_server.rs  # 阶段2：TCP 文本协议过渡版（服务端，可选）
│       ├── tcp_client.rs  # 阶段2：TCP 文本协议过渡版（客户端，可选）
│       ├── gen_cert.rs    # 阶段2：生成本地自签名证书
│       ├── server.rs      # 阶段2：QUIC 服务端
│       └── client.rs      # 阶段2：QUIC 客户端命令行工具
├── proto/
│   └── mq.proto           # 阶段2：命令与消息协议定义
├── build.rs               # 阶段2：protobuf 代码生成
├── LEARNING.md            # 阶段1 学习文档
├── LEARNING-2-quic.md     # 阶段2 学习文档
├── LEARNING-2.5-reliability.md  # 阶段2.5 学习文档
└── Cargo.toml
```

## 路线图

- [x] 内存版队列 demo
- [x] **阶段 1**：广播订阅 + ack + 超时重投（见 `LEARNING.md`）
- [ ] **阶段 2**：QUIC + protobuf 网络化（见 `LEARNING-2-quic.md`）
- [ ] **阶段 2.5**：重试上限 + 死信队列（见 `LEARNING-2.5-reliability.md`）
- [ ] **阶段 3**：持久化
- [ ] **阶段 4**：topic 路由
- [ ] **阶段 5**：salvo 管理台（REST API + Web 页面）

---

> 本项目为**学习练手**用途，供个人深入理解消息队列与 Rust 异步编程，不用于生产环境。
