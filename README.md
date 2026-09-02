# Mymq-rs

一个用 Rust + tokio 从零手写的**消息队列（MQ）**学习项目。目标是：不依赖任何现成 MQ 框架，一步一步实现一个支持**广播/订阅 + 消息确认（ack）+ 超时重投 + 网络化（QUIC）**的消息中间件，借此深入理解 Rust 异步编程与分布式系统基础。
<br>教程为 Deepseek-V4-Flash 纯Ai生成，无参考价值，不建议他人直接使用。
<br>代码部分纯手写，不依赖AI生成。

## 项目现状

当前 `src/main.rs` 是一个**最小的内存版队列 demo**：`Broker` 用 `HashMap` 管理队列，支持发布字符串消息并由单个接收者消费。这只是一个起点，真正的 MQ 能力正在规划中逐步实现。

**技术栈**：Rust（edition 2024）+ [tokio](https://tokio.rs)（异步运行时）

```toml
[dependencies]
tokio = { version = "1.53.1", features = ["rt-multi-thread", "macros", "sync"] }
```

## 核心特性（规划）

| 特性 | 说明 | 状态 |
|------|------|------|
| 广播/订阅（Pub-Sub） | 一条消息广播给所有订阅者，各自独立处理 | 🚧 规划中 |
| 消息确认（ack） | 消费者处理成功才确认，失败可重投 | 🚧 规划中 |
| 超时重投 | 拉取后超时未确认的消息自动重投 | 🚧 规划中 |
| 每订阅者独立进度 | 每个订阅者拥有独立 pending/inflight 状态 | 🚧 规划中 |
| QUIC 网络化 | 用 quinn + protobuf 提供跨网络服务 | 🔮 后续阶段 |
| 持久化 | 消息落盘，重启不丢 | 🔮 后续阶段 |
| topic 路由 | 类似 RabbitMQ 的 exchange 路由 | 🔮 后续阶段 |

## 规划路线

项目采用**分阶段**推进，每阶段都独立可运行、可验证：

```mermaid
flowchart LR
    S1[✅ 阶段1<br/>内存版广播订阅] --> S2[阶段2<br/>QUIC+protobuf 网络化]
    S2 --> S3[阶段3<br/>持久化]
    S3 --> S4[阶段4<br/>topic 路由]
    S4 --> S5[阶段5<br/>HTTP 管理接口]
```

### 学习文档

- [`LEARNING.md`](./LEARNING.md) —— **阶段 1**：内存版广播/订阅 + 每订阅者独立 ack + 超时重投（引导式分步教程）
- [`LEARNING-2-quic.md`](./LEARNING-2-quic.md) —— **阶段 2**：用 quinn（QUIC）+ protobuf 把 broker 改造成跨网络服务（引导式分步教程）

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
│       ├── server.rs      # 阶段2：QUIC 服务端
│       ├── client.rs      # 阶段2：QUIC 客户端命令行工具
│       └── gen_cert.rs    # 阶段2：生成本地自签名证书
├── proto/
│   └── mq.proto           # 阶段2：命令与消息协议定义
├── build.rs               # 阶段2：protobuf 代码生成
├── LEARNING.md            # 阶段1 学习文档
├── LEARNING-2-quic.md     # 阶段2 学习文档
└── Cargo.toml
```

## 路线图

- [x] 内存版队列 demo
- [x] **阶段 1**：广播订阅 + ack + 超时重投（见 `LEARNING.md`）
- [ ] **阶段 2**：QUIC + protobuf 网络化（见 `LEARNING-2-quic.md`）
- [ ] **阶段 3**：持久化
- [ ] **阶段 4**：topic 路由
- [ ] **阶段 5**：HTTP 管理接口

---

> 本项目为**学习练手**用途，供个人深入理解消息队列与 Rust 异步编程，不用于生产环境。
