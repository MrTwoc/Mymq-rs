---
name: Mymq-rs 学习文档扩展路径
overview: 将「广播订阅 + 每订阅者独立 ack」的 MQ 改造方案整理为一份 Markdown 学习文档，写明完整扩展路径，教学过程尽可能详细，供用户手动实现项目。
todos:
  - id: explore-current
    content: 使用 [subagent:code-explorer] 复核 src/main.rs 现状与 Cargo.toml 依赖，提取文档开篇的现状分析素材
    status: completed
  - id: write-doc
    content: 在项目根目录撰写 LEARNING.md：现状分析、广播订阅概念讲解、七个分步实现（含讲解/提示/验证）、扩展路径路线图
    status: completed
    dependencies:
      - explore-current
---

## 需求概述
将此前讨论的「广播/订阅（pub-sub）+ 每订阅者独立 ack/重投」消息队列扩展方案，整理成一份**面向学习者的 Markdown 教学文档**，供用户手动逐步实现项目。

## 核心需求
- 输出一份结构完整、过程详尽的 Markdown 学习文档，放在项目根目录（如 `LEARNING.md`）
- 教学方式为**分步引导 + 逐步构建**，避免一次性抛全部代码，让用户跟随文档自己动手实现
- 写明**完整扩展路径**（当前步 → TCP 服务化 → 持久化 → topic 路由 → HTTP 管理接口），让用户有清晰的后续学习路线
- 覆盖用户选定的四类技术方向：tokio 异步并发、并发数据结构、文件与 I/O、网络协议设计

## 文档内容要点
- 讲解当前 `src/main.rs` 的现状与三个核心缺陷（单接收者、无 ack、状态难共享）
- 讲解「广播订阅」与「竞争消费」两种语义的区别，及广播场景下 ack 的作用域问题（每订阅者独立进度）
- 数据结构设计：Message / SubscriberState / Topic / Broker 及其内部状态流转
- 核心 API 语义：publish（广播复制）、subscribe、dequeue、ack / nack、redeliver_timeout（超时巡检）
- 并发方案：Arc + Mutex + Notify（避免忙轮询）
- 每一步给出目标、讲解、实现提示、验证方法（编译 + 运行观察输出）
- 扩展路径地图，标注每一步的技术要点与前置依赖

## 交付物
一份可独立阅读、步骤可执行、含扩展路线的 Markdown 学习文档，内容详尽但以教学引导为主，代码按步骤逐步给出。


## 技术选型
- 文档为纯 Markdown 教学文档，放在项目根目录 `LEARNING.md`，不改动任何现有代码
- 技术内容基于项目现状：Rust edition 2024 + tokio（rt-multi-thread / macros / sync），数据结构不新增依赖
- 并发方案采用 `Arc<Mutex<Broker>>` + `tokio::sync::Notify`（符合当前 tokio 依赖，避免忙轮询）

## 文档结构与教学路径
文档采用「现状分析 → 概念讲解 → 分步实现 → 扩展路径」的渐进式结构。

### 分步实现（每步含：目标 / 讲解 / 实现提示 / 验证）
1. **步骤一：消息模型**——引入 `Message { id, body }` 与全局自增 id，讲解为什么要 id（ack 定位、日志跟踪）
2. **步骤二：订阅者状态**——`SubscriberState`（pending 队列 + inflight 待确认表），讲解 pending→inflight 的生命周期
3. **步骤三：Topic 与订阅注册**——`subscribe` 注册订阅者，讲解「每订阅者独立进度」与广播复制语义
4. **步骤四：发布广播**——`publish` 将消息复制推给每个订阅者的 pending，并唤醒等待者
5. **步骤五：拉取与确认**——`dequeue` / `ack` / `nack`，强调 ack 只影响该订阅者自己的 inflight
6. **步骤六：超时重投巡检**——`redeliver_timeout` 后台 task，讲解时间戳与 Instant 判定
7. **步骤七：并发共享与主流程**——`Arc<Mutex<Broker>>` + `Notify` 唤醒，多订阅者 task 并发运行，统计输出演示隔离效果

### 完整扩展路径（学习路线地图）
- **第 2 阶段：TCP 服务化**——把 publish/dequeue/ack/nack 映射为 TCP 命令，`TcpListener` + 每连接 task，自定义文本/二进制协议（对应"网络协议设计"）
- **第 3 阶段：持久化**——消息 append-log 追加写磁盘、启动恢复、性能权衡（对应"文件与 I/O"）
- **第 4 阶段：topic 路由**——exchange + routing key 绑定（对应 RabbitMQ 概念）
- **第 5 阶段：HTTP 管理接口**——用 axum 做 REST API 查询/管理（可选）

## 实现注意
- 文档中代码按步骤渐进给出，每步独立可编译运行，避免依赖前步未讲内容
- 每步末尾提供验证方式（`cargo run` 后观察各订阅者输出与统计表）
- 用 Mermaid 图讲解状态流转、广播语义、扩展路线，增强可读性
- 扩展路径用图表展示阶段划分、技术要点、前置依赖，方便用户规划学习节奏

## 架构设计
- 文档为单文件学习资料，无代码架构改动
- 使用 Mermaid 图说明：消息生命周期状态流转、广播订阅数据流向、后续扩展路线阶段图


## Agent 扩展
### SubAgent
- **code-explorer**
  - 用途：确认项目现状与 `src/main.rs` 当前实现细节，确保文档中的「现状分析」与「缺陷讲解」准确对应真实代码
  - 预期结果：准确提取当前 Broker 实现的要点，用于文档开篇的现状剖析，保证教学文档的起点与用户实际代码一致
