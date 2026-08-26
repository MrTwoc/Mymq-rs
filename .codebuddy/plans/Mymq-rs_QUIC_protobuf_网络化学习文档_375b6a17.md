---
name: Mymq-rs QUIC+protobuf 网络化学习文档
overview: 在现有内存广播订阅 MQ 基础上，编写一份延续 LEARNING.md 风格的分步学习文档，指导用户用 quinn（QUIC）+ protobuf 实现网络化改造，将 Broker 的 publish/dequeue/ack/nack 暴露为 QUIC stream 上的自定义命令，protobuf 负责消息与命令的序列化。
todos:
  - id: explore-doc-style
    content: 使用 [subagent:code-explorer] 复核 LEARNING.md 的章节结构、分步模板与扩展路径写法，作为新文档风格与衔接基准
    status: completed
  - id: write-quic-doc
    content: 在项目根目录撰写 LEARNING-2-quic.md：QUIC/HTTP3 概念、protobuf 概念、自定义命令协议设计、分步实现（依赖→proto生成→Broker命令层→quinn服务端→客户端→命令行演示）、扩展路径更新
    status: completed
    dependencies:
      - explore-doc-style
---

## 需求概述
在已完成的 `LEARNING.md`（内存版广播订阅 MQ 教学）基础上，新增一份学习文档，把扩展路径中的「第 2 阶段：TCP 服务化」升级为 **HTTP/3 + QUIC 网络化**，并引入 **protobuf** 做消息序列化。文档延续分步引导式教学风格，供用户手动实现。

## 产品定位
一份面向学习的引导式 Markdown 教学文档 `LEARNING-2-quic.md`，与 `LEARNING.md` 并列放在项目根目录，教用户把内存版 MQ 改造成「QUIC stream + protobuf」的客户端/服务端架构。

## 核心特性
- 衔接 `LEARNING.md`：明确前置条件是「内存版广播订阅 + 每订阅者 ack」已完成（或正在完成）
- 概念讲解：QUIC/HTTP3 与 TCP 对比、为什么适合做 MQ 传输、quinn 连接与 stream 模型
- 概念讲解：protobuf、`.proto` 定义、prost/prost-build 生成流程、build.rs 配置
- 自定义命令协议设计：Command（命令）+ Message（消息）的 proto 定义
- 分步实现：依赖配置 → proto 定义与代码生成 → Broker 命令处理层 → quinn 服务端监听 → quinn 客户端 → 命令行演示工具
- 每步给出「目标 / 概念讲解 / 实现提示 / 验证方式」（延续 LEARNING.md 风格）
- 更新并呼应扩展路径路线图（原 TCP 阶段改为 QUIC+protobuf）


## 技术选型
- **QUIC/HTTP3 传输**：`quinn`（tokio 原生、Rust 社区主流、异步 API 友好，最适合学习入门）
- **协议序列化**：`prost`（protobuf 编解码）+ `prost-build`（build.rs 从 `.proto` 生成 Rust 代码）
- **运行时**：继续沿用 tokio（多线程运行时）
- **加密**：QUIC 自带 TLS，用自签名证书（如 `rcgen` 生成，或教学简化用预置证书）

## 关键决策与理由
1. **用 quinn 而非 tonic/gRPC**：用户选「QUIC stream + 自定义命令」，gRPC 基于 HTTP/2，与 QUIC/HTTP3 是不同协议族，不混用。quinn 直接在 stream 上承载自定义 protobuf 命令，贴合现有 Broker API，学习成本也更可控。
2. **QUIC 承载业务命令**：利用 QUIC 的多路复用 stream（双向），每个请求开一条 stream，天然支持并发命令，且无 TCP 的粘包/拆包问题（stream 自带消息边界语义）。
3. **prost 而非手动编解码**：protobuf 提供跨语言兼容与类型安全，prost 生成代码避免手写二进制解析，贴近生产实践。
4. **自签名证书**：QUIC 强制 TLS，本地学习用 `rcgen` 生成自签名证书即可，重点在协议与业务而非 PKI。

## 网络架构
QUIC 一条连接上可开多条 stream，服务端为每个入站 stream 派一个 task，读取并解析 protobuf 命令，分发到共享的 `Arc<Mutex<Broker>>` 处理。

```mermaid
flowchart LR
    C[客户端] -->|QUIC连接| S[quinn 服务端]
    S -->|每个 stream 一个 task| H[命令分发层]
    H -->|protobuf 解码| B[Arc Mutex Broker]
    B -->|处理结果| H -->|protobuf 编码| S -->|stream 回复| C
```

## 目录结构（交付物）
```
Mymq-rs/
├── LEARNING.md          # 现有文档（内存版），不改动
├── LEARNING-2-quic.md   # [NEW] 本任务交付的教学文档
├── Cargo.toml           # 文档中指导用户追加依赖，不改动
└── proto/
    └── mq.proto         # [NEW] 文档指导用户新建的命令/消息协议定义
```

## 实现注意
- 文档为纯教学资料，不改动任何现有代码；但文档会指导用户**手动新建** `proto/mq.proto`、修改 `Cargo.toml`、配置 `build.rs`、新建 `src/bin/` 下的服务端/客户端程序。
- 依赖建议：`quinn`、`prost`、`prost-build`（build-dep）、`rcgen`、`tokio`。版本需给出合理约束与 `rust-version` 注意事项。
- 每步可独立验证（`cargo build` / `cargo run`），避免依赖前步未讲内容。
- 涉及安全：明确 QUIC 证书为本地自签名，仅用于学习，不用于生产。


## Agent 扩展
### SubAgent
- **code-explorer**
  - 用途：确认 `LEARNING.md` 现有的章节结构、7 步分步风格、以及第 3 章扩展路径的写法，确保新文档 `LEARNING-2-quic.md` 在结构、术语、风格上与其保持一致和衔接。
  - 预期结果：提取 `LEARNING.md` 的分步模板（目标/概念讲解/实现提示/验证方式）与扩展路径地图的表述，作为新文档的骨架参考，避免风格断裂。
