# go-sequent 设计文档

> **本文是 [`design.md`](./design.md) 的中文版。**  
> 英文/双语源文件：[`docs/design.md`](./design.md)；配套研究：[`sequent-research.zh.md`](./sequent-research.zh.md) / [`sequent-research.md`](./sequent-research.md)。

**状态：** 仅设计（本阶段不实现框架代码）  
**模块路径：** `github.com/go-sequent/go-sequent`  
**灵感来源：** [Sequent](https://sequent.io/)（[zilverline/sequent](https://github.com/zilverline/sequent)）  
**目标场景：** 金融系统 — 性能、高可用（HA）、安全、可审计性  
**配套研究：** [`docs/sequent-research.zh.md`](./sequent-research.zh.md)

---

## 目录

1. [目标与非目标](#1-目标与非目标)
2. [Sequent → Go 映射](#2-sequent--go-映射)
3. [核心领域模型](#3-核心领域模型)
4. [包 / 模块布局](#4-包--模块布局)
5. [持久化](#5-持久化)
6. [金融系统关注点](#6-金融系统关注点)
7. [并发与性能](#7-并发与性能)
8. [安全模型](#8-安全模型)
9. [分阶段实施路线图](#9-分阶段实施路线图)
10. [已定决策（已锁定）](#10-已定决策已锁定)

---

## 1. 目标与非目标

### 1.1 目标

| 领域 | 目标 |
|------|------|
| **CQRS + ES 保真** | 提供符合 Go 习惯的 Sequent 核心闭环等价物：Command → Handler → Aggregate → EventStore → Projector/Workflow，默认强事务语义。 |
| **金融级审计** | 不可变、仅追加的事件日志；command↔event 因果链；完整性依赖 **哈希链**（无事件签名）；监管所需的留存与重放；GDPR 采用墓碑策略。 |
| **并发正确性** | 按聚合流做乐观并发；冲突错误清晰；命令受理幂等。 |
| **性能** | 高效加载（批量 + 快照）、批量追加、投影并行重建；负载下延迟可预期。 |
| **高可用（HA）** | 文档化 PostgreSQL 主从模式；投影读扩展；故障切换说明；禁止静默双写脑裂。 |
| **安全** | 在 command 边界挂载 AuthZ；PII/加密钩子；密钥永不入日志；多租户隔离钩子。 |
| **可观测性** | 可选 OpenTelemetry（`obs`）覆盖写路径：命令延迟/成败、乐观冲突、可选 outbox 深度——运维/SLO 信号，**不是**审计事实源；更细的 projector/重建 span 相对当前 `WithObs` 钩子仍属愿景。 |
| **开发体验** | 显式接口、少魔法、测试辅助（given events / when command / expect events）。 |

### 1.2 非目标 — v1 / 近期

- 不是完整应用框架（仓库内无 HTTP/gRPC 服务器或桩、无 UI、无通用 CRUD ORM）— **纯库**（Q7）。
- MVP 不做多数据库多语言 EventStore（先 PostgreSQL；接口允许后续后端）。
- 不把 GDPR「被遗忘权」产品化为 crypto-shredding；策略定为 **仅墓碑**（Q5）。
- 不做分布式多主 EventStore / CRDT 合并引擎。
- 不内嵌完整工作流引擎（Temporal/Cadence）；Workflow 保持进程内 + 可选 outbox/任务钩子（启用 outbox 时传输为 Postgres 轮询 — Q6）。
- **永不**导入 / 迁移 Ruby Sequent 数据库（Q9）；仅概念灵感，不做 dump/线格式兼容。
- 现阶段**不公开 / 不开源**（Q10）；许可证保持私有、未发布。

### 1.3 设计原则

1. **Events 是唯一事实来源**；投影可丢弃重建。
2. **聚合在处理 command 时不得读取其他聚合的状态**（靠 API 文化 + lint 指导约束）。
3. **默认 = 单库事务**覆盖 command + events + 同步 projector（与 Sequent 对齐）；异步模式须显式选择。
4. **金融默认开启**：金额类型为 **decimal**（非整数最小单位 — Q2）、幂等键、每条 command 的审计元数据。
5. **Boring Go**：接口、构造函数、`context.Context`，不强制全局可变单例（通过 DI 配置）。

---

## 2. Sequent → Go 映射

| Sequent（Ruby） | go-sequent（Go 习惯） |
|-----------------|------------------------|
| `Sequent::Command` + `attrs` + ActiveModel validations | `Command` 接口 + struct tag / `Validate() error` / 可选 `go-playground/validator` |
| `Sequent::Event` + `attrs` | `Event` 接口；按类型名注册具体 struct |
| `Sequent::ValueObject` | 不可变值对象 struct；可选 `Validate()` |
| `Sequent::AggregateRoot` + `on Event do` | 嵌入 `aggregate.Root`；注册 applier `On(Type, func)` 或生成 switch；领域方法调用 `Apply` |
| `CommandHandler` + `on Command do` | `CommandHandler` 接口；按 `reflect.Type` 或字符串类型注册表路由 |
| `CommandService` + 线程本地队列 | `command.Service`，使用**上下文作用域**的命令队列 + UoW |
| `AggregateRepository`（线程 UoW / 身份映射） | `repository.Repository`，绑定到 `context` 中的 `UnitOfWork` |
| `EventStore` + PL/pgSQL `store_events` | `eventstore.Store` 接口；`postgres` 实现等价 SQL/过程 |
| `Projector` + Persistor 抽象 | `projector.Projector` + `Persistor` 接口（`SQL` vs `ReplayBulk`） |
| `Workflow` + `after_commit` | `workflow.Workflow`；经事务回调 / outbox 的 `AfterCommit` 钩子 |
| `Sequent.configure` 全局配置 | 显式构造 `sequent.Config` + `sequent.App` |
| Command middleware / filters | `Middleware`：`func(ctx, cmd, next) error` 链 |
| `Sequent::Secret`（BCrypt） | 可选 `types.Secret`；校验后在中间件哈希 — **更推荐应用自有加密** |
| 快照用 Marshal/Base64 | 带版本的 snapshot codec（`json`/`gob`/自定义）；**禁止**无版本的不透明 Marshal |
| Unique keys | 聚合上的 `UniqueKeyProvider` → `aggregate_unique_keys` |
| Migrations / projector replay | `migrate` / `replay` CLI，镜像 prepare→replay→catchup→golive |
| Handler 自动注册 | 在 `main` / `wire` 中显式 `Register*`（更合 Go）；可选 codegen |

### 并发模型差异（关键）

Ruby Sequent 使用**每线程** UoW。Go 必须使用存于 `context.Context`（或显式传入）的**每请求 / 每命令批次** UoW。跨 goroutine 共享同一个 UoW **禁止**。可并行的是：(a) 彼此独立的 command 请求，(b) 投影重建 worker，(c) 异步消费者 — 而不是单个聚合内部的 apply。

---

## 3. 核心领域模型

### 3.1 Command

```text
type Command interface {
    CommandType() string      // stable name, e.g. "account.OpenAccount"
    AggregateID() (uuid.UUID, bool) // optional primary aggregate
    Meta() CommandMeta        // user, tenant, idempotency, trace
}
```

**CommandMeta（面向金融）：**

- `CommandID`（uuid）— 存储与幂等相关联的唯一 id  
- `IdempotencyKey`（string，可选）— 客户端提供  
- `UserID`、`TenantID`  
- `CausedByEvent` — Workflow 触发时的 `(aggregate_id, sequence_number)`  
- `IssuedAt`  
- `AuthContext` 不透明 token / claims 引用（不一定持久化）

Command 表达**意图**，在 handler 运行前校验。成功的 command 像 Sequent 的 `commands` 表一样持久化。

### 3.2 Event

```text
type Event interface {
    EventType() string
    AggregateID() uuid.UUID
    SequenceNumber() int64
    CreatedAt() time.Time
}
```

载荷 struct 保存领域字段。信封元数据（type、ids、seq、created_at、command_id）落在存储列；JSON 体省略冗余信封字段（Sequent 8 风格）。

**规则：** 过去时命名；小而扁平；重命名需迁移类型注册表与载荷。

### 3.3 AggregateRoot

```text
type AggregateRoot interface {
    ID() uuid.UUID
    AggregateType() string
    UncommittedEvents() []Event
    ClearUncommitted()
    Version() int64              // last committed sequence
    Apply(Event)                 // only via Apply helper internally
    Invariants() error           // Sequent invariants!
    UniqueKeys() map[string]any  // optional
    PartitionKey() string        // optional multi-tenant / shard key
}
```

**行为契约（与 Sequent 对齐）：**

1. 公开方法：守卫 + 决策 → `Apply(newEvent)`。
2. 状态变更**仅**发生在 event applier 中（保证 replay == live）。
3. 聚合内无 I/O。
4. 加载后与提交前调用 `Invariants()`。

**实现草图：** 嵌入提供 Apply、序号分配、未提交缓冲的 `aggregate.Base`；用户类型注册 applier。

### 3.4 AggregateRepository

- `Add(ctx, agg)`
- `Load(ctx, id, typ)` / `LoadMany`
- `LoadUntil(ctx, id, at)` — 忽略快照（用于嵌套/时间旅行/快照任务）
- `Exists` / `EnsureExists`
- `Commit(ctx, cmd)` — 从 UoW 收集未提交事件 → EventStore → 清空

**UnitOfWork + IdentityMap：** 每个 UoW 中每个 id 仅一个聚合实例。`Commit` 按**聚合注册/加载顺序**发布事件，再按 sequence（与 Sequent 一致）。

### 3.5 CommandService / Handler

```text
type Handler interface {
    Handles() []string // command types
    Handle(ctx context.Context, cmd Command) error
}

type Service struct {
    Middleware []Middleware
    Filters    []Filter
    Router     *Router
    Repo       *repository.Repository
    Store      eventstore.Store
    Tx         TxManager
}
```

**Execute 流程：**

1. 将 command 推入队列（支持 Workflow 再入队）。
2. 开启事务。
3. 队列非空时循环：middleware → filters → validate → handle → `Repo.Commit`（存储 + 发布）。
4. 提交事务 / 运行 AfterCommit 钩子。
5. 始终清空 UoW。

### 3.6 EventStore

接口职责：

- `Append(ctx, cmd, streamsWithEvents)` — 带乐观并发与唯一键的原子追加  
- `LoadStream(ctx, id)` / `LoadStreams` — 从最新兼容快照加载  
- `StreamEvents(ctx, id, opts)` — 游标分批  
- `RegisterTypes`  
- `Snapshot` API  

PostgreSQL 实现应镜像 Sequent 的 `store_events` 过程语义（见第 5 节）。

### 3.7 Projector 与 Workflow

**Projector：**

- 声明所管理的表。
- 通过注册表处理事件。
- 仅使用 `Persistor`（禁止破坏 replay 的随意 ORM）。
- 同步模式：与 append 同一事务（默认）。
- 重放模式：批量 persistor + 隔离的 replay schema。

**Workflow：**

- 响应事件做流程编排（发送 command、入队任务）。
- 不参与 projector 重建（与 Sequent 对齐）。
- 无法回滚的副作用 → `AfterCommit` 或**事务性 outbox**。

### 3.8 Serializers（序列化）

- 默认：JSON（对齐 Sequent 的 `event_json` / `command_json`）。
- 可插拔按类型的 `Codec`；载荷演进用 upcaster。
- 快照格式 = **JSON**（Q8），独立版本（`snapshot_version`）。

### 3.9 Migrations（迁移）

两条轨道：

1. **Event store schema 迁移** — 版本化 SQL（golang-migrate / goose）。
2. **投影重建** — Sequent 8 流水线：prepare → replay → catchup → optimize → golive；CLI 在 `cmd/go-sequent`。

---

## 4. 包 / 模块布局

```text
github.com/go-sequent/go-sequent
├── go.mod
├── README.md
├── docs/
│   ├── design.md                 # this document
│   └── sequent-research.md
├── cmd/
│   └── go-sequent/               # CLI: migrate, replay, snapshot
├── sequent/                      # façade: NewApp, Config
├── command/
│   ├── command.go                # Command, Meta, validation
│   ├── service.go
│   ├── middleware.go
│   └── router.go
├── event/
│   ├── event.go
│   └── registry.go               # type name ↔ factory
├── aggregate/
│   ├── root.go                   # Base + Apply
│   └── repository.go
├── eventstore/
│   ├── store.go                  # interface
│   ├── errors.go                 # OptimisticConcurrency, UniqueKey
│   └── postgres/
│       ├── store.go
│       ├── schema/               # SQL + procedures
│       └── snapshot.go
├── projector/
│   ├── projector.go
│   ├── persistor.go
│   └── replay/
├── workflow/
│   └── workflow.go
├── publish/
│   ├── publisher.go              # sync in-txn publisher
│   └── outbox/                   # optional async; Postgres polling when used
├── types/                        # Money (decimal), Secret hooks
├── security/                     # Authz interfaces (no event signing — Q3; hash chain in eventstore)
├── tenancy/                      # shared schema + tenant_id helpers
├── obs/                          # OpenTelemetry helpers
├── migrate/
└── testkit/                      # Given/When/Expect helpers
```

**依赖方向：** `command` → `aggregate` → `eventstore`；`projector`/`workflow` ← `publish`；应用导入 `sequent` 门面 + 自身领域包。领域代码**不应**导入 `eventstore/postgres`。

---

## 5. 持久化

### 5.1 推荐模式（受 Sequent 8 启发，含金融扩展）

核心表（PostgreSQL）：

| 表 | 用途 |
|----|------|
| `commands` | 仅追加的成功 command + 元数据 |
| `aggregates` | 流元数据 + 分区键 |
| `events` | 仅追加事件；主键 `(partition_key, aggregate_id, sequence_number)` |
| `*_types` | 类型注册表 |
| `aggregate_unique_keys` | 业务唯一性 |
| `snapshot_records` / `aggregates_that_need_snapshots` | 快照 |
| `idempotency_keys`（**新增**） | `(tenant_id, key)` → `command_id` / 结果状态 |
| `outbox`（**可选**） | 异步发布用的事务性 outbox；由 **Postgres 轮询** 排空（Q6） |
| `audit_access_log`（**可选**） | 谁读取了敏感聚合（合规） |

**建议在 `commands` / 事件信封上增加的金融列：**

- `tenant_id uuid`（共享 schema 多租户 — Q4）
- `idempotency_key text`（可空；与租户组合唯一）
- `correlation_id` / `causation_id`

事件带 `prev_hash` / `event_hash`（SHA-256 哈希链，Q3）。**无**签名列。

### 5.2 乐观并发

追加时（同 Sequent `store_events` 思路）：

1. 锁定聚合行（`SELECT … FOR NO KEY UPDATE`）。
2. 读取最后的 `sequence_number`。
3. 要求首个新事件 seq == last+1（空流则为 1）。
4. 插入事件；主键冲突 ⇒ 冲突。
5. 更新 unique keys。

将数据库完整性失败映射为 `ErrOptimisticConcurrency` / `ErrUniqueKeyViolation`。

### 5.3 快照（Snapshots）

- 按聚合类型设阈值；后台任务打快照。
- 快照行：`(aggregate_id, sequence_number, snapshot_type, snapshot_json, version)`。
- 加载路径：最新兼容快照 + 之后的事件。
- 聚合结构变更时：提升 `snapshot_version` 或删除快照（Sequent 建议）。

### 5.4 投影重建

镜像 Sequent 8：

1. **prepare** — 创建 `replay_schema`，复制所管理表的 DDL（索引可选）。  
2. **replay** — 按聚合 id 区间 / 事件批次并行 worker；批量插入。  
3. **catchup** — 线上继续写入时增量追赶。  
4. **optimize** — 建索引、`VACUUM`/`ANALYZE`（视情况）。  
5. **golive** — 短暂加锁 / 将表切入 `view_schema`；归档旧表。  
6. **abort** — 丢弃 replay schema。

**一致性模式：**

| 模式 | 行为 | 用途 |
|------|------|------|
| `SyncInTxn`（默认，**Q1 已决**） | Projector 在写事务内更新 | 强一致；**账本余额必须同步** |
| `OutboxAsync` | 写 events + outbox 行；worker 经 Postgres 轮询投影（Q6） | 非账本 / 外部集成路径 |
| `Hybrid` | 关键 projector 同步；其余异步 | 账本同步；搜索/通知异步 |

### 5.5 分区与 HA 说明

- 按 `partition_key`（租户、日期或哈希）分区 `events`，类似 Sequent。
- 主库负责写；副本仅在可接受异步滞后时服务**投影读** — 同步模式下读通常打主库或等待提交可见性。
- 故障切换：单写主；文档中说明同步提交 / 法定人数等运维选择下的 RPO≈0。
- 备份：持续 WAL + 定期验证恢复演练（EventStore 是核心资产）。

---

## 6. 金融系统关注点

### 6.1 不可变性与审计轨迹

- 正常运行不提供对 events 的 update/delete API。
- 业务纠错用补偿事件（如 `PaymentReversed`），禁止静默篡改。
- Command + event 关联以支持取证重放。
- 事件完整性 = **哈希链**（按聚合流 SHA-256），篡改可发现；**无**事件签名（Q3，2026-10-26 Asia/Shanghai 修订）。DB ACL/备份仍为互补手段。

### 6.2 幂等性（Idempotency）

- 客户端按租户发送 `Idempotency-Key`。
- Handler 前：若键已见 → 返回先前结果 / 空操作。
- 在命令成功的同一事务中存储该键，避免重试双执行。
- Handler 仍应防御性编写（自然键 / 守卫）。

### 6.3 金额 / decimal 边界

- **框架不发明会计规则。**
- 提供 `types.Money`，金额使用 **decimal**（Q2）— 例如 `shopspring/decimal` / `cockroachdb/apd` — 含货币代码与固定精度策略。
- **不以**整数最小单位作为框架金额表示。
- 文档化 decimal 的 JSON 编解码规则。
- 通过 lint / testkit **禁止 Command/Event 金额载荷使用 `float64`**。
- 舍入策略由领域拥有；文档写明「存储事件中的精确金额，永不从 float 重算历史金额」。

### 6.4 静态加密 / PII

- 钩子：`PayloadProtector` 接口 — 插入前加密选定 JSON 字段；加载时解密。
- 优先信封加密（DEK/KEK）+ KMS；文档化密钥轮换。
- 对事件中的 PII，应用层字段加密优于仅依赖磁盘加密。
- GDPR / 留存：**仅墓碑**（Q5）— 应用墓碑事件并停止处理可识别数据；**不以** crypto-shredding 作为产品策略；物理删除仅作例外运维并配备流修复工具。

### 6.5 多租户钩子

- 模型 = **共享 schema + `tenant_id`**（Q4）。
- Command meta 上的 `TenantID`；`partition_key` 仍可用租户（或 tenant+hash）做共享 schema 内的物理分区。
- Repository/store 在加载时强制租户匹配（纵深防御）。
- Unique key 的 scope 应含租户（`scope = "tenant:{id}:email"`）。
- 投影表携带 `tenant_id`；可选数据库层 RLS。

### 6.6 可观测性与 SLA / HA

**框架内 OTel 的作用：** 通过厂商无关钩子（`obs` + `sequent.WithObs`）让运维看到写路径健康度，而不在库内绑定某一 APM。与 EventStore / 命令审计互补——**不是**业务审计轨迹（Q3 完整性为哈希链；Q7 将 exporter 留在宿主应用）。见 [指南 §19 作用](./guide.zh.md#作用)。

**当前已实现（`obs`）：** span `sequent.Execute`；指标 `sequent.command.duration_ms`、`sequent.command.total`、`sequent.optimistic_conflict.total`；可选 `sequent.outbox.depth`。`RecordEvents` API 存在但未自动挂到 append。

**指标（设计目标）：** 命令延迟、校验失败、乐观冲突、每秒追加事件数、projector 滞后（异步）、重建进度、outbox 深度。

**链路追踪（设计目标）：** 每个 `Execute` 一个 span（已有）；handler / store append / 各 projector 子 span（尚未）。

**日志：** 结构化；默认不记录密钥/PII 载荷（使用脱敏器）。

**面向 SLA 的 HA：**

- 写路径：HA Postgres（Patroni/CloudSQL/RDS）+ 连接池（PgBouncer）。
- 读路径：投影副本；定义最大滞后 SLO。
- 通过 catchup + 短暂 golive 窗口实现重建，避免长时间全停。
- 混沌：模拟冲突风暴；验证客户端幂等。

---

## 7. 并发与性能（Go）

### 7.1 Goroutine 边界

| 可安全并行 | 必须串行 |
|------------|----------|
| 各自拥有 UoW 的独立 HTTP/gRPC 请求 | 单个聚合内的 event apply |
| 投影重建 worker（不相交的聚合） | 一次提交内的同步 projector（顺序确定） |
| 异步 outbox 消费者（若需要则按聚合保序） | 同一 scope 上冲突事务的 unique key 更新（由 DB 串行化） |

**绝不**跨 goroutine 共享 `UnitOfWork` / 聚合实例。

### 7.2 批处理

- 一次 SQL 往返 `LoadMany` 多个聚合（Sequent 多 id 的 `load_events`）。
- 一次 `store_events` 调用追加多个聚合的事件（一个 command 引起扇出）。
- Replay：可配置批次大小与 worker 数（Sequent 起点约 events/txn ≈ 100k、workers ≈ 8 — 按硬件调优）。

### 7.3 Outbox（何时需要）

适用于：

- Projector I/O 过重以至于影响写 SLO，或  
- 外部系统必须在提交后看到事件，或  
- 跨服务集成。

模式：在与 events 同一事务中插入 `outbox` 行；publisher worker 通过 **Postgres 表轮询**（Q6）至少一次投递 — 框架默认不用 LISTEN/NOTIFY 或 Kafka；消费者幂等。账本 / 余额 projector 保持 **`SyncInTxn`**（Q1），以保证读己之写。

### 7.4 性能杠杆

- 对热点、长生命周期聚合（事件极多的账户）使用快照。
- 按租户分区剪枝。
- 预处理语句 / pgx 连接池。
- 避免把无界流整段载入内存 — 提供流式重建 API。
- 可选快照读穿缓存（冲突版本时失效）。

---

## 8. 安全模型

### 8.1 Command 边界上的 AuthZ

```text
type Authorizer interface {
    Authorize(ctx context.Context, cmd Command) error
}
```

作为 Filter/Middleware 安装：**认证上下文附着之后**、**handler 之前**。金融应用对敏感 command 类型默认拒绝。

聚合级检查（如账户归属）留在领域守卫中，仅使用聚合状态 + command claims — 不做临时 DB 查询。

### 8.2 事件完整性（哈希链；不签名）

- **范围内：** 按聚合流哈希链（`prev_hash` / `event_hash`，SHA-256）。
- **不在范围内：** 事件签名（Q3）。
- **规范字节**（UTF-8 行以 `\n` 拼接，末行无尾换行）：`event_type`、`aggregate_id`、`sequence_number`、`created_at`（UTC RFC3339Nano）、`event_json`、`prev_hash` hex（64 位小写）。序号 1 的 prev 为 genesis（32 字节全 0）。见 `eventstore/hashchain`。迁移：`005_hash_chain.sql`。
- PostgreSQL ACL、备份、运维层可选 WORM / 不可变存储仍为互补手段。
- 应用仍可通过 `PayloadProtector` 加密 PII 字段，这并不等同于事件真实性签名。

### 8.3 威胁说明

| 威胁 | 缓解 |
|------|------|
| API 重放攻击 | 幂等键 + 认证令牌 / mTLS |
| 伪造 command 提权 | AuthZ 中间件；未经验证的 claims 绝不信任客户端自设的 `UserID` |
| EventStore 篡改 | 哈希链校验 + DB ACL / WORM 备份（无事件签名） |
| 日志/链路中的 PII 泄漏 | 脱敏；加密载荷；擦除请求用墓碑策略 |
| Projector SQL 注入 | 仅参数化 persistor |
| 跨租户混淆代理 | 加载/追加时强制租户（`tenant_id`） |
| 崩溃后 Workflow 双执行 | 幂等 command + outbox 去重（Postgres 轮询） |
| 快照投毒 | 带版本的 JSON 快照；可疑时从事件再生 |

### 8.4 密钥与机密

- 优先应用层密码哈希；框架 `Secret` 类型可选。
- 尽可能不把机密写入 events（仅在必要时存密码哈希字段；永不明文）。

---

## 9. 分阶段实施路线图

### 阶段 0 — 基础（设计锁定）

- 本文档 + 研究笔记。
- Q1–Q10 已由 wei 于 **2026-10-26**（Asia/Shanghai）拍板 — 见第 10 节。

### 阶段 1 — MVP（纯库；示例银行/账本演示应用不放在本仓库）

- `Command` / `Event` / `Aggregate` 基础 + 注册表。
- `CommandService` + 中间件 + 内存或 Postgres EventStore **最小集**（events + commands + 乐观并发）。
- **同步**进程内 publisher + 一个示例 Projector（账本余额同步 — Q1）。
- `types.Money` 使用 **decimal**（Q2）。
- `testkit` Given/When/Expect。
- 文档：入门指南。仓库内无 gRPC/HTTP 桩（Q7）。

### 阶段 2 — 与 Sequent 核心对齐

- **JSON** 快照 + 后台 snapshotter（Q8）。
- Unique keys。
- Workflows + AfterCommit。
- PostgreSQL 过程对齐（`store_events`）。
- 投影重放 CLI（prepare/replay/catchup/golive）。
- 类型注册表与 upcaster v0。

### 阶段 3 — 金融就绪

- 租户作用域幂等键（共享 schema + `tenant_id` — Q4）。
- AuthZ 钩子 + 审计元数据标准。
- Money/decimal lint + `types.Money`。
- 载荷加密钩子；**仅墓碑** GDPR 指南（Q5）— 无 crypto-shredding 产品路径。
- 可选 outbox publisher，传输为 **Postgres 轮询**（Q6）；账本 projector 保持同步。
- OpenTelemetry metrics/traces。
- 共享 schema + `tenant_id` 的多租户辅助。
- 哈希链已实现；**无**事件签名（Q3）。

### 阶段 4 — 生产加固

- 负载与冲突基准测试。
- 运维手册（故障切换、重建、备份恢复）。
- 共享 schema 内的规模化分区指导。
- 安全评审 / 威胁模型刷新。
- 事件演进兼容策略（仍 **永不**导入 Ruby Sequent DB — Q9）。
- 项目保持私有 / 不公开（Q10）。

---

## 10. 已定决策（已锁定）

由 wei 于 **2026-10-26**（Asia/Shanghai）确认。本修订不新增待决问题。

### 10.1 基线决策（D1–D10）与 Q 决议（D11–D20）

| ID | 决策 |
|----|------|
| D1 | 模块路径 = `github.com/go-sequent/go-sequent`（更名历史见英文 `design.md` D1）。 |
| D2 | PostgreSQL 是首个（且 MVP 唯一）EventStore 后端。 |
| D3 | 默认发布模式 = **写事务内同步 projector**（与 Sequent 对齐；确认 Q1）。 |
| D4 | UoW 为**上下文作用域**，非进程全局。 |
| D5 | Command/Event 默认 codec 为 JSON。 |
| D6 | 业务纠错用补偿事件，而非可变历史。 |
| D7 | 金额字段禁止 `float64`；金额类型 = **decimal**（非最小单位；确认 Q2）。 |
| D8 | 显式注册 handler（不强制反射自动注册魔法）。 |
| D9 | 设计文档先于实现；设计阶段未启动框架代码。 |
| D10 | 仅概念上借鉴 Sequent — **永不**导入 Ruby Sequent DB（确认 Q9）。 |
| D11 | **Q1** 账本 / 余额投影 = **同步**（`SyncInTxn`）。 |
| D12 | **Q2** 金额表示 = **decimal**。 |
| D13 | **Q3** 事件完整性 = **哈希链**（无事件签名）。取代此前「仅 DB」。 |
| D14 | **Q4** 多租户 = **共享 schema + `tenant_id`**。 |
| D15 | **Q5** GDPR / 留存 = **仅墓碑**（非 crypto-shredding）。 |
| D16 | **Q6** Outbox 传输（启用时）= **Postgres 表轮询**。 |
| D17 | **Q7** 交付形态 = **纯库**（仓库内无 gRPC/HTTP 桩）。 |
| D18 | **Q8** 快照格式 = **JSON**。 |
| D19 | **Q9** Ruby Sequent DB 导入兼容 = **永不**。 |
| D20 | **Q10** 项目现阶段 **私有 / 不开源**；许可证未发布。 |

### 10.2 Q1–Q10 决议日志（已决）

| ID | 问题 | 状态 | 决议 |
|----|------|------|------|
| Q1 | 账本余额投影用异步还是同步？ | **已决** 2026-10-26 | **同步** — sync projectors / sync ledger balance projections |
| Q2 | 金额表示？ | **已决** 2026-10-26 | **decimal**（非整数最小单位） |
| Q3 | 事件完整性：哈希链 / 签名 / 仅 DB？ | **已决** 2026-10-26（修订） | **哈希链** — 无事件签名 |
| Q4 | 多租户模型？ | **已决** 2026-10-26 | **共享 schema + `tenant_id`** |
| Q5 | GDPR / 留存策略？ | **已决** 2026-10-26 | **仅墓碑**（tombstone only; not crypto-shredding） |
| Q6 | Outbox 传输？ | **已决** 2026-10-26 | **Postgres 轮询** outbox 表（when outbox is used） |
| Q7 | 仓库内 gRPC/HTTP 桩 vs 纯库？ | **已决** 2026-10-26 | **不要** — pure library |
| Q8 | 快照格式？ | **已决** 2026-10-26 | **JSON** |
| Q9 | 是否导入 Ruby Sequent 数据库？ | **已决** 2026-10-26 | **永不** |
| Q10 | 许可证 / 公开路线图？ | **已决** 2026-10-26 | **不公开** — private/unpublished |

---

## 附录 A — Command 生命周期（时序）

```text
Client
  → CommandService.Execute(ctx, cmds...)
      → Tx.Begin
      → for cmd in queue:
          → Middleware / AuthZ / Idempotency
          → Validate
          → CommandHandler.Handle
              → Repository.Load / Add
              → aggregate.Method → Apply(events)
          → Repository.Commit
              → EventStore.Append (optimistic + unique keys)
              → Publisher.Publish → Projectors (+ Workflows enqueue cmds)
      → Tx.Commit
      → AfterCommit / Outbox drain signal
```

## 附录 B — 最小 Go API 草图（示意，未实现）

```go
cfg := sequent.Config{PublisherMode: sequent.SyncInTxn}
app := sequent.New(cfg, sequent.WithPostgres(dsn))

app.Commands.Register(&OpenAccountHandler{})
app.Projectors.Register(&AccountProjector{})
app.Workflows.Register(&NotifyWorkflow{})

err := app.CommandService.Execute(ctx,
    &OpenAccount{
        Meta: sequent.MetaFrom(ctx),
        AccountID: id,
        Currency:  "CNY",
    },
)
```

## 附录 C — 研究出处

- 站点：https://sequent.io/（Reference Guide：AggregateRoot、Command、Event、CommandService、AggregateRepository、EventStore、Projector、Workflow、Snapshotting、Migrations、GDPR）。
- 源码快照：`zilverline/sequent` master zip（2026 年 10 月），重点包括：
  - `db/sequent_schema_tables.sql`、`sequent_schema_indexes.sql`
  - `db/migrate/sequent/store_events_v05.sql`
  - `lib/sequent/core/{command_service,event_store,aggregate_root,aggregate_repository,projector,workflow}.rb`

---

*设计文档结束。Q1–Q10 已于 2026-10-26（Asia/Shanghai）锁定。实现可按分阶段路线图推进；本修订仅为文档更新。*
