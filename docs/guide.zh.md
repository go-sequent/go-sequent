# go-sequent 框架指南

> 说明 **已实现** 的 Phase 1–4 与 `examples/bank` 示例。概念命名参考 [Sequent](https://sequent.io/) Reference Guide，但 API 与数据模型均为 Go 独立实现（**永不**导入 Ruby Sequent 数据库，Q9）。
>
> **术语约定：** 框架 / API 名称以英文为准（如 Command、EventStore、SyncInTxn）；中文多为释义。完整对照见 [§26 名词释义](#26-名词释义)。

模块：`github.com/wenweih/go-sequent` · 形态：**纯库**（仓库内无 HTTP/gRPC，Q7）

---

## 目录

1. [go-sequent 是什么 / 何时使用](#1-go-sequent-是什么--何时使用)
2. [核心闭环（Basic flow）](#2-核心闭环basic-flow)
3. [Command 与 Meta](#3-command-与-meta)
4. [Event 与 Registry](#4-event-与-registry)
5. [AggregateRoot](#5-aggregateroot)
6. [CommandHandler](#6-commandhandler)
7. [AggregateRepository 与 UnitOfWork](#7-aggregaterepository-与-unitofwork)
8. [CommandService 与中间件](#8-commandservice-与中间件)
9. [EventStore（memory / postgres）](#9-eventstorememory--postgres)
10. [Publisher：SyncInTxn 与 Outbox](#10-publishersyncintxn-与-outbox)
11. [Projector](#11-projector)
12. [Workflow 与 AfterCommit](#12-workflow-与-aftercommit)
13. [Snapshotting（JSON）](#13-snapshottingjson)
14. [Unique keys 与乐观并发](#14-unique-keys-与乐观并发)
15. [Idempotency](#15-idempotency)
16. [AuthZ](#16-authz)
17. [Tenancy](#17-tenancy)
18. [Money（decimal）](#18-moneydecimal)
19. [OpenTelemetry（obs）](#19-opentelemetryobs) — [作用](#作用)
20. [Migrations 与 Ops CLI](#20-migrations-与-ops-cli)
21. [Wiring：`sequent` 门面](#21-wiringsequent-门面)
22. [Bank 示例 walkthrough](#22-bank-示例-walkthrough)
23. [测试指引](#23-测试指引)
24. [运维与安全文档指针](#24-运维与安全文档指针)
25. [相对 Sequent Ruby：未实现 / 阶段边界](#25-相对-sequent-ruby未实现--阶段边界)
26. [名词释义](#26-名词释义)
27. [锁定决策摘要（Q1–Q10）](#27-锁定决策摘要q1q10)

---

## 1. go-sequent 是什么 / 何时使用

**go-sequent** 是面向金融账本场景的 Go 库，实现 **CQRS + Event Sourcing**。业务侧定义 Command、Aggregate、Event 与投影；框架负责串联写路径：

- 每次 `Execute`：一个数据库事务，加上挂在 `context` 上的 [UnitOfWork](#gloss-uow)（UoW）
- 追加事件时的 [乐观并发（OCC）](#gloss-occ) 与 [唯一键](#gloss-unique-keys)
- [账本投影](#gloss-ledger-projection)默认在写事务内同步更新（[`SyncInTxn`](#gloss-syncintxn)，Q1）
- decimal [Money](#gloss-money)、租户幂等、AuthZ、JSON Snapshot、墓碑式 GDPR 钩子，以及可选 Outbox 轮询
- 可选 [OpenTelemetry](#gloss-obs)（包名 `obs`）：观察写路径延迟、成败与冲突——**不能**当作业务审计源（见 [§19 作用](#作用)）

**适合：** 余额必须与写路径同事务一致（[读己之写](#gloss-read-your-writes)）；共享 schema 多租户；需要可审计的 Command / Event 记录。

术语速查：[§26 名词释义](#26-名词释义)。

**不适合：** 当作开箱即用的 Web 框架。没有 Rails / Sinatra 集成，没有 HTTP API，也不绑定 ActiveRecord。嵌入你自己的服务进程即可。

入门见 [getting-started.zh.md](./getting-started.zh.md)；设计见 [design.zh.md](./design.zh.md)。

---

## 2. 核心闭环（Basic flow）

对应 Sequent Reference Guide 的「Basic flow」。Go 侧典型路径：

```text
客户端 / 服务入口
        │
        ▼
  CommandService.Execute(ctx, cmds...)
        │  开启 Tx + UnitOfWork
        ├─ Middleware（tenancy / idempotency …）
        ├─ Filters（AuthZ）
        ├─ Command.Validate（可选）
        ▼
  CommandHandler
        │  Repo.Add / Repo.Load → 聚合方法 Apply 事件
        ▼
  Repository.Commit
        │  EventStore.Append（command_records + events + unique keys）
        │  Publisher.Publish → Projectors → Workflows
        ▼
  Tx commit
        │
        ▼
  AfterCommit hooks（通知、出站非账本副作用）
```

你通常要实现：**Command、Handler、Aggregate、Event、Projector**（可选 Workflow）。其余由 `command.Service`、`aggregate.Repository`、`eventstore`、`publish.SyncInTxn` 串起来。

---

## 3. Command 与 Meta

包：`command`

```go
type Command interface {
    CommandType() string
    AggregateID() (uuid.UUID, bool) // 可选主聚合
    Meta() *Meta
}
```

嵌入 `command.Base` 即可拿到 `Meta()`。`EnsureMeta` 会补齐 `CommandID`、`IssuedAt`、`CorrelationID`。

| Meta 字段 | 用途 |
|-----------|------|
| `CommandID` | 命令唯一 id，用于审计 |
| `IdempotencyKey` | 租户内幂等键 |
| `TenantID` / `UserID` | 租户 / 操作者 |
| `CorrelationID` / `CausationID` | 业务会话 / 因果链 |
| `Source` | 来源标签，如 `api`、`workflow` |
| `AuthContext` | 不透明鉴权声明（**不要**只信客户端传来的 UserID） |

`command.AuditOf(cmd)` 生成可落库的审计快照。实现 `Validate() error` 可在 Handler 之前自检。

Bank 示例：

```go
type OpenAccount struct {
    command.Base
    AccountID uuid.UUID
    Owner     string
    Currency  string
    Opening   types.Money
}
func (c *OpenAccount) CommandType() string { return "bank.OpenAccount" }
```

---

## 4. Event 与 Registry

包：`event`

```go
type Event interface {
    EventType() string
    AggregateID() uuid.UUID
    SequenceNumber() int64
    CreatedAt() time.Time
}
```

领域事件一般嵌入 `event.Base`，在构造函数里 `SetEventType(...)`。聚合调用 `Apply` 时会填好 envelope：aggregate id、序号、时间。

**Registry** 把稳定类型名映射到工厂，供 JSON 反序列化：

```go
r.Register("bank.AccountOpened", func() event.Event { return &AccountOpened{} })
```

Upcaster（包 `event/upcast`）可在加载时升级旧 payload，挂到 `sequent.App.Upcasters`。**不要**假设与 Ruby Sequent 的事件表兼容。

---

## 5. AggregateRoot

包：`aggregate`

`aggregate.Root` 是聚合契约。业务聚合嵌入 `aggregate.Base`：

1. 构造函数里 `Init(id, typeName)`，并用 `On(sampleEvent, applier)` 注册 **纯状态** 应用器  
2. 命令方法先做规则校验，再 `Apply(event)`——**禁止**在 applier 里做 I/O  
3. 可按需覆盖 `Invariants()`、`UniqueKeys()`、`PartitionKey`

```go
acc := NewAccount(id)
acc.On(&AccountOpened{}, func(e event.Event) { /* 突变状态 */ })
// …
func (a *Account) Deposit(amount types.Money) error {
    // 校验 …
    a.Apply(NewMoneyDeposited(amount))
    return nil
}
```

当前状态 = 历史事件（可加 JSON Snapshot）折叠的结果。未提交事件停在 `UncommittedEvents()`，由 Repository 在 Commit 时写出。

---

## 6. CommandHandler

包：`command`（`Handler` / `Router`）

```go
type Handler interface {
    Handles() []string
    Handle(ctx context.Context, cmd Command) error
}
```

Handler 通过 `aggregate.Repository` 的 `Add` / `Load` 操作聚合，**不要**自己调 `EventStore.Append`。涉及多个聚合的命令（如同行转账）应在同一 UoW、同一事务里 `Load`，再分别产生事件。

```go
func (h *Handler) Handle(ctx context.Context, cmd command.Command) error {
    switch c := cmd.(type) {
    case *OpenAccount:
        acc := NewAccount(c.AccountID)
        if err := acc.OpenAccount(c.Owner, c.Currency, c.Opening); err != nil {
            return err
        }
        return h.Repo.Add(ctx, acc)
    case *Deposit:
        agg, err := h.Repo.Load(ctx, c.AccountID, AggregateAccount)
        // …
    }
}
```

`Router.Register(h)` 按 `CommandType()` 分发；同一类型重复注册会 panic。

---

## 7. AggregateRepository 与 UnitOfWork

包：`aggregate`

**UnitOfWork** 是单次请求 / 命令批的身份映射，挂在 `context` 上（**禁止跨 goroutine 共享**）：

- `CommandService.Execute` 在事务内创建 UoW，并 `WithUnitOfWork`
- `Repo.Add` / `Repo.Load` 要求 ctx 里已有 UoW
- `Load` 命中 identity map 时复用同一实例
- `Commit` 按 **加载 / 注册顺序** 收集未提交事件 → `Store.Append` → `Publisher.Publish` → `MarkCommitted`

这与 Sequent「按 AggregateRepository 加载顺序发布事件」的意图一致。

工厂用 `RegisterFactory(aggregateType, func(id) Root)` 注册，供 Load 与快照重建。

---

## 8. CommandService 与中间件

包：`command`

```go
svc.Execute(ctx, cmd1, cmd2, ...) // 单事务、单 UoW
```

每次 `Execute` 大致步骤：

1. `workflow.WithHooks`（准备 AfterCommit 袋）
2. `Tx.WithinTx` → 创建 UoW
3. 队列中每条命令：Middleware → Filters → `Validate` → `Router.Dispatch` → `Repo.Commit`
4. 事务成功后再跑 AfterCommit

**Middleware** 签名：`func(ctx, cmd, next) error`。`Chain` **外层先执行**（切片下标 0 最先跑）。

Bank `NewDemoApp` 的常见顺序：

1. `tenancy.Middleware`——从 ctx 写入 `Meta.TenantID`  
2. `idempotency.Middleware`——依赖已有 TenantID  

**Filters**（如 `security.Filter`）在 terminal 内、`Validate` 之前执行。可选观测见 [§19 OpenTelemetry](#19-opentelemetryobs)：挂在 `Service.Obs` 上，**不是** Middleware。

---

## 9. EventStore（memory / postgres）

包：`eventstore`。实现：`eventstore/memory`、`eventstore/postgres`。

```go
type Store interface {
    Append(ctx context.Context, cmd CommandRecord, streams []StreamAppend) error
    LoadStream(ctx context.Context, aggregateID uuid.UUID) ([]event.Event, error)
    LoadStreams(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID][]event.Event, error)
}
```

`CommandRecord` 与 `command` 包解耦，避免循环依赖。`StreamAppend` 含 `ExpectedVersion`、`UniqueKeys`、`PartitionKey`。

| 错误 | 含义 |
|------|------|
| `ErrOptimisticConcurrency` | 期望版本与流尖端不符 |
| `ErrUniqueKeyViolation` | 唯一键冲突 |
| `ErrStreamNotFound` | 加载未知流 |

**memory**：单测与本地 IT。  
**postgres**：`Open` + `Migrate`；schema `001`–`004`（含 store_events、phase3 的幂等 / 墓碑等）。同一实现还可当 `TxManager` 与 `SnapshotStore`。

业务代码通常经 Repository 访问 Store，而不是直接 `Append`（测 OCC 时可以直写）。

---

## 10. Publisher：SyncInTxn 与 Outbox

包：`publish`、`publish/outbox`

### SyncInTxn（默认，Q1）

写事务内按事件顺序调用：

1. 已注册的 **Projector**  
2. 已注册的 **Workflow**

任一失败 → 整笔事务回滚。重建投影时用 `PublishProjectorsOnly`，跳过 Workflow。

### WithOutbox（Q6）

`publish.WithOutbox` 包一层 `SyncInTxn`：同步投影之后，把匹配事件写入事务性 Outbox 表；再由 `outbox.Poller` 或 CLI `go-sequent outbox` **轮询**投递（默认不是 LISTEN/NOTIFY，也不是 Kafka）。

**账本投影必须留在 SyncInTxn。** Outbox 只服务异步 / 外部副作用。

---

## 11. Projector

包：`projector`。注册到 `publish.SyncInTxn`。

```go
type Projector interface {
    publish.Handler // Handles() + Handle(ctx, e)
    ManagedTables() []string
}
```

Bank 的 `BalanceProjector` 在内存里维护余额（演示用）；生产应经 `Persistor` 写 SQL，方便 replay。`EventRouter` 可按事件类型名注册回调。

约定（与 Sequent 精神一致）：

- 一张投影表只由一个 Projector 管理  
- 重建时只依赖自己管理的表  
- 账本类投影失败必须让写事务失败  

---

## 12. Workflow 与 AfterCommit

包：`workflow`

Workflow 也实现 `publish.Handler`，但通过 `RegisterWorkflow` 注册，**不参与**投影重建。

事务内可登记：

```go
workflow.RegisterAfterCommit(ctx, func(ctx context.Context) error {
    // 邮件 / 指标 / 非账本副作用
    return nil
})
```

钩子只在 **提交成功后** 运行。Bank 的 `NotifyWorkflow` 在 `AccountOpened` 上登记 AfterCommit 通知。

---

## 13. Snapshotting（JSON）

包：`snapshot`。存储接口：`eventstore.SnapshotStore`（postgres / memory）。

Q8：Snapshot 格式为 **JSON**。聚合实现：

```go
SnapshotVersion() int
MarshalSnapshot() (json.RawMessage, error)
UnmarshalSnapshot(json.RawMessage) error
```

若 Repository 配置了 Snapshots：先加载最新快照，再应用后续事件。后台可用 `snapshot.Snapshotter` 的 `RunOnce` / `Run` 批量生成。

---

## 14. Unique keys 与乐观并发

- **乐观并发（OCC）：** `ExpectedVersion` 等于提交前已提交序号；冲突返回 `ErrOptimisticConcurrency`。  
- **唯一键：** 聚合 `UniqueKeys()` 返回 `scope → value`。Bank 把 owner 限制在 `tenant:<pk>:bank.Account.owner`。冲突 → `ErrUniqueKeyViolation`。

`tenancy.UniqueScope` / `PartitionKey` 可帮助构造租户作用域。

---

## 15. Idempotency

包：`idempotency`（幂等）

- 键空间：`(TenantID, IdempotencyKey)`  
- 成功后再提交同键：默认 **短路成功**（no-op）；`Strict=true` 时返回 `ErrReplay`  
- 实现：`NewMemory()` / `NewPostgres(pool)`  
- 安装：`sequent.WithIdempotency`，或手动挂 Middleware  

设置了 `IdempotencyKey` 时 **必须** 有 `TenantID`（可先跑 tenancy Middleware）。

---

## 16. AuthZ

包：`security`

```go
type Authorizer interface {
    Authorize(ctx context.Context, cmd command.Command) error
}
```

内置：`AllowAll`、`DenyByDefault{Allow: map[string]bool{...}}`。  
安装：`sequent.WithAuthorizer`（Filter）或 `security.Middleware`。拒绝时用 `errors.Is(err, security.ErrDenied)` 判断。

**不做** 事件哈希链或签名（Q3）。

---

## 17. Tenancy

包：`tenancy`（Q4：共享 schema + `tenant_id`）

```go
ctx = tenancy.WithTenant(ctx, tenantID)
// Middleware：若 Meta.TenantID 为空则从 ctx 写入；不一致则拒绝
```

辅助函数：`UniqueScope`、`PartitionKey`、`PartitionKeyHashed`、`MatchAggregateTenant`。没有 schema-per-tenant / DB-per-tenant 的一等支持。

---

## 18. Money（decimal）

包：`types`

```go
m := types.MustMoney("100.00", "CNY")
sum, _ := m.Add(types.MustMoney("0.25", "CNY"))
rounded := sum.Round(2)
```

JSON 里金额是 **字符串**，禁止 `float64`。可用 `types.AssertNoFloatMoney` / `testkit.BanFloat64Money` 做静态检查辅助。

---

## 19. OpenTelemetry（obs）

包：`obs`（`obs/otel.go`）。仪表名：`github.com/wenweih/go-sequent`。

### 作用

**为什么要有：** CQRS/ES 写路径（`CommandService.Execute` → 事务 → Append → SyncInTxn）对账本延迟和冲突很敏感。`obs` 通过 **厂商无关** 的 OpenTelemetry API，把命令耗时、成败、乐观并发冲突，以及可选的 Outbox 积压，交给运维 / SLO 看板——库本身不绑定某一家 APM。

**放在哪：** 可选的 `obs.Instruments`，经 `sequent.WithObs` 挂到 `command.Service.Obs`（字段，**不是** Middleware）。在 `Execute` 外层开 span、记 histogram / counter。它与 Command / Event 审计落库、EventStore **互补**：OTel 是运行时信号，**不是**事实来源。业务对账与合规仍靠事件流，以及 `command.AuditOf` / `command_records`。

**能看见什么：**

- 慢命令：`sequent.command.duration_ms` + `command.type`
- 冲突风暴：`sequent.optimistic_conflict.total`（热点聚合被并发改写时）
- Outbox 积压：`sequent.outbox.depth`（配置了 `Config.OutboxDepth` 时）
- 命令成败率：`sequent.command.total` 的 `status=ok|error`

**不做什么：** 不替代业务审计；不自动埋点 HTTP/gRPC、Projector、Outbox poller；不捆绑 exporter；Provider 由应用配置。`RecordEvents` API 存在，但 **没有** 自动挂到 append（见下）。

**与锁定决策：** 只是可观测性钩子。事件完整性仍是 **仅 DB**（Q3）；交付形态仍是 **纯库**（Q7）——导出留给宿主进程。

### 接线

`obs` **不是** Command Middleware。用 `sequent.WithObs(inst)` 写入 `App.Obs` 与 `command.Service.Obs`（`nil` 表示关闭）；也可直接 `svc.Obs = inst`。

`CommandService.Execute` 行为：

1. `StartCommand` → span `sequent.Execute`
2. 跑事务 / UoW
3. Commit 若返回 `ErrOptimisticConcurrency` → `RecordConflict`
4. 结束时 `RecordCommand`（记延迟与计数，并 `span.End`）

### 应用必须自行配置 Provider

`obs.New` 使用 **全局** `otel.Tracer` / `otel.Meter`。未设置 TracerProvider / MeterProvider 时为 **noop**（测试可安全调用）。**库不捆绑** OTLP / Prometheus / stdout exporter，由宿主应用安装。

```go
// 应用侧：otel.SetTracerProvider / SetMeterProvider + 你的 exporter
inst, err := obs.New(obs.Config{
    OutboxDepth: ob.Depth, // 可选；nil 则不注册 sequent.outbox.depth
})
app := sequent.New(sequent.Config{},
    sequent.WithObs(inst),
    sequent.WithStore(store, tx),
)
```

### 发出的 metrics / spans

| 种类 | 名称 | 属性 / 说明 |
|------|------|-------------|
| Span | `sequent.Execute` | `command.type`；出错时 `RecordError` + status Error |
| Histogram | `sequent.command.duration_ms` | `command.type`；单位 ms |
| Counter | `sequent.command.total` | `command.type`，`status`=`ok` \| `error` |
| Counter | `sequent.optimistic_conflict.total` | 无额外属性 |
| Counter | `sequent.events.appended` | API 有；**框架未在 append 路径自动调用** |
| Observable gauge | `sequent.outbox.depth` | 仅当 `Config.OutboxDepth != nil` |

批量 `Execute` 时，`command.type` 为 `FirstType+batch`。

公开 API：`Instruments`、`Config`、`New`、`StartCommand`、`RecordCommand`、`RecordConflict`、`RecordEvents`。

### 未包含

- 无内置 OTLP / Prometheus / stdout exporter；不创建 Provider
- 无 HTTP/gRPC 自动埋点；无 Projector / Workflow / Outbox poller 自动 span
- **`RecordEvents` 未挂钩** `Repository.Commit` / `EventStore.Append`（目前主要在测试里调用；应用可在自己的边界调用）
- 不是基于 Middleware 链的观测模型

---

## 20. Migrations 与 Ops CLI

Postgres schema 由 `store.Migrate` 应用：`001_phase1` … `004_phase3`（含 Outbox / 幂等 / 墓碑相关表）。分区示例 SQL 仅作文档，见 `docs/ops/`。

CLI（`cmd/go-sequent`）：

```bash
go run ./cmd/go-sequent migrate -dsn "$GO_SEQUENT_PG_DSN"
go run ./cmd/go-sequent outbox -dsn "$DSN" [-once]
go run ./cmd/go-sequent replay -dsn "$DSN" -projector name -stage prepare -tables t1
```

`replay` 的 schema 阶段（prepare / optimize / golive / abort）可用 CLI；完整事件回放需应用侧接入 `projector/replay.Pipeline` + EventSource。

GDPR：`tombstone` 包提供 `SubjectTombstoned` 与策略说明（**仅墓碑**，Q5）。`crypto.PayloadProtector` 是可选字段加密钩子，**不是** crypto-shredding 产品路径。

---

## 21. Wiring：`sequent` 门面

包：`sequent`

```go
app := sequent.New(sequent.Config{PublisherMode: sequent.SyncInTxn},
    sequent.WithStore(store, tx),
    sequent.WithAuthorizer(security.DenyByDefault{Allow: map[string]bool{...}}),
    sequent.WithIdempotency(idempotency.NewMemory(), false),
    sequent.WithObs(inst),
)
app.RegisterAggregate("bank.Account", func(id uuid.UUID) aggregate.Root { return bank.NewAccount(id) })
app.RegisterHandler(&bank.Handler{Repo: app.Repo})
app.RegisterProjector(bank.NewBalanceProjector())
app.RegisterWorkflow(&bank.NotifyWorkflow{})
app.RegisterUpcaster(...)
_ = app.Service.Execute(ctx, cmd)
```

Bank 演示封装：`bank.NewDemoApp(DemoOptions{StoreFactory, Idempotency, Authorizer, EnableTenancyMW})`，保证 Store 与 `RegisterEvents` 共用同一 `event.Registry`。

---

## 22. Bank 示例 walkthrough

路径：[`examples/bank/`](../examples/bank/)

| 文件 | 内容 |
|------|------|
| `events.go` | Opened / Deposited / Withdrawn / TransferDebited / TransferCredited |
| `account.go` | 聚合 + Snapshot + 租户作用域 unique owner |
| `commands.go` | Open / Deposit / Withdraw / Transfer |
| `handler.go` | 单 UoW 双聚合转账 |
| `projector.go` | SyncInTxn 余额视图 |
| `workflow.go` | AfterCommit 通知 |
| `app.go` | `NewDemoApp` 接线 |
| `integration_*_test.go` | 内存 IT 始终跑；Postgres 需 `GO_SEQUENT_PG_DSN` |

建议阅读顺序：`commands` → `account` → `handler` → `projector` → `app` → IT。

```bash
go test ./examples/bank/ -count=1
GO_SEQUENT_PG_DSN='postgres://…' go test ./examples/bank/ -run TestIT_Postgres
```

---

## 23. 测试指引

| 层级 | 做法 |
|------|------|
| 聚合单元 | `testkit.For(id, factory).Given(...).When(...).ExpectEvents(...)` |
| 应用 / IT | `memory.New(reg)` + `NewDemoApp`；断言 Projector |
| AuthZ / 幂等 / 租户 | 见 `TestIT_Memory_*` |
| OCC | 故意让 `ExpectedVersion` 过期，断言 `ErrOptimisticConcurrency` |
| Postgres | 设 `GO_SEQUENT_PG_DSN`；未设则 skip |

```bash
go test ./...   # 无 DSN 必须绿
```

基准：`go test ./bench/ -bench=. -benchmem`（见 [bench.md](./bench.md)）。

---

## 24. 运维与安全文档指针

| 主题 | 文档 |
|------|------|
| 故障切换 / 投影重建 / 备份 / 分区 | [ops/](./ops/) |
| 威胁模型 | [security/threat-model.zh.md](./security/threat-model.zh.md) |
| 事件演进 / Upcaster（禁止导入 Sequent DB） | [security/event-evolution.zh.md](./security/event-evolution.zh.md) |
| 基准 | [bench.md](./bench.md) |

---

## 25. 相对 Sequent Ruby：未实现 / 阶段边界

| Sequent（Ruby）概念 | go-sequent |
|---------------------|------------|
| AggregateRoot / Command / Event / CommandService / Repository / EventStore / Projector / Workflow / Snapshot / Migrations | **已实现**（Go 惯用 API） |
| ActiveRecord `Records` + SQL 投影迁移 DSL | **无** AR；用 `ManagedTables` + `projector/replay` |
| 自动注册 / Rails / Sinatra 生成器 | **无**（纯库） |
| AggregateObserver | **未**单独产品化 |
| ValueObject 一等类型系统 | 普通 Go 类型 + `types.Money` |
| LISTEN/NOTIFY / 默认消息总线 | 可选 Outbox **表轮询** |
| 事件哈希链 / 签名 | **明确不做**（Q3） |
| 导入既有 Sequent DB | **禁止**（Q9） |
| HTTP/gRPC stubs | **无**（Q7） |

阶段简述：Phase 1 核心闭环 → Phase 2 Snapshot / Upcaster / Workflow / replay 骨架 → Phase 3 幂等 / AuthZ / Outbox / tenancy / tombstone / obs → Phase 4 文档与基准 → Bank 示例与集成测试。

---

## 26. 名词释义

释义对齐 **当前 go-sequent 实现** 与锁定决策（Q1–Q10）。英文版见 [Glossary](./guide.md#26-glossary)。词条标题多为「中文（English）」或保留英文 API 名。

<a id="gloss-ledger-projection"></a>

**账本投影（ledger projection）**  
余额、台账一类金融读模型。Q1 要求在写事务内用 `SyncInTxn` 同步更新，提交成功后即可 [读己之写](#gloss-read-your-writes)，避免异步滞后造成余额不一致。Bank 的 `BalanceProjector` 属于此类（演示用内存；生产应落 SQL）。

<a id="gloss-projector"></a>

**Projector（投影器）**  
实现 `publish.Handler`（可选 `ManagedTables`）的读模型更新器。注册到 `SyncInTxn` 后，在 Append 之后、事务提交前按事件调用；失败则整笔写事务回滚。重建时用 `PublishProjectorsOnly`，跳过 Workflow。

<a id="gloss-aggregate"></a>

**AggregateRoot（聚合根）**  
领域一致性边界。嵌入 `aggregate.Base`：只在 applier 里改状态；命令方法校验后 `Apply` 事件。经 Repository + UoW 加载 / 提交，禁止跨 goroutine 共享。

<a id="gloss-command"></a>

**Command（命令）**  
改变系统的意图（尚未发生）。含 `CommandType`、可选 `AggregateID`、`Meta`；可实现 `Validate`。经 `CommandService.Execute` 进入 Middleware / AuthZ / Handler。

<a id="gloss-event"></a>

**Event（事件）**  
已经发生的事实。含类型名与 envelope（aggregate id、序号、时间）。写入 EventStore 后驱动 Projector / Workflow；经 `event.Registry` 反序列化。

<a id="gloss-eventstore"></a>

**EventStore（事件存储）**  
持久化 Command 记录与聚合事件流，并做 OCC 与唯一键检查。实现：`eventstore/memory`、`eventstore/postgres`。业务通常走 Repository，而不是直接 `Append`。

<a id="gloss-uow"></a>

**UnitOfWork / UoW（上下文作用域）**  
单次 `Execute` 事务内、挂在 `context` 上的聚合身份映射。`Add` / `Load` 复用同一实例；`Commit` 按加载顺序收集未提交事件。禁止跨 goroutine 共享。

<a id="gloss-occ"></a>

**乐观并发（OCC）**  
追加时 `ExpectedVersion` 必须等于流尖端；否则返回 `ErrOptimisticConcurrency`。`obs` 可统计冲突风暴。

<a id="gloss-unique-keys"></a>

**唯一键（Unique keys）**  
聚合 `UniqueKeys()` 返回的 `scope → value`（例如租户作用域下的 owner）。库级唯一约束冲突 → `ErrUniqueKeyViolation`。

<a id="gloss-idempotency"></a>

**幂等（Idempotency）**  
键空间为 `(TenantID, IdempotencyKey)`。成功后再用同键重试，默认短路成功；`Strict` 时返回 `ErrReplay`。实现：memory / postgres。

<a id="gloss-tenant"></a>

**租户（Tenant）与用户（User）**  
`Meta.TenantID` 是多租户隔离单元（Q4：共享 schema + `tenant_id`）。`Meta.UserID` 是操作者——**不要**只信客户端 UserID，应结合 `AuthContext` 与 AuthZ。`tenancy` Middleware 可从 ctx 写入 TenantID。

<a id="gloss-outbox"></a>

**Outbox**  
与事件同事务写入的出站消息表，由轮询器异步投递（Q6）。账本投影仍走 SyncInTxn；Outbox 只做外部 / 异步副作用。

<a id="gloss-aftercommit"></a>

**AfterCommit**  
经 `workflow.RegisterAfterCommit` 登记的钩子，只在写事务 **提交成功后** 运行（通知、非账本副作用）。钩子失败不会回滚已提交事件。

<a id="gloss-workflow"></a>

**Workflow**  
对事件做编排的处理器（`RegisterWorkflow`）。仍可在 SyncInTxn 内运行，但 **不参与** 投影重建。可登记 AfterCommit。

<a id="gloss-snapshot"></a>

**Snapshot（快照）**  
聚合状态的 JSON 折叠点（Q8），用于加速加载：先读快照，再应用后续事件。可用 `snapshot.Snapshotter` 后台批量生成。

<a id="gloss-upcaster"></a>

**Upcaster**  
在加载路径上把旧事件 payload 升到当前形状（`event/upcast`）。**不**用于导入 Ruby Sequent 库（Q9）。

<a id="gloss-syncintxn"></a>

**SyncInTxn**  
默认发布模式：在写事务内同步调用 Projector，再调用 Workflow。账本投影必须用此模式（Q1）。

<a id="gloss-compensating"></a>

**补偿事件**  
用新的领域事件表达「抵消 / 纠正」，而不是改写历史。GDPR 场景下常与墓碑配合：投影侧停止暴露可识别数据，而不是静默改旧事件。

<a id="gloss-tombstone"></a>

**墓碑（tombstone）**  
Q5 策略：发出如 `gdpr.SubjectTombstoned` 的标记，投影侧擦除或停服 PII。产品路径 **不是** crypto-shredding。

<a id="gloss-read-your-writes"></a>

**读己之写（read-your-writes）**  
写成功后，同一客户端立刻读，应能看到自己的更新。账本投影用 SyncInTxn：提交返回前，余额投影已在同一事务里更新完毕。

<a id="gloss-authz"></a>

**AuthZ**  
命令边界上的授权（`security.Authorizer` / `WithAuthorizer`）。拒绝返回 `ErrDenied`。与认证（AuthN）分离；不做事件签名（Q3）。

<a id="gloss-meta"></a>

**Meta（命令元数据）**  
`command.Meta`：CommandID、TenantID、UserID、IdempotencyKey、Correlation / Causation、Source、AuthContext 等。`AuditOf` 可生成审计快照。

<a id="gloss-partition-key"></a>

**partition_key**  
聚合 / 流上的分区提示（常由 `tenancy.PartitionKey` 派生），用于共享 schema 内的物理分区或唯一键作用域；与 `tenant_id` 配合，不是 schema-per-tenant。

<a id="gloss-money"></a>

**Money（decimal）**  
`types.Money`：十进制金额 + 币种；JSON 金额为字符串。禁止 `float64`（Q2）。

<a id="gloss-obs"></a>

**obs / OpenTelemetry**  
可选的写路径观测钩子（延迟、成败、OCC、可选 Outbox 深度）。**不是**业务审计源；Provider 由应用配置。见 [§19](#19-opentelemetryobs)。

---

## 27. 锁定决策摘要（Q1–Q10）

| # | 决策 |
|---|------|
| Q1 | [账本投影](#gloss-ledger-projection) **SyncInTxn** |
| Q2 | Money = **decimal**（禁 float64） |
| Q3 | 完整性 = **仅 DB**（无哈希链 / 签名） |
| Q4 | 多租户 = **共享 schema + tenant_id** |
| Q5 | GDPR = **仅墓碑** |
| Q6 | Outbox = **Postgres 表轮询** |
| Q7 | **纯库**（无 HTTP/gRPC） |
| Q8 | Snapshot = **JSON** |
| Q9 | **永不**导入 Ruby Sequent DB |
| Q10 | 项目 **私有 / 暂不公开** |

---

*对应 Phase 1–4 与 bank 示例实现；若 API 有变，以源码为准。*
