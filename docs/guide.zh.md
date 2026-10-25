# go-sequent 框架指南

> 面向 **已实现** 的 Phase 1–4 与 `examples/bank` 示例。概念命名受 [Sequent](https://sequent.io/) Reference Guide 启发，但 API 与数据模型均为 Go 独立实现（**永不**导入 Ruby Sequent 数据库，Q9）。

模块：`github.com/wenweih/go-sequent` · 形态：**纯库**（无内置 HTTP/gRPC，Q7）

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
19. [Migrations 与 Ops CLI](#19-migrations-与-ops-cli)
20. [Wiring：`sequent` 门面](#20-wiringsequent-门面)
21. [Bank 示例 walkthrough](#21-bank-示例-walkthrough)
22. [测试指引](#22-测试指引)
23. [运维与安全文档指针](#23-运维与安全文档指针)
24. [相对 Sequent Ruby：未实现 / 阶段边界](#24-相对-sequent-ruby未实现--阶段边界)
25. [锁定决策摘要（Q1–Q10）](#25-锁定决策摘要q1q10)

---

## 1. go-sequent 是什么 / 何时使用

**go-sequent** 是面向金融账本类场景的 **CQRS + Event Sourcing** Go 库。你定义命令、聚合、事件与投影；框架负责：

- 单次 `Execute` 内的 **事务 + 上下文作用域 UnitOfWork**
- 事件追加时的 **乐观并发** 与 **唯一键**
- 账本投影默认 **写事务内同步**（`SyncInTxn`，Q1）
- decimal **Money**、租户幂等、AuthZ 钩子、JSON 快照、墓碑式 GDPR 钩子、可选 Outbox 轮询

**适合：** 余额/台账必须与写路径同事务一致；多租户共享 schema；需要可审计的 command/event 记录。

**不适合作为开箱 Web 框架：** 不提供 Rails/Sinatra 式集成、不提供 HTTP API、不绑定 ActiveRecord。把本库嵌进你自己的服务进程即可。

快速上手另见 [getting-started.zh.md](./getting-started.zh.md)。设计细节见 [design.zh.md](./design.zh.md)。

---

## 2. 核心闭环（Basic flow）

与 Sequent Reference Guide 的「Basic flow」对应，Go 侧典型路径：

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

你需要实现的大致是：**Command / Handler / Aggregate / Event / Projector（及可选 Workflow）**；其余由 `command.Service`、`aggregate.Repository`、`eventstore`、`publish.SyncInTxn` 串联。

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

嵌入 `command.Base` 获得 `Meta()`。`EnsureMeta` 会补齐 `CommandID`、`IssuedAt`、`CorrelationID`。

| Meta 字段 | 用途 |
|-----------|------|
| `CommandID` | 命令唯一 id（审计） |
| `IdempotencyKey` | 租户内幂等键 |
| `TenantID` / `UserID` | 多租户 / 操作者 |
| `CorrelationID` / `CausationID` | 业务会话 / 因果链 |
| `Source` | 来源标签（`api` / `workflow` …） |
| `AuthContext` | 不透明鉴权声明（**勿**仅信任客户端 UserID） |

`command.AuditOf(cmd)` 产出可落库的审计快照。实现 `Validate() error` 可在 Handler 前自检。

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

领域事件通常嵌入 `event.Base`，在构造函数里 `SetEventType(...)`。聚合 `Apply` 会填充 envelope（aggregate id、序号、时间）。

**Registry** 用稳定类型名 → 工厂，供 JSON 反序列化：

```go
r.Register("bank.AccountOpened", func() event.Event { return &AccountOpened{} })
```

Upcaster（`event/upcast`）可在加载路径上升级旧 payload；注册于 `sequent.App.Upcasters`。**不要**假设与 Ruby Sequent 事件表兼容。

---

## 5. AggregateRoot

包：`aggregate`

`aggregate.Root` 是聚合契约；业务聚合嵌入 `aggregate.Base`：

1. 构造函数：`Init(id, typeName)`，并用 `On(sampleEvent, applier)` 注册 **纯状态** 应用器  
2. 命令方法里做规则校验后 `Apply(event)` — **禁止**在 applier 里做 I/O  
3. 可选覆盖 `Invariants()`、`UniqueKeys()`、`PartitionKey`

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

状态 = 历史事件（+ 可选 JSON 快照）折叠结果。未提交事件停在 `UncommittedEvents()`，由 Repository 在 Commit 时写出。

---

## 6. CommandHandler

包：`command`（`Handler` / `Router`）

```go
type Handler interface {
    Handles() []string
    Handle(ctx context.Context, cmd Command) error
}
```

Handler 通过 `aggregate.Repository` 的 `Add` / `Load` 操作聚合；**不要**自己调 `EventStore.Append`。多聚合命令（如同行转账）在同一 UoW / 同一事务中 `Load` 两个聚合并分别产生事件。

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

`Router.Register(h)` 按 `CommandType()` 分发；重复注册同类型会 panic。

---

## 7. AggregateRepository 与 UnitOfWork

包：`aggregate`

**UnitOfWork** 是 **请求/命令批** 级身份映射，挂在 `context` 上（**禁止跨 goroutine 共享**）：

- `CommandService.Execute` 在事务内 `NewUnitOfWork` + `WithUnitOfWork`
- `Repo.Add` / `Repo.Load` 要求 ctx 中已有 UoW
- `Load` 命中 identity map 时复用同一实例
- `Commit` 按 **加载/注册顺序** 收集未提交事件 → `Store.Append` → `Publisher.Publish` → `MarkCommitted`

这与 Sequent 中「按 AggregateRepository 加载顺序发布事件」的意图一致。

工厂：`RegisterFactory(aggregateType, func(id) Root)`，供 Load / 快照重建。

---

## 8. CommandService 与中间件

包：`command`

```go
svc.Execute(ctx, cmd1, cmd2, ...) // 单事务、单 UoW
```

每次 `Execute`：

1. `workflow.WithHooks`（AfterCommit 袋）
2. `Tx.WithinTx` → 创建 UoW
3. 对队列中每条命令：Middleware 链 → Filters → `Validate` → `Router.Dispatch` → `Repo.Commit`
4. 事务成功后运行 AfterCommit

**Middleware** 签名：`func(ctx, cmd, next) error`；`Chain` **外层先执行**（切片下标 0 最先跑）。

常见顺序（Bank `NewDemoApp`）：

1. `tenancy.Middleware`（从 ctx 写入 `Meta.TenantID`）  
2. `idempotency.Middleware`（需要 TenantID）

**Filters**（如 `security.Filter`）在 terminal 内、Validate 之前运行。可选 `obs.Instruments` 记录命令耗时与冲突。

---

## 9. EventStore（memory / postgres）

包：`eventstore` · 实现：`eventstore/memory` · `eventstore/postgres`

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

**memory**：单测 / 本地 IT。  
**postgres**：`Open` + `Migrate`；schema `001`–`004`（含 store_events、phase3 幂等/墓碑等）。实现同时可作为 `TxManager` 与 `SnapshotStore`。

应用代码通常经 Repository 访问 Store，而不是直接 Append（测试 OCC 时可直写）。

---

## 10. Publisher：SyncInTxn 与 Outbox

包：`publish` · `publish/outbox`

### SyncInTxn（默认，Q1）

写事务内按事件顺序调用：

1. 已注册 **Projector**  
2. 已注册 **Workflow**

任一失败 → 整笔事务回滚。重建投影时用 `PublishProjectorsOnly`（跳过 workflow）。

### WithOutbox（Q6）

`publish.WithOutbox` 包装 `SyncInTxn`：同步投影之后，将匹配事件写入事务性 outbox 表；由 `outbox.Poller` / CLI `go-sequent outbox` **轮询**投递（非 LISTEN/NOTIFY / Kafka 默认）。

**账本投影必须留在 SyncInTxn**；Outbox 只用于异步/外部副作用。

---

## 11. Projector

包：`projector` · 注册到 `publish.SyncInTxn`

```go
type Projector interface {
    publish.Handler // Handles() + Handle(ctx, e)
    ManagedTables() []string
}
```

Bank 的 `BalanceProjector` 在内存维护余额（演示）；生产应经 `Persistor` 写 SQL，以便 replay。`EventRouter` 可按事件类型名注册回调。

约束（与 Sequent 精神一致）：

- 一张投影表由一个 Projector 管理  
- 重建时只依赖自己管理的表  
- Ledger 类投影失败必须失败写事务

---

## 12. Workflow 与 AfterCommit

包：`workflow`

Workflow 实现 `publish.Handler`，但通过 `RegisterWorkflow` 注册，**不参与**投影重建。

事务内可：

```go
workflow.RegisterAfterCommit(ctx, func(ctx context.Context) error {
    // 邮件 / 指标 / 非账本副作用
    return nil
})
```

钩子仅在 **提交成功后** 运行。Bank `NotifyWorkflow` 在 `AccountOpened` 上登记 AfterCommit 通知。

---

## 13. Snapshotting（JSON）

包：`snapshot` · 存储：`eventstore.SnapshotStore`（postgres/memory）

Q8：**JSON** 快照。聚合实现：

```go
SnapshotVersion() int
MarshalSnapshot() (json.RawMessage, error)
UnmarshalSnapshot(json.RawMessage) error
```

`Repository.Load` 若配置了 Snapshots：加载最新快照 → 再应用后续事件。后台 `snapshot.Snapshotter` 可 `RunOnce` / `Run` 批量生成快照。

---

## 14. Unique keys 与乐观并发

- **乐观并发：** `ExpectedVersion` = 提交前已提交序号；冲突返回 `ErrOptimisticConcurrency`。  
- **唯一键：** 聚合 `UniqueKeys()` 返回 `scope → value`；Bank 将 owner 限定在 `tenant:<pk>:bank.Account.owner`。冲突 → `ErrUniqueKeyViolation`。

`tenancy.UniqueScope` / `PartitionKey` 帮助构造租户作用域。

---

## 15. Idempotency

包：`idempotency`

- 键空间：`(TenantID, IdempotencyKey)`  
- 成功后再次提交同键：默认 **短路成功**（no-op）；`Strict=true` 时返回 `ErrReplay`  
- 实现：`NewMemory()` / `NewPostgres(pool)`  
- 安装：`sequent.WithIdempotency` 或手动 `Middleware`

设置了 `IdempotencyKey` 时 **必须** 有 `TenantID`（可先跑 tenancy 中间件）。

---

## 16. AuthZ

包：`security`

```go
type Authorizer interface {
    Authorize(ctx context.Context, cmd command.Command) error
}
```

内置：`AllowAll`、`DenyByDefault{Allow: map[string]bool{...}}`。  
安装：`sequent.WithAuthorizer`（Filter）或 `security.Middleware`。拒绝时 `errors.Is(err, security.ErrDenied)`。

**不做** 事件哈希链 / 签名（Q3）。

---

## 17. Tenancy

包：`tenancy`（Q4：共享 schema + `tenant_id`）

```go
ctx = tenancy.WithTenant(ctx, tenantID)
// Middleware：把 ctx 租户写入 Meta.TenantID（若为空），并拒绝不一致
```

辅助：`UniqueScope`、`PartitionKey`、`PartitionKeyHashed`、`MatchAggregateTenant`。无 schema-per-tenant / DB-per-tenant 一等支持。

---

## 18. Money（decimal）

包：`types`

```go
m := types.MustMoney("100.00", "CNY")
sum, _ := m.Add(types.MustMoney("0.25", "CNY"))
rounded := sum.Round(2)
```

JSON 金额为 **字符串**，禁止 `float64`。`types.AssertNoFloatMoney` / `testkit.BanFloat64Money` 可作静态检查辅助。

---

## 19. Migrations 与 Ops CLI

Postgres schema 随 `store.Migrate` 应用：`001_phase1` … `004_phase3`（含 outbox / 幂等 / 墓碑相关表）。分区示例 SQL 仅文档用途，见 `docs/ops/`。

CLI（`cmd/go-sequent`）：

```bash
go run ./cmd/go-sequent migrate -dsn "$GO_SEQUENT_PG_DSN"
go run ./cmd/go-sequent outbox -dsn "$DSN" [-once]
go run ./cmd/go-sequent replay -dsn "$DSN" -projector name -stage prepare -tables t1
```

`replay` 的 schema 阶段（prepare/optimize/golive/abort）可用 CLI；完整事件回放需应用侧接入 `projector/replay.Pipeline` + EventSource。

GDPR：`tombstone` 包提供 `SubjectTombstoned` 与策略说明（**仅墓碑**，Q5）。`crypto.PayloadProtector` 为可选字段加密钩子，**不是** crypto-shredding 产品路径。

---

## 20. Wiring：`sequent` 门面

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

Bank 演示封装：`bank.NewDemoApp(DemoOptions{StoreFactory, Idempotency, Authorizer, EnableTenancyMW})`，保证 Store 使用与 `RegisterEvents` 相同的 `event.Registry`。

---

## 21. Bank 示例 walkthrough

路径：[`examples/bank/`](../examples/bank/)

| 文件 | 内容 |
|------|------|
| `events.go` | Opened / Deposited / Withdrawn / TransferDebited / TransferCredited |
| `account.go` | 聚合 + 快照 + 租户作用域 unique owner |
| `commands.go` | Open / Deposit / Withdraw / Transfer |
| `handler.go` | 单 UoW 双聚合转账 |
| `projector.go` | SyncInTxn 余额视图 |
| `workflow.go` | AfterCommit 通知 |
| `app.go` | `NewDemoApp` 接线 |
| `integration_*_test.go` | 内存始终跑；Postgres 需 `GO_SEQUENT_PG_DSN` |

推荐阅读顺序：`commands` → `account` → `handler` → `projector` → `app` → IT。

运行：

```bash
go test ./examples/bank/ -count=1
GO_SEQUENT_PG_DSN='postgres://…' go test ./examples/bank/ -run TestIT_Postgres
```

---

## 22. 测试指引

| 层级 | 做法 |
|------|------|
| 聚合单元 | `testkit.For(id, factory).Given(...).When(...).ExpectEvents(...)` |
| 应用 / IT | `memory.New(reg)` + `NewDemoApp`；断言 Projector |
| AuthZ / 幂等 / 租户 | 见 `TestIT_Memory_*` |
| OCC | 故意 `ExpectedVersion` 过期，断言 `ErrOptimisticConcurrency` |
| Postgres | 设 `GO_SEQUENT_PG_DSN`；未设则 skip |

```bash
go test ./...   # 无 DSN 必须绿
```

基准：`go test ./bench/ -bench=. -benchmem`（见 [bench.md](./bench.md)）。

---

## 23. 运维与安全文档指针

| 主题 | 文档 |
|------|------|
| 故障切换 / 投影重建 / 备份 / 分区 | [ops/](./ops/) |
| 威胁模型 | [security/threat-model.zh.md](./security/threat-model.zh.md) |
| 事件演进 / Upcaster（禁止导入 Sequent DB） | [security/event-evolution.zh.md](./security/event-evolution.zh.md) |
| 基准 | [bench.md](./bench.md) |

---

## 24. 相对 Sequent Ruby：未实现 / 阶段边界

| Sequent（Ruby）概念 | go-sequent |
|---------------------|------------|
| AggregateRoot / Command / Event / CommandService / Repository / EventStore / Projector / Workflow / Snapshot / Migrations | **已实现**（Go 惯用 API） |
| ActiveRecord `Records` + SQL 投影迁移 DSL | **无** AR；`ManagedTables` + `projector/replay` 管线 |
| 自动注册 / Rails / Sinatra 生成器 | **无**（纯库） |
| AggregateObserver | **未**单独产品化 |
| ValueObject 一等类型系统 | 用普通 Go 类型 + `types.Money` |
| LISTEN/NOTIFY / 默认消息总线 | Outbox **表轮询**（可选） |
| 事件哈希链 / 签名 | **明确不做**（Q3） |
| 导入既有 Sequent DB | **禁止**（Q9） |
| HTTP/gRPC stubs | **无**（Q7） |

Phase 边界简述：Phase 1 核心闭环 → Phase 2 快照/upcast/workflow/replay 骨架 → Phase 3 幂等/AuthZ/outbox/tenancy/tombstone/obs → Phase 4 文档与基准加固 → Bank 示例 + 集成测试扩展。

---

## 25. 锁定决策摘要（Q1–Q10）

| # | 决策 |
|---|------|
| Q1 | 账本投影 **SyncInTxn** |
| Q2 | Money = **decimal**（禁 float64） |
| Q3 | 完整性 = **仅 DB**（无哈希链/签名） |
| Q4 | 多租户 = **共享 schema + tenant_id** |
| Q5 | GDPR = **仅墓碑** |
| Q6 | Outbox = **Postgres 表轮询** |
| Q7 | **纯库**（无 HTTP/gRPC） |
| Q8 | 快照 = **JSON** |
| Q9 | **永不**导入 Ruby Sequent DB |
| Q10 | 项目 **私有 / 暂不公开** |

---

*文档对应实现分支含 Phase 1–4 与 bank 示例；若 API 变更以源码为准。*
