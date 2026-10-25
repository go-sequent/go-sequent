# 快速开始 — go-sequent Phase 1

本指南演示最小银行/账本闭环：**Command → Aggregate → EventStore → SyncInTxn Projector**。

## 安装

```bash
go get github.com/wenweih/go-sequent@latest
```

（私有模块请配置 `GOPRIVATE` 或 `replace`。）

## 核心闭环

1. 定义**事件**（过去式命名），在 `event.Registry` 注册工厂。
2. 嵌入 `aggregate.Base`，用 `On(event, applier)` 注册应用函数；**只在 applier 里改状态**。
3. 定义 **Command** 与 `command.Handler`：`Repo.Add` / `Repo.Load`，再调领域方法。
4. 在 `publish.SyncInTxn`（默认）上注册 **Projector**，保证账本余额与写库**同一事务**更新（Q1）。
5. 调用 `CommandService.Execute(ctx, cmds...)`。

## 金额

使用 `types.Money`（shopspring/decimal）。**禁止**在命令/事件金额字段使用 `float64`。

```go
opening := types.MustMoney("100.00", "CNY")
```

## 内存最小示例

仓库内示例见 `examples/bank`。要点：

```go
app := sequent.New(sequent.Config{PublisherMode: sequent.SyncInTxn})
bank.RegisterEvents(app.Events)
store := memory.New(app.Events)
app.Repo.Store = store
app.Repo.Publisher = app.Publisher
app.Service.Tx = store
app.Service.Repo = app.Repo

app.RegisterAggregate(bank.AggregateAccount, func(id uuid.UUID) aggregate.Root {
    return bank.NewAccount(id)
})
app.RegisterProjector(bank.NewBalanceProjector())
app.RegisterHandler(&bank.Handler{Repo: app.Repo})

err := app.Service.Execute(ctx, &bank.OpenAccount{
    AccountID: id,
    Owner:     "alice",
    Currency:  "CNY",
    Opening:   types.MustMoney("100.00", "CNY"),
})
```

## Postgres

1. 执行 schema：`eventstore/postgres/schema/001_phase1.sql`（或 `store.Migrate(ctx)`）。
2. 打开连接池并注入：

```go
pg, err := postgres.Open(ctx, dsn, app.Events)
if err != nil { /* ... */ }
defer pg.Close()
_ = pg.Migrate(ctx)
app.Repo.Store = pg
app.Service.Tx = pg
```

表含 `commands` / `aggregates` / `events`、类型注册表与 `tenant_id`（共享 schema 多租户）。无哈希链/签名列。

## 聚合测试（testkit）

```go
s := testkit.For(id, func(id uuid.UUID) aggregate.Root {
    return bank.NewAccount(id)
}).Given(
    bank.NewAccountOpened("dave", "USD", types.MustMoney("10", "USD")),
).When(func(agg aggregate.Root) error {
    return agg.(*bank.Account).Withdraw(types.MustMoney("20", "USD"))
}).Run()

_ = s.ExpectError("insufficient funds")
```

## Phase 1 已锁定默认

| 主题 | 选择 |
|------|------|
| 投影 | 写事务内同步 |
| 金额 | Decimal（`types.Money`） |
| 完整性 | 仅 DB 管控 |
| 多租户 | 共享 schema + `tenant_id` |
| 交付形态 | 纯库（无 HTTP/gRPC） |

完整路线与决策见 [design.zh.md](./design.zh.md) §9–§10。
