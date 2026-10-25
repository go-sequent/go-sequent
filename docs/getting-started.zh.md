# 快速开始 — go-sequent（Phase 1 + 2）

本指南演示银行/账本闭环：**Command → Aggregate → EventStore → SyncInTxn Projector**，并简述 Phase 2 Sequent 对等能力。

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

## Phase 2 能力

| 能力 | 说明 |
|------|------|
| **JSON 快照**（Q8） | 聚合实现 `Snapshotable`；`Repo.Snapshots` + `snapshot.Snapshotter` |
| **唯一键** | `UniqueKeys()`；冲突 → `ErrUniqueKeyViolation` |
| **Workflow + AfterCommit** | `RegisterWorkflow`；`workflow.RegisterAfterCommit` |
| **Upcaster v0** | `app.RegisterUpcaster` + `store.SetUpcasters` |
| **投影重建** | `projector/replay.Pipeline`：prepare → replay → catchup → optimize → golive |
| **CLI** | `go run ./cmd/go-sequent migrate\|replay …` |

```bash
go run ./cmd/go-sequent migrate -dsn "$GO_SEQUENT_PG_DSN"
go run ./cmd/go-sequent replay -dsn "$DSN" -projector balances -stage prepare -tables account_balances
```

完整事件回放需在应用内把 `EventSource` 与 SyncInTxn Projector 接入 `replay.Pipeline`；CLI 负责 migrate 与 schema 阶段。

`store.Migrate` 会执行 Phase1/Phase2 schema 与 `go_sequent_store_events` 过程。

## 已锁定默认

| 主题 | 选择 |
|------|------|
| 投影 | 写事务内同步（Q1） |
| 金额 | Decimal（Q2） |
| 完整性 | 仅 DB 管控（Q3） |
| 多租户 | 共享 schema + `tenant_id`（Q4） |
| 快照 | JSON（Q8） |
| 交付 | 纯库；运维 CLI 可有（Q7） |

详见 [design.zh.md](./design.zh.md) §9–§10。
