# 快速开始 — go-sequent（Phase 1–4）

账本闭环：**Command → Aggregate → EventStore → SyncInTxn Projector**，含 Phase 2–3 能力与 Phase 4 生产加固文档/基准。

**完整参考：** [框架指南（中文）](./guide.zh.md) · [Framework guide (EN)](./guide.md) — AggregateRoot、Command、Event、CommandService、Repository、EventStore、Projector、Workflow、快照等。

## Bank 示例

规范演示在 [`examples/bank/`](../examples/bank/)：开户、存款、取款、同行 **转账**、SyncInTxn 余额投影、`tenant_id` 唯一键、decimal `types.Money`，以及通过 `bank.NewDemoApp` 轻量接入 AuthZ / 幂等。

```go
demo, err := bank.NewDemoApp(bank.DemoOptions{
    StoreFactory: /* memory 或 postgres */,
    Idempotency: idempotency.NewMemory(),
    Authorizer: bank.AllowBankCommands(),
    EnableTenancyMW: true,
})
```

## 集成测试

| 路径 | 何时运行 |
|------|----------|
| 内存 ITs（`TestIT_Memory_*`） | 始终 — 包含在 `go test ./...` |
| Postgres ITs（`TestIT_Postgres_*`） | 仅当设置 `GO_SEQUENT_PG_DSN`；否则 skip |

```bash
go test ./...
go test ./examples/bank/ -count=1

GO_SEQUENT_PG_DSN='postgres://user:pass@localhost:5432/go_sequent?sslmode=disable' \
  go test ./examples/bank/ -count=1 -run TestIT_Postgres
```

覆盖：命令 → 事件 → SyncInTxn 投影（开户/存/取/转账）、幂等重试、AuthZ 拒绝、租户唯一 owner、乐观并发冲突（内存始终；Postgres 需 DSN）。

## 安装

```bash
go get github.com/wenweih/go-sequent@latest
```

## 金额（Q2）

使用 `types.Money`（decimal 字符串 JSON）。**禁止** `float64`。可用 `types.AssertNoFloatMoney` / `testkit.BanFloat64Money`。

## Phase 3 要点

| 能力 | 用法 |
|------|------|
| **租户幂等键**（Q4） | `sequent.WithIdempotency(store, strict)` |
| **AuthZ + 审计** | `WithAuthorizer`；`command.AuditOf(cmd)` |
| **加密钩子 / 墓碑** | `crypto.PayloadProtector`；GDPR = **仅墓碑**（Q5） |
| **Outbox 轮询**（Q6） | `publish.WithOutbox` + CLI `outbox`；账本仍 SyncInTxn |
| **OpenTelemetry** | `obs.New` + `WithObs` |
| **禁止** | 事件哈希链/签名（Q3） |

## Phase 4 — 生产加固

| 资源 | 路径 |
|------|------|
| 运维手册 | [ops/](./ops/) |
| 分区指引（Q4） | [ops/partitioning.zh.md](./ops/partitioning.zh.md) |
| 威胁模型 | [security/threat-model.zh.md](./security/threat-model.zh.md) |
| 事件演进（永不导入 Sequent，Q9） | [security/event-evolution.zh.md](./security/event-evolution.zh.md) |
| 基准测试 | [bench.md](./bench.md) — `go test ./bench/ -bench=. -benchmem` |

```bash
go run ./cmd/go-sequent migrate -dsn "$GO_SEQUENT_PG_DSN"
go run ./cmd/go-sequent outbox -dsn "$DSN" -once
```

## 已锁定默认

同步账本投影 · decimal · 仅 DB 完整性 · 共享 schema + `tenant_id` · 仅墓碑 · Outbox 表轮询 · 纯库 · 不公开（Q10）。

详见 [design.zh.md](./design.zh.md) §6–§10。
