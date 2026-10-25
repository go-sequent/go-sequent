# 快速开始 — go-sequent（Phase 1–4）

账本闭环：**Command → Aggregate → EventStore → SyncInTxn Projector**，含 Phase 2–3 能力与 Phase 4 生产加固文档/基准。

**完整参考：** [框架指南（中文）](./guide.zh.md) · [Framework guide (EN)](./guide.md) — AggregateRoot、Command、Event、CommandService、Repository、EventStore、Projector、Workflow、快照等。术语见 [名词释义](./guide.zh.md#26-名词释义)（含 [账本投影](./guide.zh.md#gloss-ledger-projection)）。

## Bank 示例

| 路径 | 角色 |
|------|------|
| [`examples/bank/`](../examples/bank/) | **演示** — 内存 SyncInTxn 余额，IT 始终可跑 |
| [`examples/bankpg/`](../examples/bankpg/) | **生产形态** — Postgres + SQL `bank_account_balances`（无 DSN 则 skip） |

演示：开户 / 存 / 取 / 转账、`tenant_id` 唯一键、decimal Money、`bank.NewDemoApp`。详见 [`bankpg/README.zh.md`](../examples/bankpg/README.zh.md)。

```go
demo, err := bank.NewDemoApp(bank.DemoOptions{
    StoreFactory: /* memory */,
    Idempotency: idempotency.NewMemory(),
    Authorizer: bank.AllowBankCommands(),
    EnableTenancyMW: true,
})
```

## 集成测试

| 路径 | 何时运行 |
|------|----------|
| `examples/bank` 内存 ITs | 始终 — `go test ./...` |
| `bank` / `bankpg` Postgres ITs | 设 `GO_SEQUENT_PG_DSN`；否则 skip |

```bash
go test ./...
go test ./examples/bank/ ./examples/bankpg/ -count=1

GO_SEQUENT_PG_DSN='postgres://user:pass@localhost:5432/go_sequent?sslmode=disable' \
  go test ./examples/bankpg/ -count=1 -run TestIT_BankPG
```

覆盖：命令 → 事件 → SyncInTxn 投影、幂等、AuthZ、租户唯一 owner、OCC；bankpg 另含 SQL 余额与投影回放。

## 安装

```bash
go get github.com/go-sequent/go-sequent@latest
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
| **OpenTelemetry** | `obs.New` + `WithObs`（应用自配 Provider；详见 [指南 §19](./guide.zh.md#19-opentelemetryobs)） |
| **完整性** | 哈希链；禁止事件签名（Q3） |

### OpenTelemetry 最小接线

**作用：** 可选、厂商无关的钩子，观测 CQRS/ES **写路径**（命令延迟、成败、乐观冲突、可选 outbox 积压）供运维/SLO 使用——**不能**替代事件/审计存储。详见 [指南 §19 作用](./guide.zh.md#作用)。

应用自行设置全局 Tracer/Meter（库用全局；未设则为 noop）。**无**内置 OTLP exporter。`RecordEvents` **不会**在 append 路径自动调用。完整指标/span 表见 [指南 §19](./guide.zh.md#19-opentelemetryobs)。

```go
inst, _ := obs.New(obs.Config{OutboxDepth: ob.Depth}) // OutboxDepth 可选
app := sequent.New(cfg, sequent.WithObs(inst))         // 写入 Service.Obs，非 middleware
```

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

同步账本投影 · decimal · 哈希链完整性 · 共享 schema + `tenant_id` · 仅墓碑 · Outbox 表轮询 · 纯库 · 不公开（Q10）。

详见 [design.zh.md](./design.zh.md) §6–§10。
