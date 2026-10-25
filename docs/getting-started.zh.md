# 快速开始 — go-sequent（Phase 1–3）

账本闭环：**Command → Aggregate → EventStore → SyncInTxn Projector**，含 Phase 2 对等与 Phase 3 金融就绪能力。

## 安装

```bash
go get github.com/wenweih/go-sequent@latest
```

## 金额（Q2）

使用 `types.Money`（decimal 字符串 JSON）。**禁止** `float64`。可用 `types.AssertNoFloatMoney` / `testkit.BanFloat64Money` 做检查。

## Phase 3 要点

| 能力 | 用法 |
|------|------|
| **租户幂等键**（Q4） | `sequent.WithIdempotency(store, strict)`；需 `TenantID` + `IdempotencyKey` |
| **AuthZ + 审计元数据** | `WithAuthorizer`；`command.AuditOf(cmd)` |
| **载荷加密钩子** | `crypto.PayloadProtector`（接口）；GDPR = **仅墓碑**（Q5），无 crypto-shredding 产品化 |
| **Outbox 轮询**（Q6） | `publish.WithOutbox` + `outbox.Poller` / CLI `outbox`；账本仍 SyncInTxn（Q1） |
| **OpenTelemetry** | `obs.New` + `WithObs` |
| **多租户助手** | `tenancy.UniqueScope` / `PartitionKeyHashed` / `MatchAggregateTenant` |
| **禁止** | 事件哈希链 / 签名（Q3） |

```bash
go run ./cmd/go-sequent migrate -dsn "$GO_SEQUENT_PG_DSN"
go run ./cmd/go-sequent outbox -dsn "$DSN" -once
```

## 已锁定默认

同步账本投影 · decimal 金额 · 仅 DB 完整性 · 共享 schema + `tenant_id` · 仅墓碑 · Outbox 表轮询 · 纯库。

详见 [design.zh.md](./design.zh.md) §6–§10。
