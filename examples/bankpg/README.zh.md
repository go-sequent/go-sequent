# bankpg — 面向生产形态的 Postgres 银行示例

与内存 **演示** [`examples/bank`](../bank/) 并列：复用同一套领域（Account 的 Command / Event / Handler）；账本余额经 SyncInTxn 写入 SQL。

## 能力

- Postgres EventStore（`store.Migrate`）+ 示例表 `bank_account_balances`
- `SQLBalanceProjector` + `projector.Persistor`（加入写事务）
- Tenancy 中间件、Postgres 幂等、AuthZ 白名单
- 可选 `obs.Instruments`（`Options.Instruments`）
- Outbox：**默认不接线**——账本投影仍走 SyncInTxn；异步副作用请自行包 `publish.WithOutbox`

## 已知缺口

- 无产品级 RLS 策略
- 无 HTTP/gRPC（纯库）
- 测试里的「重建」是 truncate + 流回放，不是完整 `projector/replay` golive
- OTel `RecordEvents` 仍未自动挂到 append

## 跑测试

```bash
go test./examples/bankpg/ -count=1

GO_SEQUENT_PG_DSN='postgres://user:pass@localhost:5432/go_sequent?sslmode=disable' \
 go test./examples/bankpg/ -count=1 -run TestIT_BankPG
```
