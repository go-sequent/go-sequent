# bankpg — production-shaped Postgres bank sample

Companion to the in-memory **demo** at [`examples/bank`](../bank/). Same domain (Account commands/events/handler); ledger balances land in SQL via SyncInTxn.

## What you get

- Postgres EventStore (`store.Migrate`) + example-local `bank_account_balances`
- `SQLBalanceProjector` through `projector.Persistor` (joins the write txn)
- Tenancy middleware, Postgres idempotency, AuthZ allow-list
- Optional `obs.Instruments` via `Options.Instruments`
- Outbox: **not** wired by default — keep ledgers on SyncInTxn (Q1); wrap `publish.WithOutbox` yourself for async fans

## Gaps (honest)

- No productized RLS policies
- No HTTP/gRPC (pure library, Q7)
- Projection rebuild here is a **manual** truncate + stream replay helper in tests, not the full `projector/replay` golive pipeline
- `RecordEvents` OTel counter still not auto-hooked on append

## Run tests

```bash
# Always: compiles; PG ITs skip without DSN
go test ./examples/bankpg/ -count=1

GO_SEQUENT_PG_DSN='postgres://user:pass@localhost:5432/go_sequent?sslmode=disable' \
  go test ./examples/bankpg/ -count=1 -run TestIT_BankPG
```

Apply core schema then projection DDL (also done inside the test helper):

```go
store.Migrate(ctx)
bankpg.MigrateProjection(ctx, store.Pool())
```
