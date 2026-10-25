# Getting started — go-sequent Phase 1

This guide wires a minimal bank/ledger flow: **Command → Aggregate → EventStore → SyncInTxn Projector**.

## Install

```bash
go get github.com/wenweih/go-sequent@latest
```

(Private module: use a `GOPRIVATE` / replace directive as needed.)

## Core loop

1. Define **Events** (past tense) and register factories on `event.Registry`.
2. Embed `aggregate.Base`, register `On(event, applier)`, mutate state **only** in appliers.
3. Define **Commands** + a `command.Handler` that `Repo.Add` / `Repo.Load` and calls domain methods.
4. Register a **Projector** on `publish.SyncInTxn` (default) so ledger balances update in the **same DB transaction** as the append (Q1).
5. Call `CommandService.Execute(ctx, cmds...)`.

## Money

Use `types.Money` (shopspring/decimal). **Do not** put `float64` on command/event money fields.

```go
opening := types.MustMoney("100.00", "CNY")
```

## Minimal example (in-memory)

The in-repo demo is `examples/bank`. Sketch:

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

1. Apply schema: `eventstore/postgres/schema/001_phase1.sql` (or `store.Migrate(ctx)`).
2. Open a pool and pass it into the store:

```go
pg, err := postgres.Open(ctx, dsn, app.Events)
if err != nil { /* ... */ }
defer pg.Close()
_ = pg.Migrate(ctx)
app.Repo.Store = pg
app.Service.Tx = pg
```

Tables include `commands`, `aggregates`, `events`, type registries, and `tenant_id` columns (shared-schema multi-tenancy). No hash-chain / signature columns.

## Aggregate tests (testkit)

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

## Locked Phase 1 defaults

| Topic | Choice |
|-------|--------|
| Projections | Sync in write txn |
| Money | Decimal (`types.Money`) |
| Integrity | DB controls only |
| Tenancy | Shared schema + `tenant_id` |
| Shape | Pure library (no HTTP/gRPC) |

See [design.md](./design.md) §9–§10 for the full roadmap and decisions.
