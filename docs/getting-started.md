# Getting started — go-sequent (Phase 1 + 2)

This guide wires a bank/ledger flow: **Command → Aggregate → EventStore → SyncInTxn Projector**, plus Phase 2 Sequent-parity features.

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
app.Repo.Snapshots = store // optional JSON snapshots
app.Repo.Publisher = app.Publisher
app.Service.Tx = store
app.Service.Repo = app.Repo

app.RegisterAggregate(bank.AggregateAccount, func(id uuid.UUID) aggregate.Root {
    return bank.NewAccount(id)
})
app.RegisterProjector(bank.NewBalanceProjector())
app.RegisterWorkflow(&bank.NotifyWorkflow{}) // excluded from projection rebuild
app.RegisterHandler(&bank.Handler{Repo: app.Repo})

err := app.Service.Execute(ctx, &bank.OpenAccount{
    AccountID: id,
    Owner:     "alice",
    Currency:  "CNY",
    Opening:   types.MustMoney("100.00", "CNY"),
})
```

## Phase 2 features

### Snapshots (JSON, Q8)

Implement `SnapshotVersion` / `MarshalSnapshot` / `UnmarshalSnapshot` on aggregates. Wire `Repo.Snapshots` and run `snapshot.Snapshotter` in the background.

### Unique keys

Override `UniqueKeys() map[string]any` (scope → value). Collisions return `eventstore.ErrUniqueKeyViolation`.

### Workflows + AfterCommit

```go
workflow.RegisterAfterCommit(ctx, func(ctx context.Context) error {
    // side effect after successful txn commit
    return nil
})
```

### Upcasters v0

```go
app.RegisterUpcaster(upcast.FuncUpcaster{From: "old.Type", Fn: ...})
store.SetUpcasters(app.Upcasters)
```

### Projection replay

Library API (`projector/replay.Pipeline`): **prepare → replay → catchup → optimize → golive**.

```bash
go run ./cmd/go-sequent migrate -dsn "$GO_SEQUENT_PG_DSN"
go run ./cmd/go-sequent replay -dsn "$DSN" -projector balances -stage prepare -tables account_balances
```

Full event replay needs your app to wire `EventSource` + `SyncInTxn` projectors into `replay.Pipeline` (CLI covers schema stages + migrate).

### Postgres migrate

`store.Migrate(ctx)` applies `001_phase1.sql`, `002_phase2.sql` (snapshots, unique keys, outbox stub, replay state), and `003_store_events.sql` (`go_sequent_store_events` procedure).

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

## Locked defaults

| Topic | Choice |
|-------|--------|
| Projections | Sync in write txn (Q1) |
| Money | Decimal (`types.Money`) (Q2) |
| Integrity | DB controls only (Q3) |
| Tenancy | Shared schema + `tenant_id` (Q4) |
| Snapshots | JSON (Q8) |
| Shape | Pure library; ops CLI OK (Q7) |

See [design.md](./design.md) §9–§10 for the full roadmap and decisions.
