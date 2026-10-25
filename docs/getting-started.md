# Getting started — go-sequent (Phase 1–4)

Bank/ledger loop: **Command → Aggregate → EventStore → SyncInTxn Projector**, plus Phase 2–3 features and Phase 4 production hardening docs/benches.

**Full reference:** [Framework guide (EN)](./guide.md) · [框架指南（中文）](./guide.zh.md) — AggregateRoot, Command, Event, CommandService, Repository, EventStore, Projector, Workflow, snapshots, and more. Terms: [Glossary](./guide.md#26-glossary) (includes [ledger projection](./guide.md#gloss-ledger-projection)).

## Bank example

| Path | Role |
|------|------|
| [`examples/bank/`](../examples/bank/) | **Demo** — in-memory SyncInTxn balances, always-on ITs |
| [`examples/bankpg/`](../examples/bankpg/) | **Production-shaped** — Postgres + SQL `bank_account_balances` (skip without `GO_SEQUENT_PG_DSN`) |

Demo: open / deposit / withdraw / transfer, `tenant_id` unique keys, decimal Money, AuthZ / idempotency via `bank.NewDemoApp`.

```go
demo, err := bank.NewDemoApp(bank.DemoOptions{
    StoreFactory: func(reg *event.Registry) (eventstore.Store, eventstore.TxManager, eventstore.SnapshotStore, error) {
        s := memory.New(reg)
        return s, s, s, nil
    },
    Idempotency:     idempotency.NewMemory(),
    Authorizer:      bank.AllowBankCommands(),
    EnableTenancyMW: true,
})
ctx := tenancy.WithTenant(ctx, tenantID)
_ = demo.App.Service.Execute(ctx, &bank.OpenAccount{ /* … */ })
bal, _ := demo.Projector.Balance(accountID)
```

## Integration tests

| Path | When it runs |
|------|----------------|
| In-memory ITs (`TestIT_Memory_*`) | Always — included in `go test ./...` |
| Postgres ITs (`TestIT_Postgres_*`) | Only when `GO_SEQUENT_PG_DSN` is set; otherwise skipped |

```bash
# Always-on (memory + unit tests; PG ITs skip without DSN)
go test ./...

go test ./examples/bank/ ./examples/bankpg/ -count=1

# SQL ledger + OCC + projection rebuild
GO_SEQUENT_PG_DSN='postgres://user:pass@localhost:5432/go_sequent?sslmode=disable' \
  go test ./examples/bankpg/ -count=1 -run TestIT_BankPG
```

Covered scenarios: command → events → SyncInTxn projection, idempotent retry, AuthZ deny, tenant unique owner, OCC (memory demo always; SQL balances in bankpg when DSN set).

## Install

```bash
go get github.com/go-sequent/go-sequent@latest
```

(Private module: use `GOPRIVATE` / `replace` as needed.)

## CI / CD

GitHub Actions on `main`/`master` PRs and pushes: race tests, vet, golangci-lint, mod tidy check, and Postgres ITs. Tag `v*` runs the same CI then GoReleaser for `cmd/go-sequent` binaries. **No secrets required for CI.** Details: [ci.md](./ci.md) · [ci.zh.md](./ci.zh.md).

## Core loop

1. Define **Events** and register factories on `event.Registry`.
2. Embed `aggregate.Base`; mutate state **only** in appliers.
3. Define **Commands** + `command.Handler`.
4. Register **Projectors** on `SyncInTxn` (Q1 — ledger balances in the write txn).
5. Call `CommandService.Execute(ctx, cmds...)`.

## Money (Q2)

Use `types.Money` (decimal string JSON). **Never** `float64` for money.

```go
opening := types.MustMoney("100.00", "CNY")
rounded := opening.Round(2)
_ = types.AssertNoFloatMoney(myCommand) // startup / test lint
```

## Phase 3 — finance-ready

### Tenant-scoped idempotency (Q4)

```go
idemp := idempotency.NewMemory() // or idempotency.NewPostgres(pool)
app := sequent.New(cfg, sequent.WithIdempotency(idemp, false))
cmd.Meta().TenantID = tenant
cmd.Meta().IdempotencyKey = "client-key-1"
```

Retries with the same `(tenant_id, key)` are no-ops after success.

### AuthZ + audit meta

```go
app := sequent.New(cfg, sequent.WithAuthorizer(security.DenyByDefault{
    Allow: map[string]bool{"bank.Deposit": true},
}))
audit := command.AuditOf(cmd) // CommandID, TenantID, CorrelationID, Source, …
```

### Payload encryption hooks (not crypto-shredding)

```go
var protector crypto.PayloadProtector = crypto.Noop{}
// Wire Protect/Unprotect around event JSON in your store path; GDPR = tombstone only (Q5).
```

See `tombstone.Guide` and `tombstone.SubjectTombstoned`.

### Outbox + Postgres polling (Q6)

Ledger projectors stay **SyncInTxn**. For async fans:

```go
ob := outbox.NewMemory() // or outbox.NewPostgres(pool)
pub := &publish.WithOutbox{Inner: app.Publisher, Writer: ob, Topic: "events"}
app.Repo.Publisher = pub
```

Drain:

```bash
go run ./cmd/go-sequent outbox -dsn "$GO_SEQUENT_PG_DSN" -once
# or long-running: omit -once
```

### OpenTelemetry

**Role:** optional, vendor-neutral hooks to observe the CQRS/ES **write path** (command latency, success/error, OCC conflicts, optional outbox depth) for ops/SLO — **not** a substitute for the event/audit store. Details: [guide §19 Role and purpose](./guide.md#role-and-purpose).

Host app sets global Tracer/Meter providers (library uses globals; noop if unset). No OTLP exporter is bundled. Metric/span table and caveats (`RecordEvents` not auto-hooked): [guide §19](./guide.md#19-opentelemetry-obs).

```go
// App-side: otel.SetTracerProvider / SetMeterProvider + your exporter
inst, _ := obs.New(obs.Config{OutboxDepth: ob.Depth}) // OutboxDepth optional
app := sequent.New(cfg, sequent.WithObs(inst))         // sets Service.Obs — not middleware
```

### Multi-tenant helpers

```go
tenancy.UniqueScope(tenantID, "email")
tenancy.PartitionKeyHashed(tenantID, "shard-a")
tenancy.MatchAggregateTenant(cmdTenant, agg.PartitionKey())
```

Event **hash chain** enabled; **no** signatures (Q3).

## Ops CLI

```bash
go run ./cmd/go-sequent migrate -dsn "$GO_SEQUENT_PG_DSN"
go run ./cmd/go-sequent replay -dsn "$DSN" -projector balances -stage prepare -tables t1
go run ./cmd/go-sequent outbox -dsn "$DSN" [-once]
```

`store.Migrate` applies schemas through Phase 3 (`004_phase3.sql`).

## Phase 4 — production hardening

| Resource | Path |
|----------|------|
| Ops runbooks (failover / rebuild / backup) | [ops/](./ops/) |
| Partitioning guidance (shared schema, Q4) | [ops/partitioning.md](./ops/partitioning.md) |
| Threat model | [security/threat-model.md](./security/threat-model.md) |
| Event evolution / upcasters (no Sequent import, Q9) | [security/event-evolution.md](./security/event-evolution.md) |
| Benchmarks | [bench.md](./bench.md) — `go test ./bench/ -bench=. -benchmem` |

## Locked defaults

| Topic | Choice |
|-------|--------|
| Projections | SyncInTxn for ledgers (Q1) |
| Money | Decimal (Q2) |
| Integrity | Hash chain — no signatures (Q3) |
| Tenancy | Shared schema + `tenant_id` (Q4) |
| GDPR | Tombstone only (Q5) |
| Outbox | Postgres table polling (Q6) |
| Shape | Pure library; ops CLI OK (Q7) |

See [design.md](./design.md) §6–§10.
