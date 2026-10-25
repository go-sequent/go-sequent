# Getting started — go-sequent (Phase 1–3)

Bank/ledger loop: **Command → Aggregate → EventStore → SyncInTxn Projector**, plus Phase 2 parity and Phase 3 finance-ready hooks.

## Install

```bash
go get github.com/wenweih/go-sequent@latest
```

(Private module: use `GOPRIVATE` / `replace` as needed.)

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

```go
inst, _ := obs.New(obs.Config{OutboxDepth: ob.Depth})
app := sequent.New(cfg, sequent.WithObs(inst))
```

### Multi-tenant helpers

```go
tenancy.UniqueScope(tenantID, "email")
tenancy.PartitionKeyHashed(tenantID, "shard-a")
tenancy.MatchAggregateTenant(cmdTenant, agg.PartitionKey())
```

**No** event hash-chain or signatures (Q3).

## Ops CLI

```bash
go run ./cmd/go-sequent migrate -dsn "$GO_SEQUENT_PG_DSN"
go run ./cmd/go-sequent replay -dsn "$DSN" -projector balances -stage prepare -tables t1
go run ./cmd/go-sequent outbox -dsn "$DSN" [-once]
```

`store.Migrate` applies schemas through Phase 3 (`004_phase3.sql`).

## Locked defaults

| Topic | Choice |
|-------|--------|
| Projections | SyncInTxn for ledgers (Q1) |
| Money | Decimal (Q2) |
| Integrity | DB only — no hash/sign (Q3) |
| Tenancy | Shared schema + `tenant_id` (Q4) |
| GDPR | Tombstone only (Q5) |
| Outbox | Postgres table polling (Q6) |
| Shape | Pure library; ops CLI OK (Q7) |

See [design.md](./design.md) §6–§10.
