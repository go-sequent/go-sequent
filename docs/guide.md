# go-sequent Framework Guide

> Documents **what is implemented** in Phases 1–4 and `examples/bank`. Section names follow the spirit of the [Sequent](https://sequent.io/) Reference Guide; APIs and schemas are Go-native (**never** import a Ruby Sequent database — Q9).

Module: `github.com/wenweih/go-sequent` · Shape: **pure library** (no HTTP/gRPC in-repo — Q7)

---

## Table of contents

1. [What is go-sequent / when to use it](#1-what-is-go-sequent--when-to-use-it)
2. [Core loop (basic flow)](#2-core-loop-basic-flow)
3. [Command and Meta](#3-command-and-meta)
4. [Event and Registry](#4-event-and-registry)
5. [AggregateRoot](#5-aggregateroot)
6. [CommandHandler](#6-commandhandler)
7. [AggregateRepository and UnitOfWork](#7-aggregaterepository-and-unitofwork)
8. [CommandService and middleware](#8-commandservice-and-middleware)
9. [EventStore (memory / postgres)](#9-eventstore-memory--postgres)
10. [Publisher: SyncInTxn and outbox](#10-publisher-syncintxn-and-outbox)
11. [Projector](#11-projector)
12. [Workflow and AfterCommit](#12-workflow-and-aftercommit)
13. [Snapshotting (JSON)](#13-snapshotting-json)
14. [Unique keys and optimistic concurrency](#14-unique-keys-and-optimistic-concurrency)
15. [Idempotency](#15-idempotency)
16. [AuthZ](#16-authz)
17. [Tenancy](#17-tenancy)
18. [Money (decimal)](#18-money-decimal)
19. [OpenTelemetry (`obs`)](#19-opentelemetry-obs) — [Role and purpose](#role-and-purpose)
20. [Migrations and ops CLI](#20-migrations-and-ops-cli)
21. [Wiring: the `sequent` façade](#21-wiring-the-sequent-façade)
22. [Bank example walkthrough](#22-bank-example-walkthrough)
23. [Testing guidance](#23-testing-guidance)
24. [Ops and security pointers](#24-ops-and-security-pointers)
25. [Not implemented / Phase boundaries vs Sequent Ruby](#25-not-implemented--phase-boundaries-vs-sequent-ruby)
26. [Glossary](#26-glossary)
27. [Locked decisions (Q1–Q10)](#27-locked-decisions-q1q10)

---

## 1. What is go-sequent / when to use it

**go-sequent** is a Go **CQRS + Event Sourcing** library aimed at financial ledger workloads. You define commands, aggregates, events, and projections; the framework provides:

- One **DB transaction + context-scoped UnitOfWork** per `Execute`
- **Optimistic concurrency** and **unique keys** on append
- [Ledger projections](#gloss-ledger-projection) **synchronously in the write txn** ([`SyncInTxn`](#gloss-syncintxn), Q1)
- Decimal **Money**, tenant-scoped idempotency, AuthZ hooks, JSON snapshots, tombstone GDPR hooks, optional outbox polling
- Optional **OpenTelemetry** (`obs`) for write-path latency / success-error / OCC conflicts and outbox backlog signals — **not** a business audit source; see [§19 Role and purpose](#role-and-purpose)

**Use when:** balances must not lag the write path ([read-your-writes](#gloss-read-your-writes)); you want shared-schema multi-tenancy; you need auditable command/event records.

Terms: [§26 Glossary](#26-glossary).

**Not a web framework:** no Rails/Sinatra generators, no HTTP API, no ActiveRecord. Embed the library in your own process.

See also [getting-started.md](./getting-started.md) and [design.md](./design.md).

---

## 2. Core loop (basic flow)

```text
Client / service entry
        │
        ▼
  CommandService.Execute(ctx, cmds...)
        │  Tx + UnitOfWork
        ├─ Middleware (tenancy / idempotency …)
        ├─ Filters (AuthZ)
        ├─ Command.Validate (optional)
        ▼
  CommandHandler
        │  Repo.Add / Repo.Load → aggregate Apply
        ▼
  Repository.Commit
        │  EventStore.Append
        │  Publisher.Publish → Projectors → Workflows
        ▼
  Tx commit → AfterCommit hooks
```

You implement **Command / Handler / Aggregate / Event / Projector** (and optional Workflow). `command.Service`, `aggregate.Repository`, `eventstore`, and `publish.SyncInTxn` wire the rest.

---

## 3. Command and Meta

Package: `command`

```go
type Command interface {
    CommandType() string
    AggregateID() (uuid.UUID, bool)
    Meta() *Meta
}
```

Embed `command.Base` for `Meta()`. `EnsureMeta` fills `CommandID`, `IssuedAt`, `CorrelationID`.

| Field | Role |
|-------|------|
| `CommandID` | Audit id |
| `IdempotencyKey` | Per-tenant idempotency |
| `TenantID` / `UserID` | Tenancy / actor |
| `CorrelationID` / `CausationID` | Conversation / cause |
| `Source` | Label (`api`, `workflow`, …) |
| `AuthContext` | Opaque claims — **do not** trust client `UserID` alone |

`command.AuditOf(cmd)` builds a persistence-friendly audit snapshot. Implement `Validate() error` for self-checks before the handler.

---

## 4. Event and Registry

Package: `event`

Events embed `event.Base` and set a stable type name. `Apply` fills aggregate id, sequence, and timestamp.

**Registry** maps type name → factory for JSON decode:

```go
r.Register("bank.AccountOpened", func() event.Event { return &AccountOpened{} })
```

Upcasters live in `event/upcast` and register on `sequent.App.Upcasters`. No Ruby Sequent table compatibility.

---

## 5. AggregateRoot

Package: `aggregate`

Embed `aggregate.Base`:

1. `Init(id, typeName)` and `On(sample, applier)` — appliers are **pure state** only  
2. Command methods validate then `Apply(event)` — **no I/O** in appliers  
3. Optional `Invariants()`, `UniqueKeys()`, `PartitionKey`

Uncommitted events sit in `UncommittedEvents()` until `Repository.Commit`.

---

## 6. CommandHandler

```go
type Handler interface {
    Handles() []string
    Handle(ctx context.Context, cmd Command) error
}
```

Use `Repo.Add` / `Repo.Load` only — do not call `EventStore.Append` from handlers. Multi-aggregate commands (e.g. transfer) load both aggregates in one UoW. `Router.Register` panics on duplicate command types.

---

## 7. AggregateRepository and UnitOfWork

**UnitOfWork** is a per-request identity map on `context` (**never** share across goroutines).

- `Execute` creates UoW inside the txn  
- `Add` / `Load` require UoW in ctx  
- `Load` reuses the identity-map instance  
- `Commit` appends in **load/registration order**, publishes, then `MarkCommitted`

Factories: `RegisterFactory(aggregateType, func(id) Root)`.

---

## 8. CommandService and middleware

```go
svc.Execute(ctx, cmd1, cmd2, ...) // one txn, one UoW
```

Flow: AfterCommit bag → `WithinTx` → UoW → for each command: Middleware → Filters → Validate → Dispatch → Commit → after success, AfterCommit.

`Chain` runs **outer-first** (index 0 first). Bank demo order: **tenancy before idempotency** so `Meta.TenantID` is set. Optional observability: see [§19 OpenTelemetry (`obs`)](#19-opentelemetry-obs) (field on `Service.Obs`, **not** middleware).

---

## 9. EventStore (memory / postgres)

```go
Append(ctx, CommandRecord, []StreamAppend) error
LoadStream(ctx, id) ([]event.Event, error)
VerifyChain(ctx, aggregateID) error // memory + postgres
```

Errors: `ErrOptimisticConcurrency`, `ErrUniqueKeyViolation`, `ErrStreamNotFound`, `ErrHashChainTamper`, `ErrHashChainIncomplete`.

- **memory** — unit/IT  
- **postgres** — `Open` + `Migrate` (schemas `001`–`005`); also `TxManager` + `SnapshotStore`

### Hash chain (Q3)

Per-aggregate SHA-256 chain: each row stores `prev_hash` + `event_hash`. Seq 1 uses genesis (32 zero bytes) as `prev_hash`. Canonical input (UTF-8 lines joined by `\n`, no trailing newline):

1. `event_type`
2. `aggregate_id` (UUID string)
3. `sequence_number` (decimal)
4. `created_at` (UTC RFC3339Nano)
5. `event_json` (exact stored payload bytes)
6. `prev_hash` as 64 lowercase hex

`event_hash = SHA-256(input)`. Formula lives in `eventstore/hashchain`. **No** event signatures. `LoadStream` verifies the chain; call `VerifyChain` for an explicit check. Tamper → `ErrHashChainTamper`.

---

## 10. Publisher: SyncInTxn and outbox

**SyncInTxn (default, Q1):** in the write txn, projectors then workflows; failure rolls back. Replay uses `PublishProjectorsOnly`.

**WithOutbox (Q6):** after sync handlers, enqueue matching events; drain with `outbox.Poller` / `go-sequent outbox` (**table polling**). Keep ledger projectors on SyncInTxn.

---

## 11. Projector

```go
type Projector interface {
    publish.Handler
    ManagedTables() []string
}
```

Bank `BalanceProjector` is in-memory for the demo; production should use `Persistor` for replay-safe SQL. One table → one projector; ledger failures must fail the write txn.

---

## 12. Workflow and AfterCommit

Workflows implement `publish.Handler` via `RegisterWorkflow` — **excluded** from projection rebuild.

```go
workflow.RegisterAfterCommit(ctx, func(ctx context.Context) error { /* side effect */ return nil })
```

Runs only after a successful commit. See bank `NotifyWorkflow`.

---

## 13. Snapshotting (JSON)

Q8: **JSON**. Implement `SnapshotVersion` / `MarshalSnapshot` / `UnmarshalSnapshot`. Repository loads latest snapshot then remaining events. `snapshot.Snapshotter` can batch in the background.

---

## 14. Unique keys and optimistic concurrency

- OCC: `ExpectedVersion` must match stream tip → else `ErrOptimisticConcurrency`  
- Unique keys: `UniqueKeys()` map `scope → value` (bank scopes owner by tenant partition) → `ErrUniqueKeyViolation`

---

## 15. Idempotency

Key space `(TenantID, IdempotencyKey)`. Successful retry is a no-op by default; `Strict` → `ErrReplay`. Stores: `NewMemory` / `NewPostgres`. `IdempotencyKey` requires `TenantID`.

---

## 16. AuthZ

`security.Authorizer` via `WithAuthorizer` or middleware. Built-ins: `AllowAll`, `DenyByDefault`. Check `errors.Is(err, security.ErrDenied)`. Hash chain yes; **no** event signatures (Q3).

---

## 17. Tenancy

Shared schema + `tenant_id` (Q4). `tenancy.WithTenant` + middleware copies into `Meta.TenantID`. Helpers: `UniqueScope`, `PartitionKey`, `MatchAggregateTenant`.

---

## 18. Money (decimal)

`types.Money` — decimal string JSON, never `float64`. `MustMoney` / `Add` / `Sub` / `Round`. Lint helpers: `AssertNoFloatMoney`, `testkit.BanFloat64Money`.

---

## 19. OpenTelemetry (`obs`)

Package: `obs` (`obs/otel.go`). Instrumentation name: `github.com/wenweih/go-sequent`.

### Role and purpose

**Why it exists:** The CQRS/ES write path (`CommandService.Execute` → txn → Append → SyncInTxn) is latency- and conflict-sensitive for ledgers. `obs` exposes command duration, outcomes, optimistic-concurrency conflicts, and optional outbox depth through the **vendor-neutral** OpenTelemetry API — without coupling the library to a specific APM.

**Where it sits:** Optional `obs.Instruments` attached via `sequent.WithObs` onto `command.Service.Obs` (a field, **not** middleware). Spans wrap `Execute`; metrics feed ops/SLO dashboards. This is **complementary** to audit metadata and the EventStore — OTel is a runtime signal, **not** the source of truth. Reconciliation and compliance still rely on the event stream and `command.AuditOf` / `command_records`.

**What operators / finance tech teams can see:**

- Slow commands (`sequent.command.duration_ms` + `command.type`)
- Conflict storms (`sequent.optimistic_conflict.total` under hot aggregates)
- Outbox backlog signal (`sequent.outbox.depth` when `Config.OutboxDepth` is set)
- Command success/error rates (`sequent.command.total` with `status=ok|error`)

**What it does not do:** Replace the business audit trail; auto-instrument HTTP/gRPC, projectors, or the outbox poller; ship an exporter; own the Provider (the app does). `RecordEvents` exists but is **not** auto-hooked on append (see below).

**Locked decisions:** Observability hooks only. Event integrity is **hash chain** (Q3); delivery shape remains a **pure library** (Q7) — export stays in the host process.

### Wiring

`obs` is **not** command middleware. Use `sequent.WithObs(inst)` to set `App.Obs` and `command.Service.Obs` (`nil` = no telemetry), or assign `svc.Obs = inst` directly.

`CommandService.Execute`:

1. `StartCommand` → span `sequent.Execute`
2. Run txn / UoW
3. On `ErrOptimisticConcurrency` from Commit → `RecordConflict`
4. `RecordCommand` (latency + counters, then `span.End`)

### App must configure providers

`obs.New` uses the **global** `otel.Tracer` / `otel.Meter`. With no TracerProvider / MeterProvider set, instruments are **noop** (safe in tests). The library does **not** ship OTLP / Prometheus / stdout exporters — the host app installs them.

```go
// App-side: otel.SetTracerProvider / SetMeterProvider + your exporter
inst, err := obs.New(obs.Config{
    OutboxDepth: ob.Depth, // optional; nil skips sequent.outbox.depth
})
app := sequent.New(sequent.Config{},
    sequent.WithObs(inst),
    sequent.WithStore(store, tx),
)
```

### Emitted metrics / spans

| Kind | Name | Attributes / notes |
|------|------|--------------------|
| Span | `sequent.Execute` | `command.type`; on error `RecordError` + status Error |
| Histogram | `sequent.command.duration_ms` | `command.type`; milliseconds |
| Counter | `sequent.command.total` | `command.type`, `status`=`ok` \| `error` |
| Counter | `sequent.optimistic_conflict.total` | no extra attributes |
| Counter | `sequent.events.appended` | API exists; **framework does not auto-call on append** |
| Observable gauge | `sequent.outbox.depth` | only if `Config.OutboxDepth != nil` |

Batched `Execute` uses `command.type` = `FirstType+batch`.

Public API: `Instruments`, `Config`, `New`, `StartCommand`, `RecordCommand`, `RecordConflict`, `RecordEvents`.

### Not included

- No bundled OTLP / Prometheus / stdout exporter; does not create providers
- No HTTP/gRPC auto-instrumentation; no projector / workflow / outbox-poller spans
- **`RecordEvents` is not hooked** into `Repository.Commit` / `EventStore.Append` (test-only today; apps may call it at their boundary)
- Not middleware-chain based

---

## 20. Migrations and ops CLI

`store.Migrate` applies `001`–`004`. Partition SQL under `docs/ops/` is example-only.

```bash
go run ./cmd/go-sequent migrate -dsn "$GO_SEQUENT_PG_DSN"
go run ./cmd/go-sequent outbox -dsn "$DSN" [-once]
go run ./cmd/go-sequent replay -dsn "$DSN" -projector name -stage prepare -tables t1
```

Full event replay needs an app-wired `projector/replay.Pipeline`. GDPR: `tombstone` (tombstone-only, Q5). `crypto.PayloadProtector` is optional field encryption — not crypto-shredding.

---

## 21. Wiring: the `sequent` façade

```go
app := sequent.New(sequent.Config{PublisherMode: sequent.SyncInTxn},
    sequent.WithStore(store, tx),
    sequent.WithAuthorizer(...),
    sequent.WithIdempotency(store, false),
    sequent.WithObs(inst),
)
app.RegisterAggregate(...)
app.RegisterHandler(...)
app.RegisterProjector(...)
app.RegisterWorkflow(...)
_ = app.Service.Execute(ctx, cmd)
```

Bank helper: `bank.NewDemoApp` with `StoreFactory` so the store shares the app event registry.

---

## 22. Bank example walkthrough

| Path | Role |
|------|------|
| [`examples/bank/`](../examples/bank/) | **Demo** — in-memory SyncInTxn balances |
| [`examples/bankpg/`](../examples/bankpg/) | **Production-shaped** — Postgres + SQL balances via `projector.Persistor` |

Demo: open / deposit / withdraw / transfer, tenant unique owner, decimal money, AuthZ + idempotency. See `bankpg/README.md` for SQL wiring and gaps (no RLS product, optional Outbox).

```bash
go test ./examples/bank/ ./examples/bankpg/ -count=1
GO_SEQUENT_PG_DSN='postgres://…' go test ./examples/bankpg/ -run TestIT_BankPG
```

---

## 23. Testing guidance

| Level | Approach |
|-------|----------|
| Aggregate unit | `testkit` Given/When/Expect |
| App / IT | memory store + `NewDemoApp` |
| AuthZ / idempotency / tenancy | `TestIT_Memory_*` |
| OCC | stale `ExpectedVersion` |
| Postgres | `GO_SEQUENT_PG_DSN` or skip |

`go test ./...` must pass **without** DSN. Benches: [bench.md](./bench.md).

---

## 24. Ops and security pointers

| Topic | Doc |
|-------|-----|
| Failover / rebuild / backup / partitioning | [ops/](./ops/) |
| Threat model | [security/threat-model.md](./security/threat-model.md) |
| Event evolution / upcasters | [security/event-evolution.md](./security/event-evolution.md) |
| Benchmarks | [bench.md](./bench.md) |

---

## 25. Not implemented / Phase boundaries vs Sequent Ruby

| Sequent Ruby | go-sequent |
|--------------|------------|
| AggregateRoot, Command, Event, CommandService, Repository, EventStore, Projector, Workflow, Snapshot, Migrations | **Implemented** (Go APIs) |
| ActiveRecord Records + migration DSL | **No** AR; `ManagedTables` + `projector/replay` |
| Rails/Sinatra generators / auto-registration | **No** (pure library) |
| AggregateObserver | Not productized |
| ValueObject type system | Plain Go + `types.Money` |
| Default message bus / LISTEN/NOTIFY | Optional outbox **polling** |
| Event hash-chain | **Implemented** (Q3) |
| Event signatures | **Out of scope** (Q3) |
| Import Sequent DB | **Forbidden** (Q9) |
| HTTP/gRPC stubs | **None** (Q7) |

Phases: 1 core loop → 2 snapshots/upcast/workflow/replay skeleton → 3 idempotency/AuthZ/outbox/tenancy/tombstone/obs → 4 docs/benches → bank example + ITs.

---

## 26. Glossary

Definitions match **current go-sequent behavior** and locked decisions (Q1–Q10). Chinese primary: [名词释义](./guide.zh.md#26-名词释义). Stable anchors (`#gloss-…`) are shared across EN/ZH where linked from §1.

<a id="gloss-ledger-projection"></a>

**Ledger projection（账本投影）**  
Read model for balances/ledgers. Q1 requires updates **inside the write transaction** (`SyncInTxn`) so [read-your-writes](#gloss-read-your-writes) holds after commit. Bank `BalanceProjector` is the demo (in-memory; production should persist via SQL).

<a id="gloss-projector"></a>

**Projector / projection**  
A `publish.Handler` (optionally with `ManagedTables`) that updates read models. Registered on `SyncInTxn`, invoked after Append and before commit; failure rolls back the write txn. Rebuild uses `PublishProjectorsOnly` (skips Workflows).

<a id="gloss-aggregate"></a>

**AggregateRoot / aggregate**  
Consistency boundary. Embed `aggregate.Base`; mutate state only in appliers; command methods `Apply` events after validation. Loaded/committed via Repository + UoW; never share across goroutines.

<a id="gloss-command"></a>

**Command**  
Intent to change the system. Has `CommandType`, optional `AggregateID`, `Meta`; may `Validate`. Enters via `CommandService.Execute` (middleware / AuthZ / handler).

<a id="gloss-event"></a>

**Event**  
Past-tense fact with type name and envelope (aggregate id, sequence, time). Stored then drives Projectors/Workflows; decoded via `event.Registry`.

<a id="gloss-eventstore"></a>

**EventStore**  
Persists command records and aggregate streams with OCC, unique-key checks, and a per-aggregate [hash chain](#gloss-hash-chain). Implementations: memory, postgres. Apps usually go through the Repository.

<a id="gloss-hash-chain"></a>

**Hash chain**  
Q3 integrity: each event stores `prev_hash` + `event_hash` (SHA-256 over canonical fields in `eventstore/hashchain`). Chain is **per aggregate stream**; genesis prev is 32 zero bytes. Verified on load / `VerifyChain`. Detects tampering; **not** event signatures. Adds write-path and verify cost.

<a id="gloss-uow"></a>

**UnitOfWork (context-scoped)**  
Per-`Execute` identity map on `context`. `Add`/`Load` reuse one instance; `Commit` collects uncommitted events in load order. Never share across goroutines.

<a id="gloss-occ"></a>

**Optimistic concurrency (OCC)**  
Append requires `ExpectedVersion` = stream tip; else `ErrOptimisticConcurrency`. `obs` can count conflict storms.

<a id="gloss-unique-keys"></a>

**Unique keys**  
`UniqueKeys()` map `scope → value` (e.g. tenant-scoped owner). DB unique violation → `ErrUniqueKeyViolation`.

<a id="gloss-idempotency"></a>

**Idempotency**  
Key space `(TenantID, IdempotencyKey)`. Successful retry is a no-op by default; `Strict` → `ErrReplay`. Stores: memory / postgres.

<a id="gloss-tenant"></a>

**Tenant vs User**  
`Meta.TenantID` is the multi-tenant isolation unit (Q4: shared schema + `tenant_id`). `Meta.UserID` is the actor — **do not** trust client UserID alone; use `AuthContext` + AuthZ. `tenancy` middleware can copy tenant from ctx.

<a id="gloss-outbox"></a>

**Outbox**  
Transactional outbound message rows drained by polling (Q6). Ledger projectors stay on SyncInTxn; outbox is for async/external side effects.

<a id="gloss-aftercommit"></a>

**AfterCommit**  
Hooks via `workflow.RegisterAfterCommit`, run only after a **successful** commit (notifications, non-ledger side effects). Failure does not undo committed events.

<a id="gloss-workflow"></a>

**Workflow**  
Event orchestration handler (`RegisterWorkflow`). May run SyncInTxn but is **excluded** from projection rebuild. May register AfterCommit.

<a id="gloss-snapshot"></a>

**Snapshot**  
JSON fold of aggregate state (Q8) to speed loads: snapshot + later events. `snapshot.Snapshotter` can batch in the background.

<a id="gloss-upcaster"></a>

**Upcaster**  
Transforms old event payloads on load (`event/upcast`). **Not** for importing Ruby Sequent DBs (Q9).

<a id="gloss-syncintxn"></a>

**SyncInTxn**  
Default publish mode: projectors (then workflows) run inside the write txn. Required for ledger projections (Q1).

<a id="gloss-compensating"></a>

**Compensating event**  
A new domain event that offsets/corrects prior facts without rewriting history. Used with tombstones for GDPR-style scrubbing in projections.

<a id="gloss-tombstone"></a>

**Tombstone**  
Q5 strategy: emit markers such as `gdpr.SubjectTombstoned`; projectors scrub or stop serving PII. Product path is **not** crypto-shredding.

<a id="gloss-read-your-writes"></a>

**Read-your-writes**  
After a successful write, the same client’s read should see that update. Ledger SyncInTxn achieves this: balance projection updates in the same txn before commit returns.

<a id="gloss-authz"></a>

**AuthZ**  
Authorization at the command boundary (`security.Authorizer` / `WithAuthorizer`). Denials → `ErrDenied`. Separate from AuthN; hash chain in EventStore; no event signing (Q3).

<a id="gloss-meta"></a>

**Meta (command metadata)**  
`command.Meta`: CommandID, TenantID, UserID, IdempotencyKey, Correlation/Causation, Source, AuthContext, …. `AuditOf` builds an audit snapshot.

<a id="gloss-partition-key"></a>

**partition_key**  
Partition hint on aggregates/streams (often from `tenancy.PartitionKey`), for physical partitioning or unique-key scoping inside a shared schema — not schema-per-tenant.

<a id="gloss-money"></a>

**Money (decimal)**  
`types.Money`: decimal amount + currency; JSON amount is a string. Ban `float64` (Q2).

<a id="gloss-obs"></a>

**obs / OpenTelemetry**  
Optional write-path telemetry (latency, outcome, OCC, optional outbox depth). **Not** the business audit source; app owns Providers. See [§19](#19-opentelemetry-obs).

---

## 27. Locked decisions (Q1–Q10)

| # | Choice |
|---|--------|
| Q1 | [Ledger projections](#gloss-ledger-projection) **SyncInTxn** |
| Q2 | Money = **decimal** (ban float64) |
| Q3 | Integrity = **hash chain** (no signatures) |
| Q4 | Tenancy = **shared schema + tenant_id** |
| Q5 | GDPR = **tombstone only** |
| Q6 | Outbox = **Postgres table polling** |
| Q7 | **Pure library** |
| Q8 | Snapshots = **JSON** |
| Q9 | **Never** import Ruby Sequent DBs |
| Q10 | Project stays **private** |

---

*Matches Phases 1–4 + bank example; source code wins if APIs drift.*
