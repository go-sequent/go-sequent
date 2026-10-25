# go-sequent 设计文档 / Design Document

**Status:** Design only (no framework implementation in this phase)  
**Module path:** `github.com/wenweih/go-sequent`  
**Inspired by:** [Sequent](https://sequent.io/) ([zilverline/sequent](https://github.com/zilverline/sequent))  
**Target:** Financial systems — performance, HA, security, auditability  
**Companion research:** [`docs/sequent-research.md`](./sequent-research.md) · 中文版：[`docs/design.zh.md`](./design.zh.md)、[`docs/sequent-research.zh.md`](./sequent-research.zh.md)

---

## 目录 / Table of Contents

1. [Goals & Non-Goals](#1-goals--non-goals)
2. [Sequent → Go Mapping](#2-sequent--go-mapping)
3. [Core Domain Model](#3-core-domain-model)
4. [Package / Module Layout](#4-package--module-layout)
5. [Persistence](#5-persistence)
6. [Financial-System Concerns](#6-financial-system-concerns)
7. [Concurrency & Performance](#7-concurrency--performance)
8. [Security Model](#8-security-model)
9. [Phased Roadmap](#9-phased-roadmap)
10. [Decisions (locked)](#10-decisions-locked)

---

## 1. Goals & Non-Goals

### 1.1 Goals（目标）

| Area | Goal |
|------|------|
| **CQRS + ES fidelity** | Provide a Go-idiomatic equivalent of Sequent’s core loop: Command → Handler → Aggregate → EventStore → Projector/Workflow, with strong transactional defaults. |
| **Finance-grade audit** | Immutable append-only event log; command↔event causality; integrity via DB controls (no hash chain / event signatures); retention & replay for regulators; GDPR via tombstones. |
| **Correctness under concurrency** | Optimistic concurrency per aggregate stream; clear conflict errors; idempotent command admission. |
| **Performance** | Efficient load (batch + snapshots), batch append, projection rebuild with parallelism; predictable latency under load. |
| **HA** | PostgreSQL primary/replica patterns documented; read-scale projections; failover notes; no silent split-brain writes. |
| **Security** | Authz hooks at command boundary; PII/encryption hooks; secrets never logged; multi-tenant isolation hooks. |
| **Observability** | Optional OpenTelemetry (`obs`) on the write path: command latency/outcome, OCC conflicts, optional outbox depth — ops/SLO signals, **not** the audit source of truth; finer projector/rebuild spans remain aspirational beyond current `WithObs` hooks. |
| **Developer UX** | Explicit interfaces, minimal magic, test helpers (given events / when command / expect events). |

### 1.2 Non-Goals（非目标）— v1 / near-term

- Not a full application framework (no HTTP/gRPC server or stubs in-repo, no UI, no ORM for general CRUD) — **pure library** (Q7).
- Not a multi-database polyglot event store in MVP (PostgreSQL first; interface allows later backends).
- Not automatic GDPR “right to be forgotten” productization beyond **tombstone-only** strategy hooks (Q5); no crypto-shredding productization.
- Not a distributed multi-master event store / CRDT merge engine.
- Not embedding a full workflow engine (Temporal/Cadence); Workflows stay in-process + optional outbox/job hooks (outbox transport = Postgres polling when used — Q6).
- **Never** import / migrate Ruby Sequent databases (Q9); conceptual inspiration only, not dump/wire compatibility.
- Not open-source / not public for now (Q10); license remains private/unpublished.

### 1.3 Design Principles

1. **Events are the source of truth**; projections are disposable.
2. **Aggregates never read other aggregates’ state** during command handling (enforce by API culture + lint guidance).
3. **Default = single DB transaction** for command + events + sync projectors (Sequent parity); async modes are explicit opt-in.
4. **Finance defaults on**: **decimal** money type (not integer minor units — Q2), idempotency keys, audit metadata on every command.
5. **Boring Go**: interfaces, constructors, `context.Context`, no global mutable singleton required (config via DI).

---

## 2. Sequent → Go Mapping

| Sequent (Ruby) | go-sequent (Go idiom) |
|----------------|------------------------|
| `Sequent::Command` + `attrs` + ActiveModel validations | `Command` interface + struct tags / `Validate() error` / optional `go-playground/validator` |
| `Sequent::Event` + `attrs` | `Event` interface; concrete structs registered by type name |
| `Sequent::ValueObject` | Immutable value structs; optional `Validate()` |
| `Sequent::AggregateRoot` + `on Event do` | Embed `aggregate.Root`; register appliers `On(Type, func)` or generated switch; domain methods call `Apply` |
| `CommandHandler` + `on Command do` | `CommandHandler` interface; router by `reflect.Type` or string type registry |
| `CommandService` + Thread-local queue | `command.Service` with **context-scoped** command queue + UoW |
| `AggregateRepository` (thread UoW / identity map) | `repository.Repository` bound to `UnitOfWork` in `context` |
| `EventStore` + PL/pgSQL `store_events` | `eventstore.Store` interface; `postgres` impl with equivalent SQL/procedures |
| `Projector` + Persistor abstraction | `projector.Projector` + `Persistor` interface (`SQL` vs `ReplayBulk`) |
| `Workflow` + `after_commit` | `workflow.Workflow`; `AfterCommit` hooks via txn callback / outbox |
| `Sequent.configure` globals | `sequent.Config` + `sequent.App` constructed explicitly |
| Command middleware / filters | `Middleware` `func(ctx, cmd, next) error` chain |
| `Sequent::Secret` (BCrypt) | Optional `types.Secret`; hash in middleware after validation — **prefer app-owned crypto** |
| Snapshot via Marshal/Base64 | Versioned snapshot codec (`json`/`gob`/custom); **never** opaque Marshal without version |
| Unique keys | `UniqueKeyProvider` on aggregates → `aggregate_unique_keys` |
| Migrations / projector replay | `migrate` / `replay` CLI packages mirroring prepare→replay→catchup→golive |
| Autoregistration of handlers | Explicit `Register*` in `main` / `wire` (Go-friendly); optional codegen |

### Concurrency model difference (critical)

Ruby Sequent uses **per-thread** UoW. Go must use **per-request / per-command-batch** UoW stored in `context.Context` (or passed explicitly). Sharing one UoW across goroutines is **forbidden**. Parallelism is at: (a) independent command requests, (b) projection rebuild workers, (c) async consumers — not inside a single aggregate apply.

---

## 3. Core Domain Model

### 3.1 Command

```text
type Command interface {
    CommandType() string      // stable name, e.g. "account.OpenAccount"
    AggregateID() (uuid.UUID, bool) // optional primary aggregate
    Meta() CommandMeta        // user, tenant, idempotency, trace
}
```

**CommandMeta (finance-oriented):**

- `CommandID` (uuid) — unique id for storage / idempotency correlation  
- `IdempotencyKey` (string, optional) — client-supplied  
- `UserID`, `TenantID`  
- `CausedByEvent` — `(aggregate_id, sequence_number)` when workflow-triggered  
- `IssuedAt`  
- `AuthContext` opaque token / claims ref (not necessarily persisted)

Commands are **intent**, validated before handlers run. Successful commands persist like Sequent’s `commands` table.

### 3.2 Event

```text
type Event interface {
    EventType() string
    AggregateID() uuid.UUID
    SequenceNumber() int64
    CreatedAt() time.Time
}
```

Payload structs hold domain fields. Envelope metadata (type, ids, seq, created_at, command_id) lives in store columns; JSON body omits redundant envelope fields (Sequent 8 style).

**Rules:** past tense names; small & flat; renaming requires data migration of type registry + payloads.

### 3.3 AggregateRoot

```text
type AggregateRoot interface {
    ID() uuid.UUID
    AggregateType() string
    UncommittedEvents() []Event
    ClearUncommitted()
    Version() int64              // last committed sequence
    Apply(Event)                 // only via Apply helper internally
    Invariants() error           // Sequent invariants!
    UniqueKeys() map[string]any  // optional
    PartitionKey() string        // optional multi-tenant / shard key
}
```

**Behavioral contract (Sequent parity):**

1. Public methods: guard + decide → `Apply(newEvent)`.
2. State changes **only** in event appliers (so replay == live).
3. No I/O inside aggregates.
4. `Invariants()` checked after load and before commit.

**Implementation sketch:** embed `aggregate.Base` providing Apply, seq allocation, uncommitted buffer; user type registers appliers.

### 3.4 AggregateRepository

- `Add(ctx, agg)`
- `Load(ctx, id, typ)` / `LoadMany`
- `LoadUntil(ctx, id, at)` — ignore snapshots (for nested/time-travel/snapshotting jobs)
- `Exists` / `EnsureExists`
- `Commit(ctx, cmd)` — collect uncommitted from UoW → EventStore → clear

**UnitOfWork + IdentityMap:** one aggregate instance per id per UoW. `Commit` publishes events in **aggregate registration/load order**, then sequence order (match Sequent).

### 3.5 CommandService / Handler

```text
type Handler interface {
    Handles() []string // command types
    Handle(ctx context.Context, cmd Command) error
}

type Service struct {
    Middleware []Middleware
    Filters    []Filter
    Router     *Router
    Repo       *repository.Repository
    Store      eventstore.Store
    Tx         TxManager
}
```

**Execute flow:**

1. Push commands to queue (support workflow enqueue).
2. Begin transaction.
3. While queue non-empty: middleware → filters → validate → handle → `Repo.Commit` (store + publish).
4. Commit txn / run AfterCommit hooks.
5. Always clear UoW.

### 3.6 EventStore

Interface responsibilities:

- `Append(ctx, cmd, streamsWithEvents)` — atomic with optimistic concurrency + unique keys  
- `LoadStream(ctx, id)` / `LoadStreams` — from latest compatible snapshot  
- `StreamEvents(ctx, id, opts)` — cursor batches  
- `RegisterTypes`  
- `Snapshot` APIs  

PostgreSQL implementation should mirror Sequent’s `store_events` procedure semantics (see §5).

### 3.7 Projectors & Workflows

**Projector:**

- Declares managed tables.
- Handles events via registry.
- Uses `Persistor` only (no raw ad-hoc ORM that breaks replay).
- Sync mode: same txn as append (default).
- Replay mode: bulk persistor + isolated replay schema.

**Workflow:**

- Reacts to events for process orchestration (send command, enqueue job).
- Excluded from projector rebuild (Sequent parity).
- Side effects that cannot roll back → `AfterCommit` or **transactional outbox**.

### 3.8 Serializers

- Default: JSON (aligned with Sequent `event_json` / `command_json`).
- Pluggable `Codec` per type; upcasters for payload evolution.
- Snapshot format = **JSON** (Q8), versioned independently (`snapshot_version`).

### 3.9 Migrations

Two tracks:

1. **Event store schema migrations** — versioned SQL (golang-migrate / goose).
2. **Projection rebuild** — Sequent 8 pipeline: prepare → replay → catchup → optimize → golive; CLI in `cmd/go-sequent`.

---

## 4. Package / Module Layout

```text
github.com/wenweih/go-sequent
├── go.mod
├── README.md
├── docs/
│   ├── design.md                 # this document
│   └── sequent-research.md
├── cmd/
│   └── go-sequent/               # CLI: migrate, replay, snapshot
├── sequent/                      # façade: NewApp, Config
├── command/
│   ├── command.go                # Command, Meta, validation
│   ├── service.go
│   ├── middleware.go
│   └── router.go
├── event/
│   ├── event.go
│   └── registry.go               # type name ↔ factory
├── aggregate/
│   ├── root.go                   # Base + Apply
│   └── repository.go
├── eventstore/
│   ├── store.go                  # interface
│   ├── errors.go                 # OptimisticConcurrency, UniqueKey
│   └── postgres/
│       ├── store.go
│       ├── schema/               # SQL + procedures
│       └── snapshot.go
├── projector/
│   ├── projector.go
│   ├── persistor.go
│   └── replay/
├── workflow/
│   └── workflow.go
├── publish/
│   ├── publisher.go              # sync in-txn publisher
│   └── outbox/                   # optional async; Postgres polling when used
├── types/                        # Money (decimal), Secret hooks
├── security/                     # Authz interfaces (no event signing — Q3)
├── tenancy/                      # shared schema + tenant_id helpers
├── obs/                          # OpenTelemetry helpers
├── migrate/
└── testkit/                      # Given/When/Expect helpers
```

**Dependency direction:** `command` → `aggregate` → `eventstore`; `projector`/`workflow` ← `publish`; apps import `sequent` façade + their domain packages. Domain code should **not** import `eventstore/postgres`.

---

## 5. Persistence

### 5.1 Recommended schema (Sequent 8–inspired, finance extensions)

Core tables (PostgreSQL):

| Table | Purpose |
|-------|---------|
| `commands` | Append-only successful commands + meta |
| `aggregates` | Stream metadata + partition key |
| `events` | Append-only events; PK `(partition_key, aggregate_id, sequence_number)` |
| `*_types` | Type registries |
| `aggregate_unique_keys` | Business uniqueness |
| `snapshot_records` / `aggregates_that_need_snapshots` | Snapshots |
| `idempotency_keys` (**new**) | `(tenant_id, key)` → `command_id` / result status |
| `outbox` (**optional**) | Transactional outbox for async publish; drained by **Postgres polling** (Q6) |
| `audit_access_log` (**optional**) | Who read sensitive aggregates (compliance) |

**Suggested finance columns on `commands` / event envelope:**

- `tenant_id uuid` (shared-schema multi-tenancy — Q4)
- `idempotency_key text` (nullable; unique with tenant)
- `correlation_id` / `causation_id`

No `payload_hash` / `event_hash` / signature columns — event integrity is **DB controls only** (Q3).

### 5.2 Optimistic concurrency

On append (same idea as Sequent `store_events`):

1. Lock aggregate row (`SELECT … FOR NO KEY UPDATE`).
2. Read last `sequence_number`.
3. Require first new event seq == last+1 (or 1 if empty).
4. Insert events; PK collision ⇒ conflict.
5. Update unique keys.

Map DB integrity failures → `ErrOptimisticConcurrency` / `ErrUniqueKeyViolation`.

### 5.3 Snapshots

- Threshold per aggregate type; background job takes snapshots.
- Snapshot row: `(aggregate_id, sequence_number, snapshot_type, snapshot_json, version)`.
- Load path: latest compatible snapshot + subsequent events.
- On aggregate schema change: bump `snapshot_version` or delete snapshots (Sequent guidance).

### 5.4 Projection rebuild

Mirror Sequent 8:

1. **prepare** — create `replay_schema`, copy managed table DDLs (indexes optional).  
2. **replay** — parallel workers by aggregate id ranges / event batches; bulk insert.  
3. **catchup** — incremental while live system continues.  
4. **optimize** — indexes, `VACUUM`/`ANALYZE` as applicable.  
5. **golive** — brief lock / swap tables into `view_schema`; archive old.  
6. **abort** — drop replay schema.

**Consistency modes:**

| Mode | Behavior | Use |
|------|----------|-----|
| `SyncInTxn` (default, **Q1 locked**) | Projectors update in write txn | Strong consistency; **required for ledger balances** |
| `OutboxAsync` | Write events + outbox row; workers project via Postgres polling (Q6) | Non-ledger / external integration paths |
| `Hybrid` | Critical projectors sync; others async | Ledgers sync; search/notifications async |

### 5.5 Partitioning & HA notes

- Partition `events` by `partition_key` (tenant, date, or hash) like Sequent.
- Primary handles writes; replicas serve **projection reads** only if async lag acceptable — for sync mode, reads usually hit primary or wait for commit visibility.
- Failover: single-writer primary; document RPO≈0 with sync commit / quorum as ops choice.
- Backups: continuous WAL + periodic verified restore drills (event store is crown jewel).

---

## 6. Financial-System Concerns

### 6.1 Immutability & audit trail

- No update/delete API on events in normal operation.
- Compensating events for business corrections (`PaymentReversed`), not silent edits.
- Command + event linkage for forensic replay.
- Event integrity = **DB controls only** (ACLs, backups, WORM where applicable) — **no** hash chain and **no** event signatures (Q3, locked 2026-10-26).

### 6.2 Idempotency

- Clients send `Idempotency-Key` (per tenant).
- Before handler: if key seen → return prior outcome / no-op.
- Store key in same txn as command success to avoid dual execution on retries.
- Handlers should still be written defensively (natural keys / guards).

### 6.3 Money / decimal boundaries

- **Framework does not invent accounting rules.**
- Provide `types.Money` using **decimal** amounts (Q2) — e.g. `shopspring/decimal` / `cockroachdb/apd` — with currency code + fixed scale policy.
- **Not** integer minor-units as the framework money representation.
- JSON marshal rules documented for decimal.
- **Forbid `float64` in Command/Event money payloads** via lint / testkit check.
- Rounding policy is domain-owned; document “store exact event amounts, never recompute historical money from floats”.

### 6.4 Encryption / PII at rest

- Hooks: `PayloadProtector` interface — encrypt selected JSON fields before insert; decrypt on load.
- Prefer envelope encryption (DEK/KEK) with KMS; key rotation strategy documented.
- Application-level field encryption > relying only on disk encryption for PII in events.
- GDPR / retention: **tombstone only** (Q5) — apply tombstone events and scrub/stop processing identifiable data; **not** crypto-shredding as the product strategy; physical delete only as exceptional ops with stream-repair tooling.

### 6.5 Multi-tenancy hooks

- Model = **shared schema + `tenant_id`** (Q4).
- `TenantID` on command meta; `partition_key` may still use tenant (or tenant+hash) for physical partitioning within the shared schema.
- Repository/store filters enforce tenant match on load (defense in depth).
- Unique key scopes should include tenant (`scope = "tenant:{id}:email"`).
- Projection tables carry `tenant_id`; RLS optional at DB layer.

### 6.6 Observability & SLA / HA

**Role of OTel in-framework:** vendor-neutral hooks (`obs` + `sequent.WithObs`) so operators can see write-path health without choosing an APM inside the library. Complementary to EventStore / command audit — **not** a business audit trail (Q3 integrity stays DB-only; Q7 keeps exporters in the host app). See [guide §19 Role and purpose](./guide.md#role-and-purpose).

**Implemented today (`obs`):** `sequent.Execute` span; metrics `sequent.command.duration_ms`, `sequent.command.total`, `sequent.optimistic_conflict.total`; optional `sequent.outbox.depth`. `RecordEvents` API exists but is not auto-hooked on append.

**Metrics (design target):** command latency, validation failures, optimistic conflicts, events appended/sec, projector lag (async), rebuild progress, outbox depth.

**Tracing (design target):** span per `Execute` (done); child per handler, store append, each projector (not yet).

**Logging:** structured; never log secrets/PII payloads by default (redactor).

**SLA-oriented HA:**

- Write path: HA Postgres (Patroni/CloudSQL/RDS) + connection pooling (PgBouncer).
- Read path: projection replicas; define max lag SLO.
- Rebuild without full downtime via catchup + short golive window.
- Chaos: simulate conflict storms; verify idempotent clients.

---

## 7. Concurrency & Performance (Go)

### 7.1 Goroutine boundaries

| Safe to parallelize | Must stay serial |
|---------------------|------------------|
| Independent HTTP/gRPC requests each with own UoW | Event apply within one aggregate |
| Projection rebuild workers (disjoint aggregates) | Sync projectors for one commit (deterministic order) |
| Async outbox consumers (with per-aggregate ordering if required) | Unique key updates for same scope in conflicting txns (DB serializes) |

**Never** share `UnitOfWork` / aggregate instances across goroutines.

### 7.2 Batching

- `LoadMany` aggregates in one SQL round-trip (Sequent `load_events` multi-id).
- Append multiple aggregates’ events in one `store_events` call (one command causing fan-out).
- Replay: batch size + worker count configurable (Sequent: events/txn ≈ 100k, workers ≈ 8 as starting point — tune per HW).

### 7.3 Outbox (when needed)

Use when:

- Projector I/O too heavy for write SLO, or  
- External systems must see events after commit, or  
- Cross-service integration.

Pattern: insert `outbox` row in same txn as events; publisher workers deliver with at-least-once via **Postgres table polling** (Q6) — not LISTEN/NOTIFY or Kafka as the framework default; consumers idempotent. Ledger / balance projectors stay on **`SyncInTxn`** (Q1) so read-your-write holds for balances.

### 7.4 Performance levers

- Snapshots for hot, long-lived aggregates (accounts with huge streams).
- Partition pruning by tenant.
- Prepared statements / pgx pool.
- Avoid loading unbounded streams into memory — streaming rebuild APIs.
- Optional read-through cache of snapshots (invalidate on conflict version).

---

## 8. Security Model

### 8.1 AuthZ at command boundary

```text
type Authorizer interface {
    Authorize(ctx context.Context, cmd Command) error
}
```

Installed as Filter/Middleware **after** authentication context is attached, **before** handler. Deny by default for sensitive command types in finance apps.

Aggregate-level checks (e.g. account ownership) stay in domain guards using only aggregate state + command claims — not ad-hoc DB queries.

### 8.2 Event integrity (no signing)

- **Out of scope:** framework event signing / hash chains (Q3).
- Rely on PostgreSQL access control, backups, and optional WORM / immutable storage at the ops layer.
- Apps may still encrypt PII fields via `PayloadProtector` without implying event authenticity signatures.

### 8.3 Threat notes

| Threat | Mitigation |
|--------|------------|
| Replay attack on APIs | Idempotency keys + authn tokens / mTLS |
| Privilege escalation via forged commands | AuthZ middleware; never trust client-set `UserID` without verified claims |
| Event store tampering | DB controls, immutable storage / WORM backups (no in-framework signatures/hash chain) |
| PII leakage in logs/traces | Redaction; encrypted payloads; tombstone strategy for erasure requests |
| Projector SQL injection | Parameterized persistor only |
| Confused deputy across tenants | Tenant enforcement on load/append (`tenant_id`) |
| Workflow double-exec after crash | Idempotent commands + outbox dedupe (Postgres polling) |
| Snapshot poison | Versioned JSON snapshots; regenerate from events if suspect |

### 8.4 Secrets

- Prefer app-level password hashing; framework `Secret` type optional.
- No secrets in events if avoidable (store password hash as event field only if required; never plaintext).

---

## 9. Phased Implementation Roadmap

### Phase 0 — Foundations (design locked)

- This document + research notes.
- Q1–Q10 resolved by wei on **2026-10-26** (Asia/Shanghai) — see §10.

### Phase 1 — MVP (pure library; sample bank/ledger demo app stays outside this repo)

- `Command` / `Event` / `Aggregate` base + registry.
- `CommandService` + middleware + in-memory or Postgres EventStore **minimal** (events + commands + optimistic concurrency).
- **Sync** in-process publisher + one sample Projector (ledger balances sync — Q1).
- `types.Money` as **decimal** (Q2).
- `testkit` Given/When/Expect.
- Docs: getting started. No gRPC/HTTP stubs in-repo (Q7).

### Phase 2 — Sequent-parity core

- **JSON** snapshots + background snapshotter (Q8).
- Unique keys.
- Workflows + AfterCommit.
- PostgreSQL procedures parity (`store_events`).
- Projection replay CLI (prepare/replay/catchup/golive).
- Type registries & upcasters v0.

### Phase 3 — Finance-ready

- Idempotency keys (tenant-scoped; shared schema + `tenant_id` — Q4).
- AuthZ hooks + audit metadata standards.
- Money/decimal lint + `types.Money`.
- Payload encryption hooks; **tombstone-only** GDPR guide (Q5) — no crypto-shredding product path.
- Optional outbox publisher mode with **Postgres polling** (Q6); ledger projectors remain sync.
- OpenTelemetry metrics/traces.
- Multi-tenancy helpers for shared schema + `tenant_id`.
- No event signing / hash-chain feature (Q3).

### Phase 4 — Production hardening

- Load tests & conflict benchmarks.
- Operational runbooks (failover, rebuild, backup restore).
- Schema partitioning guidance at scale (within shared schema).
- Security review / threat model refresh.
- Compatibility policy for event evolution (still **no** Ruby Sequent DB import — Q9).
- Keep project private / unpublished (Q10).

---

## 10. Decisions (locked)

Confirmed by wei on **2026-10-26** (Asia/Shanghai). No new open questions in this revision.

### 10.1 Baseline decisions (D1–D10) + Q-resolutions (D11–D20)

| ID | Decision |
|----|----------|
| D1 | Module path = `github.com/wenweih/go-sequent`. |
| D2 | PostgreSQL is the first (and MVP-only) EventStore backend. |
| D3 | Default publish mode = **sync projectors in the write transaction** (Sequent parity; affirms Q1). |
| D4 | UoW is **context-scoped**, not process-global. |
| D5 | JSON is the default codec for commands/events. |
| D6 | Compensating events over mutable history for business corrections. |
| D7 | `float64` banned in money fields; money type = **decimal** (not minor-units; affirms Q2). |
| D8 | Explicit handler registration (no mandatory reflection autoregister magic). |
| D9 | Design docs precede implementation; framework code not started in the design phase. |
| D10 | Conceptual Sequent inspiration only — **never** Ruby Sequent DB import (affirms Q9). |
| D11 | **Q1** Ledger / balance projections = **sync** (`SyncInTxn`). |
| D12 | **Q2** Money representation = **decimal**. |
| D13 | **Q3** Event integrity = **DB controls only** (no hash chain, no event signatures). |
| D14 | **Q4** Multi-tenancy = **shared schema + `tenant_id`**. |
| D15 | **Q5** GDPR / retention = **tombstone only** (not crypto-shredding). |
| D16 | **Q6** Outbox transport (when used) = **Postgres table polling**. |
| D17 | **Q7** Delivery shape = **pure library** (no gRPC/HTTP stubs in-repo). |
| D18 | **Q8** Snapshot format = **JSON**. |
| D19 | **Q9** Ruby Sequent DB import compatibility = **never**. |
| D20 | **Q10** Project stays **private / not open-source** for now; license unpublished. |

### 10.2 Q1–Q10 resolution log（已决）

| ID | Question | Status | Decision |
|----|----------|--------|----------|
| Q1 | Async vs sync projections for ledger balances? | **已决** 2026-10-26 | **同步** — sync projectors / sync ledger balance projections |
| Q2 | Money representation? | **已决** 2026-10-26 | **decimal**（非整数最小单位） |
| Q3 | Event integrity: hash chain / signatures / DB only? | **已决** 2026-10-26 | **仅 DB 管控** — no hash chain / no event signatures |
| Q4 | Multi-tenancy model? | **已决** 2026-10-26 | **共享 schema + `tenant_id`** |
| Q5 | GDPR / retention strategy? | **已决** 2026-10-26 | **仅墓碑**（tombstone only; not crypto-shredding） |
| Q6 | Outbox transport? | **已决** 2026-10-26 | **Postgres 轮询** outbox 表（when outbox is used） |
| Q7 | gRPC/HTTP stubs in-repo vs pure library? | **已决** 2026-10-26 | **不要** — pure library |
| Q8 | Snapshot format? | **已决** 2026-10-26 | **JSON** |
| Q9 | Import Ruby Sequent databases? | **已决** 2026-10-26 | **永不** |
| Q10 | License / public roadmap? | **已决** 2026-10-26 | **不公开** — private/unpublished |

---

## Appendix A — Command lifecycle (sequence)

```text
Client
  → CommandService.Execute(ctx, cmds...)
      → Tx.Begin
      → for cmd in queue:
          → Middleware / AuthZ / Idempotency
          → Validate
          → CommandHandler.Handle
              → Repository.Load / Add
              → aggregate.Method → Apply(events)
          → Repository.Commit
              → EventStore.Append (optimistic + unique keys)
              → Publisher.Publish → Projectors (+ Workflows enqueue cmds)
      → Tx.Commit
      → AfterCommit / Outbox drain signal
```

## Appendix B — Minimal Go API sketch (illustrative, not implemented)

```go
cfg := sequent.Config{PublisherMode: sequent.SyncInTxn}
app := sequent.New(cfg, sequent.WithPostgres(dsn))

app.Commands.Register(&OpenAccountHandler{})
app.Projectors.Register(&AccountProjector{})
app.Workflows.Register(&NotifyWorkflow{})

err := app.CommandService.Execute(ctx,
    &OpenAccount{
        Meta: sequent.MetaFrom(ctx),
        AccountID: id,
        Currency:  "CNY",
    },
)
```

## Appendix C — Research provenance

- Site: https://sequent.io/ (Reference Guide: AggregateRoot, Command, Event, CommandService, AggregateRepository, EventStore, Projector, Workflow, Snapshotting, Migrations, GDPR).
- Source snapshot: `zilverline/sequent` master zip (Oct 2026), notably:
  - `db/sequent_schema_tables.sql`, `sequent_schema_indexes.sql`
  - `db/migrate/sequent/store_events_v05.sql`
  - `lib/sequent/core/{command_service,event_store,aggregate_root,aggregate_repository,projector,workflow}.rb`

---

*End of design document. Q1–Q10 locked 2026-10-26 (Asia/Shanghai). Implementation may proceed per the phased roadmap; this revision is documentation-only.*
