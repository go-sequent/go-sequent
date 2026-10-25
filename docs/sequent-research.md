# Sequent (Ruby) Research Notes

> Research for go-sequent design. Sources: [sequent.io](https://sequent.io/), GitHub `zilverline/sequent` (zip snapshot of master, Oct 2026).  
> Framework version context: Sequent 8+ schema (partitioned `events` / `commands` / `aggregates`).  
> 中文版：[`sequent-research.zh.md`](./sequent-research.zh.md) · 设计文档：[`design.md`](./design.md) / [`design.zh.md`](./design.zh.md)

## 1. Positioning

Sequent is an **opinionated CQRS + Event Sourcing** framework for Ruby. It focuses on domain logic and is **web-framework agnostic** (Rails, Sinatra, etc.). Persistence is **PostgreSQL-centric**, with heavy use of PL/pgSQL procedures for atomic commit of commands + events + unique keys.

## 2. Basic Flow (Happy Path)

1. Client builds a `Command` (intent).
2. `CommandService.execute_commands(...)` opens a **single transaction**.
3. Per command: **filters** → **validate** → **parse types** → dispatch to matching `CommandHandler`s.
4. Handlers load/create `AggregateRoot`s via `AggregateRepository` (Unit-of-Work + Identity Map).
5. Aggregates `apply` Events (domain logic in methods; state mutation only in `on Event` handlers).
6. Repository `commit` → `EventStore.commit_events` stores command + events, then **publishes** events to `Projector`s / `Workflow`s **in the same transaction** (default: strong consistency of view model).
7. Workflows may enqueue more commands (processed in same txn queue after currently scheduled commands).
8. On success: commit + clear UoW. On failure: full rollback.

## 3. Core Concepts

| Concept | Role |
|--------|------|
| **Command** | Domain API / intent (`CreateUser`). Validated; stored in `commands`. |
| **Event** | Past-tense fact (`UserCreated`). Key = `(aggregate_id, sequence_number)`. |
| **ValueObject** | Nested immutable attribute groups. |
| **AggregateRoot** | Encapsulates invariants; state = fold of event stream. No cross-aggregate reads. |
| **CommandHandler** | Translates command → aggregate method calls. |
| **CommandService** | Single entry: txn, validation, middleware, commit, queue. |
| **AggregateRepository** | UoW + identity map; optimistic concurrency on commit. |
| **EventStore** | Persist/load streams, snapshots, unique keys; publish. |
| **Projector** | Builds read models (`Records`); one Record table owned by one Projector. |
| **Workflow** | Side effects / follow-up commands; **not** run during projector migrations. |
| **Migrations / Replay** | Rebuild view schema via prepare → replay → catchup → optimize → golive. |
| **Snapshotting** | Optional; threshold-based; versioned snapshot format. |
| **UniqueKeys** | Cross-aggregate uniqueness via `aggregate_unique_keys` table. |
| **Secret** | Command-side BCrypt for passwords; cleared on validation failure. |
| **Middleware / Filters** | Cross-cutting on command boundary. |

## 4. Event Store Schema (Sequent 8)

Key tables (partitioned where noted):

- `commands` — successful commands (`command_json`, optional `user_id`, causal `event_aggregate_id` / `event_sequence_number`).
- `aggregates` — stream metadata (`aggregate_type_id`, `events_partition_key`, `snapshot_threshold`).
- `events` — PK `(partition_key, aggregate_id, sequence_number)`; `event_json`; FK to command; `xact_id`.
- `command_types` / `event_types` / `aggregate_types` — type registries (SMALLINT ids).
- `aggregate_unique_keys` — UNIQUE `(scope, key)` for business uniqueness.
- `snapshot_records`, `aggregates_that_need_snapshots`.
- `saved_event_records` — audit of U/D on events (GDPR / ops).

**Optimistic locking**: `store_events` procedure checks consecutive `sequence_number` under `FOR NO KEY UPDATE` on aggregate; PK prevents duplicates → concurrent writers → integrity / OptimisticLockingError.

**Publish order**: Events published by **load order of aggregates** in the UoW, then sequence within each aggregate (not by apply-call interleaving across aggregates).

## 5. Projectors & Migrations

- View tables live in a **view schema**; Events are source of truth.
- Projectors use abstract Persistor API so **replay** can use bulk-optimized path.
- Replay pipeline (Sequent 8): `prepare` (replay schema) → `replay` → `catchup` → `optimize` → `golive` (swap into view, old → archive).
- Constraints: no `belongs_to` / FK across aggregates in view schema; only per-aggregate event order guaranteed.

## 6. GDPR Notes (Sequent)

No built-in erasure. Strategies discussed: **tombstoning**, selective **event deletion** (with care for stream integrity), scrubbing via tombstone propagation. `saved_event_records` supports tracking mutations.

## 7. Implications for Go Port

- Prefer **interfaces + composition** over Ruby inheritance/`on` DSL; codegen or registry for message routing is fine.
- Keep **PostgreSQL + stored procedures** (or equivalent SQL) for atomic commit semantics first; abstract `EventStore` interface for future backends.
- Default **sync projectors in write txn** matches Sequent; finance may need **async outbox** option for HA/latency tradeoffs — design as pluggable publisher.
- Thread-local UoW → Go: **request/context-scoped** UoW (`context.Context` + explicit `UnitOfWork`), never goroutine-global mutable maps.
- Money/decimal, authz, multi-tenancy, signed events: **beyond** Sequent; first-class in go-sequent for finance target.
