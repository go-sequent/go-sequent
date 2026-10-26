# Event evolution compatibility policy

## Principles

1. **Events are immutable facts.** Prefer new event types or upcasters over rewriting history.
2. **JSON payloads** are the default codec (D5). Envelope fields live in store columns; body should omit redundant envelope keys when practical.
3. **Past-tense, small, flat** events; renaming a type requires registry + upcaster (or data migration).
4. **Never import / migrate Ruby Sequent databases**. Conceptual inspiration only — no dump/wire compatibility.

## Upcaster v0 rules (`event/upcast`)

| Rule | Detail |
|------|--------|
| Register by stored `FromType` | One upcaster per stored type name |
| Chain until stable | Max hops default 8; avoid cycles |
| Target current type | Final type must be registered on `event.Registry` |
| Pure transform | No I/O; deterministic JSON in → JSON out |
| Wire on load | `store.SetUpcasters(chain)` before `LoadStream` |

Example: rename field `amt` → `amount` and type `bank.MoneyDeposited.v0` → `bank.MoneyDeposited`.

## Versioning strategies

1. **Additive fields** — optional JSON keys with defaults in appliers; no upcaster required.
2. **Rename / reshape** — upcaster v0 from old type name (or keep type name and transform payload only).
3. **Semantic break** — new event type (`PaymentReversed` compensating style); leave old events as-is.
4. **Snapshots ** — bump `SnapshotVersion` on incompatible aggregate state shape; delete or ignore old snapshots.

## Compatibility matrix (summary)

| Change | Events | Snapshots | Projectors |
|--------|--------|-----------|------------|
| Add optional field | OK | Usually OK | Handle missing |
| Rename field | Upcaster | Bump version | Upcaster or dual-read |
| Rename event type | Upcaster + registry | Rebuild from events | Handle both until catch-up |
| Remove field | Keep in old events; ignore in applier | Bump if required | Stop projecting field |
| Money representation | Must stay decimal strings ; never float64 | Same | Same |

## Forbidden

- Silent UPDATE/DELETE of event rows for business corrections (use compensating events).
- Event signatures as framework features. Hash chain is implemented.
- Crypto-shredding as the GDPR product path (tombstone only).
- Loading Sequent Ruby SQL dumps into go-sequent schemas.

## Test expectations

- Add upcaster unit tests for each production transform.
- Replay a fixture stream through `LoadStream` after registering upcasters.
- Rebuild projectors after type changes (`docs/ops/projection-rebuild.md`).
