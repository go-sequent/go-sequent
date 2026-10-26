# Runbook — projection rebuild

Mirrors Sequent 8: **prepare → replay → catchup → optimize → golive** (optional **abort**).

Ledger / balance projectors that must stay strongly consistent should remain **SyncInTxn** on the live path. Rebuild is for disposable read models.

## Library API

```go
pipe := &replay.Pipeline{
 Name: "balances",
 Source: eventSource, // StreamAll
 Publisher: syncInTxnWithProjectorsOnly,
 Schema: &replay.PostgresSchema{Pool: pool}, // or NoopSchema{}
 State: replay.NewMemoryState, // or DB-backed in your app
 ManagedTables: []string{"account_balances"},
}
err := pipe.RunAll(ctx)
```

`PublishProjectorsOnly` excludes workflows (Sequent parity).

## CLI (schema stages)

```bash
go run./cmd/go-sequent migrate -dsn "$GO_SEQUENT_PG_DSN"
go run./cmd/go-sequent replay -dsn "$DSN" -projector balances \
 -stage prepare -tables account_balances
# optimize | golive | abort likewise
```

Full event streaming replay must be wired in your application binary (CLI cannot know your projector registrations).

## Stage notes

| Stage | Action |
|-------|--------|
| prepare | Create `replay_*` schema; `CREATE TABLE … (LIKE …)` managed tables; truncate |
| replay | Stream historical events into replay projectors / tables |
| catchup | Apply events since high-water mark while live continues |
| optimize | `ANALYZE` / add indexes as needed |
| golive | Brief swap: move live tables aside, promote replay tables into view schema |
| abort | `DROP SCHEMA replay_* CASCADE` |

## Operational tips

- Size batches (`BatchSize`) and workers for disjoint aggregate ranges.
- During golive, pause or briefly serialize writers if your swap is not lock-free.
- After golive, keep SyncInTxn projectors as the source of truth for balances.
- Track progress via `projector_replay_states` (Phase 2 schema) or your StateStore.
