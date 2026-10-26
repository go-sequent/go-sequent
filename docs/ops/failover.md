# Runbook — PostgreSQL failover

## Goals

- Single-writer primary for the event store (no silent split-brain).
- RPO ≈ 0 when using synchronous commit / quorum as an ops choice.
- Projection **reads** may use replicas only when async lag is acceptable.

## Architecture notes

| Path | Recommendation |
|------|----------------|
| Command + SyncInTxn projectors | Always hit the **primary** (read-your-write for balances). |
| Async outbox consumers | May read primary or lag-tolerant replica for non-ledger views. |
| Replay / rebuild workers | Prefer primary or a dedicated rebuild role with consistent snapshot. |

Use Patroni, Cloud SQL HA, RDS Multi-AZ, or equivalent. Put PgBouncer (or cloud pooler) in front for connection storms.

## Failover checklist

1. **Detect** primary loss (Patroni / cloud health / missing heartbeats).
2. **Promote** a synchronous standby; confirm `pg_is_in_recovery = false` on new primary.
3. **Repoint** pooler / DSN (or update service discovery). Rolling restart app pods if DSN is sticky.
4. **Verify** writes: `go-sequent` append + one SyncInTxn projector update.
5. **Verify** no dual writers: old primary must not accept writes (fencing / STONITH).
6. **Rebuild standbys** from new primary; monitor replication lag.

## Application guidance

- Keep DB transactions short (one `CommandService.Execute` batch).
- On `ErrOptimisticConcurrency`, retry with backoff — common during failover catch-up storms.
- Do not share `UnitOfWork` across goroutines (design §7).
- Health checks should fail open to reads only if SyncInTxn path is not required for that probe.

## What go-sequent does *not* do

- No multi-master event store / CRDT merge.
- No automatic DSN failover inside the library — rely on pooler / platform.
