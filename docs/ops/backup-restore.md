# Runbook — event store backup & restore

The PostgreSQL event store is the **crown jewel**. Projections are disposable; events (+ commands) are not.

## What to back up

| Object | Priority |
|--------|----------|
| `commands`, `events`, `aggregates`, `*_types` | **Critical** |
| `aggregate_unique_keys`, `idempotency_keys` | Critical for correctness |
| `snapshot_records` | High (rebuildable from events, speeds load) |
| `outbox` | High if async integrations lag |
| Projection / view tables | Low — rebuild via replay |
| `event_tombstones` | Medium (GDPR audit trail, Q5) |

Integrity is **DB controls only** (Q3): rely on ACLs, backups, optional WORM / immutable storage — **no** framework hash chain or signatures.

## Continuous backup

1. Enable **continuous WAL archiving** (or cloud PITR).
2. Take base backups on a schedule (pgBackRest, Barman, or cloud automated backups).
3. Encrypt backups at rest; restrict restore IAM.
4. Document retention vs GDPR tombstone policy (tombstone-only product path — Q5).

## Verified restore drill (quarterly)

1. Restore into an isolated instance (never overwrite prod blindly).
2. Run `go run ./cmd/go-sequent migrate -dsn …` only if schema objects missing — prefer restoring a consistent cluster snapshot that already includes schema.
3. Smoke test:
   - `LoadStream` for a known aggregate
   - One `CommandService.Execute` with SyncInTxn projector
   - Idempotency key round-trip for a tenant
4. Optionally rebuild projections via replay pipeline.
5. Record RTO/RPO achieved; fix gaps.

## Point-in-time recovery

- Prefer PITR to just before an incident.
- After restore, **do not** import Ruby Sequent dumps (Q9).
- If dual-write occurred during a bad failover, stop writers, restore to a known-good LSN, then catch up clients via idempotent retries.

## Snapshots after restore

- If `snapshot_version` / aggregate schema changed, delete incompatible `snapshot_records` and let the snapshotter refill (or load from events only).
