# Schema partitioning guidance (shared schema + tenant_id)

**Model (Q4):** one shared schema with `tenant_id` columns/hooks — **not** schema-per-tenant or DB-per-tenant.

Physical partitioning is optional and should stay **within** that shared schema. Phase 1–3 migrations remain non-partitioned by default; the SQL under [`sql/`](./sql/) is **guidance only** (apply deliberately in ops, after capacity review).

## When to partition

| Symptom | Candidate |
|---------|-----------|
| `events` table >> tens of GB / hot vacuum | Partition by `partition_key` (often = `tenant_id` or `tenant_id:shard`) |
| Very large `commands` | Range partition by time or hash by `id` |
| Cross-tenant scans rare | Prefer prune-friendly keys matching load filters |

go-sequent already stores `events.partition_key` and `aggregates.events_partition_key`. Application code should set `PartitionKey()` from `tenancy.PartitionKey` / `PartitionKeyHashed`.

## Recommended approach

1. Keep **logical** multi-tenancy via `tenant_id` on commands/events/aggregates/projections.
2. Set `partition_key = tenant_id` (or tenant+hash) for stream locality.
3. If needed, convert `events` to a partitioned parent with partitions per tenant hash bucket or per tenant (for huge tenants).
4. Unique keys (`aggregate_unique_keys`) stay global within schema; scopes should include tenant (`tenancy.UniqueScope`).
5. Do **not** create per-tenant schemas as the product model.

## RLS (optional)

Row Level Security on `tenant_id` can deepen defense; still enforce tenant match in repository/load (defense in depth).

## HA interaction

- Partition pruning helps primary and replicas; rebuild workers should filter by partition ranges.
- Backups remain cluster-level (see [backup-restore.md](./backup-restore.md)).

## Examples

See [`sql/events_partition_by_tenant_hash.example.sql`](./sql/events_partition_by_tenant_hash.example.sql) — illustrative, not auto-applied by `Migrate`.
