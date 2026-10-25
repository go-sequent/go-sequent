-- EXAMPLE ONLY — not applied by go-sequent Migrate.
-- Illustrates hash partitioning of events by partition_key within a shared schema (Q4).
-- Adapt names/moduli after measuring real tenant cardinality and query patterns.
-- Requires a maintenance window and careful data move from the existing events table.

-- 1) Create partitioned parent (empty) matching Phase 1–3 columns:
/*
CREATE TABLE events_partitioned (
    aggregate_id     UUID NOT NULL,
    partition_key    TEXT NOT NULL DEFAULT '',
    sequence_number  BIGINT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL,
    command_id       UUID NOT NULL,
    event_type_id    SMALLINT NOT NULL,
    event_json       BYTEA NOT NULL,
    tenant_id        UUID,
    PRIMARY KEY (partition_key, aggregate_id, sequence_number)
) PARTITION BY HASH (partition_key);

-- 2) Create N hash partitions (example N=8):
CREATE TABLE events_p0 PARTITION OF events_partitioned FOR VALUES WITH (MODULUS 8, REMAINDER 0);
CREATE TABLE events_p1 PARTITION OF events_partitioned FOR VALUES WITH (MODULUS 8, REMAINDER 1);
CREATE TABLE events_p2 PARTITION OF events_partitioned FOR VALUES WITH (MODULUS 8, REMAINDER 2);
CREATE TABLE events_p3 PARTITION OF events_partitioned FOR VALUES WITH (MODULUS 8, REMAINDER 3);
CREATE TABLE events_p4 PARTITION OF events_partitioned FOR VALUES WITH (MODULUS 8, REMAINDER 4);
CREATE TABLE events_p5 PARTITION OF events_partitioned FOR VALUES WITH (MODULUS 8, REMAINDER 5);
CREATE TABLE events_p6 PARTITION OF events_partitioned FOR VALUES WITH (MODULUS 8, REMAINDER 6);
CREATE TABLE events_p7 PARTITION OF events_partitioned FOR VALUES WITH (MODULUS 8, REMAINDER 7);

-- 3) Copy data, swap names, recreate FKs/indexes (omitted — environment-specific).
-- Prefer setting aggregates.events_partition_key = tenant_id::text (or tenant:shard)
-- so pruning aligns with tenancy.PartitionKey / PartitionKeyHashed.
*/
