-- go-sequent Phase 1 minimal event store schema
-- Shared schema + tenant_id. Snapshots / unique keys / outbox in later phases.
-- Type catalogs use CREATE SEQUENCE IF NOT EXISTS (safer under concurrent Migrate
-- than IDENTITY, which races on pg_type / pg_type_typename_nsp_index).

CREATE SEQUENCE IF NOT EXISTS command_types_id_seq AS SMALLINT;
CREATE TABLE IF NOT EXISTS command_types (
    id   SMALLINT PRIMARY KEY DEFAULT nextval('command_types_id_seq'),
    type TEXT UNIQUE NOT NULL
);
ALTER SEQUENCE command_types_id_seq OWNED BY command_types.id;

CREATE SEQUENCE IF NOT EXISTS aggregate_types_id_seq AS SMALLINT;
CREATE TABLE IF NOT EXISTS aggregate_types (
    id   SMALLINT PRIMARY KEY DEFAULT nextval('aggregate_types_id_seq'),
    type TEXT UNIQUE NOT NULL
);
ALTER SEQUENCE aggregate_types_id_seq OWNED BY aggregate_types.id;

CREATE SEQUENCE IF NOT EXISTS event_types_id_seq AS SMALLINT;
CREATE TABLE IF NOT EXISTS event_types (
    id   SMALLINT PRIMARY KEY DEFAULT nextval('event_types_id_seq'),
    type TEXT UNIQUE NOT NULL
);
ALTER SEQUENCE event_types_id_seq OWNED BY event_types.id;

CREATE TABLE IF NOT EXISTS commands (
    id                 UUID PRIMARY KEY,
    created_at         TIMESTAMPTZ NOT NULL,
    user_id            UUID,
    tenant_id          UUID,
    aggregate_id       UUID,
    command_type_id    SMALLINT NOT NULL REFERENCES command_types (id),
    command_json       JSONB NOT NULL,
    idempotency_key    TEXT,
    event_aggregate_id UUID,
    event_sequence_number BIGINT,
    correlation_id     UUID,
    causation_id       UUID
);

CREATE UNIQUE INDEX IF NOT EXISTS commands_tenant_idempotency_uidx
    ON commands (tenant_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL AND tenant_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS aggregates (
    aggregate_id       UUID PRIMARY KEY,
    events_partition_key TEXT NOT NULL DEFAULT '',
    aggregate_type_id  SMALLINT NOT NULL REFERENCES aggregate_types (id),
    tenant_id          UUID,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS events (
    aggregate_id     UUID NOT NULL,
    partition_key    TEXT NOT NULL DEFAULT '',
    sequence_number  BIGINT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL,
    command_id       UUID NOT NULL REFERENCES commands (id),
    event_type_id    SMALLINT NOT NULL REFERENCES event_types (id),
    event_json       BYTEA NOT NULL,
    tenant_id        UUID,
    PRIMARY KEY (aggregate_id, sequence_number)
);

CREATE INDEX IF NOT EXISTS events_tenant_id_idx ON events (tenant_id);
CREATE INDEX IF NOT EXISTS events_command_id_idx ON events (command_id);

CREATE TABLE IF NOT EXISTS event_tombstones (
    aggregate_id    UUID NOT NULL,
    sequence_number BIGINT NOT NULL,
    tombstoned_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    reason          TEXT,
    PRIMARY KEY (aggregate_id, sequence_number)
);
