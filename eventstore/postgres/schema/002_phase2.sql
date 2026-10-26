-- go-sequent Phase 2: snapshots, unique keys, store_events alignment, replay state, outbox stub
-- Q3: no hash/signature columns. Q4: tenant_id on shared schema. Q8: JSON snapshots.

CREATE TABLE IF NOT EXISTS aggregate_unique_keys (
    aggregate_id UUID NOT NULL REFERENCES aggregates (aggregate_id) ON DELETE CASCADE,
    scope        TEXT NOT NULL,
    key          JSONB NOT NULL,
    PRIMARY KEY (aggregate_id, scope)
);

CREATE UNIQUE INDEX IF NOT EXISTS aggregate_unique_keys_scope_key_uidx
    ON aggregate_unique_keys (scope, key);

CREATE TABLE IF NOT EXISTS snapshot_records (
    aggregate_id      UUID NOT NULL,
    sequence_number   BIGINT NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    snapshot_type     TEXT NOT NULL,
    snapshot_version  INTEGER NOT NULL DEFAULT 1,
    snapshot_json     JSONB NOT NULL,
    PRIMARY KEY (aggregate_id, sequence_number, snapshot_version)
);

CREATE TABLE IF NOT EXISTS aggregates_that_need_snapshots (
    aggregate_id UUID PRIMARY KEY REFERENCES aggregates (aggregate_id) ON DELETE CASCADE,
    snapshot_sequence_number_high_water_mark BIGINT,
    snapshot_outdated_at TIMESTAMPTZ,
    snapshot_scheduled_at TIMESTAMPTZ,
    snapshot_threshold INTEGER NOT NULL DEFAULT 100
);

ALTER TABLE aggregates
    ADD COLUMN IF NOT EXISTS snapshot_threshold INTEGER NOT NULL DEFAULT 100;

-- Projection rebuild bookkeeping (prepare → replay → catchup → golive)
CREATE SEQUENCE IF NOT EXISTS projector_replay_states_id_seq;
CREATE TABLE IF NOT EXISTS projector_replay_states (
    id              BIGINT PRIMARY KEY DEFAULT nextval('projector_replay_states_id_seq'),
    projector_name  TEXT NOT NULL,
    replay_schema   TEXT NOT NULL,
    view_schema     TEXT NOT NULL DEFAULT 'public',
    status          TEXT NOT NULL, -- preparing|replaying|catching_up|optimizing|live|aborted
    high_water_mark BIGINT NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER SEQUENCE projector_replay_states_id_seq OWNED BY projector_replay_states.id;

-- Optional outbox stub for AfterCommit / polling drain
CREATE SEQUENCE IF NOT EXISTS outbox_id_seq;
CREATE TABLE IF NOT EXISTS outbox (
    id           BIGINT PRIMARY KEY DEFAULT nextval('outbox_id_seq'),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    topic        TEXT NOT NULL,
    payload      JSONB NOT NULL,
    attempts     INTEGER NOT NULL DEFAULT 0,
    locked_until TIMESTAMPTZ,
    processed_at TIMESTAMPTZ
);
ALTER SEQUENCE outbox_id_seq OWNED BY outbox.id;

CREATE INDEX IF NOT EXISTS outbox_available_idx
    ON outbox (available_at)
    WHERE processed_at IS NULL;
