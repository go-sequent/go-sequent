-- go-sequent Phase 3: tenant-scoped idempotency, outbox polling columns, no hash/sign (Q3).

CREATE TABLE IF NOT EXISTS idempotency_keys (
    tenant_id    UUID NOT NULL,
    key          TEXT NOT NULL,
    command_id   UUID NOT NULL REFERENCES commands (id),
    command_type TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'succeeded', -- succeeded|rejected
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, key)
);

CREATE INDEX IF NOT EXISTS idempotency_keys_command_id_idx ON idempotency_keys (command_id);

-- Harden outbox for Postgres polling drain (Q6)
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS tenant_id UUID;
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS aggregate_id UUID;
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS event_type TEXT;
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS last_error TEXT;

CREATE INDEX IF NOT EXISTS outbox_poll_idx
    ON outbox (available_at, id)
    WHERE processed_at IS NULL;
