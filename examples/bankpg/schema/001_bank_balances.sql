-- Example-local ledger projection table (not part of core eventstore Migrate).
-- Shared schema + tenant_id (Q4). Apply after store.Migrate via bankpg.MigrateProjection.

CREATE TABLE IF NOT EXISTS bank_account_balances (
    tenant_id   UUID           NOT NULL,
    account_id  UUID           NOT NULL,
    owner       TEXT           NOT NULL DEFAULT '',
    balance     NUMERIC(30, 10) NOT NULL,
    currency    TEXT           NOT NULL,
    updated_at  TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, account_id)
);

CREATE INDEX IF NOT EXISTS bank_account_balances_account_idx
    ON bank_account_balances (account_id);
