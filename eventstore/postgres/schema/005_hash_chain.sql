-- Q3 (updated 2026-10-26): per-aggregate event hash chain (SHA-256).
-- Additive; no event signatures. Backfill of existing rows is out of scope —
-- VerifyChain fails with incomplete hashes until streams are rewritten/rebuilt.

ALTER TABLE events ADD COLUMN IF NOT EXISTS prev_hash BYTEA;
ALTER TABLE events ADD COLUMN IF NOT EXISTS event_hash BYTEA;

COMMENT ON COLUMN events.prev_hash IS 'SHA-256 of prior event in this aggregate stream; genesis = 32 zero bytes';
COMMENT ON COLUMN events.event_hash IS 'SHA-256 of canonical event bytes including prev_hash (see eventstore/hashchain)';
