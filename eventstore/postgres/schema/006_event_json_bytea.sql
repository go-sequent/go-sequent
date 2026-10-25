-- Q3 hash chain needs exact event_json bytes. JSONB re-serializes on read
-- (key order / whitespace), which breaks event_hash verification.
-- Convert to BYTEA when still JSONB (idempotent for fresh BYTEA installs).

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'events'
      AND column_name = 'event_json'
      AND udt_name = 'jsonb'
  ) THEN
    ALTER TABLE events
      ALTER COLUMN event_json TYPE BYTEA
      USING convert_to(event_json::text, 'UTF8');
  END IF;
END $$;

COMMENT ON COLUMN events.event_json IS 'Exact event payload bytes for hash chain (BYTEA; not JSONB)';
