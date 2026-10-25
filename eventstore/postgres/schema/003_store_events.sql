-- store_events procedure — Sequent-inspired optimistic concurrency semantics (Phase 2).
-- Called optionally; Go Append path mirrors the same checks when not using the procedure.

CREATE OR REPLACE PROCEDURE go_sequent_store_events(
    _command_id UUID,
    _created_at TIMESTAMPTZ,
    _user_id UUID,
    _tenant_id UUID,
    _aggregate_id UUID,
    _command_type TEXT,
    _command_json JSONB,
    _idempotency_key TEXT,
    _streams JSONB  -- [{aggregate_id, aggregate_type, partition_key, expected_version, unique_keys, events:[{event_type, sequence_number, created_at, event_json}]}]
)
LANGUAGE plpgsql AS $$
DECLARE
    _cmd_type_id SMALLINT;
    _stream JSONB;
    _agg_id UUID;
    _agg_type TEXT;
    _pk TEXT;
    _expected BIGINT;
    _tip BIGINT;
    _agg_type_id SMALLINT;
    _evt JSONB;
    _evt_type_id SMALLINT;
    _scope TEXT;
    _key JSONB;
BEGIN
    INSERT INTO command_types (type) VALUES (_command_type)
    ON CONFLICT (type) DO NOTHING;
    SELECT id INTO STRICT _cmd_type_id FROM command_types WHERE type = _command_type;

    INSERT INTO commands (id, created_at, user_id, tenant_id, aggregate_id, command_type_id, command_json, idempotency_key)
    VALUES (_command_id, _created_at, _user_id, _tenant_id, _aggregate_id, _cmd_type_id, _command_json, _idempotency_key);

    FOR _stream IN SELECT value FROM jsonb_array_elements(_streams) ORDER BY value->>'aggregate_id'
    LOOP
        _agg_id := (_stream->>'aggregate_id')::uuid;
        _agg_type := _stream->>'aggregate_type';
        _pk := COALESCE(_stream->>'partition_key', '');
        _expected := COALESCE((_stream->>'expected_version')::bigint, 0);

        INSERT INTO aggregate_types (type) VALUES (_agg_type) ON CONFLICT (type) DO NOTHING;
        SELECT id INTO STRICT _agg_type_id FROM aggregate_types WHERE type = _agg_type;

        SELECT COALESCE((
            SELECT MAX(sequence_number) FROM events WHERE aggregate_id = _agg_id
        ), 0) INTO _tip
        FROM aggregates WHERE aggregate_id = _agg_id
        FOR NO KEY UPDATE;

        IF NOT FOUND THEN
            INSERT INTO aggregates (aggregate_id, events_partition_key, aggregate_type_id, tenant_id)
            VALUES (_agg_id, _pk, _agg_type_id, _tenant_id);
            _tip := 0;
        END IF;

        IF _tip <> _expected THEN
            RAISE EXCEPTION 'optimistic concurrency: tip=% expected=% aggregate=%', _tip, _expected, _agg_id
                USING ERRCODE = 'integrity_constraint_violation';
        END IF;

        FOR _evt IN SELECT value FROM jsonb_array_elements(COALESCE(_stream->'events', '[]'::jsonb))
                    ORDER BY (value->>'sequence_number')::bigint
        LOOP
            IF (_evt->>'sequence_number')::bigint <> _tip + 1 THEN
                RAISE EXCEPTION 'sequence must be consecutive: tip=% next=% aggregate=%',
                    _tip, (_evt->>'sequence_number')::bigint, _agg_id
                    USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            _tip := _tip + 1;

            INSERT INTO event_types (type) VALUES (_evt->>'event_type') ON CONFLICT (type) DO NOTHING;
            SELECT id INTO STRICT _evt_type_id FROM event_types WHERE type = _evt->>'event_type';

            INSERT INTO events (aggregate_id, partition_key, sequence_number, created_at, command_id, event_type_id, event_json, tenant_id)
            VALUES (
                _agg_id, _pk, _tip,
                COALESCE((_evt->>'created_at')::timestamptz, _created_at),
                _command_id, _evt_type_id, _evt->'event_json', _tenant_id
            );
        END LOOP;

        -- Unique keys: replace scopes for this aggregate
        DELETE FROM aggregate_unique_keys WHERE aggregate_id = _agg_id
            AND NOT (COALESCE(_stream->'unique_keys', '{}'::jsonb) ? scope);

        FOR _scope, _key IN SELECT key, value FROM jsonb_each(COALESCE(_stream->'unique_keys', '{}'::jsonb))
        LOOP
            INSERT INTO aggregate_unique_keys (aggregate_id, scope, key)
            VALUES (_agg_id, _scope, _key)
            ON CONFLICT (aggregate_id, scope) DO UPDATE SET key = EXCLUDED.key;
        END LOOP;

        -- Mark for snapshotting when past threshold
        INSERT INTO aggregates_that_need_snapshots (aggregate_id, snapshot_outdated_at, snapshot_threshold)
        SELECT _agg_id, NOW(), COALESCE(a.snapshot_threshold, 100)
          FROM aggregates a WHERE a.aggregate_id = _agg_id AND _tip >= COALESCE(a.snapshot_threshold, 100)
        ON CONFLICT (aggregate_id) DO UPDATE
          SET snapshot_outdated_at = NOW();
    END LOOP;
EXCEPTION
    WHEN unique_violation THEN
        RAISE EXCEPTION 'unique key or event PK violation: %', SQLERRM
            USING ERRCODE = 'unique_violation';
END;
$$;
