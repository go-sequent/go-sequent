// Package postgres implements EventStore on PostgreSQL.
package postgres

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-sequent/go-sequent/event"
	"github.com/go-sequent/go-sequent/event/upcast"
	"github.com/go-sequent/go-sequent/eventstore"
	"github.com/go-sequent/go-sequent/eventstore/hashchain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema/001_phase1.sql
var schemaPhase1 string

//go:embed schema/002_phase2.sql
var schemaPhase2 string

//go:embed schema/003_store_events.sql
var schemaStoreEvents string

//go:embed schema/004_phase3.sql
var schemaPhase3 string

//go:embed schema/005_hash_chain.sql
var schemaHashChain string

//go:embed schema/006_event_json_bytea.sql
var schemaEventJSONBytea string

type txKey struct{}

// Store is a PostgreSQL-backed event store.
type Store struct {
	pool      *pgxpool.Pool
	registry  *event.Registry
	upcasters *upcast.Chain

	typeMu   sync.Mutex
	cmdTypes map[string]int16
	aggTypes map[string]int16
	evtTypes map[string]int16
}

// New wraps an existing pool. Call Migrate once at startup.
func New(pool *pgxpool.Pool, registry *event.Registry) *Store {
	return &Store{
		pool:     pool,
		registry: registry,
		cmdTypes: make(map[string]int16),
		aggTypes: make(map[string]int16),
		evtTypes: make(map[string]int16),
	}
}

// Open creates a pool from DSN and returns a Store.
func Open(ctx context.Context, dsn string, registry *event.Registry) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return New(pool, registry), nil
}

// Close closes the underlying pool.
func (s *Store) Close() { s.pool.Close() }

// Pool exposes the pool for projectors / tests.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// SetUpcasters attaches event payload upcasters (v0).
func (s *Store) SetUpcasters(c *upcast.Chain) { s.upcasters = c }

// Migrate applies Phase 1–3 schema SQL, store_events, hash-chain columns (Q3),
// and event_json BYTEA (exact bytes for hash verify).
func (s *Store) Migrate(ctx context.Context) error {
	for i, sql := range []string{
		schemaPhase1, schemaPhase2, schemaStoreEvents, schemaPhase3,
		schemaHashChain, schemaEventJSONBytea,
	} {
		if _, err := s.pool.Exec(ctx, sql); err != nil {
			return fmt.Errorf("postgres: migrate step %d: %w", i+1, err)
		}
	}
	return nil
}

// TxFromContext returns the WithinTx transaction when present (for SyncInTxn
// helpers such as idempotency that must see uncommitted commands).
func TxFromContext(ctx context.Context) (pgx.Tx, bool) {
	tx, ok := ctx.Value(txKey{}).(pgx.Tx)
	return tx, ok
}

// WithinTx runs fn inside a transaction bound to ctx.
func (s *Store) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	if _, ok := ctx.Value(txKey{}).(pgx.Tx); ok {
		return fn(ctx) // nested: reuse
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	ctx = context.WithValue(ctx, txKey{}, tx)
	if err := fn(ctx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) querier(ctx context.Context) q {
	if tx, ok := ctx.Value(txKey{}).(pgx.Tx); ok {
		return tx
	}
	return s.pool
}

type q interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Append implements eventstore.Store with optimistic concurrency.
func (s *Store) Append(ctx context.Context, cmd eventstore.CommandRecord, streams []eventstore.StreamAppend) error {
	db := s.querier(ctx)

	cmdTypeID, err := s.ensureType(ctx, db, "command_types", cmd.Type, &s.cmdTypes)
	if err != nil {
		return err
	}
	payloadSrc := any(cmd.Payload)
	if payloadSrc == nil {
		payloadSrc = cmd
	}
	rawCmd, err := json.Marshal(payloadSrc)
	if err != nil {
		return err
	}

	issued := cmd.IssuedAt
	if issued.IsZero() {
		issued = time.Now().UTC()
	}
	var idem *string
	if cmd.IdempotencyKey != "" {
		idem = &cmd.IdempotencyKey
	}
	var tenant *uuid.UUID
	if cmd.TenantID != uuid.Nil {
		tenant = &cmd.TenantID
	}
	var user *uuid.UUID
	if cmd.UserID != uuid.Nil {
		user = &cmd.UserID
	}

	_, err = db.Exec(ctx, `
		INSERT INTO commands (id, created_at, user_id, tenant_id, aggregate_id, command_type_id, command_json, idempotency_key)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		cmd.ID, issued, user, tenant, cmd.AggregateID, cmdTypeID, rawCmd, idem,
	)
	if err != nil {
		return fmt.Errorf("postgres: insert command: %w", err)
	}

	for _, st := range streams {
		aggTypeID, err := s.ensureType(ctx, db, "aggregate_types", st.AggregateType, &s.aggTypes)
		if err != nil {
			return err
		}
		pk := st.PartitionKey

		var tip int64
		var exists bool
		err = db.QueryRow(ctx, `
			SELECT true, COALESCE((
				SELECT MAX(sequence_number) FROM events WHERE aggregate_id = $1
			), 0)
			FROM aggregates WHERE aggregate_id = $1
			FOR NO KEY UPDATE`, st.AggregateID).Scan(&exists, &tip)
		if errors.Is(err, pgx.ErrNoRows) {
			_, err = db.Exec(ctx, `
				INSERT INTO aggregates (aggregate_id, events_partition_key, aggregate_type_id, tenant_id)
				VALUES ($1,$2,$3,$4)`,
				st.AggregateID, pk, aggTypeID, tenant,
			)
			if err != nil {
				return fmt.Errorf("postgres: insert aggregate: %w", err)
			}
			tip = 0
		} else if err != nil {
			return fmt.Errorf("postgres: lock aggregate: %w", err)
		}

		if tip != st.ExpectedVersion {
			return eventstore.ErrOptimisticConcurrency
		}
		if len(st.Events) > 0 && st.Events[0].SequenceNumber() != tip+1 {
			return eventstore.ErrOptimisticConcurrency
		}

		prevHash := append([]byte(nil), hashchain.Genesis...)
		if tip > 0 {
			var tipHash []byte
			err = db.QueryRow(ctx, `
				SELECT event_hash FROM events
				WHERE aggregate_id = $1 AND sequence_number = $2`,
				st.AggregateID, tip).Scan(&tipHash)
			if err != nil {
				return fmt.Errorf("postgres: load tip hash: %w", err)
			}
			if len(tipHash) > 0 {
				prevHash = tipHash
			}
		}

		lastSeq := tip
		for _, e := range st.Events {
			evtTypeID, err := s.ensureType(ctx, db, "event_types", e.EventType(), &s.evtTypes)
			if err != nil {
				return err
			}
			payload, err := json.Marshal(e)
			if err != nil {
				return err
			}
			created := e.CreatedAt()
			if created.IsZero() {
				created = time.Now().UTC()
			}
			created = hashchain.CanonicalTime(created)
			eventHash := hashchain.Compute(e.EventType(), st.AggregateID, e.SequenceNumber(), created, payload, prevHash)
			_, err = db.Exec(ctx, `
				INSERT INTO events (aggregate_id, partition_key, sequence_number, created_at, command_id, event_type_id, event_json, tenant_id, prev_hash, event_hash)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
				st.AggregateID, pk, e.SequenceNumber(), created, cmd.ID, evtTypeID, payload, tenant, prevHash, eventHash,
			)
			if err != nil {
				var pgErr *pgconn.PgError
				if errors.As(err, &pgErr) && pgErr.Code == "23505" {
					return eventstore.ErrOptimisticConcurrency
				}
				return fmt.Errorf("postgres: insert event: %w", err)
			}
			prevHash = eventHash
			lastSeq = e.SequenceNumber()
		}
		if err := s.updateUniqueKeys(ctx, db, st); err != nil {
			return err
		}
		if lastSeq > tip {
			_, _ = db.Exec(ctx, `
				INSERT INTO aggregates_that_need_snapshots (aggregate_id, snapshot_outdated_at, snapshot_threshold)
				SELECT $1, NOW(), COALESCE(snapshot_threshold, 100) FROM aggregates WHERE aggregate_id = $1
				  AND $2 >= COALESCE(snapshot_threshold, 100)
				ON CONFLICT (aggregate_id) DO UPDATE SET snapshot_outdated_at = NOW()`,
				st.AggregateID, lastSeq)
		}
	}
	return nil
}

func (s *Store) updateUniqueKeys(ctx context.Context, db q, st eventstore.StreamAppend) error {
	keys := st.UniqueKeys
	if keys == nil {
		keys = map[string]any{}
	}
	// Delete scopes removed from the aggregate.
	rows, err := db.Query(ctx, `SELECT scope FROM aggregate_unique_keys WHERE aggregate_id = $1`, st.AggregateID)
	if err != nil {
		// Phase 1 DB without unique-keys table: ignore.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42P01" {
			return nil
		}
		return err
	}
	var existing []string
	for rows.Next() {
		var scope string
		if err := rows.Scan(&scope); err != nil {
			rows.Close()
			return err
		}
		existing = append(existing, scope)
	}
	rows.Close()
	for _, scope := range existing {
		if _, ok := keys[scope]; !ok {
			if _, err := db.Exec(ctx, `DELETE FROM aggregate_unique_keys WHERE aggregate_id = $1 AND scope = $2`, st.AggregateID, scope); err != nil {
				return err
			}
		}
	}
	for scope, val := range keys {
		raw, err := json.Marshal(val)
		if err != nil {
			return err
		}
		_, err = db.Exec(ctx, `
			INSERT INTO aggregate_unique_keys (aggregate_id, scope, key) VALUES ($1,$2,$3)
			ON CONFLICT (aggregate_id, scope) DO UPDATE SET key = EXCLUDED.key`,
			st.AggregateID, scope, raw,
		)
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				return fmt.Errorf("%w: scope=%s", eventstore.ErrUniqueKeyViolation, scope)
			}
			return err
		}
	}
	return nil
}

func (s *Store) ensureType(ctx context.Context, db q, table, name string, cache *map[string]int16) (int16, error) {
	s.typeMu.Lock()
	defer s.typeMu.Unlock()
	if id, ok := (*cache)[name]; ok {
		return id, nil
	}
	var id int16
	err := db.QueryRow(ctx, fmt.Sprintf(`SELECT id FROM %s WHERE type = $1`, table), name).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		err = db.QueryRow(ctx,
			fmt.Sprintf(`INSERT INTO %s (type) VALUES ($1) RETURNING id`, table), name,
		).Scan(&id)
	}
	if err != nil {
		return 0, fmt.Errorf("postgres: ensure type %s/%s: %w", table, name, err)
	}
	(*cache)[name] = id
	return id, nil
}

// LoadStream implements eventstore.Store and verifies the hash chain (Q3).
func (s *Store) LoadStream(ctx context.Context, aggregateID uuid.UUID) ([]event.Event, error) {
	if err := s.VerifyChain(ctx, aggregateID); err != nil {
		if errors.Is(err, eventstore.ErrStreamNotFound) {
			return nil, err
		}
		return nil, err
	}
	db := s.querier(ctx)
	rows, err := db.Query(ctx, `
		SELECT et.type, e.event_json, e.sequence_number, e.aggregate_id, e.created_at
		FROM events e
		JOIN event_types et ON et.id = e.event_type_id
		WHERE e.aggregate_id = $1
		ORDER BY e.sequence_number`, aggregateID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []event.Event
	for rows.Next() {
		var (
			typ   string
			raw   []byte
			seq   int64
			aggID uuid.UUID
			at    time.Time
		)
		if err := rows.Scan(&typ, &raw, &seq, &aggID, &at); err != nil {
			return nil, err
		}
		payload := json.RawMessage(raw)
		if s.upcasters != nil {
			var err error
			typ, payload, err = s.upcasters.Apply(typ, payload)
			if err != nil {
				return nil, err
			}
		}
		e, err := s.registry.New(typ)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(payload, e); err != nil {
			return nil, err
		}
		if env, ok := e.(event.Envelope); ok {
			env.SetAggregateID(aggID)
			env.SetSequenceNumber(seq)
			env.SetCreatedAt(at)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, eventstore.ErrStreamNotFound
	}
	return out, nil
}

// VerifyChain validates the per-aggregate SHA-256 hash chain (Q3).
func (s *Store) VerifyChain(ctx context.Context, aggregateID uuid.UUID) error {
	db := s.querier(ctx)
	rows, err := db.Query(ctx, `
		SELECT et.type, e.event_json, e.sequence_number, e.aggregate_id, e.created_at, e.prev_hash, e.event_hash
		FROM events e
		JOIN event_types et ON et.id = e.event_type_id
		WHERE e.aggregate_id = $1
		ORDER BY e.sequence_number`, aggregateID)
	if err != nil {
		return err
	}
	defer rows.Close()

	var links []hashchain.Link
	for rows.Next() {
		var l hashchain.Link
		if err := rows.Scan(&l.EventType, &l.EventJSON, &l.Seq, &l.AggregateID, &l.CreatedAt, &l.PrevHash, &l.EventHash); err != nil {
			return err
		}
		links = append(links, l)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(links) == 0 {
		return eventstore.ErrStreamNotFound
	}
	if err := hashchain.VerifyStream(links); err != nil {
		if errors.Is(err, hashchain.ErrIncomplete) {
			return fmt.Errorf("%w: %v", eventstore.ErrHashChainIncomplete, err)
		}
		return fmt.Errorf("%w: %v", eventstore.ErrHashChainTamper, err)
	}
	return nil
}

// LoadStreams implements eventstore.Store.
func (s *Store) LoadStreams(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID][]event.Event, error) {
	out := make(map[uuid.UUID][]event.Event, len(ids))
	for _, id := range ids {
		evs, err := s.LoadStream(ctx, id)
		if err == eventstore.ErrStreamNotFound {
			continue
		}
		if err != nil {
			return nil, err
		}
		out[id] = evs
	}
	return out, nil
}

var (
	_ eventstore.Store     = (*Store)(nil)
	_ eventstore.TxManager = (*Store)(nil)
)
