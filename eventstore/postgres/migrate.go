package postgres

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"

	"github.com/jackc/pgx/v5/pgconn"
)

// migrateLockKey is a cluster-wide advisory lock id for go-sequent Migrate.
// Derived once so concurrent test packages sharing a DSN serialize DDL.
var migrateLockKey = advisoryKey("github.com/go-sequent/go-sequent/eventstore/postgres.Migrate")

func advisoryKey(s string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	// pg_advisory_lock takes signed int64; keep positive.
	return int64(h.Sum64() & 0x7fffffffffffffff)
}

type migrationStep struct {
	version int
	name    string
	sql     string
}

func (s *Store) migrationSteps() []migrationStep {
	return []migrationStep{
		{1, "001_phase1", schemaPhase1},
		{2, "002_phase2", schemaPhase2},
		{3, "003_store_events", schemaStoreEvents},
		{4, "004_phase3", schemaPhase3},
		{5, "005_hash_chain", schemaHashChain},
		{6, "006_event_json_bytea", schemaEventJSONBytea},
	}
}

// Migrate applies schema SQL idempotently.
//
// Concurrent callers (e.g. go test ./eventstore/postgres ./examples/bank
// sharing one GO_SEQUENT_PG_DSN) are serialized with pg_advisory_lock.
// Applied versions are recorded in go_sequent_schema_migrations so re-runs
// no-op after the first success. Fresh empty databases still get a full apply.
func (s *Store) Migrate(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("postgres: migrate acquire: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrateLockKey); err != nil {
		return fmt.Errorf("postgres: migrate lock: %w", err)
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, migrateLockKey)
	}()

	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS go_sequent_schema_migrations (
			version    INT PRIMARY KEY,
			name       TEXT NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`); err != nil {
		return fmt.Errorf("postgres: migrate bookkeeping: %w", err)
	}

	for _, step := range s.migrationSteps() {
		var exists bool
		err := conn.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM go_sequent_schema_migrations WHERE version = $1)`,
			step.version,
		).Scan(&exists)
		if err != nil {
			return fmt.Errorf("postgres: migrate version check %d: %w", step.version, err)
		}
		if exists {
			continue
		}
		if _, err := conn.Exec(ctx, step.sql); err != nil {
			// Catalog races (historically IDENTITY → pg_type_typename_nsp_index)
			// or a partial prior attempt: treat unique_violation as retryable no-op.
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
				return fmt.Errorf("postgres: migrate step %d (%s): %w", step.version, step.name, err)
			}
		}
		if _, err := conn.Exec(ctx, `
			INSERT INTO go_sequent_schema_migrations (version, name) VALUES ($1, $2)
			ON CONFLICT (version) DO NOTHING`, step.version, step.name); err != nil {
			return fmt.Errorf("postgres: migrate record %d: %w", step.version, err)
		}
	}
	return nil
}
