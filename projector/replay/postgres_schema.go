package replay

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresSchema manages replay schemas via DDL (prepare/golive/abort).
type PostgresSchema struct {
	Pool *pgxpool.Pool
}

func (p *PostgresSchema) Prepare(ctx context.Context, replaySchema string, managedTables []string) error {
	if err := quoteIdent(replaySchema); err != nil {
		return err
	}
	if _, err := p.Pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, replaySchema)); err != nil {
		return err
	}
	for _, table := range managedTables {
		if err := quoteIdent(table); err != nil {
			return err
		}
		// Copy table definition without data from public (view) schema.
		_, err := p.Pool.Exec(ctx, fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s.%s (LIKE public.%s INCLUDING ALL)`,
			replaySchema, table, table,
		))
		if err != nil {
			return fmt.Errorf("replay prepare %s.%s: %w", replaySchema, table, err)
		}
		_, _ = p.Pool.Exec(ctx, fmt.Sprintf(`TRUNCATE %s.%s`, replaySchema, table))
	}
	return nil
}

func (p *PostgresSchema) Optimize(ctx context.Context, replaySchema string) error {
	if err := quoteIdent(replaySchema); err != nil {
		return err
	}
	_, err := p.Pool.Exec(ctx, `ANALYZE`)
	return err
}

func (p *PostgresSchema) GoLive(ctx context.Context, replaySchema, viewSchema string, managedTables []string) error {
	if err := quoteIdent(replaySchema); err != nil {
		return err
	}
	if err := quoteIdent(viewSchema); err != nil {
		return err
	}
	tx, err := p.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	archive := replaySchema + "_old"
	_, _ = tx.Exec(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, archive))
	_, err = tx.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, archive))
	if err != nil {
		return err
	}
	for _, table := range managedTables {
		if err := quoteIdent(table); err != nil {
			return err
		}
		// Move live table aside, promote replay table.
		_, err = tx.Exec(ctx, fmt.Sprintf(
			`ALTER TABLE IF EXISTS %s.%s SET SCHEMA %s`, viewSchema, table, archive,
		))
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, fmt.Sprintf(
			`ALTER TABLE %s.%s SET SCHEMA %s`, replaySchema, table, viewSchema,
		))
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (p *PostgresSchema) Abort(ctx context.Context, replaySchema string) error {
	if err := quoteIdent(replaySchema); err != nil {
		return err
	}
	_, err := p.Pool.Exec(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, replaySchema))
	return err
}

func quoteIdent(name string) error {
	if name == "" || strings.ContainsAny(name, `";' \t\n`) {
		return fmt.Errorf("replay: invalid identifier %q", name)
	}
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			continue
		}
		return fmt.Errorf("replay: invalid identifier %q", name)
	}
	return nil
}
