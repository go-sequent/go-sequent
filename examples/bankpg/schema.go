package bankpg

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema/001_bank_balances.sql
var schemaBalances string

// MigrateProjection applies example-local projection DDL (after eventstore Migrate).
func MigrateProjection(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, schemaBalances); err != nil {
		return fmt.Errorf("bankpg: migrate projection: %w", err)
	}
	return nil
}
