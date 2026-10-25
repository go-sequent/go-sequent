package postgres

import (
	"context"

	"github.com/go-sequent/go-sequent/projector"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Persistor implements projector.Persistor against the pool, joining the
// Store.WithinTx transaction when present on ctx (SyncInTxn-safe).
type Persistor struct {
	Pool *pgxpool.Pool
}

// NewPersistor wraps pool for projector SQL I/O.
func NewPersistor(pool *pgxpool.Pool) *Persistor {
	return &Persistor{Pool: pool}
}

// Persistor returns a Persistor bound to this store's pool.
func (s *Store) Persistor() *Persistor {
	return NewPersistor(s.pool)
}

func (p *Persistor) db(ctx context.Context) q {
	if tx, ok := ctx.Value(txKey{}).(pgx.Tx); ok {
		return tx
	}
	return p.Pool
}

// Exec implements projector.Persistor.
func (p *Persistor) Exec(ctx context.Context, sql string, args ...any) error {
	_, err := p.db(ctx).Exec(ctx, sql, args...)
	return err
}

type pgxRow struct {
	row pgx.Row
}

func (r pgxRow) Scan(dest ...any) error { return r.row.Scan(dest...) }

// QueryRow implements projector.Persistor.
func (p *Persistor) QueryRow(ctx context.Context, sql string, args ...any) projector.Row {
	return pgxRow{row: p.db(ctx).QueryRow(ctx, sql, args...)}
}
