package idempotency

import (
	"context"
	"errors"
	"time"

	espg "github.com/go-sequent/go-sequent/eventstore/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Postgres stores tenant-scoped idempotency keys.
// When ctx carries a Store.WithinTx transaction, Get/Put join that txn so
// idempotency_keys.command_id FK can see the uncommitted commands row.
type Postgres struct {
	Pool *pgxpool.Pool
}

func NewPostgres(pool *pgxpool.Pool) *Postgres { return &Postgres{Pool: pool} }

type querier interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func (p *Postgres) db(ctx context.Context) querier {
	if tx, ok := espg.TxFromContext(ctx); ok {
		return tx
	}
	return p.Pool
}

func (p *Postgres) Get(ctx context.Context, tenantID uuid.UUID, key string) (*Record, error) {
	var rec Record
	err := p.db(ctx).QueryRow(ctx, `
		SELECT tenant_id, key, command_id, command_type, status, created_at
		FROM idempotency_keys WHERE tenant_id = $1 AND key = $2`, tenantID, key,
	).Scan(&rec.TenantID, &rec.Key, &rec.CommandID, &rec.CommandType, &rec.Status, &rec.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &rec, nil
}

func (p *Postgres) Put(ctx context.Context, rec Record) error {
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = time.Now().UTC()
	}
	if rec.Status == "" {
		rec.Status = "succeeded"
	}
	_, err := p.db(ctx).Exec(ctx, `
		INSERT INTO idempotency_keys (tenant_id, key, command_id, command_type, status, created_at)
		VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT (tenant_id, key) DO NOTHING`,
		rec.TenantID, rec.Key, rec.CommandID, rec.CommandType, rec.Status, rec.CreatedAt,
	)
	return err
}
