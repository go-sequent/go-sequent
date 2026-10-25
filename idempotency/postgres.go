package idempotency

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Postgres stores tenant-scoped idempotency keys.
type Postgres struct {
	Pool *pgxpool.Pool
}

func NewPostgres(pool *pgxpool.Pool) *Postgres { return &Postgres{Pool: pool} }

func (p *Postgres) Get(ctx context.Context, tenantID uuid.UUID, key string) (*Record, error) {
	var rec Record
	err := p.Pool.QueryRow(ctx, `
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
	_, err := p.Pool.Exec(ctx, `
		INSERT INTO idempotency_keys (tenant_id, key, command_id, command_type, status, created_at)
		VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT (tenant_id, key) DO NOTHING`,
		rec.TenantID, rec.Key, rec.CommandID, rec.CommandType, rec.Status, rec.CreatedAt,
	)
	return err
}
