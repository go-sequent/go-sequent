package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Postgres implements Store with table polling (SELECT … FOR UPDATE SKIP LOCKED).
type Postgres struct {
	Pool *pgxpool.Pool
}

func NewPostgres(pool *pgxpool.Pool) *Postgres { return &Postgres{Pool: pool} }

func (p *Postgres) Enqueue(ctx context.Context, msg Message) error {
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now().UTC()
	}
	if msg.AvailableAt.IsZero() {
		msg.AvailableAt = msg.CreatedAt
	}
	var tenant, agg *uuid.UUID
	if msg.TenantID != uuid.Nil {
		tenant = &msg.TenantID
	}
	if msg.AggregateID != uuid.Nil {
		agg = &msg.AggregateID
	}
	var evtType *string
	if msg.EventType != "" {
		evtType = &msg.EventType
	}
	_, err := p.Pool.Exec(ctx, `
		INSERT INTO outbox (created_at, available_at, topic, payload, tenant_id, aggregate_id, event_type)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		msg.CreatedAt, msg.AvailableAt, msg.Topic, msg.Payload, tenant, agg, evtType,
	)
	return err
}

func (p *Postgres) Claim(ctx context.Context, limit int, lockFor time.Duration) ([]Message, error) {
	tx, err := p.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	rows, err := tx.Query(ctx, `
		SELECT id, created_at, available_at, topic, payload, attempts,
		       COALESCE(tenant_id, '00000000-0000-0000-0000-000000000000'::uuid),
		       COALESCE(aggregate_id, '00000000-0000-0000-0000-000000000000'::uuid),
		       COALESCE(event_type, '')
		FROM outbox
		WHERE processed_at IS NULL AND available_at <= NOW()
		ORDER BY available_at, id
		FOR UPDATE SKIP LOCKED
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}

	var out []Message
	var ids []int64
	for rows.Next() {
		var m Message
		if err := rows.Scan(&m.ID, &m.CreatedAt, &m.AvailableAt, &m.Topic, &m.Payload, &m.Attempts,
			&m.TenantID, &m.AggregateID, &m.EventType); err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, m)
		ids = append(ids, m.ID)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, tx.Commit(ctx)
	}
	secs := int64(lockFor / time.Second)
	if secs < 1 {
		secs = 30
	}
	_, err = tx.Exec(ctx, fmt.Sprintf(`
		UPDATE outbox SET attempts = attempts + 1, locked_until = NOW() + interval '%d seconds'
		WHERE id = ANY($1)`, secs), ids)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

func (p *Postgres) Ack(ctx context.Context, id int64) error {
	_, err := p.Pool.Exec(ctx, `UPDATE outbox SET processed_at = NOW(), locked_until = NULL WHERE id = $1`, id)
	return err
}

func (p *Postgres) Nack(ctx context.Context, id int64, retryAfter time.Duration, lastError string) error {
	secs := int64(retryAfter / time.Second)
	if secs < 1 {
		secs = 5
	}
	_, err := p.Pool.Exec(ctx, fmt.Sprintf(`
		UPDATE outbox SET available_at = NOW() + interval '%d seconds', locked_until = NULL, last_error = $2
		WHERE id = $1`, secs), id, lastError)
	return err
}

func (p *Postgres) Depth(ctx context.Context) (int64, error) {
	var n int64
	err := p.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM outbox WHERE processed_at IS NULL`).Scan(&n)
	return n, err
}

// EnqueueEvent is a helper for publishers writing event JSON to a topic.
func EnqueueEvent(ctx context.Context, w Writer, topic string, tenant, aggregate uuid.UUID, eventType string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return w.Enqueue(ctx, Message{
		Topic:       topic,
		Payload:     raw,
		TenantID:    tenant,
		AggregateID: aggregate,
		EventType:   eventType,
	})
}
