// Package outbox implements transactional outbox with Postgres table polling (Q6).
// Ledger / balance projectors should remain on SyncInTxn (Q1); use outbox for
// async / external integration paths.
package outbox

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Message is a pending outbox row.
type Message struct {
	ID          int64
	Topic       string
	Payload     json.RawMessage
	TenantID    uuid.UUID
	AggregateID uuid.UUID
	EventType   string
	Attempts    int
	AvailableAt time.Time
	CreatedAt   time.Time
}

// Writer enqueues messages in the same DB transaction as events.
type Writer interface {
	Enqueue(ctx context.Context, msg Message) error
}

// Store is Writer + claim/ack for pollers.
type Store interface {
	Writer
	// Claim locks up to limit available messages for processing.
	Claim(ctx context.Context, limit int, lockFor time.Duration) ([]Message, error)
	Ack(ctx context.Context, id int64) error
	Nack(ctx context.Context, id int64, retryAfter time.Duration, lastError string) error
	Depth(ctx context.Context) (int64, error)
}

// Handler delivers a claimed message (at-least-once; must be idempotent).
type Handler func(ctx context.Context, msg Message) error

// Memory is an in-process outbox for tests.
type Memory struct {
	mu      sync.Mutex
	nextID  int64
	pending []Message
}

func NewMemory() *Memory { return &Memory{} }

func (m *Memory) Enqueue(ctx context.Context, msg Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextID++
	msg.ID = m.nextID
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now().UTC()
	}
	if msg.AvailableAt.IsZero() {
		msg.AvailableAt = msg.CreatedAt
	}
	m.pending = append(m.pending, msg)
	return nil
}

func (m *Memory) Claim(ctx context.Context, limit int, lockFor time.Duration) ([]Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now().UTC()
	var out []Message
	for i := range m.pending {
		if limit > 0 && len(out) >= limit {
			break
		}
		if m.pending[i].AvailableAt.After(now) {
			continue
		}
		m.pending[i].Attempts++
		m.pending[i].AvailableAt = now.Add(lockFor)
		out = append(out, m.pending[i])
	}
	return out, nil
}

func (m *Memory) Ack(ctx context.Context, id int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.pending {
		if m.pending[i].ID == id {
			m.pending = append(m.pending[:i], m.pending[i+1:]...)
			return nil
		}
	}
	return nil
}

func (m *Memory) Nack(ctx context.Context, id int64, retryAfter time.Duration, lastError string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.pending {
		if m.pending[i].ID == id {
			m.pending[i].AvailableAt = time.Now().UTC().Add(retryAfter)
			return nil
		}
	}
	return nil
}

func (m *Memory) Depth(ctx context.Context) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return int64(len(m.pending)), nil
}

// Poller drains the outbox via periodic Claim (Postgres table polling — Q6).
type Poller struct {
	Store      Store
	Handler    Handler
	BatchSize  int
	LockFor    time.Duration
	Interval   time.Duration
	RetryAfter time.Duration
}

// RunOnce claims and processes one batch.
func (p *Poller) RunOnce(ctx context.Context) (int, error) {
	limit := p.BatchSize
	if limit <= 0 {
		limit = 10
	}
	lockFor := p.LockFor
	if lockFor <= 0 {
		lockFor = 30 * time.Second
	}
	msgs, err := p.Store.Claim(ctx, limit, lockFor)
	if err != nil {
		return 0, err
	}
	retry := p.RetryAfter
	if retry <= 0 {
		retry = 5 * time.Second
	}
	n := 0
	for _, msg := range msgs {
		if err := p.Handler(ctx, msg); err != nil {
			_ = p.Store.Nack(ctx, msg.ID, retry, err.Error())
			continue
		}
		if err := p.Store.Ack(ctx, msg.ID); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// Run loops until ctx is cancelled.
func (p *Poller) Run(ctx context.Context) error {
	iv := p.Interval
	if iv <= 0 {
		iv = time.Second
	}
	t := time.NewTicker(iv)
	defer t.Stop()
	for {
		if _, err := p.RunOnce(ctx); err != nil && ctx.Err() == nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}
