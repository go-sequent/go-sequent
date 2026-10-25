// Package idempotency provides tenant-scoped idempotency keys (Q4 / design §6.2).
package idempotency

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-sequent/go-sequent/command"
	"github.com/google/uuid"
)

// ErrReplay indicates the idempotency key was already successfully processed.
// Returned only when Middleware is configured with Strict=true.
var ErrReplay = errors.New("idempotency: key already processed")

// Record is a stored idempotency outcome.
type Record struct {
	TenantID    uuid.UUID
	Key         string
	CommandID   uuid.UUID
	CommandType string
	Status      string // succeeded
	CreatedAt   time.Time
}

// Store persists tenant-scoped idempotency keys (same txn as command success).
type Store interface {
	Get(ctx context.Context, tenantID uuid.UUID, key string) (*Record, error)
	Put(ctx context.Context, rec Record) error
}

// Memory is an in-process Store for tests.
type Memory struct {
	mu   sync.Mutex
	byTK map[string]Record
}

func NewMemory() *Memory {
	return &Memory{byTK: make(map[string]Record)}
}

func memKey(tenant uuid.UUID, key string) string {
	return tenant.String() + "|" + key
}

func (m *Memory) Get(ctx context.Context, tenantID uuid.UUID, key string) (*Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.byTK[memKey(tenantID, key)]
	if !ok {
		return nil, nil
	}
	cp := rec
	return &cp, nil
}

func (m *Memory) Put(ctx context.Context, rec Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := memKey(rec.TenantID, rec.Key)
	if existing, ok := m.byTK[k]; ok {
		if existing.CommandID != rec.CommandID {
			return fmt.Errorf("%w: conflicting command", ErrReplay)
		}
		return nil
	}
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = time.Now().UTC()
	}
	if rec.Status == "" {
		rec.Status = "succeeded"
	}
	m.byTK[k] = rec
	return nil
}

type replayKey struct{}

// PriorRecord returns the prior idempotency record when Execute was a no-op replay.
func PriorRecord(ctx context.Context) (*Record, bool) {
	rec, ok := ctx.Value(replayKey{}).(*Record)
	return rec, ok && rec != nil
}

// Configures middleware behavior.
type Config struct {
	Store Store
	// Strict returns ErrReplay on duplicate keys instead of silent no-op success.
	Strict bool
}

// Middleware short-circuits duplicate keys (tenant-scoped) and records success
// after the handler chain in the same transaction.
func Middleware(cfg Config) command.Middleware {
	return func(ctx context.Context, cmd command.Command, next command.HandlerFunc) error {
		if cfg.Store == nil {
			return next(ctx, cmd)
		}
		meta := command.EnsureMeta(cmd.Meta())
		if meta.IdempotencyKey == "" {
			return next(ctx, cmd)
		}
		if meta.TenantID == uuid.Nil {
			return fmt.Errorf("idempotency: TenantID required when IdempotencyKey is set")
		}
		existing, err := cfg.Store.Get(ctx, meta.TenantID, meta.IdempotencyKey)
		if err != nil {
			return err
		}
		if existing != nil {
			if cfg.Strict {
				return fmt.Errorf("%w: command_id=%s type=%s", ErrReplay, existing.CommandID, existing.CommandType)
			}
			// No-op success for retries.
			return nil
		}
		if err := next(ctx, cmd); err != nil {
			return err
		}
		return cfg.Store.Put(ctx, Record{
			TenantID:    meta.TenantID,
			Key:         meta.IdempotencyKey,
			CommandID:   meta.CommandID,
			CommandType: cmd.CommandType(),
			Status:      "succeeded",
			CreatedAt:   time.Now().UTC(),
		})
	}
}

// IsReplay reports whether err is (or wraps) ErrReplay.
func IsReplay(err error) bool {
	return errors.Is(err, ErrReplay)
}
