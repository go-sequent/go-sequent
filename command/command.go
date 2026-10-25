// Package command defines commands, handlers, middleware, and CommandService.
package command

import (
	"time"

	"github.com/google/uuid"
)

// Command is an intent to change the system. Validated before handlers run.
type Command interface {
	CommandType() string
	AggregateID() (uuid.UUID, bool)
	Meta() *Meta
}

// Meta holds finance-oriented audit / causality metadata (design §3.1).
type Meta struct {
	CommandID      uuid.UUID
	IdempotencyKey string
	UserID         uuid.UUID
	TenantID       uuid.UUID
	// CausedByAggregate / CausedBySequence set when a workflow triggers the command.
	CausedByAggregate uuid.UUID
	CausedBySequence  int64
	IssuedAt          time.Time
	// AuthContext is opaque claims; not necessarily persisted.
	AuthContext any
}

// EnsureMeta fills defaults (CommandID, IssuedAt) in place.
func EnsureMeta(m *Meta) *Meta {
	if m == nil {
		m = &Meta{}
	}
	if m.CommandID == uuid.Nil {
		m.CommandID = uuid.New()
	}
	if m.IssuedAt.IsZero() {
		m.IssuedAt = time.Now().UTC()
	}
	return m
}

// Base embeds common Meta for concrete commands.
type Base struct {
	M Meta
}

func (b *Base) Meta() *Meta { return EnsureMeta(&b.M) }

// Validator is implemented by commands that self-validate.
type Validator interface {
	Validate() error
}
