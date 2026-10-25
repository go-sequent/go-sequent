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

// Meta holds finance-oriented audit / causality metadata (design §3.1, §6).
type Meta struct {
	CommandID      uuid.UUID
	IdempotencyKey string
	UserID         uuid.UUID
	TenantID       uuid.UUID
	// CorrelationID groups a business conversation; CausationID links to prior command/event.
	CorrelationID uuid.UUID
	CausationID   uuid.UUID
	// CausedByAggregate / CausedBySequence set when a workflow triggers the command.
	CausedByAggregate uuid.UUID
	CausedBySequence  int64
	IssuedAt          time.Time
	// Source is an optional audit label (e.g. "api", "workflow", "admin").
	Source string
	// AuthContext is opaque claims; not necessarily persisted — never trust client UserID alone.
	AuthContext any
}

// EnsureMeta fills defaults (CommandID, IssuedAt, CorrelationID) in place.
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
	if m.CorrelationID == uuid.Nil {
		m.CorrelationID = m.CommandID
	}
	return m
}

// AuditSnapshot is the persisted audit view of command meta (Phase 3 standard).
type AuditSnapshot struct {
	CommandID      uuid.UUID `json:"command_id"`
	CommandType    string    `json:"command_type,omitempty"`
	TenantID       uuid.UUID `json:"tenant_id,omitempty"`
	UserID         uuid.UUID `json:"user_id,omitempty"`
	IdempotencyKey string    `json:"idempotency_key,omitempty"`
	CorrelationID  uuid.UUID `json:"correlation_id,omitempty"`
	CausationID    uuid.UUID `json:"causation_id,omitempty"`
	Source         string    `json:"source,omitempty"`
	IssuedAt       time.Time `json:"issued_at"`
}

// AuditOf builds an AuditSnapshot from a command.
func AuditOf(cmd Command) AuditSnapshot {
	m := EnsureMeta(cmd.Meta())
	return AuditSnapshot{
		CommandID:      m.CommandID,
		CommandType:    cmd.CommandType(),
		TenantID:       m.TenantID,
		UserID:         m.UserID,
		IdempotencyKey: m.IdempotencyKey,
		CorrelationID:  m.CorrelationID,
		CausationID:    m.CausationID,
		Source:         m.Source,
		IssuedAt:       m.IssuedAt,
	}
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
