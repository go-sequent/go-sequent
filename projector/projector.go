// Package projector defines Projector and Persistor abstractions.
package projector

import (
	"context"

	"github.com/wenweih/go-sequent/event"
	"github.com/wenweih/go-sequent/publish"
)

// Projector projects events into read models. Prefer SyncInTxn for ledgers (Q1).
type Projector interface {
	publish.Handler
	// ManagedTables lists projection tables owned by this projector (replay later).
	ManagedTables() []string
}

// Persistor is the only I/O surface projectors should use (replay-safe).
type Persistor interface {
	Exec(ctx context.Context, sql string, args ...any) error
	QueryRow(ctx context.Context, sql string, args ...any) Row
}

// Row is a minimal single-row scanner.
type Row interface {
	Scan(dest ...any) error
}

// Base helps embed managed-table metadata.
type Base struct {
	Tables []string
}

func (b Base) ManagedTables() []string { return b.Tables }

// EventRouter is a small helper for type-switch style projectors.
type EventRouter struct {
	Base
	types    []string
	handlers map[string]func(ctx context.Context, e event.Event) error
}

// NewEventRouter creates a router projector.
func NewEventRouter(tables []string) *EventRouter {
	return &EventRouter{
		Base:     Base{Tables: tables},
		handlers: make(map[string]func(ctx context.Context, e event.Event) error),
	}
}

// On registers a handler for an event type name.
func (r *EventRouter) On(eventType string, fn func(ctx context.Context, e event.Event) error) {
	r.types = append(r.types, eventType)
	r.handlers[eventType] = fn
}

func (r *EventRouter) Handles() []string { return append([]string(nil), r.types...) }

func (r *EventRouter) Handle(ctx context.Context, e event.Event) error {
	fn, ok := r.handlers[e.EventType()]
	if !ok {
		return nil
	}
	return fn(ctx, e)
}
