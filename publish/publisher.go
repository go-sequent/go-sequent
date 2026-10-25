// Package publish provides event publishers (sync in-txn by default).
package publish

import (
	"context"
	"fmt"

	"github.com/wenweih/go-sequent/event"
)

// Handler consumes a single event (projector / workflow entrypoint).
type Handler interface {
	Handles() []string
	Handle(ctx context.Context, e event.Event) error
}

// SyncInTxn publishes events to registered handlers in deterministic order
// inside the write transaction (Q1 / D3).
type SyncInTxn struct {
	handlers map[string][]Handler // event type → handlers
	ordered  []Handler            // registration order for multi-type handlers
}

// NewSyncInTxn creates an empty sync publisher.
func NewSyncInTxn() *SyncInTxn {
	return &SyncInTxn{handlers: make(map[string][]Handler)}
}

// Register adds a handler for each event type it Handles().
func (p *SyncInTxn) Register(h Handler) {
	p.ordered = append(p.ordered, h)
	for _, t := range h.Handles() {
		p.handlers[t] = append(p.handlers[t], h)
	}
}

// Publish invokes handlers for each event in order. Failures abort the txn.
func (p *SyncInTxn) Publish(ctx context.Context, events []event.Event) error {
	for _, e := range events {
		hs := p.handlers[e.EventType()]
		for _, h := range hs {
			if err := h.Handle(ctx, e); err != nil {
				return fmt.Errorf("publish: handler for %s: %w", e.EventType(), err)
			}
		}
	}
	return nil
}
