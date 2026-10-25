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
	projectors map[string][]Handler
	workflows  map[string][]Handler
}

// NewSyncInTxn creates an empty sync publisher.
func NewSyncInTxn() *SyncInTxn {
	return &SyncInTxn{
		projectors: make(map[string][]Handler),
		workflows:  make(map[string][]Handler),
	}
}

// Register adds a projector handler (included in projection rebuild).
func (p *SyncInTxn) Register(h Handler) {
	for _, t := range h.Handles() {
		p.projectors[t] = append(p.projectors[t], h)
	}
}

// RegisterWorkflow adds a workflow handler (excluded from projection rebuild).
func (p *SyncInTxn) RegisterWorkflow(h Handler) {
	for _, t := range h.Handles() {
		p.workflows[t] = append(p.workflows[t], h)
	}
}

// Publish invokes projectors then workflows for each event. Failures abort the txn.
func (p *SyncInTxn) Publish(ctx context.Context, events []event.Event) error {
	for _, e := range events {
		for _, h := range p.projectors[e.EventType()] {
			if err := h.Handle(ctx, e); err != nil {
				return fmt.Errorf("publish: projector for %s: %w", e.EventType(), err)
			}
		}
		for _, h := range p.workflows[e.EventType()] {
			if err := h.Handle(ctx, e); err != nil {
				return fmt.Errorf("publish: workflow for %s: %w", e.EventType(), err)
			}
		}
	}
	return nil
}

// PublishProjectorsOnly skips workflows (used by replay).
func (p *SyncInTxn) PublishProjectorsOnly(ctx context.Context, events []event.Event) error {
	for _, e := range events {
		for _, h := range p.projectors[e.EventType()] {
			if err := h.Handle(ctx, e); err != nil {
				return fmt.Errorf("publish: projector for %s: %w", e.EventType(), err)
			}
		}
	}
	return nil
}
