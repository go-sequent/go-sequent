// Package workflow provides process managers and AfterCommit hooks.
package workflow

import (
	"context"

	"github.com/go-sequent/go-sequent/event"
	"github.com/go-sequent/go-sequent/publish"
)

// Workflow reacts to events for orchestration. Excluded from projector replay.
type Workflow interface {
	publish.Handler
}

// AfterCommitFunc runs after the write transaction commits successfully.
type AfterCommitFunc func(ctx context.Context) error

type hookKey struct{}

type hookBag struct {
	fns []AfterCommitFunc
}

// WithHooks attaches an AfterCommit bag to ctx (one per Execute transaction).
func WithHooks(ctx context.Context) (context.Context, *hookBag) {
	bag := &hookBag{}
	return context.WithValue(ctx, hookKey{}, bag), bag
}

// RegisterAfterCommit queues fn to run after successful txn commit.
func RegisterAfterCommit(ctx context.Context, fn AfterCommitFunc) {
	if bag, ok := ctx.Value(hookKey{}).(*hookBag); ok && bag != nil {
		bag.fns = append(bag.fns, fn)
	}
}

// Run executes all registered AfterCommit hooks in order.
func (b *hookBag) Run(ctx context.Context) error {
	if b == nil {
		return nil
	}
	for _, fn := range b.fns {
		if err := fn(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Router publishes to workflows only (separate from projectors for replay exclusion).
type Router struct {
	handlers map[string][]Workflow
}

// NewRouter creates an empty workflow router.
func NewRouter() *Router {
	return &Router{handlers: make(map[string][]Workflow)}
}

// Register adds a workflow for each event type it Handles().
func (r *Router) Register(w Workflow) {
	for _, t := range w.Handles() {
		r.handlers[t] = append(r.handlers[t], w)
	}
}

// Publish delivers events to workflows (still SyncInTxn when called from publisher).
func (r *Router) Publish(ctx context.Context, events []event.Event) error {
	for _, e := range events {
		for _, w := range r.handlers[e.EventType()] {
			if err := w.Handle(ctx, e); err != nil {
				return err
			}
		}
	}
	return nil
}
