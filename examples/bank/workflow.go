package bank

import (
	"context"
	"sync"

	"github.com/wenweih/go-sequent/event"
	"github.com/wenweih/go-sequent/workflow"
)

// NotifyWorkflow records opened accounts and schedules an AfterCommit side effect.
type NotifyWorkflow struct {
	mu      sync.Mutex
	Opened  []string
	Notified []string
}

func (w *NotifyWorkflow) Handles() []string { return []string{EventAccountOpened} }

func (w *NotifyWorkflow) Handle(ctx context.Context, e event.Event) error {
	ev := e.(*AccountOpened)
	w.mu.Lock()
	w.Opened = append(w.Opened, ev.Owner)
	owner := ev.Owner
	w.mu.Unlock()

	workflow.RegisterAfterCommit(ctx, func(ctx context.Context) error {
		w.mu.Lock()
		w.Notified = append(w.Notified, owner)
		w.mu.Unlock()
		return nil
	})
	return nil
}
