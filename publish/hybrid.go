package publish

import (
	"context"
	"encoding/json"

	"github.com/wenweih/go-sequent/event"
	"github.com/wenweih/go-sequent/publish/outbox"
)

// WithOutbox wraps SyncInTxn so that after sync projectors/workflows run,
// selected event types are also written to the transactional outbox (Q6).
// Ledger projectors stay on SyncInTxn; outbox is for async / external fans.
type WithOutbox struct {
	Inner     *SyncInTxn
	Writer    outbox.Writer
	Topic     string
	EventTypes map[string]bool // if empty, all events are enqueued
}

// Publish runs sync handlers then enqueues matching events.
func (p *WithOutbox) Publish(ctx context.Context, events []event.Event) error {
	if p.Inner != nil {
		if err := p.Inner.Publish(ctx, events); err != nil {
			return err
		}
	}
	if p.Writer == nil {
		return nil
	}
	topic := p.Topic
	if topic == "" {
		topic = "events"
	}
	for _, e := range events {
		if p.EventTypes != nil && !p.EventTypes[e.EventType()] {
			continue
		}
		raw, err := json.Marshal(e)
		if err != nil {
			return err
		}
		if err := p.Writer.Enqueue(ctx, outbox.Message{
			Topic:       topic,
			Payload:     raw,
			AggregateID: e.AggregateID(),
			EventType:   e.EventType(),
		}); err != nil {
			return err
		}
	}
	return nil
}
