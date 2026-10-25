package eventstore

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/wenweih/go-sequent/event"
)

// StreamAppend is one aggregate's uncommitted events for an Append call.
type StreamAppend struct {
	AggregateID   uuid.UUID
	AggregateType string
	PartitionKey  string
	// ExpectedVersion is the last committed sequence before these events
	// (0 if the stream is new). First new event must be ExpectedVersion+1.
	ExpectedVersion int64
	Events          []event.Event
	// UniqueKeys maps scope → JSON-serializable key value (Sequent parity).
	UniqueKeys map[string]any
}

// CommandRecord is the persistence view of a successful command (keeps
// eventstore free of importing package command — dependency direction).
type CommandRecord struct {
	ID             uuid.UUID
	Type           string
	AggregateID    *uuid.UUID
	TenantID       uuid.UUID
	UserID         uuid.UUID
	IdempotencyKey string
	IssuedAt       time.Time
	Payload        any // marshaled to JSON
}

// Store persists commands and events with optimistic concurrency.
type Store interface {
	// Append writes the command row and all streams atomically.
	Append(ctx context.Context, cmd CommandRecord, streams []StreamAppend) error

	// LoadStream returns all events for aggregate id in sequence order.
	LoadStream(ctx context.Context, aggregateID uuid.UUID) ([]event.Event, error)

	// LoadStreams loads multiple streams; missing ids are omitted.
	LoadStreams(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID][]event.Event, error)
}

// TxManager begins / commits / rolls back DB transactions for CommandService.
type TxManager interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
}

// Publisher publishes newly appended events (e.g. sync projectors in-txn).
type Publisher interface {
	Publish(ctx context.Context, events []event.Event) error
}
