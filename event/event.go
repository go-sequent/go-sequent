// Package event defines the Event interface and type registry.
package event

import (
	"time"

	"github.com/google/uuid"
)

// Event is a past-tense domain fact applied to an aggregate stream.
type Event interface {
	EventType() string
	AggregateID() uuid.UUID
	SequenceNumber() int64
	CreatedAt() time.Time
}

// Envelope carries store-level metadata set by the aggregate Apply path
// and/or EventStore. Domain payload structs typically embed Base.
type Envelope interface {
	Event
	SetAggregateID(uuid.UUID)
	SetSequenceNumber(int64)
	SetCreatedAt(time.Time)
}

// Base provides common event envelope fields for embedding.
type Base struct {
	aggregateID    uuid.UUID
	sequenceNumber int64
	createdAt      time.Time
	eventType      string
}

// NewBase constructs a Base with a fixed type name (usually set once in the
// concrete event constructor).
func NewBase(eventType string) Base {
	return Base{eventType: eventType}
}

func (b *Base) EventType() string           { return b.eventType }
func (b *Base) AggregateID() uuid.UUID      { return b.aggregateID }
func (b *Base) SequenceNumber() int64       { return b.sequenceNumber }
func (b *Base) CreatedAt() time.Time        { return b.createdAt }
func (b *Base) SetAggregateID(id uuid.UUID) { b.aggregateID = id }
func (b *Base) SetSequenceNumber(n int64)   { b.sequenceNumber = n }
func (b *Base) SetCreatedAt(t time.Time)    { b.createdAt = t }
func (b *Base) SetEventType(t string)       { b.eventType = t }

// Ensure Base implements Envelope.
var _ Envelope = (*Base)(nil)
