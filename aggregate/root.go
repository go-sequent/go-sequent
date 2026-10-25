// Package aggregate provides AggregateRoot base types and repository/UoW.
package aggregate

import (
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
	"github.com/wenweih/go-sequent/event"
)

// Root is the AggregateRoot contract (design §3.3).
type Root interface {
	ID() uuid.UUID
	AggregateType() string
	UncommittedEvents() []event.Event
	ClearUncommitted()
	Version() int64
	LoadFromHistory(events []event.Event) error
	Invariants() error
	UniqueKeys() map[string]any
	PartitionKey() string
}

// Applier mutates aggregate state from an event. Must be side-effect free
// aside from state mutation (no I/O).
type Applier func(e event.Event)

// Base provides Apply, sequence allocation, and uncommitted buffering.
// Embed in domain aggregates and register appliers in the constructor.
type Base struct {
	id             uuid.UUID
	aggregateType  string
	version        int64 // last committed sequence
	uncommitted    []event.Event
	appliers       map[reflect.Type]Applier
	appliersByName map[string]Applier
	partitionKey   string
	uniqueKeys     map[string]any
}

// Init sets identity. Call from aggregate constructors before Apply.
func (b *Base) Init(id uuid.UUID, aggregateType string) {
	b.id = id
	b.aggregateType = aggregateType
	if b.appliers == nil {
		b.appliers = make(map[reflect.Type]Applier)
	}
	if b.appliersByName == nil {
		b.appliersByName = make(map[string]Applier)
	}
}

func (b *Base) ID() uuid.UUID              { return b.id }
func (b *Base) AggregateType() string      { return b.aggregateType }
func (b *Base) Version() int64             { return b.version }
func (b *Base) UncommittedEvents() []event.Event {
	out := make([]event.Event, len(b.uncommitted))
	copy(out, b.uncommitted)
	return out
}
func (b *Base) ClearUncommitted() {
	b.uncommitted = nil
}
func (b *Base) UniqueKeys() map[string]any {
	if b.uniqueKeys == nil {
		return nil
	}
	out := make(map[string]any, len(b.uniqueKeys))
	for k, v := range b.uniqueKeys {
		out[k] = v
	}
	return out
}

// SetUniqueKeys replaces the unique-key map (scope → value) for commit.
func (b *Base) SetUniqueKeys(keys map[string]any) {
	b.uniqueKeys = keys
}

func (b *Base) PartitionKey() string {
	if b.partitionKey != "" {
		return b.partitionKey
	}
	return ""
}
func (b *Base) SetPartitionKey(k string) { b.partitionKey = k }

// SetVersion sets the last committed sequence (used when restoring snapshots).
func (b *Base) SetVersion(v int64) { b.version = v }

// Invariants default to nil; override on the concrete aggregate.
func (b *Base) Invariants() error { return nil }

// On registers an applier for a concrete event pointer/value type.
func (b *Base) On(sample event.Event, fn Applier) {
	if b.appliers == nil {
		b.appliers = make(map[reflect.Type]Applier)
	}
	if b.appliersByName == nil {
		b.appliersByName = make(map[string]Applier)
	}
	t := reflect.TypeOf(sample)
	b.appliers[t] = fn
	b.appliersByName[sample.EventType()] = fn
}

// Apply records a new domain event: assigns envelope fields, mutates state,
// and buffers for commit.
func (b *Base) Apply(e event.Event) {
	seq := b.version + int64(len(b.uncommitted)) + 1
	if env, ok := e.(event.Envelope); ok {
		env.SetAggregateID(b.id)
		env.SetSequenceNumber(seq)
		if env.CreatedAt().IsZero() {
			env.SetCreatedAt(time.Now().UTC())
		}
	}
	b.applyState(e)
	b.uncommitted = append(b.uncommitted, e)
}

// LoadFromHistory replays committed events (no uncommitted buffer).
func (b *Base) LoadFromHistory(events []event.Event) error {
	for _, e := range events {
		if e.AggregateID() != b.id && b.id != uuid.Nil {
			return fmt.Errorf("aggregate: event aggregate_id mismatch")
		}
		b.applyState(e)
		b.version = e.SequenceNumber()
	}
	return nil
}

// MarkCommitted advances version after a successful store append and clears buffer.
func (b *Base) MarkCommitted() {
	if n := len(b.uncommitted); n > 0 {
		b.version = b.uncommitted[n-1].SequenceNumber()
	}
	b.uncommitted = nil
}

func (b *Base) applyState(e event.Event) {
	t := reflect.TypeOf(e)
	if fn, ok := b.appliers[t]; ok {
		fn(e)
		return
	}
	if fn, ok := b.appliersByName[e.EventType()]; ok {
		fn(e)
		return
	}
	panic(fmt.Sprintf("aggregate %s: no applier for event %T (%s)", b.aggregateType, e, e.EventType()))
}
