// Package memory provides an in-memory EventStore for tests.
package memory

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wenweih/go-sequent/event"
	"github.com/wenweih/go-sequent/eventstore"
)

// Store is a concurrency-safe in-memory event store.
type Store struct {
	mu       sync.Mutex
	registry *event.Registry
	streams  map[uuid.UUID]*stream
	commands []storedCommand
}

type stream struct {
	aggregateType string
	partitionKey  string
	events        []storedEvent
}

type storedEvent struct {
	Type        string
	Payload     json.RawMessage
	Seq         int64
	AggregateID uuid.UUID
	CreatedAt   time.Time
	CommandID   uuid.UUID
	TenantID    uuid.UUID
}

type storedCommand struct {
	ID   uuid.UUID
	Type string
	Raw  json.RawMessage
}

// New returns an empty memory store. registry is used to deserialize on load.
func New(registry *event.Registry) *Store {
	return &Store{
		registry: registry,
		streams:  make(map[uuid.UUID]*stream),
	}
}

// WithinTx is a no-op transaction scope (still serializes via Store.mu on Append).
func (s *Store) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	return fn(ctx)
}

// Append implements eventstore.Store.
func (s *Store) Append(ctx context.Context, cmd eventstore.CommandRecord, streams []eventstore.StreamAppend) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	rawCmd, err := json.Marshal(cmd.Payload)
	if err != nil {
		return err
	}
	if cmd.Payload == nil {
		rawCmd, err = json.Marshal(cmd)
		if err != nil {
			return err
		}
	}

	for _, st := range streams {
		cur := s.streams[st.AggregateID]
		var tip int64
		if cur != nil && len(cur.events) > 0 {
			tip = cur.events[len(cur.events)-1].Seq
		}
		if tip != st.ExpectedVersion {
			return eventstore.ErrOptimisticConcurrency
		}
		if len(st.Events) == 0 {
			continue
		}
		if st.Events[0].SequenceNumber() != tip+1 {
			return eventstore.ErrOptimisticConcurrency
		}
		for i := 1; i < len(st.Events); i++ {
			if st.Events[i].SequenceNumber() != st.Events[i-1].SequenceNumber()+1 {
				return eventstore.ErrOptimisticConcurrency
			}
		}
	}

	s.commands = append(s.commands, storedCommand{
		ID:   cmd.ID,
		Type: cmd.Type,
		Raw:  rawCmd,
	})

	for _, st := range streams {
		cur := s.streams[st.AggregateID]
		if cur == nil {
			cur = &stream{aggregateType: st.AggregateType, partitionKey: st.PartitionKey}
			s.streams[st.AggregateID] = cur
		}
		for _, e := range st.Events {
			payload, err := json.Marshal(e)
			if err != nil {
				return err
			}
			created := e.CreatedAt()
			if created.IsZero() {
				created = time.Now().UTC()
			}
			cur.events = append(cur.events, storedEvent{
				Type:        e.EventType(),
				Payload:     payload,
				Seq:         e.SequenceNumber(),
				AggregateID: e.AggregateID(),
				CreatedAt:   created,
				CommandID:   cmd.ID,
				TenantID:    cmd.TenantID,
			})
		}
	}
	return nil
}

// LoadStream implements eventstore.Store.
func (s *Store) LoadStream(ctx context.Context, aggregateID uuid.UUID) ([]event.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.streams[aggregateID]
	if !ok || len(st.events) == 0 {
		return nil, eventstore.ErrStreamNotFound
	}
	return s.decode(st.events)
}

// LoadStreams implements eventstore.Store.
func (s *Store) LoadStreams(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID][]event.Event, error) {
	out := make(map[uuid.UUID][]event.Event, len(ids))
	for _, id := range ids {
		evs, err := s.LoadStream(ctx, id)
		if err == eventstore.ErrStreamNotFound {
			continue
		}
		if err != nil {
			return nil, err
		}
		out[id] = evs
	}
	return out, nil
}

func (s *Store) decode(stored []storedEvent) ([]event.Event, error) {
	out := make([]event.Event, 0, len(stored))
	for _, se := range stored {
		e, err := s.registry.New(se.Type)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(se.Payload, e); err != nil {
			return nil, err
		}
		if env, ok := e.(event.Envelope); ok {
			env.SetAggregateID(se.AggregateID)
			env.SetSequenceNumber(se.Seq)
			env.SetCreatedAt(se.CreatedAt)
		}
		out = append(out, e)
	}
	return out, nil
}

var (
	_ eventstore.Store     = (*Store)(nil)
	_ eventstore.TxManager = (*Store)(nil)
)
