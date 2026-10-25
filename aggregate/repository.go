package aggregate

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-sequent/go-sequent/event"
	"github.com/go-sequent/go-sequent/eventstore"
	"github.com/google/uuid"
)

// Factory reconstructs an empty aggregate of a given type for rehydration.
type Factory func(id uuid.UUID) Root

// Repository loads/adds aggregates against the context-scoped UoW and commits
// uncommitted events via the EventStore + Publisher.
type Repository struct {
	Store     eventstore.Store
	Snapshots eventstore.SnapshotStore // optional; enables JSON snapshot load (Phase 2)
	Publisher eventstore.Publisher
	Factories map[string]Factory // aggregate type → factory
}

// NewRepository constructs a repository.
func NewRepository(store eventstore.Store, pub eventstore.Publisher) *Repository {
	return &Repository{
		Store:     store,
		Publisher: pub,
		Factories: make(map[string]Factory),
	}
}

// RegisterFactory registers how to construct an aggregate by AggregateType().
func (r *Repository) RegisterFactory(aggregateType string, f Factory) {
	r.Factories[aggregateType] = f
}

// Add tracks a new aggregate in the current UoW.
func (r *Repository) Add(ctx context.Context, agg Root) error {
	uow := UoWFrom(ctx)
	if uow == nil {
		return fmt.Errorf("aggregate: no UnitOfWork in context")
	}
	return uow.Add(agg)
}

// Load loads an aggregate by id and type into the UoW (identity map).
func (r *Repository) Load(ctx context.Context, id uuid.UUID, aggregateType string) (Root, error) {
	uow := UoWFrom(ctx)
	if uow == nil {
		return nil, fmt.Errorf("aggregate: no UnitOfWork in context")
	}
	if existing, ok := uow.Get(id); ok {
		return existing, nil
	}
	f, ok := r.Factories[aggregateType]
	if !ok {
		return nil, fmt.Errorf("aggregate: no factory for type %q", aggregateType)
	}
	agg, err := r.loadAggregate(ctx, id, f)
	if err != nil {
		return nil, err
	}
	if err := agg.Invariants(); err != nil {
		return nil, err
	}
	uow.Track(agg)
	return agg, nil
}

type snapshotable interface {
	SnapshotVersion() int
	UnmarshalSnapshot(data json.RawMessage) error
}

func (r *Repository) loadAggregate(ctx context.Context, id uuid.UUID, f Factory) (Root, error) {
	agg := f(id)
	fromSeq := int64(0)

	if r.Snapshots != nil {
		ver := 1
		if s, ok := agg.(snapshotable); ok {
			ver = s.SnapshotVersion()
		}
		snap, err := r.Snapshots.LoadLatestSnapshot(ctx, id, agg.AggregateType(), ver)
		if err != nil {
			return nil, err
		}
		if snap != nil {
			if s, ok := agg.(snapshotable); ok {
				if err := s.UnmarshalSnapshot(snap.JSON); err != nil {
					return nil, err
				}
			}
			if sv, ok := agg.(interface{ SetVersion(int64) }); ok {
				sv.SetVersion(snap.SequenceNumber)
			}
			fromSeq = snap.SequenceNumber
		}
	}

	events, err := r.Store.LoadStream(ctx, id)
	if err == eventstore.ErrStreamNotFound {
		if fromSeq == 0 {
			return nil, err
		}
		events = nil
	} else if err != nil {
		return nil, err
	}

	var rest []event.Event
	for _, e := range events {
		if e.SequenceNumber() > fromSeq {
			rest = append(rest, e)
		}
	}
	if err := agg.LoadFromHistory(rest); err != nil {
		return nil, err
	}
	return agg, nil
}

// Exists reports whether a stream has any events.
func (r *Repository) Exists(ctx context.Context, id uuid.UUID) (bool, error) {
	events, err := r.Store.LoadStream(ctx, id)
	if err == eventstore.ErrStreamNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return len(events) > 0, nil
}

// Commit collects uncommitted events from the UoW, appends via EventStore,
// publishes (sync projectors), and marks aggregates committed.
func (r *Repository) Commit(ctx context.Context, cmd eventstore.CommandRecord) error {
	uow := UoWFrom(ctx)
	if uow == nil {
		return fmt.Errorf("aggregate: no UnitOfWork in context")
	}

	var streams []eventstore.StreamAppend
	var allEvents []event.Event
	for _, agg := range uow.Aggregates() {
		evs := agg.UncommittedEvents()
		if len(evs) == 0 {
			continue
		}
		if err := agg.Invariants(); err != nil {
			return err
		}
		streams = append(streams, eventstore.StreamAppend{
			AggregateID:     agg.ID(),
			AggregateType:   agg.AggregateType(),
			PartitionKey:    agg.PartitionKey(),
			ExpectedVersion: agg.Version(),
			Events:          evs,
			UniqueKeys:      agg.UniqueKeys(),
		})
		allEvents = append(allEvents, evs...)
	}

	if len(streams) == 0 {
		return nil
	}

	if err := r.Store.Append(ctx, cmd, streams); err != nil {
		return err
	}

	if r.Publisher != nil {
		if err := r.Publisher.Publish(ctx, allEvents); err != nil {
			return err
		}
	}

	for _, agg := range uow.Aggregates() {
		if m, ok := agg.(interface{ MarkCommitted() }); ok {
			m.MarkCommitted()
		} else {
			agg.ClearUncommitted()
		}
	}
	return nil
}
