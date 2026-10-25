// Package snapshot provides JSON snapshot helpers and a background snapshotter (Q8).
package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/wenweih/go-sequent/aggregate"
	"github.com/wenweih/go-sequent/event"
	"github.com/wenweih/go-sequent/eventstore"
)

// Snapshotable aggregates can dump/restore JSON state independently of events.
type Snapshotable interface {
	SnapshotVersion() int
	MarshalSnapshot() (json.RawMessage, error)
	UnmarshalSnapshot(data json.RawMessage) error
}

// Take builds a Snapshot from a loaded Snapshotable aggregate.
func Take(agg aggregate.Root) (eventstore.Snapshot, error) {
	s, ok := agg.(Snapshotable)
	if !ok {
		return eventstore.Snapshot{}, fmt.Errorf("snapshot: aggregate %T is not Snapshotable", agg)
	}
	raw, err := s.MarshalSnapshot()
	if err != nil {
		return eventstore.Snapshot{}, err
	}
	return eventstore.Snapshot{
		AggregateID:     agg.ID(),
		SequenceNumber:  agg.Version(),
		SnapshotType:    agg.AggregateType(),
		SnapshotVersion: s.SnapshotVersion(),
		JSON:            raw,
		CreatedAt:       time.Now().UTC(),
	}, nil
}

// Rehydrate loads optional snapshot + subsequent events into factory(id).
func Rehydrate(
	ctx context.Context,
	store eventstore.Store,
	snaps eventstore.SnapshotStore,
	id uuid.UUID,
	factory aggregate.Factory,
) (aggregate.Root, error) {
	agg := factory(id)
	fromSeq := int64(0)

	if snaps != nil {
		ver := 1
		if s, ok := agg.(Snapshotable); ok {
			ver = s.SnapshotVersion()
		}
		snap, err := snaps.LoadLatestSnapshot(ctx, id, agg.AggregateType(), ver)
		if err != nil {
			return nil, err
		}
		if snap != nil {
			if s, ok := agg.(Snapshotable); ok {
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

	events, err := store.LoadStream(ctx, id)
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

// Snapshotter periodically takes snapshots for aggregates that need them.
type Snapshotter struct {
	Store       eventstore.Store
	Snapshots   eventstore.SnapshotStore
	Factories   map[string]aggregate.Factory
	ResolveType func(ctx context.Context, id uuid.UUID) (string, error)
	Interval    time.Duration
	BatchSize   int
}

// RunOnce processes up to BatchSize aggregates needing snapshots.
func (s *Snapshotter) RunOnce(ctx context.Context) (int, error) {
	if s.Snapshots == nil {
		return 0, fmt.Errorf("snapshot: SnapshotStore required")
	}
	limit := s.BatchSize
	if limit <= 0 {
		limit = 10
	}
	ids, err := s.Snapshots.ListNeedingSnapshots(ctx, limit)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, id := range ids {
		typ, err := s.ResolveType(ctx, id)
		if err != nil {
			return n, err
		}
		f, ok := s.Factories[typ]
		if !ok {
			return n, fmt.Errorf("snapshot: no factory for %q", typ)
		}
		agg := f(id)
		events, err := s.Store.LoadStream(ctx, id)
		if err != nil {
			return n, err
		}
		if err := agg.LoadFromHistory(events); err != nil {
			return n, err
		}
		snap, err := Take(agg)
		if err != nil {
			return n, err
		}
		if err := s.Snapshots.SaveSnapshot(ctx, snap); err != nil {
			return n, err
		}
		if err := s.Snapshots.MarkSnapshotTaken(ctx, id, snap.SequenceNumber); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// Run loops until ctx is cancelled.
func (s *Snapshotter) Run(ctx context.Context) error {
	iv := s.Interval
	if iv <= 0 {
		iv = time.Minute
	}
	t := time.NewTicker(iv)
	defer t.Stop()
	for {
		if _, err := s.RunOnce(ctx); err != nil && ctx.Err() == nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}
