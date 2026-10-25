// Package memory provides an in-memory EventStore for tests.
package memory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-sequent/go-sequent/event"
	"github.com/go-sequent/go-sequent/event/upcast"
	"github.com/go-sequent/go-sequent/eventstore"
	"github.com/go-sequent/go-sequent/eventstore/hashchain"
	"github.com/google/uuid"
)

// Store is a concurrency-safe in-memory event store.
type Store struct {
	mu         sync.Mutex
	registry   *event.Registry
	upcasters  *upcast.Chain
	streams    map[uuid.UUID]*stream
	commands   []storedCommand
	uniqueKeys map[string]map[string]uuid.UUID // scope → keyJSON → aggregateID
	snapshots  map[uuid.UUID][]eventstore.Snapshot
	needSnap   map[uuid.UUID]bool
	threshold  int
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
	PrevHash    []byte
	EventHash   []byte
}

type storedCommand struct {
	ID   uuid.UUID
	Type string
	Raw  json.RawMessage
}

// New returns an empty memory store. registry is used to deserialize on load.
func New(registry *event.Registry) *Store {
	return &Store{
		registry:   registry,
		streams:    make(map[uuid.UUID]*stream),
		uniqueKeys: make(map[string]map[string]uuid.UUID),
		snapshots:  make(map[uuid.UUID][]eventstore.Snapshot),
		needSnap:   make(map[uuid.UUID]bool),
		threshold:  50,
	}
}

// SetUpcasters attaches a payload upcaster chain (v0).
func (s *Store) SetUpcasters(c *upcast.Chain) { s.upcasters = c }

// SetSnapshotThreshold sets when aggregates are marked for snapshotting.
func (s *Store) SetSnapshotThreshold(n int) { s.threshold = n }

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
		if err := s.checkUniqueKeys(st); err != nil {
			return err
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
		prevHash := append([]byte(nil), hashchain.Genesis...)
		if len(cur.events) > 0 {
			prevHash = append([]byte(nil), cur.events[len(cur.events)-1].EventHash...)
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
			created = hashchain.CanonicalTime(created)
			eventHash := hashchain.Compute(e.EventType(), st.AggregateID, e.SequenceNumber(), created, payload, prevHash)
			cur.events = append(cur.events, storedEvent{
				Type:        e.EventType(),
				Payload:     payload,
				Seq:         e.SequenceNumber(),
				AggregateID: e.AggregateID(),
				CreatedAt:   created,
				CommandID:   cmd.ID,
				TenantID:    cmd.TenantID,
				PrevHash:    append([]byte(nil), prevHash...),
				EventHash:   eventHash,
			})
			prevHash = eventHash
		}
		s.applyUniqueKeys(st)
		tip := int64(0)
		if len(cur.events) > 0 {
			tip = cur.events[len(cur.events)-1].Seq
		}
		if tip >= int64(s.threshold) {
			s.needSnap[st.AggregateID] = true
		}
	}
	return nil
}

func (s *Store) checkUniqueKeys(st eventstore.StreamAppend) error {
	for scope, val := range st.UniqueKeys {
		kb, err := json.Marshal(val)
		if err != nil {
			return err
		}
		key := string(kb)
		byScope := s.uniqueKeys[scope]
		if byScope == nil {
			continue
		}
		if owner, ok := byScope[key]; ok && owner != st.AggregateID {
			return fmt.Errorf("%w: scope=%s key=%s", eventstore.ErrUniqueKeyViolation, scope, key)
		}
	}
	return nil
}

func (s *Store) applyUniqueKeys(st eventstore.StreamAppend) {
	// Drop scopes no longer present for this aggregate.
	for scope, byKey := range s.uniqueKeys {
		for key, owner := range byKey {
			if owner != st.AggregateID {
				continue
			}
			if _, keep := st.UniqueKeys[scope]; !keep {
				delete(byKey, key)
			}
		}
	}
	for scope, val := range st.UniqueKeys {
		kb, _ := json.Marshal(val)
		key := string(kb)
		if s.uniqueKeys[scope] == nil {
			s.uniqueKeys[scope] = make(map[string]uuid.UUID)
		}
		// Remove previous key for this aggregate in scope.
		for k, owner := range s.uniqueKeys[scope] {
			if owner == st.AggregateID && k != key {
				delete(s.uniqueKeys[scope], k)
			}
		}
		s.uniqueKeys[scope][key] = st.AggregateID
	}
}

// LoadStream implements eventstore.Store and verifies the hash chain (Q3).
func (s *Store) LoadStream(ctx context.Context, aggregateID uuid.UUID) ([]event.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.streams[aggregateID]
	if !ok || len(st.events) == 0 {
		return nil, eventstore.ErrStreamNotFound
	}
	if err := s.verifyLocked(st); err != nil {
		return nil, err
	}
	return s.decode(st.events)
}

// VerifyChain validates the per-aggregate SHA-256 hash chain (Q3).
func (s *Store) VerifyChain(ctx context.Context, aggregateID uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.streams[aggregateID]
	if !ok || len(st.events) == 0 {
		return eventstore.ErrStreamNotFound
	}
	return s.verifyLocked(st)
}

func (s *Store) verifyLocked(st *stream) error {
	links := make([]hashchain.Link, 0, len(st.events))
	for _, se := range st.events {
		links = append(links, hashchain.Link{
			EventType:   se.Type,
			AggregateID: se.AggregateID,
			Seq:         se.Seq,
			CreatedAt:   se.CreatedAt,
			EventJSON:   se.Payload,
			PrevHash:    se.PrevHash,
			EventHash:   se.EventHash,
		})
	}
	if err := hashchain.VerifyStream(links); err != nil {
		if errors.Is(err, hashchain.ErrIncomplete) {
			return fmt.Errorf("%w: %v", eventstore.ErrHashChainIncomplete, err)
		}
		return fmt.Errorf("%w: %v", eventstore.ErrHashChainTamper, err)
	}
	return nil
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

// StreamAll returns all events across aggregates ordered by (aggregate_id, seq).
// Used by projection replay.
func (s *Store) StreamAll(ctx context.Context, afterGlobal int64, limit int) ([]event.Event, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	type pair struct {
		e   storedEvent
		idx int64
	}
	var all []pair
	var global int64
	// stable iteration: collect then sort by aggregate id string + seq
	ids := make([]uuid.UUID, 0, len(s.streams))
	for id := range s.streams {
		ids = append(ids, id)
	}
	for i := 0; i < len(ids); i++ {
		for j := i + 1; j < len(ids); j++ {
			if ids[j].String() < ids[i].String() {
				ids[i], ids[j] = ids[j], ids[i]
			}
		}
	}
	for _, id := range ids {
		for _, se := range s.streams[id].events {
			global++
			all = append(all, pair{e: se, idx: global})
		}
	}
	var out []event.Event
	var hw int64
	for _, p := range all {
		if p.idx <= afterGlobal {
			continue
		}
		evs, err := s.decode([]storedEvent{p.e})
		if err != nil {
			return nil, 0, err
		}
		out = append(out, evs...)
		hw = p.idx
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, hw, nil
}

// AggregateTypeOf returns the stored aggregate type for id.
func (s *Store) AggregateTypeOf(id uuid.UUID) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.streams[id]
	if !ok {
		return "", false
	}
	return st.aggregateType, true
}

func (s *Store) decode(stored []storedEvent) ([]event.Event, error) {
	out := make([]event.Event, 0, len(stored))
	for _, se := range stored {
		typ, payload := se.Type, se.Payload
		if s.upcasters != nil {
			var err error
			typ, payload, err = s.upcasters.Apply(typ, payload)
			if err != nil {
				return nil, err
			}
		}
		e, err := s.registry.New(typ)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(payload, e); err != nil {
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

// --- SnapshotStore ---

func (s *Store) SaveSnapshot(ctx context.Context, snap eventstore.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if snap.CreatedAt.IsZero() {
		snap.CreatedAt = time.Now().UTC()
	}
	s.snapshots[snap.AggregateID] = append(s.snapshots[snap.AggregateID], snap)
	return nil
}

func (s *Store) LoadLatestSnapshot(ctx context.Context, aggregateID uuid.UUID, snapshotType string, snapshotVersion int) (*eventstore.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := s.snapshots[aggregateID]
	var best *eventstore.Snapshot
	for i := range list {
		sn := list[i]
		if sn.SnapshotType != snapshotType || sn.SnapshotVersion != snapshotVersion {
			continue
		}
		if best == nil || sn.SequenceNumber > best.SequenceNumber {
			cp := sn
			best = &cp
		}
	}
	return best, nil
}

func (s *Store) MarkSnapshotTaken(ctx context.Context, aggregateID uuid.UUID, sequenceNumber int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.needSnap, aggregateID)
	return nil
}

func (s *Store) ListNeedingSnapshots(ctx context.Context, limit int) ([]uuid.UUID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []uuid.UUID
	for id := range s.needSnap {
		out = append(out, id)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

// MarkNeedsSnapshot forces an aggregate onto the snapshot queue (tests).
func (s *Store) MarkNeedsSnapshot(id uuid.UUID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.needSnap[id] = true
}

var (
	_ eventstore.Store         = (*Store)(nil)
	_ eventstore.TxManager     = (*Store)(nil)
	_ eventstore.SnapshotStore = (*Store)(nil)
)
