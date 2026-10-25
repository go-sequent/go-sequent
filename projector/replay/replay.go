// Package replay implements Sequent-style projection rebuild stages.
package replay

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-sequent/go-sequent/event"
	"github.com/go-sequent/go-sequent/publish"
)

// Stage is a rebuild pipeline step.
type Stage string

const (
	StagePrepare  Stage = "preparing"
	StageReplay   Stage = "replaying"
	StageCatchup  Stage = "catching_up"
	StageOptimize Stage = "optimizing"
	StageLive     Stage = "live"
	StageAborted  Stage = "aborted"
)

// EventSource streams events for rebuild (memory or postgres).
type EventSource interface {
	// StreamAll returns events after the global cursor, plus new high-water mark.
	StreamAll(ctx context.Context, afterGlobal int64, limit int) (events []event.Event, highWater int64, err error)
}

// SchemaManager prepares / swaps projection schemas (SQL backends).
type SchemaManager interface {
	Prepare(ctx context.Context, replaySchema string, managedTables []string) error
	Optimize(ctx context.Context, replaySchema string) error
	GoLive(ctx context.Context, replaySchema, viewSchema string, managedTables []string) error
	Abort(ctx context.Context, replaySchema string) error
}

// StateStore persists rebuild progress.
type StateStore interface {
	Save(ctx context.Context, st State) error
	Load(ctx context.Context, projectorName string) (*State, error)
}

// State is replay bookkeeping.
type State struct {
	ProjectorName string
	ReplaySchema  string
	ViewSchema    string
	Status        Stage
	HighWaterMark int64
	UpdatedAt     time.Time
}

// MemoryState is an in-process StateStore for tests / demos.
type MemoryState struct {
	mu     sync.Mutex
	byName map[string]State
}

func NewMemoryState() *MemoryState {
	return &MemoryState{byName: make(map[string]State)}
}

func (m *MemoryState) Save(ctx context.Context, st State) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	st.UpdatedAt = time.Now().UTC()
	m.byName[st.ProjectorName] = st
	return nil
}

func (m *MemoryState) Load(ctx context.Context, projectorName string) (*State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, ok := m.byName[projectorName]
	if !ok {
		return nil, nil
	}
	cp := st
	return &cp, nil
}

// Pipeline runs prepare → replay → catchup → optimize → golive.
type Pipeline struct {
	Name          string
	Source        EventSource
	Publisher     *publish.SyncInTxn // projectors only via PublishProjectorsOnly
	ManagedTables []string
	Schema        SchemaManager // optional
	State         StateStore
	ReplaySchema  string
	ViewSchema    string
	BatchSize     int
}

// Prepare creates the replay schema (or no-op for in-memory).
func (p *Pipeline) Prepare(ctx context.Context) error {
	rs := p.replaySchema()
	vs := p.viewSchema()
	if p.Schema != nil {
		if err := p.Schema.Prepare(ctx, rs, p.ManagedTables); err != nil {
			return err
		}
	}
	return p.save(ctx, StagePrepare, 0, rs, vs)
}

// Replay projects historical events into the replay target.
func (p *Pipeline) Replay(ctx context.Context) error {
	if p.Publisher == nil || p.Source == nil {
		return fmt.Errorf("replay: Publisher and Source required")
	}
	batch := p.BatchSize
	if batch <= 0 {
		batch = 1000
	}
	var hw int64
	for {
		evs, next, err := p.Source.StreamAll(ctx, hw, batch)
		if err != nil {
			return err
		}
		if len(evs) == 0 {
			break
		}
		if err := p.Publisher.PublishProjectorsOnly(ctx, evs); err != nil {
			return err
		}
		hw = next
		if err := p.save(ctx, StageReplay, hw, p.replaySchema(), p.viewSchema()); err != nil {
			return err
		}
		if len(evs) < batch {
			break
		}
	}
	return p.save(ctx, StageReplay, hw, p.replaySchema(), p.viewSchema())
}

// Catchup applies events appended since the last high-water mark.
func (p *Pipeline) Catchup(ctx context.Context) error {
	st, err := p.State.Load(ctx, p.Name)
	if err != nil {
		return err
	}
	hw := int64(0)
	if st != nil {
		hw = st.HighWaterMark
	}
	evs, next, err := p.Source.StreamAll(ctx, hw, p.BatchSize)
	if err != nil {
		return err
	}
	if len(evs) > 0 {
		if err := p.Publisher.PublishProjectorsOnly(ctx, evs); err != nil {
			return err
		}
		hw = next
	}
	return p.save(ctx, StageCatchup, hw, p.replaySchema(), p.viewSchema())
}

// Optimize runs ANALYZE / index rebuilds when SchemaManager is present.
func (p *Pipeline) Optimize(ctx context.Context) error {
	if p.Schema != nil {
		if err := p.Schema.Optimize(ctx, p.replaySchema()); err != nil {
			return err
		}
	}
	st, _ := p.State.Load(ctx, p.Name)
	hw := int64(0)
	if st != nil {
		hw = st.HighWaterMark
	}
	return p.save(ctx, StageOptimize, hw, p.replaySchema(), p.viewSchema())
}

// GoLive swaps replay tables into the view schema.
func (p *Pipeline) GoLive(ctx context.Context) error {
	if p.Schema != nil {
		if err := p.Schema.GoLive(ctx, p.replaySchema(), p.viewSchema(), p.ManagedTables); err != nil {
			return err
		}
	}
	st, _ := p.State.Load(ctx, p.Name)
	hw := int64(0)
	if st != nil {
		hw = st.HighWaterMark
	}
	return p.save(ctx, StageLive, hw, p.replaySchema(), p.viewSchema())
}

// Abort drops the replay schema.
func (p *Pipeline) Abort(ctx context.Context) error {
	if p.Schema != nil {
		if err := p.Schema.Abort(ctx, p.replaySchema()); err != nil {
			return err
		}
	}
	return p.save(ctx, StageAborted, 0, p.replaySchema(), p.viewSchema())
}

// RunAll executes the full pipeline in order.
func (p *Pipeline) RunAll(ctx context.Context) error {
	for _, step := range []func(context.Context) error{
		p.Prepare, p.Replay, p.Catchup, p.Optimize, p.GoLive,
	} {
		if err := step(ctx); err != nil {
			_ = p.Abort(ctx)
			return err
		}
	}
	return nil
}

func (p *Pipeline) replaySchema() string {
	if p.ReplaySchema != "" {
		return p.ReplaySchema
	}
	return "replay_" + p.Name
}

func (p *Pipeline) viewSchema() string {
	if p.ViewSchema != "" {
		return p.ViewSchema
	}
	return "public"
}

func (p *Pipeline) save(ctx context.Context, stage Stage, hw int64, rs, vs string) error {
	if p.State == nil {
		return nil
	}
	return p.State.Save(ctx, State{
		ProjectorName: p.Name,
		ReplaySchema:  rs,
		ViewSchema:    vs,
		Status:        stage,
		HighWaterMark: hw,
	})
}

// NoopSchema is a SchemaManager that does nothing (in-memory projectors).
type NoopSchema struct{}

func (NoopSchema) Prepare(context.Context, string, []string) error { return nil }
func (NoopSchema) Optimize(context.Context, string) error          { return nil }
func (NoopSchema) GoLive(context.Context, string, string, []string) error {
	return nil
}
func (NoopSchema) Abort(context.Context, string) error { return nil }
