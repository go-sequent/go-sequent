package memory_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wenweih/go-sequent/event"
	"github.com/wenweih/go-sequent/eventstore"
	"github.com/wenweih/go-sequent/eventstore/memory"
)

type testEvt struct {
	event.Base
	Value string `json:"value"`
}

func (e *testEvt) EventType() string { return "test.Evt" }

func TestAppendAndLoad(t *testing.T) {
	reg := event.NewRegistry()
	reg.Register("test.Evt", func() event.Event {
		return &testEvt{}
	})
	store := memory.New(reg)
	ctx := context.Background()
	id := uuid.New()

	e := &testEvt{Value: "hello"}
	e.SetEventType("test.Evt")
	e.SetAggregateID(id)
	e.SetSequenceNumber(1)

	cmd := eventstore.CommandRecord{
		ID:       uuid.New(),
		Type:     "test.Cmd",
		IssuedAt: time.Now().UTC(),
		Payload:  map[string]string{"ok": "1"},
	}
	err := store.Append(ctx, cmd, []eventstore.StreamAppend{{
		AggregateID:     id,
		AggregateType:   "test.Agg",
		ExpectedVersion: 0,
		Events:          []event.Event{e},
	}})
	if err != nil {
		t.Fatal(err)
	}

	loaded, err := store.LoadStream(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 1 || loaded[0].(*testEvt).Value != "hello" {
		t.Fatalf("unexpected %#v", loaded)
	}

	e2 := &testEvt{Value: "world"}
	e2.SetEventType("test.Evt")
	e2.SetAggregateID(id)
	e2.SetSequenceNumber(2)
	cmd2 := eventstore.CommandRecord{ID: uuid.New(), Type: "test.Cmd", IssuedAt: time.Now().UTC()}
	if err := store.Append(ctx, cmd2, []eventstore.StreamAppend{{
		AggregateID:     id,
		AggregateType:   "test.Agg",
		ExpectedVersion: 1,
		Events:          []event.Event{e2},
	}}); err != nil {
		t.Fatal(err)
	}

	e3 := &testEvt{Value: "nope"}
	e3.SetEventType("test.Evt")
	e3.SetAggregateID(id)
	e3.SetSequenceNumber(2)
	err = store.Append(ctx, eventstore.CommandRecord{ID: uuid.New(), Type: "test.Cmd"}, []eventstore.StreamAppend{{
		AggregateID:     id,
		AggregateType:   "test.Agg",
		ExpectedVersion: 1,
		Events:          []event.Event{e3},
	}})
	if !errors.Is(err, eventstore.ErrOptimisticConcurrency) {
		t.Fatalf("got %v", err)
	}
}
