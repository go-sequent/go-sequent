package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-sequent/go-sequent/event"
	"github.com/go-sequent/go-sequent/eventstore"
	"github.com/go-sequent/go-sequent/eventstore/postgres"
	"github.com/google/uuid"
)

type testEvt struct {
	event.Base
	Value string `json:"value"`
}

func (e *testEvt) EventType() string { return "pg.Evt" }

func TestPostgresAppendLoad(t *testing.T) {
	dsn := os.Getenv("GO_SEQUENT_PG_DSN")
	if dsn == "" {
		t.Skip("set GO_SEQUENT_PG_DSN to run postgres tests")
	}
	ctx := context.Background()
	reg := event.NewRegistry()
	reg.Register("pg.Evt", func() event.Event { return &testEvt{} })

	store, err := postgres.Open(ctx, dsn, reg)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	id := uuid.New()
	e := &testEvt{Value: "pg"}
	e.SetEventType("pg.Evt")
	e.SetAggregateID(id)
	e.SetSequenceNumber(1)

	err = store.WithinTx(ctx, func(ctx context.Context) error {
		return store.Append(ctx, eventstore.CommandRecord{
			ID:       uuid.New(),
			Type:     "pg.Cmd",
			IssuedAt: time.Now().UTC(),
			Payload:  map[string]string{"x": "1"},
		}, []eventstore.StreamAppend{{
			AggregateID:     id,
			AggregateType:   "pg.Agg",
			ExpectedVersion: 0,
			Events:          []event.Event{e},
		}})
	})
	if err != nil {
		t.Fatal(err)
	}

	loaded, err := store.LoadStream(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if loaded[0].(*testEvt).Value != "pg" {
		t.Fatalf("%#v", loaded)
	}

	err = store.WithinTx(ctx, func(ctx context.Context) error {
		e2 := &testEvt{Value: "conflict"}
		e2.SetAggregateID(id)
		e2.SetSequenceNumber(1)
		return store.Append(ctx, eventstore.CommandRecord{
			ID: uuid.New(), Type: "pg.Cmd", IssuedAt: time.Now().UTC(),
		}, []eventstore.StreamAppend{{
			AggregateID:     id,
			AggregateType:   "pg.Agg",
			ExpectedVersion: 0,
			Events:          []event.Event{e2},
		}})
	})
	if !errors.Is(err, eventstore.ErrOptimisticConcurrency) {
		t.Fatalf("got %v", err)
	}
}

// Concurrent Store instances (as in go test ./pkgA ./pkgB sharing one DSN) must
// not fail ensureType on command_types / event_types unique keys.
func TestPostgresEnsureTypeConcurrent(t *testing.T) {
	dsn := os.Getenv("GO_SEQUENT_PG_DSN")
	if dsn == "" {
		t.Skip("set GO_SEQUENT_PG_DSN to run postgres tests")
	}
	ctx := context.Background()
	reg := event.NewRegistry()
	reg.Register("pg.Evt", func() event.Event { return &testEvt{} })

	const n = 8
	var wg sync.WaitGroup
	errs := make(chan error, n)
	cmdType := fmt.Sprintf("pg.CmdConcurrent.%d", time.Now().UnixNano())

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			store, err := postgres.Open(ctx, dsn, reg)
			if err != nil {
				errs <- err
				return
			}
			defer store.Close()
			if err := store.Migrate(ctx); err != nil {
				errs <- err
				return
			}
			id := uuid.New()
			e := &testEvt{Value: fmt.Sprintf("%d", i)}
			e.SetAggregateID(id)
			e.SetSequenceNumber(1)
			err = store.WithinTx(ctx, func(ctx context.Context) error {
				return store.Append(ctx, eventstore.CommandRecord{
					ID: uuid.New(), Type: cmdType, IssuedAt: time.Now().UTC(),
				}, []eventstore.StreamAppend{{
					AggregateID: id, AggregateType: "pg.AggConcurrent",
					ExpectedVersion: 0, Events: []event.Event{e},
				}})
			})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}
