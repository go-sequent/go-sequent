// Package bench holds load / conflict benchmarks for the event store.
//
//	go test ./bench/ -bench=. -benchmem
//	GO_SEQUENT_PG_DSN='postgres://…' go test ./bench/ -bench=BenchmarkPostgres -benchmem
package bench_test

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sequent/go-sequent/event"
	"github.com/go-sequent/go-sequent/eventstore"
	"github.com/go-sequent/go-sequent/eventstore/memory"
	"github.com/go-sequent/go-sequent/eventstore/postgres"
	"github.com/google/uuid"
)

type benchEvt struct {
	event.Base
	N int `json:"n"`
}

func (e *benchEvt) EventType() string { return "bench.Evt" }

func newRegistry() *event.Registry {
	r := event.NewRegistry()
	r.Register("bench.Evt", func() event.Event { return &benchEvt{} })
	return r
}

func makeEvent(id uuid.UUID, seq int64, n int) event.Event {
	e := &benchEvt{N: n}
	e.SetEventType("bench.Evt")
	e.SetAggregateID(id)
	e.SetSequenceNumber(seq)
	e.SetCreatedAt(time.Now().UTC())
	return e
}

func appendOne(store eventstore.Store, id uuid.UUID, expected int64, n int) error {
	return store.Append(context.Background(), eventstore.CommandRecord{
		ID: uuid.New(), Type: "bench.Cmd", IssuedAt: time.Now().UTC(),
	}, []eventstore.StreamAppend{{
		AggregateID: id, AggregateType: "bench.Agg",
		ExpectedVersion: expected,
		Events:          []event.Event{makeEvent(id, expected+1, n)},
	}})
}

// BenchmarkMemoryAppendThroughput measures sequential appends on distinct streams.
func BenchmarkMemoryAppendThroughput(b *testing.B) {
	store := memory.New(newRegistry())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := uuid.New()
		if err := appendOne(store, id, 0, i); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMemoryAppendSameStream appends many events to one aggregate (hot stream).
func BenchmarkMemoryAppendSameStream(b *testing.B) {
	store := memory.New(newRegistry())
	id := uuid.New()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := appendOne(store, id, int64(i), i); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMemoryOptimisticConflicts storms concurrent appends with stale ExpectedVersion.
func BenchmarkMemoryOptimisticConflicts(b *testing.B) {
	store := memory.New(newRegistry())
	id := uuid.New()
	if err := appendOne(store, id, 0, 0); err != nil {
		b.Fatal(err)
	}
	var conflicts atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// All workers race with ExpectedVersion=0 while tip is already ≥1.
			err := appendOne(store, id, 0, 1)
			if err == eventstore.ErrOptimisticConcurrency {
				conflicts.Add(1)
			}
		}
	})
	b.StopTimer()
	b.ReportMetric(float64(conflicts.Load())/float64(b.N), "conflicts/op")
}

// BenchmarkMemoryConflictStormWithProgress measures contended correct advances
// (retry until commit). Reports conflict retries vs successful commits.
func BenchmarkMemoryConflictStormWithProgress(b *testing.B) {
	store := memory.New(newRegistry())
	id := uuid.New()
	var tip atomic.Int64
	var conflicts, commits atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for {
				expected := tip.Load()
				err := appendOne(store, id, expected, int(expected))
				if err == nil {
					tip.CompareAndSwap(expected, expected+1)
					commits.Add(1)
					break
				}
				if err == eventstore.ErrOptimisticConcurrency {
					conflicts.Add(1)
					continue
				}
				b.Fatal(err)
			}
		}
	})
	b.StopTimer()
	b.ReportMetric(float64(conflicts.Load()), "conflicts")
	b.ReportMetric(float64(commits.Load()), "commits")
}

func BenchmarkPostgresAppendThroughput(b *testing.B) {
	dsn := os.Getenv("GO_SEQUENT_PG_DSN")
	if dsn == "" {
		b.Skip("set GO_SEQUENT_PG_DSN to run postgres benches")
	}
	ctx := context.Background()
	store, err := postgres.Open(ctx, dsn, newRegistry())
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := uuid.New()
		err := store.WithinTx(ctx, func(ctx context.Context) error {
			return store.Append(ctx, eventstore.CommandRecord{
				ID: uuid.New(), Type: "bench.Cmd", IssuedAt: time.Now().UTC(),
			}, []eventstore.StreamAppend{{
				AggregateID: id, AggregateType: "bench.Agg", ExpectedVersion: 0,
				Events: []event.Event{makeEvent(id, 1, i)},
			}})
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPostgresOptimisticConflicts(b *testing.B) {
	dsn := os.Getenv("GO_SEQUENT_PG_DSN")
	if dsn == "" {
		b.Skip("set GO_SEQUENT_PG_DSN to run postgres benches")
	}
	ctx := context.Background()
	store, err := postgres.Open(ctx, dsn, newRegistry())
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		b.Fatal(err)
	}
	id := uuid.New()
	err = store.WithinTx(ctx, func(ctx context.Context) error {
		return appendOne(store, id, 0, 0)
	})
	if err != nil {
		b.Fatal(err)
	}
	var conflicts atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := store.WithinTx(ctx, func(ctx context.Context) error {
				return appendOne(store, id, 0, 1)
			})
			if err == eventstore.ErrOptimisticConcurrency {
				conflicts.Add(1)
			}
		}
	})
	b.StopTimer()
	b.ReportMetric(float64(conflicts.Load())/float64(b.N), "conflicts/op")
}
