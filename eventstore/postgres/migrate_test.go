package postgres_test

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/go-sequent/go-sequent/event"
	"github.com/go-sequent/go-sequent/eventstore/postgres"
	"github.com/go-sequent/go-sequent/examples/bank"
)

func TestIT_Migrate_IdempotentAndConcurrent(t *testing.T) {
	dsn := os.Getenv("GO_SEQUENT_PG_DSN")
	if dsn == "" {
		t.Skip("set GO_SEQUENT_PG_DSN")
	}
	ctx := context.Background()
	reg := event.NewRegistry()
	bank.RegisterEvents(reg)

	store, err := postgres.Open(ctx, dsn, reg)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("first migrate: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("second migrate (idempotent): %v", err)
	}

	const n = 8
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := postgres.Open(ctx, dsn, reg)
			if err != nil {
				errs <- err
				return
			}
			defer s.Close()
			errs <- s.Migrate(ctx)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}
