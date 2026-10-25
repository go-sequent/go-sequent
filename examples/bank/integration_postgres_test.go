package bank_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/go-sequent/go-sequent/event"
	"github.com/go-sequent/go-sequent/eventstore"
	"github.com/go-sequent/go-sequent/eventstore/postgres"
	"github.com/go-sequent/go-sequent/examples/bank"
	"github.com/go-sequent/go-sequent/idempotency"
	"github.com/go-sequent/go-sequent/tenancy"
	"github.com/go-sequent/go-sequent/types"
	"github.com/google/uuid"
)

func newPostgresDemo(t *testing.T) (*bank.DemoApp, *postgres.Store) {
	t.Helper()
	dsn := os.Getenv("GO_SEQUENT_PG_DSN")
	if dsn == "" {
		t.Skip("set GO_SEQUENT_PG_DSN to run postgres integration tests")
	}
	ctx := context.Background()
	var store *postgres.Store
	demo, err := bank.NewDemoApp(bank.DemoOptions{
		StoreFactory: func(reg *event.Registry) (eventstore.Store, eventstore.TxManager, eventstore.SnapshotStore, error) {
			s, err := postgres.Open(ctx, dsn, reg)
			if err != nil {
				return nil, nil, nil, err
			}
			if err := s.Migrate(ctx); err != nil {
				s.Close()
				return nil, nil, nil, err
			}
			store = s
			return s, s, s, nil
		},
		IdempotencyFactory: func(s eventstore.Store) idempotency.Store {
			return idempotency.NewPostgres(s.(*postgres.Store).Pool())
		},
		Authorizer:      bank.AllowBankCommands(),
		EnableTenancyMW: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	return demo, store
}

func TestIT_Postgres_CommandEventProjection(t *testing.T) {
	demo, store := newPostgresDemo(t)
	tenant := uuid.New()
	ctx := tenancy.WithTenant(context.Background(), tenant)

	alice, bob := uuid.New(), uuid.New()
	for _, spec := range []struct {
		id          uuid.UUID
		owner, open string
	}{
		{alice, "pg-alice-" + alice.String()[:8], "200.00"},
		{bob, "pg-bob-" + bob.String()[:8], "80.00"},
	} {
		cmd := &bank.OpenAccount{
			AccountID: spec.id, Owner: spec.owner, Currency: "USD",
			Opening: types.MustMoney(spec.open, "USD"),
		}
		cmd.Meta().IdempotencyKey = "pg-open-" + spec.owner
		if err := demo.App.Service.Execute(ctx, cmd); err != nil {
			t.Fatal(err)
		}
	}

	if err := demo.App.Service.Execute(ctx, &bank.Deposit{
		AccountID: alice, Amount: types.MustMoney("20.00", "USD"),
	}); err != nil {
		t.Fatal(err)
	}
	tr := &bank.Transfer{
		FromAccountID: alice, ToAccountID: bob,
		Amount: types.MustMoney("30.00", "USD"), TransferID: "pg-xfer-1",
	}
	tr.Meta().IdempotencyKey = "pg-xfer-1"
	if err := demo.App.Service.Execute(ctx, tr); err != nil {
		t.Fatal(err)
	}

	assertBal(t, demo, alice, types.MustMoney("190.00", "USD"))
	assertBal(t, demo, bob, types.MustMoney("110.00", "USD"))

	evs, err := store.LoadStream(ctx, alice)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) < 3 {
		t.Fatalf("expected ≥3 events, got %d", len(evs))
	}
	acc := bank.NewAccount(alice)
	if err := acc.LoadFromHistory(evs); err != nil {
		t.Fatal(err)
	}
	if !acc.Balance.Equal(types.MustMoney("190.00", "USD")) {
		t.Fatalf("rehydrated balance=%s", acc.Balance)
	}
}

func TestIT_Postgres_OptimisticConcurrency(t *testing.T) {
	demo, store := newPostgresDemo(t)
	ctx := context.Background()
	id := uuid.New()
	if err := demo.App.Service.Execute(ctx, &bank.OpenAccount{
		AccountID: id, Owner: "pg-occ-" + id.String()[:8], Currency: "USD",
		Opening: types.MustMoney("5", "USD"),
	}); err != nil {
		t.Fatal(err)
	}
	acc := bank.NewAccount(id)
	acc.Open = true
	acc.Currency = "USD"
	acc.Balance = types.MustMoney("5", "USD")
	if err := acc.Deposit(types.MustMoney("1", "USD")); err != nil {
		t.Fatal(err)
	}
	err := store.WithinTx(ctx, func(ctx context.Context) error {
		return store.Append(ctx, eventstore.CommandRecord{
			ID: uuid.New(), Type: "stale", IssuedAt: time.Now().UTC(),
		}, []eventstore.StreamAppend{{
			AggregateID: id, AggregateType: bank.AggregateAccount, ExpectedVersion: 0,
			Events: acc.UncommittedEvents(),
		}})
	})
	if !errors.Is(err, eventstore.ErrOptimisticConcurrency) {
		t.Fatalf("want optimistic concurrency, got %v", err)
	}
}
