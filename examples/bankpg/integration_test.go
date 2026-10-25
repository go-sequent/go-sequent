package bankpg_test

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
	"github.com/go-sequent/go-sequent/examples/bankpg"
	"github.com/go-sequent/go-sequent/idempotency"
	"github.com/go-sequent/go-sequent/projector"
	"github.com/go-sequent/go-sequent/tenancy"
	"github.com/go-sequent/go-sequent/types"
	"github.com/google/uuid"
)

func newPGApp(t *testing.T) (*bankpg.App, *postgres.Store) {
	t.Helper()
	dsn := os.Getenv("GO_SEQUENT_PG_DSN")
	if dsn == "" {
		t.Skip("set GO_SEQUENT_PG_DSN to run bankpg integration tests")
	}
	ctx := context.Background()
	var store *postgres.Store
	app, err := bankpg.NewApp(bankpg.Options{
		StoreFactory: func(reg *event.Registry) (eventstore.Store, eventstore.TxManager, eventstore.SnapshotStore, error) {
			s, err := postgres.Open(ctx, dsn, reg)
			if err != nil {
				return nil, nil, nil, err
			}
			if err := s.Migrate(ctx); err != nil {
				s.Close()
				return nil, nil, nil, err
			}
			if err := bankpg.MigrateProjection(ctx, s.Pool()); err != nil {
				s.Close()
				return nil, nil, nil, err
			}
			store = s
			return s, s, s, nil
		},
		PersistorFactory: func(s eventstore.Store) projector.Persistor {
			return s.(*postgres.Store).Persistor()
		},
		IdempotencyFactory: func(s eventstore.Store) idempotency.Store {
			return idempotency.NewPostgres(s.(*postgres.Store).Pool())
		},
		Authorizer: bank.AllowBankCommands(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	return app, store
}

func TestIT_BankPG_LedgerRoundTrip(t *testing.T) {
	app, _ := newPGApp(t)
	tenant := uuid.New()
	ctx := tenancy.WithTenant(context.Background(), tenant)

	alice, bob := uuid.New(), uuid.New()
	for _, spec := range []struct {
		id          uuid.UUID
		owner, open string
	}{
		{alice, "bp-alice-" + alice.String()[:8], "200.00"},
		{bob, "bp-bob-" + bob.String()[:8], "80.00"},
	} {
		cmd := &bank.OpenAccount{
			AccountID: spec.id, Owner: spec.owner, Currency: "USD",
			Opening: types.MustMoney(spec.open, "USD"),
		}
		cmd.Meta().IdempotencyKey = "bp-open-" + spec.owner
		if err := app.Sequent.Service.Execute(ctx, cmd); err != nil {
			t.Fatal(err)
		}
	}

	if err := app.Sequent.Service.Execute(ctx, &bank.Deposit{
		AccountID: alice, Amount: types.MustMoney("20.00", "USD"),
	}); err != nil {
		t.Fatal(err)
	}
	tr := &bank.Transfer{
		FromAccountID: alice, ToAccountID: bob,
		Amount: types.MustMoney("30.00", "USD"), TransferID: "bp-xfer-1",
	}
	tr.Meta().IdempotencyKey = "bp-xfer-1"
	if err := app.Sequent.Service.Execute(ctx, tr); err != nil {
		t.Fatal(err)
	}

	assertSQLBal(t, app, tenant, alice, types.MustMoney("190.00", "USD"))
	assertSQLBal(t, app, tenant, bob, types.MustMoney("110.00", "USD"))
}

func TestIT_BankPG_OptimisticConcurrency(t *testing.T) {
	app, store := newPGApp(t)
	tenant := uuid.New()
	ctx := tenancy.WithTenant(context.Background(), tenant)
	id := uuid.New()
	if err := app.Sequent.Service.Execute(ctx, &bank.OpenAccount{
		AccountID: id, Owner: "bp-occ-" + id.String()[:8], Currency: "USD",
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
		t.Fatalf("want OCC, got %v", err)
	}
}

func TestIT_BankPG_ProjectionRebuild(t *testing.T) {
	app, store := newPGApp(t)
	tenant := uuid.New()
	ctx := tenancy.WithTenant(context.Background(), tenant)
	id := uuid.New()
	if err := app.Sequent.Service.Execute(ctx, &bank.OpenAccount{
		AccountID: id, Owner: "bp-rb-" + id.String()[:8], Currency: "USD",
		Opening: types.MustMoney("10.00", "USD"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := app.Sequent.Service.Execute(ctx, &bank.Deposit{
		AccountID: id, Amount: types.MustMoney("5.50", "USD"),
	}); err != nil {
		t.Fatal(err)
	}
	assertSQLBal(t, app, tenant, id, types.MustMoney("15.50", "USD"))

	// Truncate projection, then replay stream through the same SyncInTxn projector.
	if _, err := store.Pool().Exec(ctx, `DELETE FROM bank_account_balances WHERE account_id = $1`, id); err != nil {
		t.Fatal(err)
	}
	evs, err := store.LoadStream(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	err = store.WithinTx(ctx, func(ctx context.Context) error {
		ctx = tenancy.WithTenant(ctx, tenant)
		for _, e := range evs {
			if err := app.Projector.Handle(ctx, e); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	assertSQLBal(t, app, tenant, id, types.MustMoney("15.50", "USD"))
}

func assertSQLBal(t *testing.T, app *bankpg.App, tenant, id uuid.UUID, want types.Money) {
	t.Helper()
	got, err := app.Projector.Balance(context.Background(), tenant, id)
	if err != nil {
		t.Fatalf("balance read: %v", err)
	}
	if !got.Equal(want) {
		t.Fatalf("balance got=%s want=%s", got, want)
	}
}
