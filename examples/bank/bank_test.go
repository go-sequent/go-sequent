package bank_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/wenweih/go-sequent/aggregate"
	"github.com/wenweih/go-sequent/command"
	"github.com/wenweih/go-sequent/eventstore"
	"github.com/wenweih/go-sequent/eventstore/memory"
	"github.com/wenweih/go-sequent/examples/bank"
	"github.com/wenweih/go-sequent/sequent"
	"github.com/wenweih/go-sequent/testkit"
	"github.com/wenweih/go-sequent/types"
)

func newApp(t *testing.T) (*sequent.App, *bank.BalanceProjector) {
	t.Helper()
	app := sequent.New(sequent.Config{PublisherMode: sequent.SyncInTxn})
	bank.RegisterEvents(app.Events)
	store := memory.New(app.Events)
	app.Repo.Store = store
	app.Repo.Publisher = app.Publisher
	app.Service.Tx = store
	app.Service.Repo = app.Repo
	app.RegisterAggregate(bank.AggregateAccount, func(id uuid.UUID) aggregate.Root {
		return bank.NewAccount(id)
	})
	proj := bank.NewBalanceProjector()
	app.RegisterProjector(proj)
	app.RegisterHandler(&bank.Handler{Repo: app.Repo})
	return app, proj
}

func TestOpenDepositWithdrawProjection(t *testing.T) {
	app, proj := newApp(t)
	ctx := context.Background()
	id := uuid.New()

	err := app.Service.Execute(ctx, &bank.OpenAccount{
		AccountID: id,
		Owner:     "alice",
		Currency:  "CNY",
		Opening:   types.MustMoney("100.00", "CNY"),
	})
	if err != nil {
		t.Fatal(err)
	}

	err = app.Service.Execute(ctx, &bank.Deposit{
		AccountID: id,
		Amount:    types.MustMoney("50.25", "CNY"),
	})
	if err != nil {
		t.Fatal(err)
	}

	err = app.Service.Execute(ctx, &bank.Withdraw{
		AccountID: id,
		Amount:    types.MustMoney("20.00", "CNY"),
	})
	if err != nil {
		t.Fatal(err)
	}

	bal, ok := proj.Balance(id)
	if !ok {
		t.Fatal("missing projection")
	}
	want := types.MustMoney("130.25", "CNY")
	if !bal.Amount.Equal(want.Amount) || bal.Currency != want.Currency {
		t.Fatalf("balance=%s want=%s", bal, want)
	}
}

func TestOptimisticConcurrency(t *testing.T) {
	app, _ := newApp(t)
	ctx := context.Background()
	id := uuid.New()
	if err := app.Service.Execute(ctx, &bank.OpenAccount{
		AccountID: id,
		Owner:     "bob",
		Currency:  "USD",
		Opening:   types.MustMoney("10", "USD"),
	}); err != nil {
		t.Fatal(err)
	}

	// Simulate stale expected version by appending with wrong version directly.
	store := app.Repo.Store
	acc := bank.NewAccount(id)
	acc.Open = true
	acc.Currency = "USD"
	acc.Balance = types.MustMoney("10", "USD")
	if err := acc.Deposit(types.MustMoney("1", "USD")); err != nil {
		t.Fatal(err)
	}
	rec := command.RecordOf(&bank.Deposit{AccountID: id, Amount: types.MustMoney("1", "USD")})
	err := store.Append(ctx, rec, []eventstore.StreamAppend{{
		AggregateID:     id,
		AggregateType:   bank.AggregateAccount,
		ExpectedVersion: 0,
		Events:          acc.UncommittedEvents(),
	}})
	if !errors.Is(err, eventstore.ErrOptimisticConcurrency) {
		t.Fatalf("want optimistic concurrency, got %v", err)
	}
}

func TestKitOpenAccount(t *testing.T) {
	id := uuid.New()
	s := testkit.For(id, func(id uuid.UUID) aggregate.Root {
		return bank.NewAccount(id)
	}).Given().When(func(agg aggregate.Root) error {
		return agg.(*bank.Account).OpenAccount("carol", "EUR", types.MustMoney("5.00", "EUR"))
	}).Run()

	if err := s.ExpectEvents(bank.NewAccountOpened("carol", "EUR", types.MustMoney("5.00", "EUR"))); err != nil {
		t.Fatal(err)
	}
}

func TestKitInsufficientFunds(t *testing.T) {
	id := uuid.New()
	s := testkit.For(id, func(id uuid.UUID) aggregate.Root {
		return bank.NewAccount(id)
	}).Given(
		bank.NewAccountOpened("dave", "USD", types.MustMoney("10", "USD")),
	).When(func(agg aggregate.Root) error {
		return agg.(*bank.Account).Withdraw(types.MustMoney("20", "USD"))
	}).Run()

	if err := s.ExpectError("insufficient funds"); err != nil {
		t.Fatal(err)
	}
}

func TestBanFloat64Money(t *testing.T) {
	type bad struct {
		Amount float64
	}
	if err := testkit.BanFloat64Money(bad{Amount: 1.23}); err == nil {
		t.Fatal("expected ban")
	}
	if err := testkit.BanFloat64Money(&bank.Deposit{Amount: types.MustMoney("1", "USD")}); err != nil {
		t.Fatal(err)
	}
}

func TestReloadFromStore(t *testing.T) {
	app, _ := newApp(t)
	ctx := context.Background()
	id := uuid.New()
	if err := app.Service.Execute(ctx, &bank.OpenAccount{
		AccountID: id,
		Owner:     "eve",
		Currency:  "JPY",
		Opening:   types.MustMoney("1000", "JPY"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := app.Service.Execute(ctx, &bank.Deposit{
		AccountID: id,
		Amount:    types.MustMoney("500", "JPY"),
	}); err != nil {
		t.Fatal(err)
	}

	// New UoW load
	err := app.Service.Tx.WithinTx(ctx, func(ctx context.Context) error {
		ctx = aggregate.WithUnitOfWork(ctx, aggregate.NewUnitOfWork())
		agg, err := app.Repo.Load(ctx, id, bank.AggregateAccount)
		if err != nil {
			return err
		}
		acc := agg.(*bank.Account)
		want := types.MustMoney("1500", "JPY")
		if !acc.Balance.Amount.Equal(want.Amount) {
			t.Fatalf("reloaded balance=%s", acc.Balance)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
