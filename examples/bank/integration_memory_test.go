package bank_test

import (
	"context"
	"errors"
	"testing"

	"github.com/go-sequent/go-sequent/event"
	"github.com/go-sequent/go-sequent/eventstore"
	"github.com/go-sequent/go-sequent/eventstore/memory"
	"github.com/go-sequent/go-sequent/examples/bank"
	"github.com/go-sequent/go-sequent/idempotency"
	"github.com/go-sequent/go-sequent/security"
	"github.com/go-sequent/go-sequent/tenancy"
	"github.com/go-sequent/go-sequent/types"
	"github.com/google/uuid"
)

func newMemoryDemo(t *testing.T, auth security.Authorizer, idemp idempotency.Store, tenancyMW bool) (*bank.DemoApp, *memory.Store) {
	t.Helper()
	var store *memory.Store
	demo, err := bank.NewDemoApp(bank.DemoOptions{
		StoreFactory: func(reg *event.Registry) (eventstore.Store, eventstore.TxManager, eventstore.SnapshotStore, error) {
			store = memory.New(reg)
			return store, store, store, nil
		},
		Idempotency:     idemp,
		Authorizer:      auth,
		EnableTenancyMW: tenancyMW,
	})
	if err != nil {
		t.Fatal(err)
	}
	return demo, store
}

func TestIT_Memory_LedgerRoundTrip(t *testing.T) {
	demo, _ := newMemoryDemo(t, bank.AllowBankCommands(), idempotency.NewMemory(), true)
	tenant := uuid.New()
	ctx := tenancy.WithTenant(context.Background(), tenant)

	alice, bob := uuid.New(), uuid.New()
	open := func(id uuid.UUID, owner, opening string) {
		t.Helper()
		cmd := &bank.OpenAccount{
			AccountID: id, Owner: owner, Currency: "USD",
			Opening: types.MustMoney(opening, "USD"),
		}
		cmd.Meta().IdempotencyKey = "open-" + owner
		cmd.Meta().Source = "itest"
		if err := demo.App.Service.Execute(ctx, cmd); err != nil {
			t.Fatal(err)
		}
	}
	open(alice, "alice-it", "100.00")
	open(bob, "bob-it", "50.00")

	dep := &bank.Deposit{AccountID: alice, Amount: types.MustMoney("25.50", "USD")}
	dep.Meta().IdempotencyKey = "dep-1"
	if err := demo.App.Service.Execute(ctx, dep); err != nil {
		t.Fatal(err)
	}
	if err := demo.App.Service.Execute(ctx, dep); err != nil {
		t.Fatal(err)
	}

	if err := demo.App.Service.Execute(ctx, &bank.Withdraw{
		AccountID: alice, Amount: types.MustMoney("10.00", "USD"),
	}); err != nil {
		t.Fatal(err)
	}

	tr := &bank.Transfer{
		FromAccountID: alice, ToAccountID: bob,
		Amount: types.MustMoney("15.25", "USD"), TransferID: "xfer-1",
	}
	tr.Meta().IdempotencyKey = "xfer-1"
	if err := demo.App.Service.Execute(ctx, tr); err != nil {
		t.Fatal(err)
	}

	assertBal(t, demo, alice, types.MustMoney("100.25", "USD"))
	assertBal(t, demo, bob, types.MustMoney("65.25", "USD"))

	if len(demo.Workflow.Notified) < 2 {
		t.Fatalf("expected AfterCommit notifies, got %v", demo.Workflow.Notified)
	}
}

func TestIT_Memory_AuthZDenied(t *testing.T) {
	demo, _ := newMemoryDemo(t, security.DenyByDefault{Allow: map[string]bool{
		bank.CmdOpenAccount: true,
	}}, nil, false)
	ctx := context.Background()
	id := uuid.New()
	if err := demo.App.Service.Execute(ctx, &bank.OpenAccount{
		AccountID: id, Owner: "authz", Currency: "EUR", Opening: types.MustMoney("1", "EUR"),
	}); err != nil {
		t.Fatal(err)
	}
	err := demo.App.Service.Execute(ctx, &bank.Deposit{AccountID: id, Amount: types.MustMoney("1", "EUR")})
	if !errors.Is(err, security.ErrDenied) {
		t.Fatalf("want denied, got %v", err)
	}
}

func TestIT_Memory_OptimisticConcurrency(t *testing.T) {
	demo, store := newMemoryDemo(t, bank.AllowBankCommands(), nil, false)
	ctx := context.Background()
	id := uuid.New()
	if err := demo.App.Service.Execute(ctx, &bank.OpenAccount{
		AccountID: id, Owner: "occ", Currency: "USD", Opening: types.MustMoney("10", "USD"),
	}); err != nil {
		t.Fatal(err)
	}
	acc := bank.NewAccount(id)
	acc.Open = true
	acc.Currency = "USD"
	acc.Balance = types.MustMoney("10", "USD")
	if err := acc.Deposit(types.MustMoney("1", "USD")); err != nil {
		t.Fatal(err)
	}
	err := store.Append(ctx, eventstore.CommandRecord{ID: uuid.New(), Type: "stale"},
		[]eventstore.StreamAppend{{
			AggregateID: id, AggregateType: bank.AggregateAccount, ExpectedVersion: 0,
			Events: acc.UncommittedEvents(),
		}})
	if !errors.Is(err, eventstore.ErrOptimisticConcurrency) {
		t.Fatalf("got %v", err)
	}
}

func TestIT_Memory_TenantUniqueOwner(t *testing.T) {
	demo, _ := newMemoryDemo(t, bank.AllowBankCommands(), nil, true)
	tenant := uuid.New()
	ctx := tenancy.WithTenant(context.Background(), tenant)
	if err := demo.App.Service.Execute(ctx, &bank.OpenAccount{
		AccountID: uuid.New(), Owner: "same", Currency: "USD", Opening: types.MustMoney("1", "USD"),
	}); err != nil {
		t.Fatal(err)
	}
	err := demo.App.Service.Execute(ctx, &bank.OpenAccount{
		AccountID: uuid.New(), Owner: "same", Currency: "USD", Opening: types.MustMoney("1", "USD"),
	})
	if !errors.Is(err, eventstore.ErrUniqueKeyViolation) {
		t.Fatalf("want unique violation, got %v", err)
	}
}

func assertBal(t *testing.T, demo *bank.DemoApp, id uuid.UUID, want types.Money) {
	t.Helper()
	got, ok := demo.Projector.Balance(id)
	if !ok || !got.Equal(want) {
		t.Fatalf("balance id=%s got=%s want=%s ok=%v", id, got, want, ok)
	}
}
