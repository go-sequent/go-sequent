package snapshot_test

import (
	"context"
	"testing"

	"github.com/go-sequent/go-sequent/aggregate"
	"github.com/go-sequent/go-sequent/event"
	"github.com/go-sequent/go-sequent/eventstore/memory"
	"github.com/go-sequent/go-sequent/examples/bank"
	"github.com/go-sequent/go-sequent/snapshot"
	"github.com/go-sequent/go-sequent/types"
	"github.com/google/uuid"
)

func TestTakeAndRehydrate(t *testing.T) {
	reg := event.NewRegistry()
	bank.RegisterEvents(reg)
	store := memory.New(reg)
	store.SetSnapshotThreshold(1)
	ctx := context.Background()
	id := uuid.New()

	acc := bank.NewAccount(id)
	if err := acc.OpenAccount("snap", "USD", types.MustMoney("10", "USD")); err != nil {
		t.Fatal(err)
	}
	if err := acc.Deposit(types.MustMoney("5", "USD")); err != nil {
		t.Fatal(err)
	}
	// Persist via direct append path through repository would be heavier; take from in-memory state.
	acc.MarkCommitted() // version still 0 — fix by loading history style
	// Re-apply via history for correct version:
	acc2 := bank.NewAccount(id)
	evs := []event.Event{
		bank.NewAccountOpened("snap", "USD", types.MustMoney("10", "USD")),
		bank.NewMoneyDeposited(types.MustMoney("5", "USD")),
	}
	for i, e := range evs {
		if env, ok := e.(event.Envelope); ok {
			env.SetAggregateID(id)
			env.SetSequenceNumber(int64(i + 1))
		}
	}
	if err := acc2.LoadFromHistory(evs); err != nil {
		t.Fatal(err)
	}
	snap, err := snapshot.Take(acc2)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSnapshot(ctx, snap); err != nil {
		t.Fatal(err)
	}

	// Store only snapshot (no events) — Rehydrate should restore state from snapshot alone.
	got, err := snapshot.Rehydrate(ctx, store, store, id, func(id uuid.UUID) aggregate.Root {
		return bank.NewAccount(id)
	})
	// LoadStream will fail with not found; Rehydrate allows snapshot-only when fromSeq > 0
	if err != nil {
		// memory LoadStream returns not found — Rehydrate should still work
		t.Fatalf("rehydrate: %v", err)
	}
	a := got.(*bank.Account)
	if !a.Balance.Amount.Equal(types.MustMoney("15", "USD").Amount) {
		t.Fatalf("balance=%s", a.Balance)
	}
}
