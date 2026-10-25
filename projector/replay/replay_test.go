package replay_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/wenweih/go-sequent/aggregate"
	"github.com/wenweih/go-sequent/eventstore/memory"
	"github.com/wenweih/go-sequent/examples/bank"
	"github.com/wenweih/go-sequent/projector/replay"
	"github.com/wenweih/go-sequent/sequent"
	"github.com/wenweih/go-sequent/types"
)

func TestPipelineInMemory(t *testing.T) {
	app := sequent.New(sequent.Config{})
	bank.RegisterEvents(app.Events)
	store := memory.New(app.Events)
	app.Repo.Store = store
	app.Repo.Publisher = app.Publisher
	app.Service.Tx = store
	app.Service.Repo = app.Repo
	app.RegisterAggregate(bank.AggregateAccount, func(id uuid.UUID) aggregate.Root {
		return bank.NewAccount(id)
	})
	live := bank.NewBalanceProjector()
	app.RegisterProjector(live)
	app.RegisterHandler(&bank.Handler{Repo: app.Repo})

	ctx := context.Background()
	id := uuid.New()
	if err := app.Service.Execute(ctx, &bank.OpenAccount{
		AccountID: id, Owner: "r1", Currency: "USD", Opening: types.MustMoney("10", "USD"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := app.Service.Execute(ctx, &bank.Deposit{
		AccountID: id, Amount: types.MustMoney("5", "USD"),
	}); err != nil {
		t.Fatal(err)
	}

	// Rebuild into a fresh projector via replay pipeline (projectors-only).
	rebuild := bank.NewBalanceProjector()
	replayPub := sequent.New(sequent.Config{}).Publisher
	replayPub.Register(rebuild)

	pipe := &replay.Pipeline{
		Name:      "balances",
		Source:    store,
		Publisher: replayPub,
		Schema:    replay.NoopSchema{},
		State:     replay.NewMemoryState(),
		BatchSize: 10,
	}
	if err := pipe.RunAll(ctx); err != nil {
		t.Fatal(err)
	}
	bal, ok := rebuild.Balance(id)
	if !ok || !bal.Amount.Equal(types.MustMoney("15", "USD").Amount) {
		t.Fatalf("rebuild bal=%v ok=%v", bal, ok)
	}
	st, err := pipe.State.Load(ctx, "balances")
	if err != nil || st == nil || st.Status != replay.StageLive {
		t.Fatalf("state=%v err=%v", st, err)
	}
}
