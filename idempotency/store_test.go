package idempotency_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/wenweih/go-sequent/aggregate"
	"github.com/wenweih/go-sequent/command"
	"github.com/wenweih/go-sequent/eventstore/memory"
	"github.com/wenweih/go-sequent/examples/bank"
	"github.com/wenweih/go-sequent/idempotency"
	"github.com/wenweih/go-sequent/sequent"
	"github.com/wenweih/go-sequent/types"
)

func TestIdempotentRetryNoOp(t *testing.T) {
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
	app.RegisterProjector(bank.NewBalanceProjector())
	app.RegisterHandler(&bank.Handler{Repo: app.Repo})

	idemp := idempotency.NewMemory()
	app.Service.Middleware = append(app.Service.Middleware, idempotency.Middleware(idempotency.Config{Store: idemp}))

	tenant := uuid.New()
	ctx := context.Background()
	id := uuid.New()
	cmd := &bank.OpenAccount{
		AccountID: id,
		Owner:     "idem-alice",
		Currency:  "USD",
		Opening:   types.MustMoney("10", "USD"),
	}
	cmd.Meta().TenantID = tenant
	cmd.Meta().IdempotencyKey = "open-1"

	if err := app.Service.Execute(ctx, cmd); err != nil {
		t.Fatal(err)
	}
	// Retry same key — should no-op without unique-key / open errors.
	cmd2 := &bank.OpenAccount{
		AccountID: id,
		Owner:     "idem-alice",
		Currency:  "USD",
		Opening:   types.MustMoney("10", "USD"),
	}
	cmd2.Meta().TenantID = tenant
	cmd2.Meta().IdempotencyKey = "open-1"
	if err := app.Service.Execute(ctx, cmd2); err != nil {
		t.Fatal(err)
	}
}

func TestIdempotencyRequiresTenant(t *testing.T) {
	store := idempotency.NewMemory()
	mw := idempotency.Middleware(idempotency.Config{Store: store})
	cmd := &bank.OpenAccount{AccountID: uuid.New(), Owner: "x", Currency: "USD", Opening: types.MustMoney("1", "USD")}
	cmd.Meta().IdempotencyKey = "k"
	err := mw(context.Background(), cmd, func(ctx context.Context, cmd command.Command) error { return nil })
	if err == nil {
		t.Fatal("expected tenant required")
	}
}
