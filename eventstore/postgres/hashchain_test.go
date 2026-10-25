package postgres_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/go-sequent/go-sequent/event"
	"github.com/go-sequent/go-sequent/eventstore"
	"github.com/go-sequent/go-sequent/eventstore/postgres"
	"github.com/go-sequent/go-sequent/examples/bank"
	"github.com/go-sequent/go-sequent/types"
	"github.com/google/uuid"
)

func TestIT_HashChain_Tamper(t *testing.T) {
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
		t.Fatal(err)
	}

	id := uuid.New()
	acc := bank.NewAccount(id)
	_ = acc.OpenAccount("hc-"+id.String()[:8], "USD", types.MustMoney("3", "USD"))
	err = store.WithinTx(ctx, func(ctx context.Context) error {
		return store.Append(ctx, eventstore.CommandRecord{ID: uuid.New(), Type: "bank.OpenAccount"},
			[]eventstore.StreamAppend{{
				AggregateID: id, AggregateType: bank.AggregateAccount, ExpectedVersion: 0,
				Events: acc.UncommittedEvents(),
			}})
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.VerifyChain(ctx, id); err != nil {
		t.Fatal(err)
	}

	_, err = store.Pool().Exec(ctx, `
UPDATE events SET event_json = convert_to(
  '{"owner":"tampered","currency":"USD","opening":{"amount":"3","currency":"USD"}}',
  'UTF8')
WHERE aggregate_id = $1 AND sequence_number = 1`, id)
	if err != nil {
		t.Fatal(err)
	}
	err = store.VerifyChain(ctx, id)
	if !errors.Is(err, eventstore.ErrHashChainTamper) {
		t.Fatalf("want tamper, got %v", err)
	}
}
