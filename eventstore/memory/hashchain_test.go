package memory

import (
	"context"
	"errors"
	"testing"

	"github.com/go-sequent/go-sequent/event"
	"github.com/go-sequent/go-sequent/eventstore"
	"github.com/go-sequent/go-sequent/examples/bank"
	"github.com/go-sequent/go-sequent/types"
	"github.com/google/uuid"
)

func TestHashChain_HappyAndTamper(t *testing.T) {
	reg := event.NewRegistry()
	bank.RegisterEvents(reg)
	s := New(reg)
	ctx := context.Background()
	id := uuid.New()
	acc := bank.NewAccount(id)
	if err := acc.OpenAccount("alice", "USD", types.MustMoney("10", "USD")); err != nil {
		t.Fatal(err)
	}
	if err := acc.Deposit(types.MustMoney("1", "USD")); err != nil {
		t.Fatal(err)
	}
	err := s.Append(ctx, eventstore.CommandRecord{ID: uuid.New(), Type: "open"},
		[]eventstore.StreamAppend{{
			AggregateID: id, AggregateType: bank.AggregateAccount, ExpectedVersion: 0,
			Events: acc.UncommittedEvents(),
		}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.VerifyChain(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := s.LoadStream(ctx, id); err != nil {
		t.Fatal(err)
	}

	// Tamper stored payload.
	s.mu.Lock()
	s.streams[id].events[0].Payload = []byte(`{"owner":"eve","currency":"USD","opening":{"amount":"10","currency":"USD"}}`)
	s.mu.Unlock()
	err = s.VerifyChain(ctx, id)
	if !errors.Is(err, eventstore.ErrHashChainTamper) {
		t.Fatalf("want tamper, got %v", err)
	}
}
