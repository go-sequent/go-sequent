package security_test

import (
	"context"
	"errors"
	"testing"

	"github.com/go-sequent/go-sequent/command"
	"github.com/go-sequent/go-sequent/examples/bank"
	"github.com/go-sequent/go-sequent/security"
	"github.com/go-sequent/go-sequent/types"
	"github.com/google/uuid"
)

func TestDenyByDefault(t *testing.T) {
	auth := security.DenyByDefault{Allow: map[string]bool{bank.CmdDeposit: true}}
	cmd := &bank.OpenAccount{AccountID: uuid.New(), Owner: "a", Currency: "USD", Opening: types.MustMoney("1", "USD")}
	err := auth.Authorize(context.Background(), cmd)
	if !errors.Is(err, security.ErrDenied) {
		t.Fatalf("got %v", err)
	}
	dep := &bank.Deposit{AccountID: uuid.New(), Amount: types.MustMoney("1", "USD")}
	if err := auth.Authorize(context.Background(), dep); err != nil {
		t.Fatal(err)
	}
}

func TestMiddleware(t *testing.T) {
	called := false
	mw := security.Middleware(security.AllowAll{})
	err := mw(context.Background(), &bank.Deposit{AccountID: uuid.New(), Amount: types.MustMoney("1", "USD")},
		func(ctx context.Context, cmd command.Command) error {
			called = true
			return nil
		})
	if err != nil || !called {
		t.Fatalf("err=%v called=%v", err, called)
	}
}
