package command_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/wenweih/go-sequent/command"
	"github.com/wenweih/go-sequent/eventstore/memory"
	"github.com/wenweih/go-sequent/event"
)

type pingCmd struct {
	command.Base
}

func (c *pingCmd) CommandType() string            { return "test.Ping" }
func (c *pingCmd) AggregateID() (uuid.UUID, bool) { return uuid.Nil, false }

type pingHandler struct {
	calls int
}

func (h *pingHandler) Handles() []string { return []string{"test.Ping"} }
func (h *pingHandler) Handle(ctx context.Context, cmd command.Command) error {
	h.calls++
	return nil
}

func TestMiddlewareOrder(t *testing.T) {
	var order []string
	reg := event.NewRegistry()
	store := memory.New(reg)
	h := &pingHandler{}
	router := command.NewRouter()
	router.Register(h)
	svc := &command.Service{
		Router: router,
		Tx:     store,
		Middleware: []command.Middleware{
			func(ctx context.Context, cmd command.Command, next command.HandlerFunc) error {
				order = append(order, "a")
				err := next(ctx, cmd)
				order = append(order, "a2")
				return err
			},
			func(ctx context.Context, cmd command.Command, next command.HandlerFunc) error {
				order = append(order, "b")
				err := next(ctx, cmd)
				order = append(order, "b2")
				return err
			},
		},
	}
	if err := svc.Execute(context.Background(), &pingCmd{}); err != nil {
		t.Fatal(err)
	}
	want := []string{"a", "b", "b2", "a2"}
	if len(order) != len(want) {
		t.Fatalf("%v", order)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("%v != %v", order, want)
		}
	}
	if h.calls != 1 {
		t.Fatalf("calls=%d", h.calls)
	}
}
