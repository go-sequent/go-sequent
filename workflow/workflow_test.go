package workflow_test

import (
	"context"
	"testing"

	"github.com/go-sequent/go-sequent/workflow"
)

func TestAfterCommitHooks(t *testing.T) {
	ctx, bag := workflow.WithHooks(context.Background())
	var order []string
	workflow.RegisterAfterCommit(ctx, func(ctx context.Context) error {
		order = append(order, "a")
		return nil
	})
	workflow.RegisterAfterCommit(ctx, func(ctx context.Context) error {
		order = append(order, "b")
		return nil
	})
	if err := bag.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(order) != 2 || order[0] != "a" || order[1] != "b" {
		t.Fatalf("%v", order)
	}
}
