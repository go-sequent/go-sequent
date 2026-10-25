package outbox_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-sequent/go-sequent/publish/outbox"
)

func TestMemoryPoller(t *testing.T) {
	store := outbox.NewMemory()
	ctx := context.Background()
	if err := store.Enqueue(ctx, outbox.Message{Topic: "events", Payload: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	got := 0
	p := &outbox.Poller{
		Store: store,
		Handler: func(ctx context.Context, msg outbox.Message) error {
			got++
			return nil
		},
		BatchSize: 5,
		LockFor:   time.Second,
	}
	n, err := p.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || got != 1 {
		t.Fatalf("n=%d got=%d", n, got)
	}
	depth, _ := store.Depth(ctx)
	if depth != 0 {
		t.Fatalf("depth=%d", depth)
	}
}
