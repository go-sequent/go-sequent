package obs_test

import (
	"context"
	"testing"
	"time"

	"github.com/wenweih/go-sequent/obs"
)

func TestInstrumentsNoopProvider(t *testing.T) {
	inst, err := obs.New(obs.Config{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, span := inst.StartCommand(context.Background(), "test.Cmd")
	inst.RecordCommand(ctx, span, "test.Cmd", time.Now().Add(-time.Millisecond), nil)
	inst.RecordConflict(ctx)
	inst.RecordEvents(ctx, 2)
}
