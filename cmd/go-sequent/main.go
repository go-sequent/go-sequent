// Command go-sequent provides migrate, projection-replay, and outbox poller CLI (Q7).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/go-sequent/go-sequent/event"
	"github.com/go-sequent/go-sequent/eventstore/postgres"
	"github.com/go-sequent/go-sequent/projector/replay"
	"github.com/go-sequent/go-sequent/publish/outbox"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	ctx := context.Background()
	switch os.Args[1] {
	case "migrate":
		fs := flag.NewFlagSet("migrate", flag.ExitOnError)
		dsn := fs.String("dsn", envOr("GO_SEQUENT_PG_DSN", ""), "Postgres DSN")
		_ = fs.Parse(os.Args[2:])
		if *dsn == "" {
			fail("-dsn or GO_SEQUENT_PG_DSN required")
		}
		reg := event.NewRegistry()
		store, err := postgres.Open(ctx, *dsn, reg)
		if err != nil {
			fail(err.Error())
		}
		defer store.Close()
		if err := store.Migrate(ctx); err != nil {
			fail(err.Error())
		}
		fmt.Println("migrate: ok (phase1–3 + store_events)")
	case "replay":
		fs := flag.NewFlagSet("replay", flag.ExitOnError)
		dsn := fs.String("dsn", envOr("GO_SEQUENT_PG_DSN", ""), "Postgres DSN")
		name := fs.String("projector", "default", "projector name / replay state key")
		stage := fs.String("stage", "all", "prepare|replay|catchup|optimize|golive|abort|all")
		tables := fs.String("tables", "", "comma-separated managed tables (for schema swap)")
		replaySchema := fs.String("replay-schema", "", "replay schema name")
		viewSchema := fs.String("view-schema", "public", "live view schema")
		_ = fs.Parse(os.Args[2:])
		if *dsn == "" {
			fail("-dsn or GO_SEQUENT_PG_DSN required for SQL replay schema ops")
		}
		reg := event.NewRegistry()
		store, err := postgres.Open(ctx, *dsn, reg)
		if err != nil {
			fail(err.Error())
		}
		defer store.Close()

		var managed []string
		if *tables != "" {
			managed = strings.Split(*tables, ",")
		}
		pipe := &replay.Pipeline{
			Name:          *name,
			ReplaySchema:  *replaySchema,
			ViewSchema:    *viewSchema,
			ManagedTables: managed,
			Schema:        &replay.PostgresSchema{Pool: store.Pool()},
			State:         replay.NewMemoryState(),
		}
		if *replaySchema == "" {
			pipe.ReplaySchema = "replay_" + *name
		}
		run := func(label string, fn func(context.Context) error) {
			fmt.Printf("replay %s: starting\n", label)
			if err := fn(ctx); err != nil {
				fail(err.Error())
			}
			fmt.Printf("replay %s: ok\n", label)
		}
		switch *stage {
		case "prepare":
			run("prepare", pipe.Prepare)
		case "optimize":
			run("optimize", pipe.Optimize)
		case "golive":
			run("golive", pipe.GoLive)
		case "abort":
			run("abort", pipe.Abort)
		case "replay", "catchup", "all":
			fail("stage " + *stage + " requires an application-wired EventSource + SyncInTxn projectors; use the replay.Pipeline API from your binary, or run prepare/optimize/golive/abort for schema ops")
		default:
			fail("unknown stage " + *stage)
		}
	case "outbox":
		fs := flag.NewFlagSet("outbox", flag.ExitOnError)
		dsn := fs.String("dsn", envOr("GO_SEQUENT_PG_DSN", ""), "Postgres DSN")
		once := fs.Bool("once", false, "process one batch and exit")
		interval := fs.Duration("interval", time.Second, "poll interval")
		batch := fs.Int("batch", 10, "claim batch size")
		_ = fs.Parse(os.Args[2:])
		if *dsn == "" {
			fail("-dsn or GO_SEQUENT_PG_DSN required")
		}
		reg := event.NewRegistry()
		store, err := postgres.Open(ctx, *dsn, reg)
		if err != nil {
			fail(err.Error())
		}
		defer store.Close()
		ob := outbox.NewPostgres(store.Pool())
		poller := &outbox.Poller{
			Store:     ob,
			BatchSize: *batch,
			Interval:  *interval,
			Handler: func(ctx context.Context, msg outbox.Message) error {
				fmt.Printf("outbox: deliver id=%d topic=%s type=%s\n", msg.ID, msg.Topic, msg.EventType)
				return nil
			},
		}
		if *once {
			n, err := poller.RunOnce(ctx)
			if err != nil {
				fail(err.Error())
			}
			fmt.Printf("outbox: processed %d\n", n)
			return
		}
		ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		fmt.Println("outbox: polling (ctrl-c to stop)")
		if err := poller.Run(ctx); err != nil && ctx.Err() == nil {
			fail(err.Error())
		}
	case "help", "-h", "--help":
		usage()
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `go-sequent — ops CLI (migrate / replay / outbox poller)

Usage:
  go-sequent migrate -dsn $GO_SEQUENT_PG_DSN
  go-sequent replay -dsn $DSN -projector NAME -stage prepare|optimize|golive|abort [-tables t1,t2]
  go-sequent outbox -dsn $DSN [-once] [-interval 1s] [-batch 10]

Outbox drain uses Postgres table polling (Q6). Replace the default print handler
in your own binary for real delivery. Ledger projectors stay SyncInTxn (Q1).

`)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func fail(msg string) {
	fmt.Fprintln(os.Stderr, "error:", msg)
	os.Exit(1)
}
