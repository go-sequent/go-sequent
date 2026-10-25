package bankpg

import (
	"fmt"

	"github.com/go-sequent/go-sequent/aggregate"
	"github.com/go-sequent/go-sequent/event"
	"github.com/go-sequent/go-sequent/eventstore"
	"github.com/go-sequent/go-sequent/examples/bank"
	"github.com/go-sequent/go-sequent/idempotency"
	"github.com/go-sequent/go-sequent/obs"
	"github.com/go-sequent/go-sequent/projector"
	"github.com/go-sequent/go-sequent/security"
	"github.com/go-sequent/go-sequent/sequent"
	"github.com/go-sequent/go-sequent/tenancy"
	"github.com/google/uuid"
)

// Options configures a production-shaped Postgres bank app.
type Options struct {
	StoreFactory StoreFactory
	// PersistorFactory is called after the store opens (same registry).
	PersistorFactory   func(store eventstore.Store) projector.Persistor
	IdempotencyFactory func(store eventstore.Store) idempotency.Store
	Authorizer         security.Authorizer
	// Instruments is optional OpenTelemetry; nil skips WithObs.
	Instruments *obs.Instruments
	// EnableOutbox docs-only stub: wire publish.WithOutbox yourself if needed.
	// Ledger projectors always stay on SyncInTxn (Q1).
}

// StoreFactory builds Postgres (or test double) backends bound to the app registry.
type StoreFactory func(reg *event.Registry) (store eventstore.Store, tx eventstore.TxManager, snaps eventstore.SnapshotStore, err error)

// App is a wired bank with SQL SyncInTxn balance projection.
type App struct {
	Sequent   *sequent.App
	Projector *SQLBalanceProjector
	Store     eventstore.Store
}

// NewApp wires domain from examples/bank + SQL BalanceProjector (pure library; no HTTP).
func NewApp(opts Options) (*App, error) {
	if opts.Authorizer == nil {
		opts.Authorizer = bank.AllowBankCommands()
	}
	var seqOpts []sequent.Option
	seqOpts = append(seqOpts, sequent.WithAuthorizer(opts.Authorizer))
	if opts.Instruments != nil {
		seqOpts = append(seqOpts, sequent.WithObs(opts.Instruments))
	}
	app := sequent.New(sequent.Config{PublisherMode: sequent.SyncInTxn}, seqOpts...)
	bank.RegisterEvents(app.Events)

	if opts.StoreFactory == nil {
		return nil, fmt.Errorf("bankpg: StoreFactory required")
	}
	store, tx, snaps, err := opts.StoreFactory(app.Events)
	if err != nil {
		return nil, err
	}
	if store == nil || tx == nil {
		return nil, fmt.Errorf("bankpg: store and tx required")
	}
	if opts.PersistorFactory == nil {
		return nil, fmt.Errorf("bankpg: PersistorFactory required")
	}
	db := opts.PersistorFactory(store)
	if db == nil {
		return nil, fmt.Errorf("bankpg: PersistorFactory returned nil")
	}

	// Tenancy before idempotency so Meta.TenantID is set from ctx.
	app.Service.Middleware = append(app.Service.Middleware, tenancy.Middleware)
	if opts.IdempotencyFactory != nil {
		if idemp := opts.IdempotencyFactory(store); idemp != nil {
			app.Service.Middleware = append(app.Service.Middleware,
				idempotency.Middleware(idempotency.Config{Store: idemp}))
		}
	}

	app.Repo.Store = store
	app.Repo.Publisher = app.Publisher
	if snaps != nil {
		app.Repo.Snapshots = snaps
	}
	app.Service.Tx = tx
	app.Service.Repo = app.Repo

	app.RegisterAggregate(bank.AggregateAccount, func(id uuid.UUID) aggregate.Root {
		return bank.NewAccount(id)
	})
	proj := NewSQLBalanceProjector(db)
	app.RegisterProjector(proj)
	app.RegisterHandler(&bank.Handler{Repo: app.Repo})

	return &App{Sequent: app, Projector: proj, Store: store}, nil
}
