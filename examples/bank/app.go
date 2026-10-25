package bank

import (
	"fmt"

	"github.com/go-sequent/go-sequent/aggregate"
	"github.com/go-sequent/go-sequent/event"
	"github.com/go-sequent/go-sequent/eventstore"
	"github.com/go-sequent/go-sequent/idempotency"
	"github.com/go-sequent/go-sequent/security"
	"github.com/go-sequent/go-sequent/sequent"
	"github.com/go-sequent/go-sequent/tenancy"
	"github.com/google/uuid"
)

// StoreFactory builds store backends bound to the app event registry.
type StoreFactory func(reg *event.Registry) (store eventstore.Store, tx eventstore.TxManager, snaps eventstore.SnapshotStore, err error)

// DemoOptions configures the in-memory / lightweight demo bank app.
// For a Postgres SQL ledger sample see examples/bankpg.
type DemoOptions struct {
	// StoreFactory is preferred: receives the app registry for correct decode.
	StoreFactory StoreFactory
	// Legacy direct wiring (Store must share the same registry as RegisterEvents).
	Store     eventstore.Store
	Tx        eventstore.TxManager
	Snapshots eventstore.SnapshotStore
	// Idempotency is used when set; otherwise IdempotencyFactory may build one after the store opens.
	Idempotency        idempotency.Store
	IdempotencyFactory func(store eventstore.Store) idempotency.Store
	Authorizer         security.Authorizer
	EnableTenancyMW    bool
}

// DemoApp is a wired bank demo with SyncInTxn balance projection.
type DemoApp struct {
	App       *sequent.App
	Projector *BalanceProjector
	Workflow  *NotifyWorkflow
}

// NewDemoApp wires CommandService + bank domain (pure library; no HTTP).
func NewDemoApp(opts DemoOptions) (*DemoApp, error) {
	if opts.Authorizer == nil {
		opts.Authorizer = security.AllowAll{}
	}
	app := sequent.New(sequent.Config{PublisherMode: sequent.SyncInTxn},
		sequent.WithAuthorizer(opts.Authorizer),
	)
	RegisterEvents(app.Events)

	store, tx, snaps := opts.Store, opts.Tx, opts.Snapshots
	if opts.StoreFactory != nil {
		var err error
		store, tx, snaps, err = opts.StoreFactory(app.Events)
		if err != nil {
			return nil, err
		}
	}
	if store == nil || tx == nil {
		return nil, fmt.Errorf("bank: Store and Tx required (use StoreFactory)")
	}

	// Tenancy must run before idempotency so Meta.TenantID is populated from ctx.
	if opts.EnableTenancyMW {
		app.Service.Middleware = append(app.Service.Middleware, tenancy.Middleware)
	}
	idemp := opts.Idempotency
	if idemp == nil && opts.IdempotencyFactory != nil {
		idemp = opts.IdempotencyFactory(store)
	}
	if idemp != nil {
		app.Service.Middleware = append(app.Service.Middleware,
			idempotency.Middleware(idempotency.Config{Store: idemp}))
	}

	app.Repo.Store = store
	app.Repo.Publisher = app.Publisher
	if snaps != nil {
		app.Repo.Snapshots = snaps
	}
	app.Service.Tx = tx
	app.Service.Repo = app.Repo

	app.RegisterAggregate(AggregateAccount, func(id uuid.UUID) aggregate.Root {
		return NewAccount(id)
	})
	proj := NewBalanceProjector()
	app.RegisterProjector(proj)
	wf := &NotifyWorkflow{}
	app.RegisterWorkflow(wf)
	app.RegisterHandler(&Handler{Repo: app.Repo})

	return &DemoApp{App: app, Projector: proj, Workflow: wf}, nil
}

// MustDemoApp is NewDemoApp that panics on error (tests/fixtures).
func MustDemoApp(opts DemoOptions) *DemoApp {
	d, err := NewDemoApp(opts)
	if err != nil {
		panic(err)
	}
	return d
}

// AllowBankCommands is a deny-by-default AuthZ allow-list for the demo.
func AllowBankCommands() security.Authorizer {
	return security.DenyByDefault{Allow: map[string]bool{
		CmdOpenAccount: true,
		CmdDeposit:     true,
		CmdWithdraw:    true,
		CmdTransfer:    true,
	}}
}
