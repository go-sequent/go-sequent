// Package sequent is the application façade for wiring go-sequent.
package sequent

import (
	"github.com/wenweih/go-sequent/aggregate"
	"github.com/wenweih/go-sequent/command"
	"github.com/wenweih/go-sequent/event"
	"github.com/wenweih/go-sequent/event/upcast"
	"github.com/wenweih/go-sequent/eventstore"
	"github.com/wenweih/go-sequent/idempotency"
	"github.com/wenweih/go-sequent/obs"
	"github.com/wenweih/go-sequent/publish"
	"github.com/wenweih/go-sequent/security"
	"github.com/wenweih/go-sequent/workflow"
)

// PublisherMode selects how events are published after append.
type PublisherMode int

const (
	// SyncInTxn runs projectors inside the write transaction (default, Q1).
	SyncInTxn PublisherMode = iota
)

// Config holds framework options.
type Config struct {
	PublisherMode PublisherMode
}

// App is a wired CommandService + registries + publisher.
type App struct {
	Config    Config
	Events    *event.Registry
	Upcasters *upcast.Chain
	Commands  *command.Router
	Repo      *aggregate.Repository
	Publisher *publish.SyncInTxn
	Service   *command.Service
	Obs       *obs.Instruments
}

// Option configures App construction.
type Option func(*App)

// WithStore sets EventStore + TxManager (memory or postgres).
func WithStore(store eventstore.Store, tx eventstore.TxManager) Option {
	return func(a *App) {
		a.Repo.Store = store
		a.Service.Tx = tx
		a.Repo.Publisher = a.Publisher
		if snaps, ok := store.(eventstore.SnapshotStore); ok {
			a.Repo.Snapshots = snaps
		}
	}
}

// WithAuthorizer installs AuthZ as a command filter.
func WithAuthorizer(auth security.Authorizer) Option {
	return func(a *App) {
		a.Service.Filters = append(a.Service.Filters, security.Filter{Auth: auth})
	}
}

// WithIdempotency installs tenant-scoped idempotency middleware.
func WithIdempotency(store idempotency.Store, strict bool) Option {
	return func(a *App) {
		a.Service.Middleware = append(a.Service.Middleware, idempotency.Middleware(idempotency.Config{
			Store:  store,
			Strict: strict,
		}))
	}
}

// WithObs attaches OpenTelemetry instruments to CommandService.
func WithObs(inst *obs.Instruments) Option {
	return func(a *App) {
		a.Obs = inst
		a.Service.Obs = inst
	}
}

// New constructs an App with SyncInTxn publisher by default.
func New(cfg Config, opts ...Option) *App {
	events := event.NewRegistry()
	pub := publish.NewSyncInTxn()
	repo := aggregate.NewRepository(nil, pub)
	router := command.NewRouter()
	svc := &command.Service{
		Router: router,
		Repo:   repo,
	}
	app := &App{
		Config:    cfg,
		Events:    events,
		Upcasters: upcast.NewChain(),
		Commands:  router,
		Repo:      repo,
		Publisher: pub,
		Service:   svc,
	}
	for _, opt := range opts {
		opt(app)
	}
	return app
}

// RegisterProjector registers a sync projector on the publisher.
func (a *App) RegisterProjector(h publish.Handler) {
	a.Publisher.Register(h)
}

// RegisterWorkflow registers a workflow (excluded from projection rebuild).
func (a *App) RegisterWorkflow(w workflow.Workflow) {
	a.Publisher.RegisterWorkflow(w)
}

// RegisterHandler registers a command handler.
func (a *App) RegisterHandler(h command.Handler) {
	a.Commands.Register(h)
}

// RegisterAggregate registers an aggregate factory by type name.
func (a *App) RegisterAggregate(aggregateType string, f aggregate.Factory) {
	a.Repo.RegisterFactory(aggregateType, f)
}

// RegisterUpcaster registers an event payload upcaster (v0).
func (a *App) RegisterUpcaster(u upcast.Upcaster) {
	a.Upcasters.Register(u)
}
