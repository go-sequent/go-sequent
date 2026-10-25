// Package sequent is the application façade for wiring go-sequent.
package sequent

import (
	"github.com/wenweih/go-sequent/aggregate"
	"github.com/wenweih/go-sequent/command"
	"github.com/wenweih/go-sequent/event"
	"github.com/wenweih/go-sequent/eventstore"
	"github.com/wenweih/go-sequent/publish"
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
	Commands  *command.Router
	Repo      *aggregate.Repository
	Publisher *publish.SyncInTxn
	Service   *command.Service
}

// Option configures App construction.
type Option func(*App)

// WithStore sets EventStore + TxManager (memory or postgres).
func WithStore(store eventstore.Store, tx eventstore.TxManager) Option {
	return func(a *App) {
		a.Repo.Store = store
		a.Service.Tx = tx
		a.Repo.Publisher = a.Publisher
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

// RegisterHandler registers a command handler.
func (a *App) RegisterHandler(h command.Handler) {
	a.Commands.Register(h)
}

// RegisterAggregate registers an aggregate factory by type name.
func (a *App) RegisterAggregate(aggregateType string, f aggregate.Factory) {
	a.Repo.RegisterFactory(aggregateType, f)
}
