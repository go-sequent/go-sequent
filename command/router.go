package command

import (
	"context"
	"fmt"
	"sync"
)

// Handler handles one or more command types.
type Handler interface {
	Handles() []string
	Handle(ctx context.Context, cmd Command) error
}

// Router dispatches commands by CommandType() string.
type Router struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

// NewRouter returns an empty router.
func NewRouter() *Router {
	return &Router{handlers: make(map[string]Handler)}
}

// Register associates the handler with each type it Handles().
// Panics on duplicate type registration.
func (r *Router) Register(h Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, t := range h.Handles() {
		if _, ok := r.handlers[t]; ok {
			panic(fmt.Sprintf("command: handler already registered for %q", t))
		}
		r.handlers[t] = h
	}
}

// Dispatch finds a handler for cmd and invokes it.
func (r *Router) Dispatch(ctx context.Context, cmd Command) error {
	r.mu.RLock()
	h, ok := r.handlers[cmd.CommandType()]
	r.mu.RUnlock()
	if !ok {
		return fmt.Errorf("command: no handler for %q", cmd.CommandType())
	}
	return h.Handle(ctx, cmd)
}
