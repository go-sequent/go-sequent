package event

import (
	"fmt"
	"sync"
)

// Factory constructs a zero-value event of a registered type.
type Factory func() Event

// Registry maps stable event type names to factories for (de)serialization.
type Registry struct {
	mu   sync.RWMutex
	byName map[string]Factory
}

// NewRegistry returns an empty type registry.
func NewRegistry() *Registry {
	return &Registry{byName: make(map[string]Factory)}
}

// Register associates a type name with a factory. Panics on duplicate.
func (r *Registry) Register(name string, f Factory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.byName[name]; ok {
		panic(fmt.Sprintf("event: type %q already registered", name))
	}
	r.byName[name] = f
}

// New returns a fresh event instance for name.
func (r *Registry) New(name string) (Event, error) {
	r.mu.RLock()
	f, ok := r.byName[name]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("event: unknown type %q", name)
	}
	return f(), nil
}

// Has reports whether name is registered.
func (r *Registry) Has(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.byName[name]
	return ok
}
