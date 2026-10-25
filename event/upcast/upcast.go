// Package upcast provides event payload upcasters v0 for type evolution.
package upcast

import (
	"encoding/json"
	"fmt"
	"sync"
)

// Upcaster transforms a raw JSON payload for a stored event type name
// into a (possibly new) type name + payload understood by the current code.
type Upcaster interface {
	// FromType is the stored event type this upcaster accepts.
	FromType() string
	// Upcast returns the target type name and transformed JSON.
	Upcast(payload json.RawMessage) (toType string, out json.RawMessage, err error)
}

// Chain applies upcasters until the type stabilizes or MaxHops is reached.
type Chain struct {
	mu      sync.RWMutex
	byFrom  map[string]Upcaster
	MaxHops int
}

// NewChain returns an empty upcaster chain (v0).
func NewChain() *Chain {
	return &Chain{byFrom: make(map[string]Upcaster), MaxHops: 8}
}

// Register adds an upcaster. Panics on duplicate FromType.
func (c *Chain) Register(u Upcaster) {
	c.mu.Lock()
	defer c.mu.Unlock()
	from := u.FromType()
	if _, ok := c.byFrom[from]; ok {
		panic(fmt.Sprintf("upcast: already registered for %q", from))
	}
	c.byFrom[from] = u
}

// Apply runs the chain starting from storedType / payload.
func (c *Chain) Apply(storedType string, payload json.RawMessage) (string, json.RawMessage, error) {
	if c == nil {
		return storedType, payload, nil
	}
	max := c.MaxHops
	if max <= 0 {
		max = 8
	}
	typ := storedType
	raw := payload
	for i := 0; i < max; i++ {
		c.mu.RLock()
		u, ok := c.byFrom[typ]
		c.mu.RUnlock()
		if !ok {
			return typ, raw, nil
		}
		nextType, nextRaw, err := u.Upcast(raw)
		if err != nil {
			return "", nil, fmt.Errorf("upcast %s: %w", typ, err)
		}
		if nextType == typ {
			return typ, nextRaw, nil
		}
		typ, raw = nextType, nextRaw
	}
	return "", nil, fmt.Errorf("upcast: exceeded max hops for %q", storedType)
}

// FuncUpcaster adapts a function to Upcaster.
type FuncUpcaster struct {
	From string
	Fn   func(json.RawMessage) (string, json.RawMessage, error)
}

func (f FuncUpcaster) FromType() string { return f.From }
func (f FuncUpcaster) Upcast(p json.RawMessage) (string, json.RawMessage, error) {
	return f.Fn(p)
}
