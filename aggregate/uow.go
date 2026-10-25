package aggregate

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

type ctxKey int

const uowKey ctxKey = 1

// UnitOfWork is an identity map for one command-batch / request.
// Must never be shared across goroutines (design §2, D4).
type UnitOfWork struct {
	// order preserves registration/load order for commit publish order.
	order []uuid.UUID
	byID  map[uuid.UUID]Root
}

// NewUnitOfWork creates an empty UoW.
func NewUnitOfWork() *UnitOfWork {
	return &UnitOfWork{byID: make(map[uuid.UUID]Root)}
}

// WithUnitOfWork attaches uow to ctx.
func WithUnitOfWork(ctx context.Context, uow *UnitOfWork) context.Context {
	return context.WithValue(ctx, uowKey, uow)
}

// UoWFrom extracts the UnitOfWork from ctx, or nil.
func UoWFrom(ctx context.Context) *UnitOfWork {
	u, _ := ctx.Value(uowKey).(*UnitOfWork)
	return u
}

// Add registers a new aggregate in the identity map.
func (u *UnitOfWork) Add(agg Root) error {
	id := agg.ID()
	if _, ok := u.byID[id]; ok {
		return fmt.Errorf("aggregate: already tracked %s", id)
	}
	u.byID[id] = agg
	u.order = append(u.order, id)
	return nil
}

// Track puts an already-loaded aggregate into the map (idempotent by id).
func (u *UnitOfWork) Track(agg Root) {
	id := agg.ID()
	if _, ok := u.byID[id]; ok {
		return
	}
	u.byID[id] = agg
	u.order = append(u.order, id)
}

// Get returns a tracked aggregate by id.
func (u *UnitOfWork) Get(id uuid.UUID) (Root, bool) {
	a, ok := u.byID[id]
	return a, ok
}

// Aggregates returns tracked aggregates in registration/load order.
func (u *UnitOfWork) Aggregates() []Root {
	out := make([]Root, 0, len(u.order))
	for _, id := range u.order {
		out = append(out, u.byID[id])
	}
	return out
}

// Clear empties the identity map.
func (u *UnitOfWork) Clear() {
	u.byID = make(map[uuid.UUID]Root)
	u.order = nil
}
