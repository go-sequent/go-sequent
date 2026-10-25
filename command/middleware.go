package command

import "context"

// Middleware wraps command handling. Chain runs outer-first.
type Middleware func(ctx context.Context, cmd Command, next HandlerFunc) error

// HandlerFunc is the next step in a middleware chain (or the real handler).
type HandlerFunc func(ctx context.Context, cmd Command) error

// Chain composes middleware around a terminal HandlerFunc.
func Chain(terminal HandlerFunc, mw ...Middleware) HandlerFunc {
	h := terminal
	for i := len(mw) - 1; i >= 0; i-- {
		m := mw[i]
		next := h
		h = func(ctx context.Context, cmd Command) error {
			return m(ctx, cmd, next)
		}
	}
	return h
}

// Filter may reject a command before the handler (e.g. AuthZ).
type Filter interface {
	Filter(ctx context.Context, cmd Command) error
}
