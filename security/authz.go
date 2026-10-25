// Package security provides AuthZ hooks at the command boundary (design §8).
// Event signing / hash chains are intentionally out of scope (Q3).
package security

import (
	"context"
	"errors"
	"fmt"

	"github.com/wenweih/go-sequent/command"
)

// ErrDenied is returned when authorization fails.
var ErrDenied = errors.New("security: authorization denied")

// Authorizer checks whether a command may proceed.
// Install after authn context is attached, before the handler.
type Authorizer interface {
	Authorize(ctx context.Context, cmd command.Command) error
}

// AuthorizerFunc adapts a function to Authorizer.
type AuthorizerFunc func(ctx context.Context, cmd command.Command) error

func (f AuthorizerFunc) Authorize(ctx context.Context, cmd command.Command) error {
	return f(ctx, cmd)
}

// AllowAll authorizes every command (dev/test only).
type AllowAll struct{}

func (AllowAll) Authorize(context.Context, command.Command) error { return nil }

// DenyByDefault denies unless the command type is listed.
type DenyByDefault struct {
	Allow map[string]bool
}

func (d DenyByDefault) Authorize(_ context.Context, cmd command.Command) error {
	if d.Allow != nil && d.Allow[cmd.CommandType()] {
		return nil
	}
	return fmt.Errorf("%w: %s", ErrDenied, cmd.CommandType())
}

// Filter wraps an Authorizer as a command.Filter.
type Filter struct {
	Auth Authorizer
}

func (f Filter) Filter(ctx context.Context, cmd command.Command) error {
	if f.Auth == nil {
		return nil
	}
	return f.Auth.Authorize(ctx, cmd)
}

// Middleware wraps an Authorizer as command middleware.
func Middleware(auth Authorizer) command.Middleware {
	return func(ctx context.Context, cmd command.Command, next command.HandlerFunc) error {
		if auth != nil {
			if err := auth.Authorize(ctx, cmd); err != nil {
				return err
			}
		}
		return next(ctx, cmd)
	}
}
