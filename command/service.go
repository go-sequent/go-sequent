package command

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/wenweih/go-sequent/aggregate"
	"github.com/wenweih/go-sequent/eventstore"
)

// Committer commits the UoW for a command (typically *aggregate.Repository).
type Committer interface {
	Commit(ctx context.Context, cmd eventstore.CommandRecord) error
}

// Service executes commands with middleware, filters, context-scoped UoW,
// and a single DB transaction per Execute call (design Appendix A).
type Service struct {
	Middleware []Middleware
	Filters    []Filter
	Router     *Router
	Repo       Committer
	Tx         eventstore.TxManager
}

// Execute runs one or more commands in a single transaction / UoW.
func (s *Service) Execute(ctx context.Context, cmds ...Command) error {
	if s.Router == nil {
		return fmt.Errorf("command: Router is required")
	}
	if s.Tx == nil {
		return fmt.Errorf("command: Tx manager is required")
	}
	if len(cmds) == 0 {
		return nil
	}

	queue := make([]Command, len(cmds))
	copy(queue, cmds)

	return s.Tx.WithinTx(ctx, func(ctx context.Context) error {
		uow := aggregate.NewUnitOfWork()
		ctx = aggregate.WithUnitOfWork(ctx, uow)
		defer uow.Clear()

		for len(queue) > 0 {
			cmd := queue[0]
			queue = queue[1:]

			EnsureMeta(cmd.Meta())

			terminal := func(ctx context.Context, cmd Command) error {
				for _, f := range s.Filters {
					if err := f.Filter(ctx, cmd); err != nil {
						return err
					}
				}
				if v, ok := cmd.(Validator); ok {
					if err := v.Validate(); err != nil {
						return err
					}
				}
				if err := s.Router.Dispatch(ctx, cmd); err != nil {
					return err
				}
				if s.Repo != nil {
					if err := s.Repo.Commit(ctx, RecordOf(cmd)); err != nil {
						return err
					}
				}
				return nil
			}

			if err := Chain(terminal, s.Middleware...)(ctx, cmd); err != nil {
				return err
			}
		}
		return nil
	})
}

// RecordOf maps a Command to an eventstore.CommandRecord for persistence.
func RecordOf(cmd Command) eventstore.CommandRecord {
	meta := EnsureMeta(cmd.Meta())
	rec := eventstore.CommandRecord{
		ID:             meta.CommandID,
		Type:           cmd.CommandType(),
		TenantID:       meta.TenantID,
		UserID:         meta.UserID,
		IdempotencyKey: meta.IdempotencyKey,
		IssuedAt:       meta.IssuedAt,
		Payload:        cmd,
	}
	if id, ok := cmd.AggregateID(); ok && id != uuid.Nil {
		rec.AggregateID = &id
	}
	return rec
}

type enqueueKey struct{}

// WithQueue attaches a mutable command queue for workflow enqueue.
func WithQueue(ctx context.Context, q *[]Command) context.Context {
	return context.WithValue(ctx, enqueueKey{}, q)
}

// Enqueue appends a command to the context queue if present.
func Enqueue(ctx context.Context, cmd Command) {
	if q, ok := ctx.Value(enqueueKey{}).(*[]Command); ok && q != nil {
		*q = append(*q, cmd)
	}
}
