package bank

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/wenweih/go-sequent/aggregate"
	"github.com/wenweih/go-sequent/command"
	"github.com/wenweih/go-sequent/tenancy"
)

// Handler handles bank commands.
type Handler struct {
	Repo *aggregate.Repository
}

func (h *Handler) Handles() []string {
	return []string{CmdOpenAccount, CmdDeposit, CmdWithdraw}
}

func (h *Handler) Handle(ctx context.Context, cmd command.Command) error {
	switch c := cmd.(type) {
	case *OpenAccount:
		acc := NewAccount(c.AccountID)
		if tid := c.Meta().TenantID; tid != uuid.Nil {
			acc.SetPartitionKey(tenancy.PartitionKey(tid))
		}
		if err := acc.OpenAccount(c.Owner, c.Currency, c.Opening); err != nil {
			return err
		}
		return h.Repo.Add(ctx, acc)
	case *Deposit:
		agg, err := h.Repo.Load(ctx, c.AccountID, AggregateAccount)
		if err != nil {
			return err
		}
		return agg.(*Account).Deposit(c.Amount)
	case *Withdraw:
		agg, err := h.Repo.Load(ctx, c.AccountID, AggregateAccount)
		if err != nil {
			return err
		}
		return agg.(*Account).Withdraw(c.Amount)
	default:
		return fmt.Errorf("bank: unhandled command %T", cmd)
	}
}
