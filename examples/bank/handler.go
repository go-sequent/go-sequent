package bank

import (
	"context"
	"fmt"

	"github.com/go-sequent/go-sequent/aggregate"
	"github.com/go-sequent/go-sequent/command"
	"github.com/go-sequent/go-sequent/tenancy"
	"github.com/google/uuid"
)

// Handler handles bank commands.
type Handler struct {
	Repo *aggregate.Repository
}

func (h *Handler) Handles() []string {
	return []string{CmdOpenAccount, CmdDeposit, CmdWithdraw, CmdTransfer}
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
	case *Transfer:
		from, err := h.Repo.Load(ctx, c.FromAccountID, AggregateAccount)
		if err != nil {
			return err
		}
		to, err := h.Repo.Load(ctx, c.ToAccountID, AggregateAccount)
		if err != nil {
			return err
		}
		fromAcc, toAcc := from.(*Account), to.(*Account)
		if fromAcc.Currency != toAcc.Currency {
			return fmt.Errorf("currency mismatch between accounts")
		}
		if err := fromAcc.DebitTransfer(c.Amount, c.ToAccountID.String(), c.TransferID); err != nil {
			return err
		}
		return toAcc.CreditTransfer(c.Amount, c.FromAccountID.String(), c.TransferID)
	default:
		return fmt.Errorf("bank: unhandled command %T", cmd)
	}
}
