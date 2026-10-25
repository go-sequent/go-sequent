package bank

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/wenweih/go-sequent/command"
	"github.com/wenweih/go-sequent/types"
)

const (
	CmdOpenAccount = "bank.OpenAccount"
	CmdDeposit     = "bank.Deposit"
	CmdWithdraw    = "bank.Withdraw"
	CmdTransfer    = "bank.Transfer"
)

// OpenAccount opens a new account.
type OpenAccount struct {
	command.Base
	AccountID uuid.UUID   `json:"account_id"`
	Owner     string      `json:"owner"`
	Currency  string      `json:"currency"`
	Opening   types.Money `json:"opening"`
}

func (c *OpenAccount) CommandType() string { return CmdOpenAccount }
func (c *OpenAccount) AggregateID() (uuid.UUID, bool) {
	return c.AccountID, c.AccountID != uuid.Nil
}
func (c *OpenAccount) Validate() error {
	if c.AccountID == uuid.Nil {
		return fmt.Errorf("account_id required")
	}
	if c.Owner == "" {
		return fmt.Errorf("owner required")
	}
	if c.Currency == "" {
		return fmt.Errorf("currency required")
	}
	return nil
}

// Deposit money into an account.
type Deposit struct {
	command.Base
	AccountID uuid.UUID   `json:"account_id"`
	Amount    types.Money `json:"amount"`
}

func (c *Deposit) CommandType() string { return CmdDeposit }
func (c *Deposit) AggregateID() (uuid.UUID, bool) {
	return c.AccountID, c.AccountID != uuid.Nil
}
func (c *Deposit) Validate() error {
	if c.AccountID == uuid.Nil {
		return fmt.Errorf("account_id required")
	}
	if c.Amount.IsZero() || c.Amount.IsNegative() {
		return fmt.Errorf("amount must be positive")
	}
	return nil
}

// Withdraw money from an account.
type Withdraw struct {
	command.Base
	AccountID uuid.UUID   `json:"account_id"`
	Amount    types.Money `json:"amount"`
}

func (c *Withdraw) CommandType() string { return CmdWithdraw }
func (c *Withdraw) AggregateID() (uuid.UUID, bool) {
	return c.AccountID, c.AccountID != uuid.Nil
}
func (c *Withdraw) Validate() error {
	if c.AccountID == uuid.Nil {
		return fmt.Errorf("account_id required")
	}
	if c.Amount.IsZero() || c.Amount.IsNegative() {
		return fmt.Errorf("amount must be positive")
	}
	return nil
}

// Transfer moves money between two accounts in one UoW (demo same-bank transfer).
type Transfer struct {
	command.Base
	FromAccountID uuid.UUID   `json:"from_account_id"`
	ToAccountID   uuid.UUID   `json:"to_account_id"`
	Amount        types.Money `json:"amount"`
	TransferID    string      `json:"transfer_id"`
}

func (c *Transfer) CommandType() string { return CmdTransfer }
func (c *Transfer) AggregateID() (uuid.UUID, bool) {
	return c.FromAccountID, c.FromAccountID != uuid.Nil
}
func (c *Transfer) Validate() error {
	if c.FromAccountID == uuid.Nil || c.ToAccountID == uuid.Nil {
		return fmt.Errorf("from and to account_id required")
	}
	if c.FromAccountID == c.ToAccountID {
		return fmt.Errorf("cannot transfer to same account")
	}
	if c.Amount.IsZero() || c.Amount.IsNegative() {
		return fmt.Errorf("amount must be positive")
	}
	if c.TransferID == "" {
		return fmt.Errorf("transfer_id required")
	}
	return nil
}
