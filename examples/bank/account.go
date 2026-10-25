package bank

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/wenweih/go-sequent/aggregate"
	"github.com/wenweih/go-sequent/event"
	"github.com/wenweih/go-sequent/types"
)

const AggregateAccount = "bank.Account"

// Account is a simple ledger aggregate (demo).
type Account struct {
	aggregate.Base
	Owner    string
	Currency string
	Balance  types.Money
	Open     bool
}

// NewAccount constructs an empty account ready for history or Open.
func NewAccount(id uuid.UUID) *Account {
	a := &Account{}
	a.Init(id, AggregateAccount)
	a.On(&AccountOpened{}, func(e event.Event) {
		ev := e.(*AccountOpened)
		a.Owner = ev.Owner
		a.Currency = ev.Currency
		a.Balance = ev.Opening
		a.Open = true
		a.SetUniqueKeys(map[string]any{
			"bank.Account.owner": a.Owner,
		})
	})
	a.On(&MoneyDeposited{}, func(e event.Event) {
		ev := e.(*MoneyDeposited)
		bal, err := a.Balance.Add(ev.Amount)
		if err != nil {
			panic(err)
		}
		a.Balance = bal
	})
	a.On(&MoneyWithdrawn{}, func(e event.Event) {
		ev := e.(*MoneyWithdrawn)
		bal, err := a.Balance.Sub(ev.Amount)
		if err != nil {
			panic(err)
		}
		a.Balance = bal
	})
	return a
}

// OpenAccount opens the account with an opening balance (decimal money).
func (a *Account) OpenAccount(owner, currency string, opening types.Money) error {
	if a.Open {
		return fmt.Errorf("account already open")
	}
	if owner == "" {
		return fmt.Errorf("owner required")
	}
	if opening.Currency != currency {
		return fmt.Errorf("opening currency mismatch")
	}
	if opening.IsNegative() {
		return fmt.Errorf("opening balance cannot be negative")
	}
	a.Apply(NewAccountOpened(owner, currency, opening))
	return nil
}

// Deposit credits money.
func (a *Account) Deposit(amount types.Money) error {
	if !a.Open {
		return fmt.Errorf("account not open")
	}
	if amount.IsZero() || amount.IsNegative() {
		return fmt.Errorf("deposit amount must be positive")
	}
	if amount.Currency != a.Currency {
		return fmt.Errorf("currency mismatch")
	}
	a.Apply(NewMoneyDeposited(amount))
	return nil
}

// Withdraw debits money if funds allow.
func (a *Account) Withdraw(amount types.Money) error {
	if !a.Open {
		return fmt.Errorf("account not open")
	}
	if amount.IsZero() || amount.IsNegative() {
		return fmt.Errorf("withdraw amount must be positive")
	}
	if amount.Currency != a.Currency {
		return fmt.Errorf("currency mismatch")
	}
	next, err := a.Balance.Sub(amount)
	if err != nil {
		return err
	}
	if next.IsNegative() {
		return fmt.Errorf("insufficient funds")
	}
	a.Apply(NewMoneyWithdrawn(amount))
	return nil
}

// Invariants ensures ledger consistency.
func (a *Account) Invariants() error {
	if a.Open && a.Balance.IsNegative() {
		return fmt.Errorf("invariant: balance negative")
	}
	return nil
}

// UniqueKeys scopes owner uniqueness (Sequent-style).
func (a *Account) UniqueKeys() map[string]any {
	if !a.Open {
		return nil
	}
	return map[string]any{"bank.Account.owner": a.Owner}
}

const accountSnapshotVersion = 1

type accountSnap struct {
	Owner    string      `json:"owner"`
	Currency string      `json:"currency"`
	Balance  types.Money `json:"balance"`
	Open     bool        `json:"open"`
}

func (a *Account) SnapshotVersion() int { return accountSnapshotVersion }

func (a *Account) MarshalSnapshot() (json.RawMessage, error) {
	return json.Marshal(accountSnap{
		Owner: a.Owner, Currency: a.Currency, Balance: a.Balance, Open: a.Open,
	})
}

func (a *Account) UnmarshalSnapshot(data json.RawMessage) error {
	var s accountSnap
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	a.Owner, a.Currency, a.Balance, a.Open = s.Owner, s.Currency, s.Balance, s.Open
	if a.Open {
		a.SetUniqueKeys(map[string]any{"bank.Account.owner": a.Owner})
	}
	return nil
}
