package bank

import (
	"github.com/wenweih/go-sequent/event"
	"github.com/wenweih/go-sequent/types"
)

const (
	EventAccountOpened   = "bank.AccountOpened"
	EventMoneyDeposited  = "bank.MoneyDeposited"
	EventMoneyWithdrawn  = "bank.MoneyWithdrawn"
)

// AccountOpened is emitted when an account is created.
type AccountOpened struct {
	event.Base
	Owner    string      `json:"owner"`
	Currency string      `json:"currency"`
	Opening  types.Money `json:"opening"`
}

func NewAccountOpened(owner, currency string, opening types.Money) *AccountOpened {
	e := &AccountOpened{Owner: owner, Currency: currency, Opening: opening}
	e.SetEventType(EventAccountOpened)
	return e
}

func (e *AccountOpened) EventType() string { return EventAccountOpened }

// MoneyDeposited credits the account.
type MoneyDeposited struct {
	event.Base
	Amount types.Money `json:"amount"`
}

func NewMoneyDeposited(amount types.Money) *MoneyDeposited {
	e := &MoneyDeposited{Amount: amount}
	e.SetEventType(EventMoneyDeposited)
	return e
}

func (e *MoneyDeposited) EventType() string { return EventMoneyDeposited }

// MoneyWithdrawn debits the account.
type MoneyWithdrawn struct {
	event.Base
	Amount types.Money `json:"amount"`
}

func NewMoneyWithdrawn(amount types.Money) *MoneyWithdrawn {
	e := &MoneyWithdrawn{Amount: amount}
	e.SetEventType(EventMoneyWithdrawn)
	return e
}

func (e *MoneyWithdrawn) EventType() string { return EventMoneyWithdrawn }

// RegisterEvents registers bank event factories.
func RegisterEvents(r *event.Registry) {
	r.Register(EventAccountOpened, func() event.Event { return &AccountOpened{} })
	r.Register(EventMoneyDeposited, func() event.Event { return &MoneyDeposited{} })
	r.Register(EventMoneyWithdrawn, func() event.Event { return &MoneyWithdrawn{} })
}
