package bank

import (
	"github.com/wenweih/go-sequent/event"
	"github.com/wenweih/go-sequent/types"
)

const (
	EventAccountOpened      = "bank.AccountOpened"
	EventMoneyDeposited     = "bank.MoneyDeposited"
	EventMoneyWithdrawn     = "bank.MoneyWithdrawn"
	EventTransferDebited    = "bank.TransferDebited"
	EventTransferCredited   = "bank.TransferCredited"
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

// TransferDebited debits the source account in a same-bank transfer.
type TransferDebited struct {
	event.Base
	Amount          types.Money `json:"amount"`
	CounterpartyID  string      `json:"counterparty_id"`
	TransferID      string      `json:"transfer_id"`
}

func NewTransferDebited(amount types.Money, counterpartyID, transferID string) *TransferDebited {
	e := &TransferDebited{Amount: amount, CounterpartyID: counterpartyID, TransferID: transferID}
	e.SetEventType(EventTransferDebited)
	return e
}

func (e *TransferDebited) EventType() string { return EventTransferDebited }

// TransferCredited credits the destination account in a same-bank transfer.
type TransferCredited struct {
	event.Base
	Amount         types.Money `json:"amount"`
	CounterpartyID string      `json:"counterparty_id"`
	TransferID     string      `json:"transfer_id"`
}

func NewTransferCredited(amount types.Money, counterpartyID, transferID string) *TransferCredited {
	e := &TransferCredited{Amount: amount, CounterpartyID: counterpartyID, TransferID: transferID}
	e.SetEventType(EventTransferCredited)
	return e
}

func (e *TransferCredited) EventType() string { return EventTransferCredited }

// RegisterEvents registers bank event factories.
func RegisterEvents(r *event.Registry) {
	r.Register(EventAccountOpened, func() event.Event { return &AccountOpened{} })
	r.Register(EventMoneyDeposited, func() event.Event { return &MoneyDeposited{} })
	r.Register(EventMoneyWithdrawn, func() event.Event { return &MoneyWithdrawn{} })
	r.Register(EventTransferDebited, func() event.Event { return &TransferDebited{} })
	r.Register(EventTransferCredited, func() event.Event { return &TransferCredited{} })
}
