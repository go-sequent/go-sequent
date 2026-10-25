package bankpg

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-sequent/go-sequent/event"
	"github.com/go-sequent/go-sequent/examples/bank"
	"github.com/go-sequent/go-sequent/projector"
	"github.com/go-sequent/go-sequent/tenancy"
	"github.com/go-sequent/go-sequent/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const tableBalances = "bank_account_balances"

// SQLBalanceProjector updates bank_account_balances inside the write txn (Q1).
type SQLBalanceProjector struct {
	DB projector.Persistor
}

// NewSQLBalanceProjector constructs a SyncInTxn SQL projector.
func NewSQLBalanceProjector(db projector.Persistor) *SQLBalanceProjector {
	return &SQLBalanceProjector{DB: db}
}

func (p *SQLBalanceProjector) ManagedTables() []string { return []string{tableBalances} }

func (p *SQLBalanceProjector) Handles() []string {
	return []string{
		bank.EventAccountOpened, bank.EventMoneyDeposited, bank.EventMoneyWithdrawn,
		bank.EventTransferDebited, bank.EventTransferCredited,
	}
}

func (p *SQLBalanceProjector) Handle(ctx context.Context, e event.Event) error {
	tenant, ok := tenancy.FromContext(ctx)
	if !ok {
		return fmt.Errorf("bankpg: tenant required in context for ledger projection")
	}
	switch ev := e.(type) {
	case *bank.AccountOpened:
		return p.DB.Exec(ctx, `
INSERT INTO bank_account_balances (tenant_id, account_id, owner, balance, currency, updated_at)
VALUES ($1, $2, $3, $4::numeric, $5, NOW())
ON CONFLICT (tenant_id, account_id) DO UPDATE
SET owner = EXCLUDED.owner, balance = EXCLUDED.balance, currency = EXCLUDED.currency, updated_at = NOW()`,
			tenant, ev.AggregateID(), ev.Owner, ev.Opening.Amount.String(), ev.Opening.Currency)
	case *bank.MoneyDeposited:
		return p.adjust(ctx, tenant, ev.AggregateID(), ev.Amount, true)
	case *bank.MoneyWithdrawn:
		return p.adjust(ctx, tenant, ev.AggregateID(), ev.Amount, false)
	case *bank.TransferDebited:
		return p.adjust(ctx, tenant, ev.AggregateID(), ev.Amount, false)
	case *bank.TransferCredited:
		return p.adjust(ctx, tenant, ev.AggregateID(), ev.Amount, true)
	}
	return nil
}

func (p *SQLBalanceProjector) adjust(ctx context.Context, tenant, id uuid.UUID, amount types.Money, add bool) error {
	op := "-"
	if add {
		op = "+"
	}
	sql := fmt.Sprintf(`
UPDATE bank_account_balances
SET balance = balance %s $3::numeric, updated_at = NOW()
WHERE tenant_id = $1 AND account_id = $2 AND currency = $4
RETURNING 1`, op)
	var one int
	err := p.DB.QueryRow(ctx, sql, tenant, id, amount.Amount.String(), amount.Currency).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("bankpg: unknown account %s (tenant %s)", id, tenant)
	}
	return err
}

// Balance reads the projected balance (outside or inside txn).
func (p *SQLBalanceProjector) Balance(ctx context.Context, tenant, accountID uuid.UUID) (types.Money, error) {
	var amount, currency string
	err := p.DB.QueryRow(ctx, `
SELECT balance::text, currency FROM bank_account_balances
WHERE tenant_id = $1 AND account_id = $2`, tenant, accountID).Scan(&amount, &currency)
	if err != nil {
		return types.Money{}, err
	}
	return types.NewMoney(amount, currency)
}
