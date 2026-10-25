package bank

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/wenweih/go-sequent/event"
	"github.com/wenweih/go-sequent/types"
)

// BalanceProjector keeps an in-memory ledger balance view (SyncInTxn demo).
// Production apps would use projector.Persistor against SQL tables.
type BalanceProjector struct {
	mu       sync.RWMutex
	balances map[uuid.UUID]types.Money
	owners   map[uuid.UUID]string
}

// NewBalanceProjector constructs an empty balance view.
func NewBalanceProjector() *BalanceProjector {
	return &BalanceProjector{
		balances: make(map[uuid.UUID]types.Money),
		owners:   make(map[uuid.UUID]string),
	}
}

func (p *BalanceProjector) ManagedTables() []string { return []string{"account_balances"} }

func (p *BalanceProjector) Handles() []string {
	return []string{EventAccountOpened, EventMoneyDeposited, EventMoneyWithdrawn}
}

func (p *BalanceProjector) Handle(ctx context.Context, e event.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch ev := e.(type) {
	case *AccountOpened:
		p.balances[ev.AggregateID()] = ev.Opening
		p.owners[ev.AggregateID()] = ev.Owner
	case *MoneyDeposited:
		cur, ok := p.balances[ev.AggregateID()]
		if !ok {
			return fmt.Errorf("projector: unknown account %s", ev.AggregateID())
		}
		next, err := cur.Add(ev.Amount)
		if err != nil {
			return err
		}
		p.balances[ev.AggregateID()] = next
	case *MoneyWithdrawn:
		cur, ok := p.balances[ev.AggregateID()]
		if !ok {
			return fmt.Errorf("projector: unknown account %s", ev.AggregateID())
		}
		next, err := cur.Sub(ev.Amount)
		if err != nil {
			return err
		}
		p.balances[ev.AggregateID()] = next
	}
	return nil
}

// Balance returns the projected balance for an account.
func (p *BalanceProjector) Balance(id uuid.UUID) (types.Money, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	m, ok := p.balances[id]
	return m, ok
}
