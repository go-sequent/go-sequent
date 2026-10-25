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
	tenants  map[uuid.UUID]uuid.UUID // optional; filled when Open meta is known via events only
}

// NewBalanceProjector constructs an empty balance view.
func NewBalanceProjector() *BalanceProjector {
	return &BalanceProjector{
		balances: make(map[uuid.UUID]types.Money),
		owners:   make(map[uuid.UUID]string),
		tenants:  make(map[uuid.UUID]uuid.UUID),
	}
}

func (p *BalanceProjector) ManagedTables() []string { return []string{"account_balances"} }

func (p *BalanceProjector) Handles() []string {
	return []string{
		EventAccountOpened, EventMoneyDeposited, EventMoneyWithdrawn,
		EventTransferDebited, EventTransferCredited,
	}
}

func (p *BalanceProjector) Handle(ctx context.Context, e event.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch ev := e.(type) {
	case *AccountOpened:
		p.balances[ev.AggregateID()] = ev.Opening
		p.owners[ev.AggregateID()] = ev.Owner
	case *MoneyDeposited:
		return p.add(ev.AggregateID(), ev.Amount)
	case *MoneyWithdrawn:
		return p.sub(ev.AggregateID(), ev.Amount)
	case *TransferDebited:
		return p.sub(ev.AggregateID(), ev.Amount)
	case *TransferCredited:
		return p.add(ev.AggregateID(), ev.Amount)
	}
	return nil
}

func (p *BalanceProjector) add(id uuid.UUID, amount types.Money) error {
	cur, ok := p.balances[id]
	if !ok {
		return fmt.Errorf("projector: unknown account %s", id)
	}
	next, err := cur.Add(amount)
	if err != nil {
		return err
	}
	p.balances[id] = next
	return nil
}

func (p *BalanceProjector) sub(id uuid.UUID, amount types.Money) error {
	cur, ok := p.balances[id]
	if !ok {
		return fmt.Errorf("projector: unknown account %s", id)
	}
	next, err := cur.Sub(amount)
	if err != nil {
		return err
	}
	p.balances[id] = next
	return nil
}

// Balance returns the projected balance for an account.
func (p *BalanceProjector) Balance(id uuid.UUID) (types.Money, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	m, ok := p.balances[id]
	return m, ok
}

// Owner returns the projected owner name.
func (p *BalanceProjector) Owner(id uuid.UUID) (string, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	o, ok := p.owners[id]
	return o, ok
}
