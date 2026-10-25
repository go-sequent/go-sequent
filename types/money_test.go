package types_test

import (
	"encoding/json"
	"testing"

	"github.com/wenweih/go-sequent/types"
)

func TestMoneyRoundTripJSON(t *testing.T) {
	m := types.MustMoney("12.34", "CNY")
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var got types.Money
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if !got.Amount.Equal(m.Amount) || got.Currency != m.Currency {
		t.Fatalf("got %#v want %#v", got, m)
	}
	if string(b) != `{"amount":"12.34","currency":"CNY"}` {
		t.Fatalf("unexpected json: %s", b)
	}
}

func TestMoneyAddSub(t *testing.T) {
	a := types.MustMoney("10.00", "USD")
	b := types.MustMoney("2.50", "USD")
	sum, err := a.Add(b)
	if err != nil {
		t.Fatal(err)
	}
	if sum.String() != "12.5 USD" {
		t.Fatalf("sum=%s", sum)
	}
	diff, err := a.Sub(b)
	if err != nil {
		t.Fatal(err)
	}
	if diff.String() != "7.5 USD" {
		t.Fatalf("diff=%s", diff)
	}
}
