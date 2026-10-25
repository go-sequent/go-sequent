package types_test

import (
	"encoding/json"
	"testing"

	"github.com/shopspring/decimal"
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

func TestMoneyRoundMulCmp(t *testing.T) {
	m := types.MustMoney("10.006", "USD").Round(2)
	if m.String() != "10.01 USD" {
		t.Fatalf("%s", m)
	}
	tax := m.Mul(decimal.RequireFromString("0.1"))
	if !tax.Amount.Equal(decimal.RequireFromString("1.001")) {
		t.Fatalf("%s", tax)
	}
	cmp, err := types.MustMoney("2", "USD").Cmp(types.MustMoney("1", "USD"))
	if err != nil || cmp != 1 {
		t.Fatalf("cmp=%d err=%v", cmp, err)
	}
}

func TestAssertNoFloatMoney(t *testing.T) {
	type bad struct{ Amount float64 }
	if err := types.AssertNoFloatMoney(bad{1}); err == nil {
		t.Fatal("expected error")
	}
	type good struct{ Amount types.Money }
	if err := types.AssertNoFloatMoney(good{types.MustMoney("1", "USD")}); err != nil {
		t.Fatal(err)
	}
}

func TestRejectScientificNotation(t *testing.T) {
	var m types.Money
	err := json.Unmarshal([]byte(`{"amount":1e2,"currency":"USD"}`), &m)
	if err == nil {
		t.Fatal("expected reject")
	}
}
