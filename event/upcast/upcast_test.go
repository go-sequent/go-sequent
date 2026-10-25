package upcast_test

import (
	"encoding/json"
	"testing"

	"github.com/go-sequent/go-sequent/event/upcast"
)

func TestChainRenameField(t *testing.T) {
	c := upcast.NewChain()
	c.Register(upcast.FuncUpcaster{
		From: "bank.MoneyDeposited.v0",
		Fn: func(p json.RawMessage) (string, json.RawMessage, error) {
			var m map[string]any
			if err := json.Unmarshal(p, &m); err != nil {
				return "", nil, err
			}
			if amt, ok := m["amt"]; ok {
				m["amount"] = amt
				delete(m, "amt")
			}
			out, err := json.Marshal(m)
			return "bank.MoneyDeposited", out, err
		},
	})
	typ, raw, err := c.Apply("bank.MoneyDeposited.v0", json.RawMessage(`{"amt":{"amount":"1","currency":"USD"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if typ != "bank.MoneyDeposited" {
		t.Fatalf("typ=%s", typ)
	}
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	if _, ok := m["amount"]; !ok {
		t.Fatalf("%v", m)
	}
}
