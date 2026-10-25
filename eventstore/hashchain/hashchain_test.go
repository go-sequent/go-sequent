package hashchain_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wenweih/go-sequent/eventstore/hashchain"
)

func TestComputeDeterministic(t *testing.T) {
	id := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	at := time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC)
	payload := []byte(`{"x":1}`)
	a := hashchain.Compute("t.E", id, 1, at, payload, hashchain.Genesis)
	b := hashchain.Compute("t.E", id, 1, at, payload, hashchain.Genesis)
	if hashchain.Hex(a) != hashchain.Hex(b) {
		t.Fatal("not deterministic")
	}
	c := hashchain.Compute("t.E", id, 1, at, []byte(`{"x":2}`), hashchain.Genesis)
	if hashchain.Hex(a) == hashchain.Hex(c) {
		t.Fatal("payload change should change hash")
	}
}

func TestVerifyStreamTamper(t *testing.T) {
	id := uuid.New()
	at := time.Now().UTC()
	p1 := []byte(`{"n":1}`)
	h1 := hashchain.Compute("e.A", id, 1, at, p1, hashchain.Genesis)
	p2 := []byte(`{"n":2}`)
	h2 := hashchain.Compute("e.B", id, 2, at, p2, h1)
	links := []hashchain.Link{
		{EventType: "e.A", AggregateID: id, Seq: 1, CreatedAt: at, EventJSON: p1, PrevHash: hashchain.Genesis, EventHash: h1},
		{EventType: "e.B", AggregateID: id, Seq: 2, CreatedAt: at, EventJSON: p2, PrevHash: h1, EventHash: h2},
	}
	if err := hashchain.VerifyStream(links); err != nil {
		t.Fatal(err)
	}
	links[1].EventJSON = []byte(`{"n":99}`)
	if err := hashchain.VerifyStream(links); err == nil {
		t.Fatal("expected tamper")
	}
}
