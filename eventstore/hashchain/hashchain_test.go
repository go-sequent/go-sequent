package hashchain_test

import (
	"testing"
	"time"

	"github.com/go-sequent/go-sequent/eventstore/hashchain"
	"github.com/google/uuid"
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
	at := hashchain.CanonicalTime(time.Now().UTC())
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

func TestCanonicalTimeMatchesMicrosecondRoundTrip(t *testing.T) {
	at := time.Date(2026, 10, 26, 12, 0, 0, 123456789, time.UTC)
	c := hashchain.CanonicalTime(at)
	if c.Nanosecond() != 123456000 {
		t.Fatalf("want microsecond truncate, got %d", c.Nanosecond())
	}
	id := uuid.New()
	payload := []byte(`{"ok":true}`)
	h1 := hashchain.Compute("e", id, 1, at, payload, nil)
	// Simulate PG TIMESTAMPTZ reload (microseconds only).
	reloaded := time.Unix(0, c.UnixNano()).UTC()
	h2 := hashchain.Compute("e", id, 1, reloaded, payload, nil)
	if hashchain.Hex(h1) != hashchain.Hex(h2) {
		t.Fatal("hash must survive microsecond round-trip")
	}
}
