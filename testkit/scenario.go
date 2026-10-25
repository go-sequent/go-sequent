// Package testkit provides Given/When/Expect helpers for aggregate testing.
package testkit

import (
	"fmt"
	"reflect"

	"github.com/google/uuid"
	"github.com/wenweih/go-sequent/aggregate"
	"github.com/wenweih/go-sequent/event"
	"github.com/wenweih/go-sequent/types"
)

// Scenario runs Given → When → Expect against an aggregate.
type Scenario struct {
	factory  func(id uuid.UUID) aggregate.Root
	id       uuid.UUID
	given    []event.Event
	when     func(agg aggregate.Root) error
	err      error
	produced []event.Event
}

// For starts a scenario with an aggregate factory and id.
func For(id uuid.UUID, factory func(id uuid.UUID) aggregate.Root) *Scenario {
	return &Scenario{factory: factory, id: id}
}

// Given sets prior history events (envelope fields filled automatically).
func (s *Scenario) Given(events ...event.Event) *Scenario {
	s.given = events
	return s
}

// When runs a domain action on the rehydrated aggregate.
func (s *Scenario) When(fn func(agg aggregate.Root) error) *Scenario {
	s.when = fn
	return s
}

// Run executes Given/When and captures produced events / error.
func (s *Scenario) Run() *Scenario {
	agg := s.factory(s.id)
	for i, e := range s.given {
		if env, ok := e.(event.Envelope); ok {
			env.SetAggregateID(s.id)
			env.SetSequenceNumber(int64(i + 1))
		}
	}
	if err := agg.LoadFromHistory(s.given); err != nil {
		s.err = err
		return s
	}
	if s.when != nil {
		s.err = s.when(agg)
	}
	s.produced = agg.UncommittedEvents()
	return s
}

// ExpectError asserts When returned an error (optionally matching message substring).
func (s *Scenario) ExpectError(substr string) error {
	if s.err == nil {
		return fmt.Errorf("testkit: expected error containing %q, got nil", substr)
	}
	if substr != "" && !contains(s.err.Error(), substr) {
		return fmt.Errorf("testkit: error %q does not contain %q", s.err.Error(), substr)
	}
	return nil
}

// ExpectEvents asserts exact event types (and optional deep-equal payloads).
func (s *Scenario) ExpectEvents(want ...event.Event) error {
	if s.err != nil {
		return fmt.Errorf("testkit: unexpected error: %w", s.err)
	}
	if len(s.produced) != len(want) {
		return fmt.Errorf("testkit: got %d events, want %d (%v)", len(s.produced), len(want), typeNames(s.produced))
	}
	for i := range want {
		if s.produced[i].EventType() != want[i].EventType() {
			return fmt.Errorf("testkit: event[%d] type %s, want %s", i, s.produced[i].EventType(), want[i].EventType())
		}
		if !payloadEqual(s.produced[i], want[i]) {
			return fmt.Errorf("testkit: event[%d] payload mismatch\n got %#v\nwant %#v", i, s.produced[i], want[i])
		}
	}
	return nil
}

// ExpectNoEvents asserts the action produced no events and no error.
func (s *Scenario) ExpectNoEvents() error {
	if s.err != nil {
		return fmt.Errorf("testkit: unexpected error: %w", s.err)
	}
	if len(s.produced) != 0 {
		return fmt.Errorf("testkit: expected no events, got %v", typeNames(s.produced))
	}
	return nil
}

// Produced returns events emitted by When.
func (s *Scenario) Produced() []event.Event { return s.produced }

// Err returns the When/load error.
func (s *Scenario) Err() error { return s.err }

func typeNames(evs []event.Event) []string {
	out := make([]string, len(evs))
	for i, e := range evs {
		out[i] = e.EventType()
	}
	return out
}

func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && (s == sub || len(sub) == 0 || stringIndex(s, sub) >= 0))
}

func stringIndex(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func payloadEqual(a, b event.Event) bool {
	// Compare exported fields via reflect, ignoring envelope Base.
	va, vb := reflect.ValueOf(a), reflect.ValueOf(b)
	if va.Type() != vb.Type() {
		return false
	}
	if va.Kind() == reflect.Pointer {
		va, vb = va.Elem(), vb.Elem()
	}
	for i := 0; i < va.NumField(); i++ {
		f := va.Type().Field(i)
		if !f.IsExported() {
			continue
		}
		if f.Anonymous {
			continue // skip embedded Base
		}
		fa, fb := va.Field(i).Interface(), vb.Field(i).Interface()
		if m, ok := fa.(types.Money); ok {
			om, ok2 := fb.(types.Money)
			if !ok2 || m.Currency != om.Currency || !m.Amount.Equal(om.Amount) {
				return false
			}
			continue
		}
		if !reflect.DeepEqual(fa, fb) {
			return false
		}
	}
	return true
}

// BanFloat64Money walks cmd/event structs and fails if any float64 field is
// named like money (Amount, Balance, …) — soft lint for examples/tests.
func BanFloat64Money(v any) error {
	return walkBanFloat(reflect.ValueOf(v), reflect.TypeOf(v))
}

func walkBanFloat(v reflect.Value, t reflect.Type) error {
	if !v.IsValid() {
		return nil
	}
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return nil
		}
		return walkBanFloat(v.Elem(), v.Elem().Type())
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			f := t.Field(i)
			if f.Type.Kind() == reflect.Float64 && looksLikeMoney(f.Name) {
				return fmt.Errorf("testkit: float64 money field %s.%s forbidden (use types.Money)", t.Name(), f.Name)
			}
			if err := walkBanFloat(v.Field(i), f.Type); err != nil {
				return err
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			if err := walkBanFloat(v.Index(i), v.Index(i).Type()); err != nil {
				return err
			}
		}
	}
	return nil
}

func looksLikeMoney(name string) bool {
	switch name {
	case "Amount", "Balance", "Money", "Credit", "Debit", "Price", "Fee", "Total":
		return true
	default:
		return false
	}
}
