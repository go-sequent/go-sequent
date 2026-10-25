// Package types provides shared value types for go-sequent applications.
package types

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/shopspring/decimal"
)

// Money is a decimal monetary amount with an ISO-4217 currency code.
// float64 must never be used for money (design Q2 / D7).
//
// JSON encodes Amount as a decimal *string* (never a JSON number) so parsers
// cannot silently coerce to float64. Store exact event amounts; never recompute
// historical money from floats. Rounding policy is domain-owned — use Round.
type Money struct {
	Amount   decimal.Decimal `json:"amount"`
	Currency string          `json:"currency"`
}

// NewMoney parses amount as a decimal string with the given currency.
func NewMoney(amount, currency string) (Money, error) {
	currency = strings.ToUpper(strings.TrimSpace(currency))
	if currency == "" {
		return Money{}, fmt.Errorf("types.Money: currency is required")
	}
	d, err := decimal.NewFromString(strings.TrimSpace(amount))
	if err != nil {
		return Money{}, fmt.Errorf("types.Money: invalid amount %q: %w", amount, err)
	}
	return Money{Amount: d, Currency: currency}, nil
}

// MustMoney is like NewMoney but panics on error (for fixtures/tests).
func MustMoney(amount, currency string) Money {
	m, err := NewMoney(amount, currency)
	if err != nil {
		panic(err)
	}
	return m
}

// Zero returns a zero amount in the given currency.
func Zero(currency string) Money {
	return Money{Amount: decimal.Zero, Currency: strings.ToUpper(strings.TrimSpace(currency))}
}

// Add returns m + other. Currencies must match.
func (m Money) Add(other Money) (Money, error) {
	if err := m.sameCurrency(other); err != nil {
		return Money{}, err
	}
	return Money{Amount: m.Amount.Add(other.Amount), Currency: m.Currency}, nil
}

// Sub returns m - other. Currencies must match.
func (m Money) Sub(other Money) (Money, error) {
	if err := m.sameCurrency(other); err != nil {
		return Money{}, err
	}
	return Money{Amount: m.Amount.Sub(other.Amount), Currency: m.Currency}, nil
}

// Mul multiplies by a decimal factor (e.g. tax rate). Currency unchanged.
func (m Money) Mul(factor decimal.Decimal) Money {
	return Money{Amount: m.Amount.Mul(factor), Currency: m.Currency}
}

// Round returns m rounded to places decimal places (domain chooses mode via shopspring).
func (m Money) Round(places int32) Money {
	return Money{Amount: m.Amount.Round(places), Currency: m.Currency}
}

// Cmp compares amounts; -1 if m < other, 0 if equal, 1 if m > other.
func (m Money) Cmp(other Money) (int, error) {
	if err := m.sameCurrency(other); err != nil {
		return 0, err
	}
	return m.Amount.Cmp(other.Amount), nil
}

// Equal reports same currency and amount.
func (m Money) Equal(other Money) bool {
	return m.Currency == other.Currency && m.Amount.Equal(other.Amount)
}

func (m Money) sameCurrency(other Money) error {
	if m.Currency != other.Currency {
		return fmt.Errorf("types.Money: currency mismatch %s vs %s", m.Currency, other.Currency)
	}
	return nil
}

// IsNegative reports whether the amount is strictly less than zero.
func (m Money) IsNegative() bool { return m.Amount.IsNegative() }

// IsPositive reports whether the amount is strictly greater than zero.
func (m Money) IsPositive() bool { return m.Amount.IsPositive() }

// IsZero reports whether the amount is zero.
func (m Money) IsZero() bool { return m.Amount.IsZero() }

// String returns a human-readable representation.
func (m Money) String() string {
	return m.Amount.String() + " " + m.Currency
}

// MarshalJSON encodes amount as a decimal string (never float).
func (m Money) MarshalJSON() ([]byte, error) {
	type wire struct {
		Amount   string `json:"amount"`
		Currency string `json:"currency"`
	}
	return json.Marshal(wire{Amount: m.Amount.String(), Currency: m.Currency})
}

// UnmarshalJSON decodes amount from a decimal string.
// JSON numbers without quotes are rejected to avoid float coercion.
func (m *Money) UnmarshalJSON(data []byte) error {
	type wire struct {
		Amount   json.RawMessage `json:"amount"`
		Currency string          `json:"currency"`
	}
	var w wire
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}
	m.Currency = strings.ToUpper(strings.TrimSpace(w.Currency))
	if m.Currency == "" {
		return fmt.Errorf("types.Money: currency is required")
	}
	raw := strings.TrimSpace(string(w.Amount))
	if len(raw) == 0 {
		return fmt.Errorf("types.Money: amount is required")
	}
	// Prefer quoted decimal strings; bare JSON numbers are accepted only if
	// they parse as decimal without exponent (still discouraged).
	if raw[0] == '"' {
		raw = strings.Trim(raw, `"`)
	} else if strings.ContainsAny(raw, "eE") {
		return fmt.Errorf("types.Money: scientific notation not allowed (use decimal string)")
	}
	d, err := decimal.NewFromString(raw)
	if err != nil {
		return fmt.Errorf("types.Money: invalid amount %s: %w", raw, err)
	}
	m.Amount = d
	return nil
}
