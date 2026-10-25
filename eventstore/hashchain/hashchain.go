// Package hashchain implements per-aggregate-stream event integrity (Q3).
//
// Canonical formula (SHA-256): hash input is UTF-8 lines joined by '\n'
// (no trailing newline after the last line):
//
//  1. event_type       — stable type name
//  2. aggregate_id     — UUID string
//  3. sequence_number  — decimal
//  4. created_at       — UTC RFC3339Nano
//  5. event_json       — exact stored payload bytes
//  6. prev_hash_hex    — 64 lowercase hex chars (genesis = 64 '0's)
//
// event_hash = SHA-256(input). Seq 1 uses Genesis (32 zero bytes) as prev_hash.
// No event signatures — Q3 is hash chain only.
package hashchain

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Genesis is the prev_hash of the first event in a stream (32 zero bytes).
var Genesis = make([]byte, sha256.Size)

// ErrTamper is returned when a stored hash does not match the recomputed chain.
var ErrTamper = errors.New("hashchain: integrity check failed")

// ErrIncomplete is returned when hash columns are missing on a row.
var ErrIncomplete = errors.New("hashchain: missing hash columns")

// Hex encodes a hash as lowercase hex (64 chars for SHA-256).
func Hex(b []byte) string { return hex.EncodeToString(b) }

// ParseHex decodes a lowercase/uppercase hex hash.
func ParseHex(s string) ([]byte, error) {
	return hex.DecodeString(strings.TrimSpace(s))
}

// Compute returns SHA-256 over the canonical lines for one event.
func Compute(eventType string, aggregateID uuid.UUID, seq int64, createdAt time.Time, eventJSON []byte, prevHash []byte) []byte {
	if prevHash == nil {
		prevHash = Genesis
	}
	if len(prevHash) == 0 {
		prevHash = Genesis
	}
	created := createdAt.UTC().Format(time.RFC3339Nano)
	var b strings.Builder
	b.Grow(len(eventType) + 36 + 20 + len(created) + len(eventJSON) + 64 + 16)
	b.WriteString(eventType)
	b.WriteByte('\n')
	b.WriteString(aggregateID.String())
	b.WriteByte('\n')
	b.WriteString(strconv.FormatInt(seq, 10))
	b.WriteByte('\n')
	b.WriteString(created)
	b.WriteByte('\n')
	b.Write(eventJSON)
	b.WriteByte('\n')
	b.WriteString(Hex(prevHash))
	sum := sha256.Sum256([]byte(b.String()))
	out := make([]byte, sha256.Size)
	copy(out, sum[:])
	return out
}

// Link holds one stored chain node for verification.
type Link struct {
	EventType   string
	AggregateID uuid.UUID
	Seq         int64
	CreatedAt   time.Time
	EventJSON   []byte
	PrevHash    []byte
	EventHash   []byte
}

// VerifyStream checks a full aggregate stream in sequence order.
func VerifyStream(links []Link) error {
	prev := append([]byte(nil), Genesis...)
	for i, l := range links {
		if len(l.EventHash) == 0 || len(l.PrevHash) == 0 {
			return fmt.Errorf("%w: seq=%d", ErrIncomplete, l.Seq)
		}
		if !bytesEqual(l.PrevHash, prev) {
			return fmt.Errorf("%w: prev_hash mismatch at seq=%d index=%d", ErrTamper, l.Seq, i)
		}
		want := Compute(l.EventType, l.AggregateID, l.Seq, l.CreatedAt, l.EventJSON, l.PrevHash)
		if !bytesEqual(want, l.EventHash) {
			return fmt.Errorf("%w: event_hash mismatch at seq=%d index=%d", ErrTamper, l.Seq, i)
		}
		prev = l.EventHash
	}
	return nil
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
