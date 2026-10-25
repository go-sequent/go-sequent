// Package tombstone provides GDPR / retention hooks using tombstone-only strategy (Q5).
// Crypto-shredding is intentionally not productized.
package tombstone

import (
	"time"

	"github.com/go-sequent/go-sequent/event"
	"github.com/google/uuid"
)

const EventType = "gdpr.SubjectTombstoned"

// SubjectTombstoned marks a stream (or subject) as erased for processing purposes.
// Projectors should stop exposing identifiable data after this event.
type SubjectTombstoned struct {
	event.Base
	SubjectID string    `json:"subject_id"`
	Reason    string    `json:"reason,omitempty"`
	At        time.Time `json:"at"`
}

func NewSubjectTombstoned(subjectID, reason string) *SubjectTombstoned {
	e := &SubjectTombstoned{
		SubjectID: subjectID,
		Reason:    reason,
		At:        time.Now().UTC(),
	}
	e.SetEventType(EventType)
	return e
}

func (e *SubjectTombstoned) EventType() string { return EventType }

// Record is a row-oriented tombstone marker (matches event_tombstones table).
type Record struct {
	AggregateID    uuid.UUID
	SequenceNumber int64
	TombstonedAt   time.Time
	Reason         string
}

// Guide summarizes the product strategy for operators/docs.
const Guide = `GDPR / retention (Q5): tombstone only.
1. Emit compensating / tombstone events (e.g. gdpr.SubjectTombstoned).
2. Projectors scrub or stop serving identifiable fields.
3. Do not rely on crypto-shredding as the product path.
4. Physical delete of event rows is exceptional ops with stream-repair tooling.`
