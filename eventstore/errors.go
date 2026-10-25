// Package eventstore defines the EventStore interface and shared errors.
package eventstore

import "errors"

var (
	// ErrOptimisticConcurrency is returned when the expected next sequence
	// does not match the stream tip (or a PK conflict occurs on append).
	ErrOptimisticConcurrency = errors.New("eventstore: optimistic concurrency conflict")

	// ErrUniqueKeyViolation is returned when an aggregate unique key collides.
	ErrUniqueKeyViolation = errors.New("eventstore: unique key violation")

	// ErrStreamNotFound is returned when loading an unknown aggregate.
	ErrStreamNotFound = errors.New("eventstore: stream not found")

	// ErrHashChainTamper is returned when event hash chain verification fails (Q3).
	ErrHashChainTamper = errors.New("eventstore: hash chain integrity failure")

	// ErrHashChainIncomplete is returned when hash columns are missing.
	ErrHashChainIncomplete = errors.New("eventstore: hash chain incomplete")
)
