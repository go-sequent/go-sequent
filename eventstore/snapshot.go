package eventstore

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Snapshot is a versioned JSON aggregate state checkpoint (Q8).
type Snapshot struct {
	AggregateID     uuid.UUID
	SequenceNumber  int64
	SnapshotType    string // usually aggregate type name
	SnapshotVersion int    // payload schema version; bump on incompatible changes
	JSON            json.RawMessage
	CreatedAt       time.Time
}

// SnapshotStore persists and loads JSON snapshots.
type SnapshotStore interface {
	SaveSnapshot(ctx context.Context, snap Snapshot) error
	// LoadLatestSnapshot returns the newest compatible snapshot (matching version), or nil.
	LoadLatestSnapshot(ctx context.Context, aggregateID uuid.UUID, snapshotType string, snapshotVersion int) (*Snapshot, error)
	// MarkSnapshotTaken clears outdated flag / updates HWM after a successful snapshot.
	MarkSnapshotTaken(ctx context.Context, aggregateID uuid.UUID, sequenceNumber int64) error
	// ListNeedingSnapshots returns aggregate ids whose streams need a snapshot.
	ListNeedingSnapshots(ctx context.Context, limit int) ([]uuid.UUID, error)
}
