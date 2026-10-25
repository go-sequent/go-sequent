package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/wenweih/go-sequent/eventstore"
)

// SaveSnapshot implements eventstore.SnapshotStore.
func (s *Store) SaveSnapshot(ctx context.Context, snap eventstore.Snapshot) error {
	db := s.querier(ctx)
	if snap.CreatedAt.IsZero() {
		snap.CreatedAt = time.Now().UTC()
	}
	_, err := db.Exec(ctx, `
		INSERT INTO snapshot_records (aggregate_id, sequence_number, created_at, snapshot_type, snapshot_version, snapshot_json)
		VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT (aggregate_id, sequence_number, snapshot_version) DO UPDATE
		SET snapshot_json = EXCLUDED.snapshot_json, created_at = EXCLUDED.created_at`,
		snap.AggregateID, snap.SequenceNumber, snap.CreatedAt, snap.SnapshotType, snap.SnapshotVersion, snap.JSON,
	)
	return err
}

// LoadLatestSnapshot implements eventstore.SnapshotStore.
func (s *Store) LoadLatestSnapshot(ctx context.Context, aggregateID uuid.UUID, snapshotType string, snapshotVersion int) (*eventstore.Snapshot, error) {
	db := s.querier(ctx)
	var snap eventstore.Snapshot
	err := db.QueryRow(ctx, `
		SELECT aggregate_id, sequence_number, created_at, snapshot_type, snapshot_version, snapshot_json
		FROM snapshot_records
		WHERE aggregate_id = $1 AND snapshot_type = $2 AND snapshot_version = $3
		ORDER BY sequence_number DESC
		LIMIT 1`, aggregateID, snapshotType, snapshotVersion,
	).Scan(&snap.AggregateID, &snap.SequenceNumber, &snap.CreatedAt, &snap.SnapshotType, &snap.SnapshotVersion, &snap.JSON)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &snap, nil
}

// MarkSnapshotTaken implements eventstore.SnapshotStore.
func (s *Store) MarkSnapshotTaken(ctx context.Context, aggregateID uuid.UUID, sequenceNumber int64) error {
	db := s.querier(ctx)
	_, err := db.Exec(ctx, `
		UPDATE aggregates_that_need_snapshots
		SET snapshot_sequence_number_high_water_mark = $2,
		    snapshot_outdated_at = NULL,
		    snapshot_scheduled_at = NULL
		WHERE aggregate_id = $1`, aggregateID, sequenceNumber)
	return err
}

// ListNeedingSnapshots implements eventstore.SnapshotStore.
func (s *Store) ListNeedingSnapshots(ctx context.Context, limit int) ([]uuid.UUID, error) {
	db := s.querier(ctx)
	if limit <= 0 {
		limit = 10
	}
	rows, err := db.Query(ctx, `
		SELECT aggregate_id FROM aggregates_that_need_snapshots
		WHERE snapshot_outdated_at IS NOT NULL
		ORDER BY snapshot_outdated_at
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

var _ eventstore.SnapshotStore = (*Store)(nil)
