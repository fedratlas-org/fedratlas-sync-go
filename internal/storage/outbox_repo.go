package storage

import (
	"encoding/json"
	"fedratlas-sync/pkg/types"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
)

// ----Outbox----//
// AddToOutbox adds an activity to the outbox
func (s *PostgresStorage) AddToOutbox(activity *types.Activity) error {
	activityJSON, err := json.Marshal(activity)
	if err != nil {
		return fmt.Errorf("failed to marshal activity: %w", err)
	}

	query := `INSERT INTO federation_outbox (activity, status) VALUES ($1, 'PENDING') RETURNING id`

	var id int64
	err = s.pool.QueryRow(s.ctx, query, activityJSON).Scan(&id)
	if err != nil {
		return fmt.Errorf("failed to add to outbox: %w", err)
	}

	log.Printf("Activity %d added to outbox", id)
	return nil
}

// AddToOutboxBatch adds multiple activities using batch insert
func (s *PostgresStorage) AddToOutboxBatch(activities []*types.Activity) error {
	batch := &pgx.Batch{}

	for _, activity := range activities {
		activityJSON, err := json.Marshal(activity)
		if err != nil {
			return fmt.Errorf("failed to marshal activity: %w", err)
		}

		query := `INSERT INTO federation_outbox (activity, status) VALUES ($1, 'PENDING')`
		batch.Queue(query, activityJSON)
	}

	results := s.pool.SendBatch(s.ctx, batch)
	defer results.Close()

	for i := 0; i < len(activities); i++ {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("failed to insert activity %d: %w", i, err)
		}
	}

	log.Printf("Batch inserted %d activities into outbox", len(activities))
	return nil
}

// GetPendingOutboxActivities retrieves pending activities with FOR UPDATE SKIP LOCKED for concurrency
func (s *PostgresStorage) GetPendingOutboxActivities(limit int) ([]*OutboxEntry, error) {
	// Use FOR UPDATE SKIP LOCKED to avoid multiple workers processing same items
	query := `
        SELECT id, activity, status, created_at
        FROM federation_outbox
        WHERE status = 'PENDING'
        ORDER BY created_at
        LIMIT $1
        FOR UPDATE SKIP LOCKED`

	rows, err := s.pool.Query(s.ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query pending activities: %w", err)
	}
	defer rows.Close()

	var entries []*OutboxEntry
	for rows.Next() {
		var entry OutboxEntry
		var activityJSON []byte

		if err := rows.Scan(&entry.ID, &activityJSON, &entry.Status, &entry.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan activity: %w", err)
		}

		var activity types.Activity
		if err := json.Unmarshal(activityJSON, &activity); err != nil {
			return nil, fmt.Errorf("failed to unmarshal activity: %w", err)
		}
		entry.Activity = &activity

		entries = append(entries, &entry)
	}

	// Mark as processing
	if len(entries) > 0 {
		s.markActivitiesAsProcessing(entries)
	}

	return entries, nil
}

// markActivitiesAsProcessing updates status to PROCESSING
func (s *PostgresStorage) markActivitiesAsProcessing(entries []*OutboxEntry) error {
	ids := make([]int64, len(entries))
	for i, entry := range entries {
		ids[i] = entry.ID
	}

	query := `UPDATE federation_outbox SET status = 'PROCESSING', updated_at = NOW() 
              WHERE id = ANY($1)`

	if _, err := s.pool.Exec(s.ctx, query, ids); err != nil {
		return fmt.Errorf("failed to mark activities as processing: %w", err)
	}

	return nil
}

// MarkOutboxDelivered marks an activity as delivered to a specific peer
func (s *PostgresStorage) MarkOutboxDelivered(outboxID int64, peerID string) error {
	query := `
        INSERT INTO outbox_delivery (outbox_id, peer_id, delivered_at)
        VALUES ($1, $2, NOW())
        ON CONFLICT (outbox_id, peer_id) DO UPDATE SET
            delivered_at = NOW(),
            last_error = NULL,
            retry_count = 0`

	_, err := s.pool.Exec(s.ctx, query, outboxID, peerID)
	if err != nil {
		return fmt.Errorf("failed to mark delivered: %w", err)
	}

	return nil
}

// GetOutboxRetryInfo gets retry information for an outbox activity
func (s *PostgresStorage) GetOutboxRetryInfo(outboxID int64, peerID string) (*OutboxDelivery, error) {
	query := `SELECT outbox_id, peer_id, retry_count, next_retry, last_error, delivered_at
              FROM outbox_delivery
              WHERE outbox_id = $1 AND peer_id = $2`

	var delivery OutboxDelivery
	err := s.pool.QueryRow(s.ctx, query, outboxID, peerID).Scan(
		&delivery.OutboxID, &delivery.PeerID, &delivery.RetryCount,
		&delivery.NextRetry, &delivery.LastError, &delivery.DeliveredAt,
	)

	if err != nil {
		if err == pgx.ErrNoRows {
			// Return default delivery info if not found
			return &OutboxDelivery{
				OutboxID:   outboxID,
				PeerID:     peerID,
				RetryCount: 0,
			}, nil
		}
		return nil, fmt.Errorf("failed to get retry info: %w", err)
	}

	return &delivery, nil
}

// UpdateOutboxRetry updates retry information with exponential backoff
func (s *PostgresStorage) UpdateOutboxRetry(outboxID int64, peerID string, retryCount int, nextRetry time.Time, lastError string) error {
	query := `
        INSERT INTO outbox_delivery (outbox_id, peer_id, retry_count, next_retry, last_error)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (outbox_id, peer_id) DO UPDATE SET
            retry_count = EXCLUDED.retry_count,
            next_retry = EXCLUDED.next_retry,
            last_error = EXCLUDED.last_error,
            delivered_at = NULL`

	_, err := s.pool.Exec(s.ctx, query, outboxID, peerID, retryCount, nextRetry, lastError)
	if err != nil {
		return fmt.Errorf("failed to update retry info: %w", err)
	}

	return nil
}

// MarkOutboxFailed marks an activity as failed after max retries
func (s *PostgresStorage) MarkOutboxFailed(outboxID int64, peerID string, lastError string) error {
	query := `UPDATE federation_outbox SET status = 'FAILED', updated_at = NOW() WHERE id = $1`

	if _, err := s.pool.Exec(s.ctx, query, outboxID); err != nil {
		return fmt.Errorf("failed to mark outbox as failed: %w", err)
	}

	// Also update delivery record
	updateDelivery := `
        UPDATE outbox_delivery 
        SET last_error = $3, next_retry = NULL
        WHERE outbox_id = $1 AND peer_id = $2`

	if _, err := s.pool.Exec(s.ctx, updateDelivery, outboxID, peerID, lastError); err != nil {
		return fmt.Errorf("failed to update delivery error: %w", err)
	}

	return nil
}

// GetPendingOutboxCount returns count of pending outbox activities
func (s *PostgresStorage) GetPendingOutboxCount() (int, error) {
	var count int
	query := `SELECT COUNT(*) FROM federation_outbox WHERE status = 'PENDING'`
	err := s.pool.QueryRow(s.ctx, query).Scan(&count)
	return count, err
}
