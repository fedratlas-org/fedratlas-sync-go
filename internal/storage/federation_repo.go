package storage

import (
	"encoding/json"
	"fmt"

	"fedratlas-sync/pkg/types"
)

// ----Federation----//
// LogFederationEvent logs a federation event
func (s *PostgresStorage) LogFederationEvent(serverID string, activityType string, details interface{}) error {
	detailsJSON, err := json.Marshal(details)
	if err != nil {
		return fmt.Errorf("failed to marshal details: %w", err)
	}

	query := `INSERT INTO federation_log (server_id, activity_type, details) VALUES ($1, $2, $3)`

	_, err = s.pool.Exec(s.ctx, query, serverID, activityType, detailsJSON)
	if err != nil {
		return fmt.Errorf("failed to log event: %w", err)
	}

	return nil
}

// UpdatePeerTrustScore updates a peer's trust score
func (s *PostgresStorage) UpdatePeerTrustScore(serverID string, newScore float64) error {
	query := `UPDATE peer_registry SET trust_score = $2, updated_at = NOW() WHERE server_id = $1`

	result, err := s.pool.Exec(s.ctx, query, serverID, newScore)
	if err != nil {
		return fmt.Errorf("failed to update trust score: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("peer %s not found", serverID)
	}

	return nil
}

// GetFailedActivities gets activities that need retry
func (s *PostgresStorage) GetFailedActivities(limit int) ([]*OutboxEntry, error) {
	query := `
        SELECT o.id, o.activity, o.status, o.created_at
        FROM federation_outbox o
        JOIN outbox_delivery d ON o.id = d.outbox_id
        WHERE o.status = 'PROCESSING'
          AND d.delivered_at IS NULL
          AND d.next_retry <= NOW()
        LIMIT $1
        FOR UPDATE SKIP LOCKED`

	rows, err := s.pool.Query(s.ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query failed activities: %w", err)
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

	return entries, nil
}
