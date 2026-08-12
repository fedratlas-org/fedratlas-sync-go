package postgres

import (
	"encoding/json"
	"fmt"
)

// ----Federation----//
// LogFederationEvent logs a federation event
func (s *Repository) LogFederationEvent(serverID string, activityType string, details interface{}) error {
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
func (s *Repository) UpdatePeerTrustScore(serverID string, newScore float64) error {
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
