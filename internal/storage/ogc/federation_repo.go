package ogc

import (
	"github.com/fedratlas-org/fedratlas-sync-go/internal/storage"
)

// FederationRepository implements storage.FederationRepository using PostgreSQL
type FederationRepository struct {
	pg storage.FederationRepository
}

// NewFederationRepository creates a new federation repository
func NewFederationRepository(pg storage.FederationRepository) *FederationRepository {
	return &FederationRepository{pg: pg}
}

func (r *FederationRepository) LogFederationEvent(serverID string, activityType string, details interface{}) error {
	return r.pg.LogFederationEvent(serverID, activityType, details)
}

func (r *FederationRepository) UpdatePeerTrustScore(serverID string, newScore float64) error {
	return r.pg.UpdatePeerTrustScore(serverID, newScore)
}
