package storage

import (
	"context"
	"time"

	"github.com/fedratlas-org/fedratlas-sync-go/pkg/types"

	"github.com/jackc/pgx/v5"
)

// Repository defines the interface that all storage backends must implement
type Repository interface {
	// Peer operations
	Peer() PeerRepository
	Feature() FeatureRepository
	Outbox() OutboxRepository
	Federation() FederationRepository

	// Common DB operations
	Ping() error
	Close() error
	//WithTransaction(fn func(Repository) error) error
	WithTransaction(ctx context.Context, fn func(tx pgx.Tx) error) error

	// Get the underlying driver (for advanced use)
	//Driver() interface{}
}

// PeerRepository defines peer-related operations
type PeerRepository interface {
	AddPeer(peer *types.Peer) error
	GetPeer(serverID string) (*types.Peer, error)
	AddPeerTx(ctx context.Context, tx pgx.Tx, peer *types.Peer) error
	GetAllPeers() ([]*types.Peer, error)
	GetActivePeers() ([]*types.Peer, error)
	//UpdatePeer( peer *types.Peer) error
	//UpdatePeerStatus( serverID string, status types.PeerStatus) error
	//UpdatePeerTrustScore( serverID string, score float64) error
	//UpdatePeerLastSeen( serverID string, lastSeen time.Time) error
	//DeletePeer( serverID string) error
}

// FeatureRepository defines geospatial feature operations
type FeatureRepository interface {
	CreateFeature(geometry interface{}, properties map[string]interface{}) (int64, error)
	GetFeature(featureID int64) (*types.GeoJSONFeature, error)
	GetFeatures(collectionID string, bbox string, limit string, offset string) ([]*types.GeoJSONFeature, int, error)
	UpdateFeature(featureID int64, geometry interface{}, properties map[string]interface{}, version int) error
	DeleteFeature(featureID int64) error
	GetFeaturesByGeometry(collectionID string, geometry interface{}, distance float64) ([]*types.GeoJSONFeature, error)
	//GetFeatureCount( collectionID string) (int64, error)
}

// OutboxRepository defines federation outbox operations
type OutboxRepository interface {
	AddToOutbox(activity *types.Activity) error
	AddToOutboxBatch(activities []*types.Activity) error
	GetPendingOutboxActivities(limit int) ([]*OutboxEntry, error)
	GetFailedOutboxActivities(limit int) ([]*OutboxEntry, error)
	MarkActivitiesAsProcessing(entries []*OutboxEntry) error
	MarkOutboxDelivered(outboxID int64, peerID string) error
	MarkOutboxFailed(outboxID int64, peerID string, err string) error
	ResetOutboxStatus(outboxID int64) error
	GetOutboxRetryInfo(outboxID int64, peerID string) (*OutboxDelivery, error)
	UpdateOutboxRetry(outboxID int64, peerID string, retryCount int, nextRetry time.Time, lastError string) error
	GetPendingOutboxCount() (int, error)
}

// FederationRepository defines federation logging operations
type FederationRepository interface {
	LogFederationEvent(serverID string, activityType string, details interface{}) error
	UpdatePeerTrustScore(serverID string, newScore float64) error
	//GetFederationLogs(limit int, offset int) ([]*FederationLog, error)
	//GetPeerLogs(serverID string, limit int) ([]*FederationLog, error)
}
