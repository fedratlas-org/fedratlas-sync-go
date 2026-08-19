package ogc

import (
	"time"

	"github.com/fedratlas-org/fedratlas-sync-go/internal/storage"
	"github.com/fedratlas-org/fedratlas-sync-go/pkg/types"
)

// OutboxRepository implements storage.OutboxRepository using PostgreSQL
type OutboxRepository struct {
	pg storage.OutboxRepository
}

// NewOutboxRepository creates a new outbox repository
func NewOutboxRepository(pg storage.OutboxRepository) *OutboxRepository {
	return &OutboxRepository{pg: pg}
}

func (r *OutboxRepository) AddToOutbox(activity *types.Activity) error {
	return r.pg.AddToOutbox(activity)
}

func (r *OutboxRepository) AddToOutboxBatch(activities []*types.Activity) error {
	return r.pg.AddToOutboxBatch(activities)
}

func (r *OutboxRepository) GetPendingOutboxActivities(limit int) ([]*storage.OutboxEntry, error) {
	return r.pg.GetPendingOutboxActivities(limit)
}

func (r *OutboxRepository) GetFailedOutboxActivities(limit int) ([]*storage.OutboxEntry, error) {
	return r.pg.GetFailedOutboxActivities(limit)
}

func (r *OutboxRepository) MarkActivitiesAsProcessing(entries []*storage.OutboxEntry) error {
	return r.pg.MarkActivitiesAsProcessing(entries)
}

func (r *OutboxRepository) MarkOutboxDelivered(outboxID int64, peerID string) error {
	return r.pg.MarkOutboxDelivered(outboxID, peerID)
}

func (r *OutboxRepository) MarkOutboxFailed(outboxID int64, peerID string, err string) error {
	return r.pg.MarkOutboxFailed(outboxID, peerID, err)
}

func (r *OutboxRepository) ResetOutboxStatus(outboxID int64) error {
	return r.pg.ResetOutboxStatus(outboxID)
}

func (r *OutboxRepository) GetOutboxRetryInfo(outboxID int64, peerID string) (*storage.OutboxDelivery, error) {
	return r.pg.GetOutboxRetryInfo(outboxID, peerID)
}

func (r *OutboxRepository) UpdateOutboxRetry(outboxID int64, peerID string, retryCount int, nextRetry time.Time, lastError string) error {
	return r.pg.UpdateOutboxRetry(outboxID, peerID, retryCount, nextRetry, lastError)
}

func (r *OutboxRepository) GetPendingOutboxCount() (int, error) {
	return r.pg.GetPendingOutboxCount()
}
