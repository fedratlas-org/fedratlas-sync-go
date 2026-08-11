package storage

import (
	"encoding/json"
	"time"

	"fedratlas-sync/pkg/types"
)

type OutboxEntry struct {
	ID        int64
	Activity  *types.Activity
	Status    string
	CreatedAt time.Time
}

type OutboxDelivery struct {
	OutboxID    int64
	PeerID      string
	RetryCount  int
	NextRetry   *time.Time
	LastError   *string
	DeliveredAt *time.Time
}

type FederationLog struct {
	LogID        int64
	ServerID     string
	ActivityType string
	Timestamp    time.Time
	Details      json.RawMessage
}
