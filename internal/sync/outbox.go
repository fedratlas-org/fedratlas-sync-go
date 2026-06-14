package sync

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"fedratlas-sync/internal/crypto"
	"fedratlas-sync/internal/storage"
	"fedratlas-sync/pkg/types"
)

type OutboxProcessor struct {
	engine  *SyncEngine
	storage *storage.PostgresStorage
	signer  *crypto.Signer
	stopCh  chan struct{}
	client  *http.Client
}

type PendingActivity struct {
	ID         int64
	Activity   *types.Activity
	TargetPeer string
	RetryCount int
	NextRetry  time.Time
}

// OutboxDelivery represents delivery status to a peer
type OutboxDelivery struct {
	OutboxID    int64
	PeerID      string
	RetryCount  int
	NextRetry   *time.Time
	LastError   *string
	DeliveredAt *time.Time
}

// FederationLog represents an audit log entry
type FederationLog struct {
	LogID        int64
	ServerID     string
	ActivityType string
	Timestamp    time.Time
	Details      []byte
}

func NewOutboxProcessor(engine *SyncEngine, storage *storage.PostgresStorage, signer *crypto.Signer) *OutboxProcessor {
	return &OutboxProcessor{
		engine:  engine,
		storage: storage,
		signer:  signer,
		stopCh:  make(chan struct{}),
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (o *OutboxProcessor) Start() {
	log.Println("Outbox processor started")

	// Start worker pool
	for i := 0; i < o.engine.config.ConcurrentWorkers; i++ {
		go o.worker(i)
	}

	<-o.stopCh
	log.Println("Outbox processor stopped")
}

func (o *OutboxProcessor) Stop() {
	close(o.stopCh)
}

func (o *OutboxProcessor) worker(workerID int) {
	log.Printf("Outbox worker %d started", workerID)

	ticker := time.NewTicker(o.engine.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-o.stopCh:
			log.Printf("Outbox worker %d stopping", workerID)
			return
		case <-ticker.C:
			o.processPendingActivities(workerID)
		}
	}
}

func (o *OutboxProcessor) processPendingActivities(workerID int) {
	// Get pending activities from database
	pending, err := o.storage.GetPendingOutboxActivities(10)
	if err != nil {
		log.Printf("Worker %d: failed to get pending activities: %v", workerID, err)
		return
	}

	for _, activity := range pending {
		o.deliverToAllPeers(workerID, activity)
	}
}

func (o *OutboxProcessor) deliverToAllPeers(workerID int, activity *storage.OutboxEntry) {
	peers := o.engine.GetPeers()

	for _, peer := range peers {
		if err := o.deliverToPeer(activity.Activity, peer); err != nil {
			log.Printf("Worker %d: failed to deliver to peer %s: %v", workerID, peer.ServerID, err)

			// Update retry with exponential backoff
			o.scheduleRetry(activity.ID, peer.ServerID, err)
		} else {
			// Mark as delivered
			o.storage.MarkOutboxDelivered(activity.ID, peer.ServerID)
			log.Printf("Worker %d: delivered activity %d to peer %s", workerID, activity.ID, peer.ServerID)
		}
	}
}

func (o *OutboxProcessor) deliverToPeer(activity *types.Activity, peer *types.Peer) error {
	// Create sync message
	featureData, _ := json.Marshal(activity.FeatureData)

	syncMsg := types.SyncMessage{
		TargetObjectData: featureData,
		Timestamp:        time.Now().UTC(),
		PayloadChecksum:  activity.Checksum,
		SenderSignature:  activity.Signature,
		SenderServerID:   o.engine.config.ServerID,
	}

	// Sign the message
	msgData, err := json.Marshal(syncMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal sync message: %w", err)
	}

	syncMsg.SenderSignature, err = o.signer.Sign(msgData)
	if err != nil {
		return fmt.Errorf("failed to sign message: %w", err)
	}

	// Send to peer's inbox (FR-04)
	inboxURL := fmt.Sprintf("%s/fedmap/v1/inbox", peer.EndpointURL)

	body, err := json.Marshal(syncMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequest("POST", inboxURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := o.client.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("peer returned %d: %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

func (o *OutboxProcessor) scheduleRetry(outboxID int64, peerID string, err error) {
	// Get current retry count
	retryInfo, _ := o.storage.GetOutboxRetryInfo(outboxID, peerID)

	newRetryCount := retryInfo.RetryCount + 1

	if newRetryCount >= o.engine.config.MaxRetries {
		log.Printf("Max retries reached for outbox %d to peer %s, marking as failed", outboxID, peerID)
		o.storage.MarkOutboxFailed(outboxID, peerID, err.Error())
		return
	}

	// Calculate exponential backoff delay
	delay := o.engine.config.BaseRetryDelay * time.Duration(1<<newRetryCount)
	if delay > o.engine.config.MaxRetryDelay {
		delay = o.engine.config.MaxRetryDelay
	}

	nextRetry := time.Now().UTC().Add(delay)

	o.storage.UpdateOutboxRetry(outboxID, peerID, newRetryCount, nextRetry, err.Error())
	log.Printf("Scheduled retry %d for outbox %d to peer %s at %v", newRetryCount, outboxID, peerID, nextRetry)
}
