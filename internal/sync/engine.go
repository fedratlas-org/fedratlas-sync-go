package sync

import (
	"context"
	"encoding/json"
	"fedratlas-sync/internal/storage"
	"fmt"
	"log"
	"sync"
	"time"

	"fedratlas-sync/internal/crypto"
	"fedratlas-sync/pkg/types"
)

type SyncEngine struct {
	storage   storage.Repository
	Signer    *crypto.Signer
	outbox    *OutboxProcessor
	inbox     *InboxHandler
	peers     map[string]*types.Peer
	peersMu   sync.RWMutex
	ctx       context.Context
	cancel    context.CancelFunc
	config    *EngineConfig
	startTime time.Time
	running   bool
}

type EngineConfig struct {
	ServerID          string
	PollInterval      time.Duration
	MaxRetries        int
	BaseRetryDelay    time.Duration
	MaxRetryDelay     time.Duration
	ConcurrentWorkers int
}

func DefaultConfig() *EngineConfig {
	return &EngineConfig{
		PollInterval:      5 * time.Second,
		MaxRetries:        5,
		BaseRetryDelay:    2 * time.Second,
		MaxRetryDelay:     60 * time.Second,
		ConcurrentWorkers: 4,
	}
}

// constructor: Creates and wires up all components
func NewSyncEngine(storage storage.Repository, signer *crypto.Signer, config *EngineConfig) *SyncEngine {
	ctx, cancel := context.WithCancel(context.Background())

	engine := &SyncEngine{
		storage:   storage,
		Signer:    signer,
		peers:     make(map[string]*types.Peer),
		ctx:       ctx,
		cancel:    cancel,
		config:    config,
		startTime: time.Now(),
		running:   false,
	}

	engine.outbox = NewOutboxProcessor(engine, storage, signer)
	engine.inbox = NewInboxHandler(engine, storage)

	return engine
}

// Launches Everything: Initialize all sybsystems
// Update the Start method to set running = true
func (e *SyncEngine) Start() error {
	log.Printf("Starting Fedratlas Sync Engine on server: %s", e.config.ServerID)

	e.peersMu.Lock()
	e.running = true
	e.peersMu.Unlock()

	if err := e.loadPeers(); err != nil {
		e.peersMu.Lock()
		e.running = false
		e.peersMu.Unlock()
		return fmt.Errorf("failed to load peers: %w", err)
	}

	go e.outbox.Start()
	go e.healthCheckLoop()

	return nil
}

// OnFeatureChange implements FR-03: Change Log Generation
// Calls when map data changes
func (e *SyncEngine) OnFeatureChange(featureID int64, activityType types.ActivityType, featureData json.RawMessage, version int) error {
	activity := &types.Activity{
		ID:          fmt.Sprintf("%s_%d_%d", e.config.ServerID, featureID, time.Now().UnixNano()),
		Type:        activityType,
		Timestamp:   time.Now().UTC(),
		ServerID:    e.config.ServerID,
		FeatureID:   featureID,
		FeatureData: featureData,
		Version:     version,
	}

	// Sign the activity
	signature, err := e.Signer.SignActivity(activity)
	if err != nil {
		return fmt.Errorf("failed to sign activity: %w", err)
	}
	activity.Signature = signature

	// Store in outbox (change log)
	if err := e.storage.Outbox().AddToOutbox(activity); err != nil {
		return fmt.Errorf("failed to add to outbox: %w", err)
	}

	log.Printf("Activity %s added to outbox for feature %d", activity.Type, featureID)
	return nil
}

// IsRunning returns whether the sync engine is running
func (e *SyncEngine) IsRunning() bool {
	e.peersMu.RLock()
	defer e.peersMu.RUnlock()
	return e.running
}

// GetStartTime returns when the engine started
func (e *SyncEngine) GetStartTime() time.Time {
	return e.startTime
}

// GetOutboxCount returns the number of pending outbox activities
func (e *SyncEngine) GetOutboxCount() int {
	count, err := e.storage.Outbox().GetPendingOutboxCount()
	if err != nil {
		return 0
	}
	return count
}

// GetLastSyncTime returns the last successful sync time
func (e *SyncEngine) GetLastSyncTime() time.Time {
	// Return last sync time from database or current time
	return time.Now()
}

// Update the Stop method to set running = false
func (e *SyncEngine) Stop() {
	e.peersMu.Lock()
	e.running = false
	e.peersMu.Unlock()

	log.Println("Stopping sync engine...")
	e.cancel()
	e.outbox.Stop()
}
