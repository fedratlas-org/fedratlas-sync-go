package sync

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/fedratlas-org/fedratlas-sync-go/internal/storage"
	"github.com/fedratlas-org/fedratlas-sync-go/pkg/types"
)

type InboxHandler struct {
	engine  *SyncEngine
	storage storage.Repository
}

func NewInboxHandler(engine *SyncEngine, storage storage.Repository) *InboxHandler {
	return &InboxHandler{
		engine:  engine,
		storage: storage,
	}
}

// verifyMessage cryptographically verifies the message signature
func (h *InboxHandler) VerifyMessage(msg *types.SyncMessage, peer *types.Peer) bool {
	// Save signature
	originalSignature := msg.SenderSignature

	// Remove signature for verification
	msg.SenderSignature = ""
	msgData, _ := json.Marshal(msg)
	msg.SenderSignature = originalSignature

	// Verify using engine's signer
	valid, err := h.engine.Signer.Verify(msgData, originalSignature, peer.PublicKey)
	if err != nil {
		return false
	}
	return valid
}

// applyFeatureChange applies the change to the database
func (h *InboxHandler) ApplyFeatureChange(msg *types.SyncMessage, peer *types.Peer) error {
	// Parse feature data
	var featureData map[string]interface{}
	if err := json.Unmarshal(msg.TargetObjectData, &featureData); err != nil {
		return fmt.Errorf("failed to parse feature data: %w", err)
	}

	// Extract geometry and properties
	geometry, ok := featureData["geometry"]
	if !ok {
		return fmt.Errorf("missing geometry")
	}

	properties, ok := featureData["properties"].(map[string]interface{})
	if !ok {
		properties = make(map[string]interface{})
	}

	properties["source_server"] = msg.SenderServerID
	properties["received_at"] = time.Now().UTC()
	properties["version"] = msg.Version

	// Save to database
	_, err := h.storage.Feature().CreateFeature(geometry, properties)
	return err
}

// propagateToOtherPeers forwards the update to other peers
func (h *InboxHandler) PropagateToOtherPeers(msg *types.SyncMessage, originServerID string) {
	// Get all peers
	peers := h.engine.GetPeers()

	for _, peer := range peers {
		// Skip origin and blocked peers
		if peer.ServerID == originServerID || peer.Status == "BLOCKED" {
			continue
		}

		go func(targetPeer *types.Peer) {
			// Send to peer's inbox
			inboxURL := fmt.Sprintf("%s/fedmap/v1/inbox", targetPeer.EndpointURL)
			msgData, _ := json.Marshal(msg)

			resp, err := http.Post(inboxURL, "application/json", bytes.NewReader(msgData))
			if err != nil {
				log.Printf("Failed to propagate to %s: %v", targetPeer.ServerID, err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusAccepted {
				log.Printf("✅ Propagated to %s", targetPeer.ServerID)
			}
		}(peer)
	}
}
