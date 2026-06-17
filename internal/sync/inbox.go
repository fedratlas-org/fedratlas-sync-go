package sync

import (
	"bytes"
	"encoding/json"
	"fedratlas-sync/internal/storage"
	"fedratlas-sync/pkg/types"
	"fmt"
	"log"
	"net/http"
	"time"
)

type InboxHandler struct {
	engine  *SyncEngine
	storage *storage.PostgresStorage
}

func NewInboxHandler(engine *SyncEngine, storage *storage.PostgresStorage) *InboxHandler {
	return &InboxHandler{
		engine:  engine,
		storage: storage,
	}
}

/*/ HandleInbox processes incoming sync messages (FR-05)
func (h *InboxHandler) HandleInbox(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var msg types.SyncMessage
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Verify sender
	peer, err := h.storage.GetPeer(msg.SenderServerID)
	if err != nil {
		log.Printf("Unknown peer: %s", msg.SenderServerID)
		http.Error(w, "Unknown peer", http.StatusUnauthorized)
		return
	}

	// Verify signature (FR-02)
	// Implementation would verify the cryptographic signature
	if !h.verifyMessage(&msg, peer) {
		http.Error(w, "Invalid signature", http.StatusUnauthorized)
		return
	}

	// Apply feature change atomically (FR-05)
	if err := h.applyFeatureChange(&msg); err != nil {
		log.Printf("Failed to apply feature change: %v", err)
		http.Error(w, "Failed to apply change", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"status": "accepted"})
}*/

/*func (h *InboxHandler) verifyMessage(msg *types.SyncMessage, peer *types.Peer) bool {
	// Remove signature before verification
	originalSignature := msg.SenderSignature
	msg.SenderSignature = ""

	//msgData, err := json.Marshal(msg)
	_, err := json.Marshal(msg)
	if err != nil {
		return false
	}

	msg.SenderSignature = originalSignature

	// Verify using peer's public key
	// This would use the crypto package's Verify method
	return true // Placeholder
}*/

/*func (h *InboxHandler) applyFeatureChange(msg *types.SyncMessage) error {
	var featureData map[string]interface{}
	if err := json.Unmarshal(msg.TargetObjectData, &featureData); err != nil {
		return fmt.Errorf("failed to parse feature data: %w", err)
	}

	// Apply to local database
	// This would update GEO_FEATURES table with the new data
	log.Printf("Applied feature change from peer %s", msg.SenderServerID)

	return nil
}*/

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
	_, err := h.storage.CreateFeature(geometry, properties)
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
