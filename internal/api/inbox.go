package api

import (
	"encoding/json"
	"fedratlas-sync/internal/storage"
	"fedratlas-sync/internal/sync"
	"fedratlas-sync/pkg/types"
	"log"
	"net/http"
)

// InboxHandler handles incoming federation messages directly
// No need for syncInboxHandler - everything happens here!
type InboxHandler struct {
	syncService *sync.InboxHandler
	engine      *sync.SyncEngine   // For engine operations
	storage     storage.Repository // For database operations
}

func NewInboxHandler(engine *sync.SyncEngine, storage storage.Repository) *InboxHandler {
	return &InboxHandler{
		engine:  engine,
		storage: storage,
	}
}

// HandleInbox is the HTTP handler for POST /fedmap/v1/inbox
// All logic happens here - no delegation needed!
func (h *InboxHandler) HandleInbox(w http.ResponseWriter, r *http.Request) {
	// ========== HTTP LAYER ==========
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// ========== PARSE REQUEST ==========
	var msg types.SyncMessage
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	log.Printf("Inbox received message from: %s", msg.SenderServerID)

	// ========== BUSINESS LOGIC ==========
	// 1. Verify sender
	peer, err := h.storage.Peer().GetPeer(msg.SenderServerID)
	if err != nil {
		log.Printf("Unknown peer: %s", msg.SenderServerID)
		http.Error(w, "Unknown peer", http.StatusUnauthorized)
		return
	}

	if peer.Status == "BLOCKED" {
		http.Error(w, "Peer is blocked", http.StatusForbidden)
		return
	}

	// 2. Verify signature (using engine's signer)
	if !h.syncService.VerifyMessage(&msg, peer) {
		log.Printf("Invalid signature from: %s", msg.SenderServerID)
		http.Error(w, "Invalid signature", http.StatusUnauthorized)

		// Reduce trust score
		newScore := peer.TrustScore * 0.8
		h.storage.Federation().UpdatePeerTrustScore(peer.ServerID, newScore)
		return
	}

	log.Printf("Signature verified for peer: %s", peer.ServerID)

	// 3. Apply to database
	if err := h.syncService.ApplyFeatureChange(&msg, peer); err != nil {
		log.Printf("Failed to apply change: %v", err)
		http.Error(w, "Failed to apply change", http.StatusInternalServerError)
		return
	}

	// 4. Update trust score (valid data = increase trust)
	newTrustScore := peer.TrustScore * 1.02
	if newTrustScore > 1.0 {
		newTrustScore = 1.0
	}
	h.storage.Federation().UpdatePeerTrustScore(peer.ServerID, newTrustScore)

	// 5. Propagate to other peers
	go h.syncService.PropagateToOtherPeers(&msg, peer.ServerID)

	// ========== RESPONSE ==========
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "accepted",
		"message": "Feature change applied successfully",
	})
}
