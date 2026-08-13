package api

import (
	"encoding/json"
	"net/http"

	"github.com/fedratlas-org/fedratlas-sync-go/internal/storage"
	"github.com/fedratlas-org/fedratlas-sync-go/internal/sync"
	"github.com/fedratlas-org/fedratlas-sync-go/pkg/types"
)

// PeersHandler handles peer management endpoints
type PeersHandler struct {
	engine  *sync.SyncEngine
	storage storage.Repository
}

// NewPeersHandler creates a new peers handler
func NewPeersHandler(engine *sync.SyncEngine, storage storage.Repository) *PeersHandler {
	return &PeersHandler{
		engine:  engine,
		storage: storage,
	}
}

// ListPeers handles GET /fedmap/v1/peers
func (h *PeersHandler) ListPeers(w http.ResponseWriter, r *http.Request) {
	peers, err := h.storage.Peer().GetAllPeers()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"peers": peers,
		"count": len(peers),
	})
}

// AddPeer handles POST /fedmap/v1/peers
func (h *PeersHandler) AddPeer(w http.ResponseWriter, r *http.Request) {
	var peer types.Peer
	if err := json.NewDecoder(r.Body).Decode(&peer); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if err := h.engine.AddPeer(&peer); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(peer)
}
