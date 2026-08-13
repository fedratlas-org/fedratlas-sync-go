package api

import (
	"encoding/json"
	"net/http"

	"github.com/fedratlas-org/fedratlas-sync-go/internal/crypto"
	"github.com/fedratlas-org/fedratlas-sync-go/pkg/types"
)

// ManifestHandler handles the federation manifest endpoint
type ManifestHandler struct {
	serverID string
	signer   *crypto.Signer
}

// NewManifestHandler creates a new manifest handler
func NewManifestHandler(serverID string, signer *crypto.Signer) *ManifestHandler {
	return &ManifestHandler{
		serverID: serverID,
		signer:   signer,
	}
}

// GetManifest handles GET /fedmap/v1/manifest
func (h *ManifestHandler) GetManifest(w http.ResponseWriter, r *http.Request) {
	manifest := types.Manifest{
		ProtocolVersion: "1.0",
		ServerID:        h.serverID,
		Status:          "active",
		PublicKey:       h.signer.GetPublicKeyBase64(),
		Endpoints: types.EndpointConfig{
			InboxURL:    "/fedmap/v1/inbox",
			OutboxURL:   "/fedmap/v1/outbox",
			ManifestURL: "/fedmap/v1/manifest",
		},
		Datasets: []types.DatasetInfo{
			{ID: "roads", Name: "Road Network", FeatureCount: 0},
			{ID: "pois", Name: "Points of Interest", FeatureCount: 0},
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "public, max-age=3600")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	json.NewEncoder(w).Encode(manifest)
}
