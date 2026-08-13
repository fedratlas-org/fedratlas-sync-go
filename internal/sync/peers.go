package sync

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/fedratlas-org/fedratlas-sync-go/pkg/types"
)

func (e *SyncEngine) loadPeers() error {
	peers, err := e.storage.Peer().GetAllPeers()
	if err != nil {
		return err
	}

	e.peersMu.Lock()
	defer e.peersMu.Unlock()

	for _, peer := range peers {
		if peer.Status == "FOLLOWING" {
			e.peers[peer.ServerID] = peer
		}
	}

	log.Printf("Loaded %d active peers", len(e.peers))
	return nil
}

func (e *SyncEngine) GetPeers() []*types.Peer {
	e.peersMu.RLock()
	defer e.peersMu.RUnlock()

	peers := make([]*types.Peer, 0, len(e.peers))
	for _, peer := range e.peers {
		peers = append(peers, peer)
	}
	return peers
}

func (e *SyncEngine) AddPeer(peer *types.Peer) error {
	e.peersMu.Lock()
	defer e.peersMu.Unlock()

	if err := e.storage.Peer().AddPeer(peer); err != nil {
		return err
	}

	e.peers[peer.ServerID] = peer
	log.Printf("Added new peer: %s (trust_score: %.2f)", peer.ServerID, peer.TrustScore)
	return nil
}

// healthCheckLoop periodically checks peer health
func (e *SyncEngine) healthCheckLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.checkPeerHealth()
		}
	}
}

// checkPeerHealth pings peers and updates last_seen
func (e *SyncEngine) checkPeerHealth() {
	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	peers := e.GetPeers()

	if len(peers) == 0 {
		return
	}

	for _, peer := range peers {
		log.Printf("Checking health of peer: %s", peer.ServerID)

		if peer.Status == types.PeerStatusBlocked {
			continue
		}
		// Send to peer's inbox (FR-04)
		healthURL := fmt.Sprintf("%s/fedmap/v1/health", peer.EndpointURL)

		req, err := http.NewRequest("POST", healthURL, nil)
		if err != nil {
			log.Printf("failed to create request: %w", err)
			continue
		}
		req.Header.Set("Content-Type", "application/json")

		startTime := time.Now()
		resp, err := client.Do(req)
		duration := time.Since(startTime)

		//Here if we had an error or resp.statiscode != 200 we mark it as unhealthy
		if err != nil {
			log.Printf("Peer %v health check failed: %w", peer.ServerID, err)
			e.handleUnhealthyPeer(peer)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			log.Printf("peer returned %d: %s", resp.StatusCode, string(bodyBytes))
			e.handleUnhealthyPeer(peer)
			continue
		}

		log.Printf("Peer %s is healthy (response time: %v)", peer.ServerID, duration)

		e.updatePeerLastSeen(peer.ServerID)

		e.increasePeerTrust(peer.ServerID)

	}
}

func (e *SyncEngine) handleUnhealthyPeer(peer *types.Peer) {
	// Reduce trust score
	newScore := peer.TrustScore * 0.95
	if newScore < 0.1 {
		newScore = 0.1
	}

	e.peersMu.Lock()
	defer e.peersMu.Unlock()

	peer.TrustScore = newScore
	e.storage.Federation().UpdatePeerTrustScore(peer.ServerID, newScore)

	// Block if trust gets too low
	if newScore < 0.3 {
		log.Printf("Blocking peer %s due to low trust score (%.2f)", peer.ServerID, newScore)
		peer.Status = types.PeerStatusBlocked
	}
}

// Helper: Increase trust for healthy peer
func (e *SyncEngine) increasePeerTrust(serverID string) {
	e.peersMu.Lock()
	defer e.peersMu.Unlock()

	peer, exists := e.peers[serverID]
	if !exists {
		return
	}

	newScore := peer.TrustScore * 1.02
	if newScore > 1.0 {
		newScore = 1.0
	}
	peer.TrustScore = newScore
}

// Update last_seen
func (e *SyncEngine) updatePeerLastSeen(serverID string) {
	e.peersMu.Lock()
	defer e.peersMu.Unlock()

	peer, exists := e.peers[serverID]
	if exists {
		peer.LastSeen = time.Now().UTC()
	}
}
