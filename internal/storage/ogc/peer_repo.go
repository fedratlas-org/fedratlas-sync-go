package ogc

import (
	"context"

	"github.com/fedratlas-org/fedratlas-sync-go/internal/storage"
	"github.com/fedratlas-org/fedratlas-sync-go/pkg/types"
	"github.com/jackc/pgx/v5"
)

// PeerRepository implements storage.PeerRepository using PostgreSQL
// Peers are federation metadata, not map features
type PeerRepository struct {
	pg storage.PeerRepository
}

// NewPeerRepository creates a new peer repository
func NewPeerRepository(pg storage.PeerRepository) *PeerRepository {
	return &PeerRepository{pg: pg}
}

func (r *PeerRepository) AddPeer(peer *types.Peer) error {
	return r.pg.AddPeer(peer)
}

func (r *PeerRepository) GetPeer(serverID string) (*types.Peer, error) {
	return r.pg.GetPeer(serverID)
}

func (r *PeerRepository) AddPeerTx(ctx context.Context, tx pgx.Tx, peer *types.Peer) error {
	return r.pg.AddPeerTx(ctx, tx, peer)
}

func (r *PeerRepository) GetAllPeers() ([]*types.Peer, error) {
	return r.pg.GetAllPeers()
}

func (r *PeerRepository) GetActivePeers() ([]*types.Peer, error) {
	return r.pg.GetActivePeers()
}
