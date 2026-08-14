package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/fedratlas-org/fedratlas-sync-go/pkg/types"

	"github.com/jackc/pgx/v5"
)

// ----Peers---//
// AddPeer adds a new peer to the registry using pgx
func (s *PostgresStorage) AddPeer(peer *types.Peer) error {
	query := `INSERT INTO peer_registry (server_id, public_key, trust_score, endpoint_url, status, last_seen)
              VALUES ($1, $2, $3, $4, $5, $6)
              ON CONFLICT (server_id) DO UPDATE SET
                public_key = EXCLUDED.public_key,
                trust_score = EXCLUDED.trust_score,
                endpoint_url = EXCLUDED.endpoint_url,
                status = EXCLUDED.status,
                last_seen = EXCLUDED.last_seen,
                updated_at = NOW()`

	_, err := s.pool.Exec(s.ctx, query,
		peer.ServerID, peer.PublicKey, peer.TrustScore,
		peer.EndpointURL, peer.Status, time.Now().UTC(),
	)

	return err
}

// AddPeerTx adds a peer within a transaction
func (s *PostgresStorage) AddPeerTx(ctx context.Context, tx pgx.Tx, peer *types.Peer) error {
	query := `INSERT INTO peer_registry (server_id, public_key, trust_score, endpoint_url, status, last_seen)
              VALUES ($1, $2, $3, $4, $5, $6)
              ON CONFLICT (server_id) DO UPDATE SET
                public_key = EXCLUDED.public_key,
                trust_score = EXCLUDED.trust_score,
                endpoint_url = EXCLUDED.endpoint_url,
                status = EXCLUDED.status,
                last_seen = EXCLUDED.last_seen,
                updated_at = NOW()`

	_, err := tx.Exec(ctx, query,
		peer.ServerID, peer.PublicKey, peer.TrustScore,
		peer.EndpointURL, peer.Status, time.Now().UTC(),
	)

	return err
}

// GetPeer retrieves a peer by server_id using pgx
func (s *PostgresStorage) GetPeer(serverID string) (*types.Peer, error) {
	query := `SELECT server_id, public_key, trust_score, endpoint_url, status, last_seen, created_at
              FROM peer_registry WHERE server_id = $1`

	var peer types.Peer
	err := s.pool.QueryRow(s.ctx, query, serverID).Scan(
		&peer.ServerID, &peer.PublicKey, &peer.TrustScore, &peer.EndpointURL,
		&peer.Status, &peer.LastSeen, &peer.CreatedAt,
	)

	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("peer %s not found", serverID)
		}
		return nil, fmt.Errorf("failed to get peer: %w", err)
	}

	return &peer, nil
}

// GetAllPeers retrieves all peers using pgx batch
func (s *PostgresStorage) GetAllPeers() ([]*types.Peer, error) {
	query := `SELECT server_id, public_key, trust_score, endpoint_url, status, last_seen, created_at
              FROM peer_registry ORDER BY trust_score DESC`

	rows, err := s.pool.Query(s.ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query peers: %w", err)
	}
	defer rows.Close()

	var peers []*types.Peer
	for rows.Next() {
		var peer types.Peer
		if err := rows.Scan(
			&peer.ServerID, &peer.PublicKey, &peer.TrustScore, &peer.EndpointURL,
			&peer.Status, &peer.LastSeen, &peer.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan peer: %w", err)
		}
		peers = append(peers, &peer)
	}

	return peers, nil
}

// GetActivePeers retrieves only following peers
func (s *PostgresStorage) GetActivePeers() ([]*types.Peer, error) {
	query := `SELECT server_id, public_key, trust_score, endpoint_url, status, last_seen, created_at
              FROM peer_registry WHERE status = 'FOLLOWING' ORDER BY trust_score DESC`

	rows, err := s.pool.Query(s.ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query active peers: %w", err)
	}
	defer rows.Close()

	var peers []*types.Peer
	for rows.Next() {
		var peer types.Peer
		if err := rows.Scan(
			&peer.ServerID, &peer.PublicKey, &peer.TrustScore, &peer.EndpointURL,
			&peer.Status, &peer.LastSeen, &peer.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan peer: %w", err)
		}
		peers = append(peers, &peer)
	}

	return peers, nil
}
