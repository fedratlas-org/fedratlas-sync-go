package grpc

import (
	"context"
	//"encoding/json"
	//"fmt"
	"log"
	//"net"
	"time"

	"github.com/fedratlas-org/fedratlas-sync-go/internal/storage"
	"github.com/fedratlas-org/fedratlas-sync-go/internal/sync"
	"github.com/fedratlas-org/fedratlas-sync-go/pkg/types"
	pb "github.com/fedratlas-org/fedratlas-sync-go/proto"
	//"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	pb.UnimplementedFedratlasServiceServer
	engine *sync.SyncEngine
	repo   storage.Repository
}

func NewServer(engine *sync.SyncEngine, repo storage.Repository) *Server {
	return &Server{
		engine: engine,
		repo:   repo,
	}
}

// OnFeatureChange implements the gRPC method
func (s *Server) OnFeatureChange(ctx context.Context, req *pb.FeatureChangeRequest) (*pb.FeatureChangeResponse, error) {
	log.Printf("📨 gRPC OnFeatureChange: ID=%d, Type=%s", req.FeatureId, req.ActivityType)

	// Validate
	if req.FeatureId == 0 {
		return nil, status.Error(codes.InvalidArgument, "feature_id is required")
	}

	activityType := types.ActivityType(req.ActivityType)
	if activityType != types.ActivityCreate &&
		activityType != types.ActivityUpdate &&
		activityType != types.ActivityDelete {
		return nil, status.Error(codes.InvalidArgument, "invalid activity_type")
	}

	// Trigger sync engine
	err := s.engine.OnFeatureChange(
		req.FeatureId,
		activityType,
		req.FeatureData,
		int(req.Version),
	)

	if err != nil {
		log.Printf("❌ OnFeatureChange failed: %v", err)
		return &pb.FeatureChangeResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	log.Printf("✅ OnFeatureChange success: ID=%d", req.FeatureId)
	return &pb.FeatureChangeResponse{
		Success: true,
	}, nil
}

// AddPeer implements the gRPC method
func (s *Server) AddPeer(ctx context.Context, req *pb.AddPeerRequest) (*pb.AddPeerResponse, error) {
	if req.Peer == nil || req.Peer.ServerId == "" {
		return nil, status.Error(codes.InvalidArgument, "peer is required")
	}

	peer := &types.Peer{
		ServerID:    req.Peer.ServerId,
		PublicKey:   req.Peer.PublicKey,
		TrustScore:  req.Peer.TrustScore,
		EndpointURL: req.Peer.EndpointUrl,
		Status:      types.PeerStatus(req.Peer.Status),
		LastSeen:    time.Unix(req.Peer.LastSeen, 0),
		CreatedAt:   time.Unix(req.Peer.CreatedAt, 0),
	}

	if err := s.engine.AddPeer(peer); err != nil {
		return &pb.AddPeerResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &pb.AddPeerResponse{
		Success: true,
	}, nil
}

// GetPeers implements the gRPC method
func (s *Server) GetPeers(ctx context.Context, req *pb.GetPeersRequest) (*pb.GetPeersResponse, error) {
	peers := s.engine.GetPeers()

	pbPeers := make([]*pb.Peer, len(peers))
	for i, p := range peers {
		pbPeers[i] = &pb.Peer{
			ServerId:    p.ServerID,
			PublicKey:   p.PublicKey,
			TrustScore:  p.TrustScore,
			EndpointUrl: p.EndpointURL,
			Status:      string(p.Status),
			LastSeen:    p.LastSeen.Unix(),
			CreatedAt:   p.CreatedAt.Unix(),
		}
	}

	return &pb.GetPeersResponse{
		Peers: pbPeers,
	}, nil
}

// GetServerID implements the gRPC method
func (s *Server) GetServerID(ctx context.Context, req *pb.GetServerIDRequest) (*pb.GetServerIDResponse, error) {
	return &pb.GetServerIDResponse{
		ServerId: s.engine.GetServerID(),
	}, nil
}

// GetManifest implements the gRPC method
// here We returns the struct for gRPC clients (other services using gRPC).
func (s *Server) GetManifest(ctx context.Context, req *pb.GetManifestRequest) (*pb.Manifest, error) {
	manifest := s.engine.GetManifest()

	return &pb.Manifest{
		ProtocolVersion: manifest.ProtocolVersion,
		ServerId:        manifest.ServerID,
		Status:          manifest.Status,
		PublicKey:       manifest.PublicKey,
		Endpoints: &pb.Manifest_EndpointConfig{
			InboxUrl:    manifest.Endpoints.InboxURL,
			OutboxUrl:   manifest.Endpoints.OutboxURL,
			ManifestUrl: manifest.Endpoints.ManifestURL,
		},
		Datasets: convertDatasets(manifest.Datasets),
	}, nil
}

func convertDatasets(datasets []types.DatasetInfo) []*pb.Manifest_DatasetInfo {
	result := make([]*pb.Manifest_DatasetInfo, len(datasets))
	for i, d := range datasets {
		result[i] = &pb.Manifest_DatasetInfo{
			Id:           d.ID,
			Name:         d.Name,
			Description:  d.Description,
			FeatureCount: d.FeatureCount,
		}
	}
	return result
}
