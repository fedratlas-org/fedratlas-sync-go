package main

import (
	"log"
	"net"
	"os"

	"google.golang.org/grpc"

	"github.com/fedratlas-org/fedratlas-sync-go/internal/crypto"
	grpcserver "github.com/fedratlas-org/fedratlas-sync-go/internal/grpc"
	"github.com/fedratlas-org/fedratlas-sync-go/internal/storage/postgres"
	"github.com/fedratlas-org/fedratlas-sync-go/internal/sync"
	pb "github.com/fedratlas-org/fedratlas-sync-go/proto"
)

func main() {
	// Load config
	serverID := os.Getenv("FEDRATLAS_SERVER_ID")
	if serverID == "" {
		serverID = "map-server-001"
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://user:pass@localhost:5432/fedratlas?sslmode=disable"
	}

	// Initialize storage
	repo, err := postgres.NewPostgresStorage(dbURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer repo.Close()

	// Initialize crypto
	signer, err := crypto.NewSigner()
	if err != nil {
		log.Fatalf("Failed to create signer: %v", err)
	}

	// Initialize sync engine
	config := sync.DefaultConfig()
	config.ServerID = serverID

	engine := sync.NewSyncEngine(repo, signer, config)
	if err := engine.Start(); err != nil {
		log.Fatalf("Failed to start engine: %v", err)
	}
	defer engine.Stop()

	// Create gRPC server
	grpcServer := grpc.NewServer()
	pb.RegisterFedratlasServiceServer(grpcServer, grpcserver.NewServer(engine, repo))

	// Start listening
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	log.Printf("Fedratlas gRPC server listening on :50051")
	log.Fatalf("Server failed: %v", grpcServer.Serve(listener))
}
