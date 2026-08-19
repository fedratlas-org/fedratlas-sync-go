package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fedratlas-org/fedratlas-sync-go/internal/api"
	"github.com/fedratlas-org/fedratlas-sync-go/internal/crypto"
	grpcserver "github.com/fedratlas-org/fedratlas-sync-go/internal/grpc"
	"github.com/fedratlas-org/fedratlas-sync-go/internal/storage/ogc"
	"github.com/fedratlas-org/fedratlas-sync-go/internal/sync"
	pb "github.com/fedratlas-org/fedratlas-sync-go/proto"
	"github.com/go-chi/chi/v5"
	"google.golang.org/grpc"
)

func main() {
	// Load configuration from environment
	serverID := os.Getenv("FEDRATLAS_SERVER_ID")
	if serverID == "" {
		serverID = "server-001"
	}

	dbConnString := os.Getenv("DATABASE_URL")
	if dbConnString == "" {
		dbConnString = "postgres://fedratlas:fedratlas123@localhost:5432/fedratlas?sslmode=disable"
	}

	port := os.Getenv("HTTP_PORT")
	if port == "" {
		port = "8080"
	}

	grpcPort := os.Getenv("GRPC_PORT")
	if grpcPort == "" {
		grpcPort = "50051"
	}

	ogcBaseURL := os.Getenv("OGC_BASE_URL")
	if ogcBaseURL == "" {
		ogcBaseURL = "http://localhost:8081" // Default
	}

	ogcCollection := os.Getenv("OGC_COLLECTION")
	if ogcCollection == "" {
		ogcCollection = "places" // Default
	}

	// Initialize storage with pgx
	/*db, err := postgres.NewPostgresStorage(dbConnString)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()*/

	//NEW - OGC API for features, PostgreSQL for federation data
	db, err := ogc.NewOGCStorage(ogc.Config{
		BaseURL:      ogcBaseURL,    // Your map backend OGC API
		Collection:   ogcCollection, // Default collection
		DBConnString: dbConnString,  // PostgreSQL for federation data
	})
	if err != nil {
		log.Fatalf("Failed to connect to OGC storage: %v", err)
	}
	defer db.Close()

	// Initialize crypto signer
	signer, err := crypto.NewSigner()
	if err != nil {
		log.Fatalf("Failed to create signer: %v", err)
	}
	log.Printf("Server ID: %s", serverID)
	log.Printf("Public key: %s", signer.GetPublicKeyBase64())

	// Configure sync engine
	config := sync.DefaultConfig()
	config.ServerID = serverID
	config.ConcurrentWorkers = 4
	config.PollInterval = 5 * time.Second
	config.MaxRetries = 5
	config.BaseRetryDelay = 2 * time.Second
	config.MaxRetryDelay = 60 * time.Second

	// Create and start sync engine
	engine := sync.NewSyncEngine(db, signer, config)
	if err := engine.Start(); err != nil {
		log.Fatalf("Failed to start engine: %v", err)
	}
	defer engine.Stop()
	// Create HTTP handlers
	//inboxHandler := sync.NewInboxHandler(engine, db)

	// Create API handlers
	inboxHandler := api.NewInboxHandler(engine, db)
	healthHandler := api.NewHealthHandler(serverID, engine, db)
	manifestHandler := api.NewManifestHandler(serverID, signer)
	peersHandler := api.NewPeersHandler(engine, db)
	featuresHandler := api.NewFeaturesHandler(engine, db)

	// Register HTTP routes
	r := chi.NewRouter()

	// Health endpoints
	r.Get("/health", healthHandler.HealthCheck)
	r.Get("/ready", healthHandler.ReadinessCheck)
	r.Get("/health/detailed", healthHandler.DetailedHealth)
	r.Get("/ping", healthHandler.Ping)

	// Federation endpoints
	r.Post("/fedmap/v1/inbox", inboxHandler.HandleInbox)
	r.Get("/fedmap/v1/manifest", manifestHandler.GetManifest)

	// Peer management endpoints
	r.Get("/fedmap/v1/peers", peersHandler.ListPeers)
	r.Post("/fedmap/v1/peers", peersHandler.AddPeer)

	// Features endpoints
	r.Get("/fedmap/v1/collections", featuresHandler.ListCollections)
	r.Get("/fedmap/v1/collections/{collectionId}/items", featuresHandler.ListFeatures)
	r.Post("/fedmap/v1/collections/{collectionId}/items", featuresHandler.CreateFeature)
	r.Get("/fedmap/v1/collections/{collectionId}/items/{featureId}", featuresHandler.GetFeature)
	r.Put("/fedmap/v1/collections/{collectionId}/items/{featureId}", featuresHandler.UpdateFeature)
	r.Delete("/fedmap/v1/collections/{collectionId}/items/{featureId}", featuresHandler.DeleteFeature)

	// Create HTTP server with timeouts
	httpServer := &http.Server{
		Addr:         ":" + port,
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start HTTP server
	go func() {
		log.Printf("Starting Fedratlas HTTP server on :%s", port)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	// Create gRPC server
	grpcServer := grpc.NewServer()
	pb.RegisterFedratlasServiceServer(grpcServer, grpcserver.NewServer(engine, db))

	// Start listening with grpc Server
	go func() {
		listener, err := net.Listen("tcp", ":"+grpcPort)
		if err != nil {
			log.Fatalf("Failed to listen: %v", err)
		}

		log.Printf("Fedratlas gRPC server listening on :%s", grpcPort)
		log.Fatalf("Server failed: %v", grpcServer.Serve(listener))
	}()

	// Wait for shutdown signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down gracefully...")

	// Create shutdown context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Shutdown HTTP server
	if err := httpServer.Shutdown(ctx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}

	//Shutdown gRPC
	grpcServer.GracefulStop()

	// Stop sync engine
	engine.Stop()

	log.Println("Shutdown complete")
}
