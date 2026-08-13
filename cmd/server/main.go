package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fedratlas-org/fedratlas-sync-go/internal/api"
	"github.com/fedratlas-org/fedratlas-sync-go/internal/crypto"
	"github.com/fedratlas-org/fedratlas-sync-go/internal/storage/postgres"
	"github.com/fedratlas-org/fedratlas-sync-go/internal/sync"

	"github.com/go-chi/chi/v5"
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

	// Initialize storage with pgx
	db, err := postgres.NewRepository(dbConnString)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
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

	// Start sync engine
	if err := engine.Start(); err != nil {
		log.Fatalf("Failed to start sync engine: %v", err)
	}

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

	// Stop sync engine
	engine.Stop()

	log.Println("Shutdown complete")
}
