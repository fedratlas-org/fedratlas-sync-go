package ogc

import (
	"context"
	"fmt"
	"log"

	//"log"
	"net/http"
	"time"

	"github.com/fedratlas-org/fedratlas-sync-go/internal/storage"
	"github.com/fedratlas-org/fedratlas-sync-go/internal/storage/postgres"
	"github.com/jackc/pgx/v5"
)

// Repository implements storage.Repository using OGC API for features
// and PostgreSQL for federation data (peers, outbox, logs)
type Repository struct {
	baseURL    string
	collection string
	client     *http.Client
	ctx        context.Context

	// PostgreSQL for federation data (peers, outbox, logs)
	pgPeer       storage.PeerRepository
	pgOutbox     storage.OutboxRepository
	pgFederation storage.FederationRepository

	// OGC API for features
	ogcFeature storage.FeatureRepository
}

// Config holds configuration for OGC repository
type Config struct {
	BaseURL      string // e.g., "http://localhost:8081"
	Collection   string // e.g., "places"
	DBConnString string // PostgreSQL connection string for federation data
}

// NewRepository creates a new OGC repository
func NewOGCStorage(config Config) (*Repository, error) {
	ctx := context.Background()

	// Initialize PostgreSQL for federation data (peers, outbox, logs)
	pgRepo, err := postgres.NewPostgresStorage(config.DBConnString)
	if err != nil {
		return nil, fmt.Errorf("failed to init PostgreSQL for federation data: %w", err)
	}

	log.Printf("✅ PostgreSQL connection established for federation data")

	repo := &Repository{
		baseURL:    config.BaseURL,
		collection: config.Collection,
		ctx:        ctx,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		pgPeer:       pgRepo,
		pgOutbox:     pgRepo,
		pgFederation: pgRepo,
	}

	// Initialize OGC feature repository
	repo.ogcFeature = NewFeatureRepository(repo)

	log.Printf("✅ OGC Repository initialized: baseURL=%s, collection=%s", config.BaseURL, config.Collection)
	return repo, nil
}

// GetBaseURL returns the OGC API base URL
func (r *Repository) GetBaseURL() string {
	return r.baseURL
}

// GetCollection returns the default collection name
func (r *Repository) GetCollection() string {
	return r.collection
}

// Peer returns the peer repository (PostgreSQL)
func (r *Repository) Peer() storage.PeerRepository {
	return r.pgPeer
}

// Feature returns the feature repository (OGC API)
func (r *Repository) Feature() storage.FeatureRepository {
	return r.ogcFeature
}

// Outbox returns the outbox repository (PostgreSQL)
func (r *Repository) Outbox() storage.OutboxRepository {
	return r.pgOutbox
}

// Federation returns the federation repository (PostgreSQL)
func (r *Repository) Federation() storage.FederationRepository {
	return r.pgFederation
}

// Ping checks connectivity to both OGC API and PostgreSQL
func (r *Repository) Ping() error {
	// Check OGC API
	resp, err := r.client.Get(r.baseURL + "/health")
	if err != nil {
		return fmt.Errorf("OGC API health check failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("OGC API returned %d", resp.StatusCode)
	}

	// Check PostgreSQL
	return r.pgPeer.(interface{ Ping() error }).Ping()
}

// Close closes PostgreSQL connection
func (r *Repository) Close() error {
	return r.pgPeer.(interface{ Close() error }).Close()
}

// WithTransaction executes a function within a PostgreSQL transaction
func (r *Repository) WithTransaction(ctx context.Context, fn func(tx pgx.Tx) error) error {
	// Use PostgreSQL transaction for federation data
	return r.pgPeer.(interface {
		WithTransaction(ctx context.Context, fn func(tx pgx.Tx) error) error
	}).WithTransaction(ctx, fn)
}
