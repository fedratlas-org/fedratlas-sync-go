package postgres

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/fedratlas-org/fedratlas-sync-go/internal/storage"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	pool *pgxpool.Pool
	ctx  context.Context
}

// NewRepository creates a new connection pool using pgx
func NewRepository(connString string) (*Repository, error) {
	ctx := context.Background()

	// Parse connection config
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	// Configure pool settings for better performance
	config.MaxConns = 25
	config.MinConns = 5
	config.MaxConnLifetime = 1 * time.Hour
	config.MaxConnIdleTime = 30 * time.Minute
	config.HealthCheckPeriod = 1 * time.Minute

	// Create connection pool
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Verify connection
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	log.Printf("Connected to PostgreSQL with pgx (max_conns=%d, min_conns=%d)",
		config.MaxConns, config.MinConns)

	storage := &Repository{
		pool: pool,
		ctx:  ctx,
	}

	// Initialize schema
	if err := storage.createTables(); err != nil {
		return nil, fmt.Errorf("failed to create tables: %w", err)
	}

	return storage, nil
}

func (s *Repository) Peer() storage.PeerRepository             { return s }
func (s *Repository) Feature() storage.FeatureRepository       { return s }
func (s *Repository) Outbox() storage.OutboxRepository         { return s }
func (s *Repository) Federation() storage.FederationRepository { return s }

// Close closes the connection pool
func (s *Repository) Close() error {
	s.pool.Close()
	log.Println("Database connection pool closed")
	return nil
}

// GetPool returns the underlying connection pool (for advanced operations)
func (s *Repository) GetPool() *pgxpool.Pool {
	return s.pool
}

// BeginTx starts a new transaction
func (s *Repository) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return s.pool.Begin(ctx)
}

// WithTransaction executes a function within a transaction
func (s *Repository) WithTransaction(ctx context.Context, fn func(tx pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if err != nil {
			tx.Rollback(ctx)
		}
	}()

	if err := fn(tx); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// Ping checks database connectivity
func (s *Repository) Ping() error {
	return s.pool.Ping(s.ctx)
}
