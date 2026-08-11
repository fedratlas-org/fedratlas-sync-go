package storage

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresStorage struct {
	pool *pgxpool.Pool
	ctx  context.Context
}

// NewPostgresStorage creates a new connection pool using pgx
func NewPostgresStorage(connString string) (*PostgresStorage, error) {
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

	storage := &PostgresStorage{
		pool: pool,
		ctx:  ctx,
	}

	// Initialize schema
	if err := storage.createTables(); err != nil {
		return nil, fmt.Errorf("failed to create tables: %w", err)
	}

	return storage, nil
}

// Close closes the connection pool
func (s *PostgresStorage) Close() error {
	s.pool.Close()
	log.Println("Database connection pool closed")
	return nil
}

// GetPool returns the underlying connection pool (for advanced operations)
func (s *PostgresStorage) GetPool() *pgxpool.Pool {
	return s.pool
}

// BeginTx starts a new transaction
func (s *PostgresStorage) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return s.pool.Begin(ctx)
}

// WithTransaction executes a function within a transaction
func (s *PostgresStorage) WithTransaction(ctx context.Context, fn func(tx pgx.Tx) error) error {
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
func (s *PostgresStorage) Ping() error {
	return s.pool.Ping(s.ctx)
}
