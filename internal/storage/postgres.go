package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"fedratlas-sync/pkg/types"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresStorage struct {
	pool *pgxpool.Pool
	ctx  context.Context
}

type OutboxEntry struct {
	ID        int64
	Activity  *types.Activity
	Status    string
	CreatedAt time.Time
}

type OutboxDelivery struct {
	OutboxID    int64
	PeerID      string
	RetryCount  int
	NextRetry   *time.Time
	LastError   *string
	DeliveredAt *time.Time
}

type FederationLog struct {
	LogID        int64
	ServerID     string
	ActivityType string
	Timestamp    time.Time
	Details      json.RawMessage
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

// createTables creates all necessary tables using pgx
func (s *PostgresStorage) createTables() error {
	queries := []string{
		// Enable PostGIS extension (if available)
		`CREATE EXTENSION IF NOT EXISTS postgis`,

		// PEER_REGISTRY table
		`CREATE TABLE IF NOT EXISTS peer_registry (
            server_id VARCHAR(100) PRIMARY KEY,
            public_key TEXT NOT NULL,
            trust_score NUMERIC(3,2) DEFAULT 0.5,
            endpoint_url TEXT NOT NULL,
            status VARCHAR(20) DEFAULT 'PENDING',
            last_seen TIMESTAMP WITH TIME ZONE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            CONSTRAINT valid_status CHECK (status IN ('FOLLOWING', 'BLOCKED', 'PENDING'))
        )`,

		// Create index for faster lookups
		`CREATE INDEX IF NOT EXISTS idx_peer_registry_status ON peer_registry(status)`,
		`CREATE INDEX IF NOT EXISTS idx_peer_registry_trust_score ON peer_registry(trust_score DESC)`,

		// GEO_FEATURES table (spatial data)
		`CREATE TABLE IF NOT EXISTS geo_features (
            feature_id BIGSERIAL PRIMARY KEY,
            geom GEOMETRY(Geometry, 4326),
            version INTEGER NOT NULL DEFAULT 1,
            trust_score NUMERIC(3,2) DEFAULT 0.5,
            feature_data JSONB,
            last_edited_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            created_by VARCHAR(100),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )`,

		// Spatial indexes
		`CREATE INDEX IF NOT EXISTS idx_geo_features_geom ON geo_features USING GIST(geom)`,
		`CREATE INDEX IF NOT EXISTS idx_geo_features_version ON geo_features(version)`,

		// FEDERATION_OUTBOX table
		`CREATE TABLE IF NOT EXISTS federation_outbox (
            id BIGSERIAL PRIMARY KEY,
            activity JSONB NOT NULL,
            status VARCHAR(20) DEFAULT 'PENDING',
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            CONSTRAINT valid_status CHECK (status IN ('PENDING', 'PROCESSING', 'DELIVERED', 'FAILED'))
        )`,

		// Index for outbox polling
		`CREATE INDEX IF NOT EXISTS idx_federation_outbox_status_created 
            ON federation_outbox(status, created_at) WHERE status = 'PENDING'`,

		// OUTBOX_DELIVERY table (track deliveries per peer)
		`CREATE TABLE IF NOT EXISTS outbox_delivery (
            outbox_id BIGINT REFERENCES federation_outbox(id) ON DELETE CASCADE,
            peer_id VARCHAR(100) REFERENCES peer_registry(server_id) ON DELETE CASCADE,
            retry_count INT DEFAULT 0,
            next_retry TIMESTAMP WITH TIME ZONE,
            last_error TEXT,
            delivered_at TIMESTAMP WITH TIME ZONE,
            PRIMARY KEY (outbox_id, peer_id)
        )`,

		// Index for retry scheduling
		`CREATE INDEX IF NOT EXISTS idx_outbox_delivery_next_retry 
            ON outbox_delivery(next_retry) WHERE delivered_at IS NULL`,

		// FEDERATION_LOG table
		`CREATE TABLE IF NOT EXISTS federation_log (
            log_id BIGSERIAL PRIMARY KEY,
            server_id VARCHAR(100) REFERENCES peer_registry(server_id),
            activity_type VARCHAR(50),
            timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            details JSONB
        )`,

		// Index for log queries
		`CREATE INDEX IF NOT EXISTS idx_federation_log_timestamp ON federation_log(timestamp DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_federation_log_server_id ON federation_log(server_id)`,

		// MODERATION_QUEUE table
		`CREATE TABLE IF NOT EXISTS moderation_queue (
            task_id BIGSERIAL PRIMARY KEY,
            user_id VARCHAR(50),
            feature_id BIGINT REFERENCES geo_features(feature_id),
            submission_data JSONB NOT NULL,
            status VARCHAR(20) DEFAULT 'PENDING',
            submitted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            reviewed_at TIMESTAMP WITH TIME ZONE,
            reviewed_by VARCHAR(100),
            CONSTRAINT valid_mod_status CHECK (status IN ('PENDING', 'APPROVED', 'REJECTED'))
        )`,

		// PLACE_REVIEWS table
		`CREATE TABLE IF NOT EXISTS place_reviews (
            review_id BIGSERIAL PRIMARY KEY,
            user_id VARCHAR(50),
            feature_id BIGINT REFERENCES geo_features(feature_id),
            rating INTEGER CHECK (rating >= 1 AND rating <= 5),
            review_text TEXT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )`,

		// Create composite indexes for common queries
		`CREATE INDEX IF NOT EXISTS idx_place_reviews_feature_id ON place_reviews(feature_id)`,
		`CREATE INDEX IF NOT EXISTS idx_moderation_queue_status ON moderation_queue(status) WHERE status = 'PENDING'`,

		// Create a function to update updated_at timestamp
		`CREATE OR REPLACE FUNCTION update_updated_at_column()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = NOW();
                RETURN NEW;
            END;
            $$ language 'plpgsql'`,

		// Triggers for updated_at
		`DROP TRIGGER IF EXISTS update_peer_registry_updated_at ON peer_registry`,
		`CREATE TRIGGER update_peer_registry_updated_at
            BEFORE UPDATE ON peer_registry
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column()`,

		`DROP TRIGGER IF EXISTS update_federation_outbox_updated_at ON federation_outbox`,
		`CREATE TRIGGER update_federation_outbox_updated_at
            BEFORE UPDATE ON federation_outbox
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column()`,
	}

	// Execute each query with context
	for _, query := range queries {
		if _, err := s.pool.Exec(s.ctx, query); err != nil {
			return fmt.Errorf("failed to execute query: %w\nQuery: %s", err, query)
		}
	}

	log.Println("Database schema initialized successfully")
	return nil
}

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

// ----Outbox----//
// AddToOutbox adds an activity to the outbox
func (s *PostgresStorage) AddToOutbox(activity *types.Activity) error {
	activityJSON, err := json.Marshal(activity)
	if err != nil {
		return fmt.Errorf("failed to marshal activity: %w", err)
	}

	query := `INSERT INTO federation_outbox (activity, status) VALUES ($1, 'PENDING') RETURNING id`

	var id int64
	err = s.pool.QueryRow(s.ctx, query, activityJSON).Scan(&id)
	if err != nil {
		return fmt.Errorf("failed to add to outbox: %w", err)
	}

	log.Printf("Activity %d added to outbox", id)
	return nil
}

// AddToOutboxBatch adds multiple activities using batch insert
func (s *PostgresStorage) AddToOutboxBatch(activities []*types.Activity) error {
	batch := &pgx.Batch{}

	for _, activity := range activities {
		activityJSON, err := json.Marshal(activity)
		if err != nil {
			return fmt.Errorf("failed to marshal activity: %w", err)
		}

		query := `INSERT INTO federation_outbox (activity, status) VALUES ($1, 'PENDING')`
		batch.Queue(query, activityJSON)
	}

	results := s.pool.SendBatch(s.ctx, batch)
	defer results.Close()

	for i := 0; i < len(activities); i++ {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("failed to insert activity %d: %w", i, err)
		}
	}

	log.Printf("Batch inserted %d activities into outbox", len(activities))
	return nil
}

// GetPendingOutboxActivities retrieves pending activities with FOR UPDATE SKIP LOCKED for concurrency
func (s *PostgresStorage) GetPendingOutboxActivities(limit int) ([]*OutboxEntry, error) {
	// Use FOR UPDATE SKIP LOCKED to avoid multiple workers processing same items
	query := `
        SELECT id, activity, status, created_at
        FROM federation_outbox
        WHERE status = 'PENDING'
        ORDER BY created_at
        LIMIT $1
        FOR UPDATE SKIP LOCKED`

	rows, err := s.pool.Query(s.ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query pending activities: %w", err)
	}
	defer rows.Close()

	var entries []*OutboxEntry
	for rows.Next() {
		var entry OutboxEntry
		var activityJSON []byte

		if err := rows.Scan(&entry.ID, &activityJSON, &entry.Status, &entry.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan activity: %w", err)
		}

		var activity types.Activity
		if err := json.Unmarshal(activityJSON, &activity); err != nil {
			return nil, fmt.Errorf("failed to unmarshal activity: %w", err)
		}
		entry.Activity = &activity

		entries = append(entries, &entry)
	}

	// Mark as processing
	if len(entries) > 0 {
		s.markActivitiesAsProcessing(entries)
	}

	return entries, nil
}

// markActivitiesAsProcessing updates status to PROCESSING
func (s *PostgresStorage) markActivitiesAsProcessing(entries []*OutboxEntry) error {
	ids := make([]int64, len(entries))
	for i, entry := range entries {
		ids[i] = entry.ID
	}

	query := `UPDATE federation_outbox SET status = 'PROCESSING', updated_at = NOW() 
              WHERE id = ANY($1)`

	if _, err := s.pool.Exec(s.ctx, query, ids); err != nil {
		return fmt.Errorf("failed to mark activities as processing: %w", err)
	}

	return nil
}

// MarkOutboxDelivered marks an activity as delivered to a specific peer
func (s *PostgresStorage) MarkOutboxDelivered(outboxID int64, peerID string) error {
	query := `
        INSERT INTO outbox_delivery (outbox_id, peer_id, delivered_at)
        VALUES ($1, $2, NOW())
        ON CONFLICT (outbox_id, peer_id) DO UPDATE SET
            delivered_at = NOW(),
            last_error = NULL,
            retry_count = 0`

	_, err := s.pool.Exec(s.ctx, query, outboxID, peerID)
	if err != nil {
		return fmt.Errorf("failed to mark delivered: %w", err)
	}

	return nil
}

// GetOutboxRetryInfo gets retry information for an outbox activity
func (s *PostgresStorage) GetOutboxRetryInfo(outboxID int64, peerID string) (*OutboxDelivery, error) {
	query := `SELECT outbox_id, peer_id, retry_count, next_retry, last_error, delivered_at
              FROM outbox_delivery
              WHERE outbox_id = $1 AND peer_id = $2`

	var delivery OutboxDelivery
	err := s.pool.QueryRow(s.ctx, query, outboxID, peerID).Scan(
		&delivery.OutboxID, &delivery.PeerID, &delivery.RetryCount,
		&delivery.NextRetry, &delivery.LastError, &delivery.DeliveredAt,
	)

	if err != nil {
		if err == pgx.ErrNoRows {
			// Return default delivery info if not found
			return &OutboxDelivery{
				OutboxID:   outboxID,
				PeerID:     peerID,
				RetryCount: 0,
			}, nil
		}
		return nil, fmt.Errorf("failed to get retry info: %w", err)
	}

	return &delivery, nil
}

// UpdateOutboxRetry updates retry information with exponential backoff
func (s *PostgresStorage) UpdateOutboxRetry(outboxID int64, peerID string, retryCount int, nextRetry time.Time, lastError string) error {
	query := `
        INSERT INTO outbox_delivery (outbox_id, peer_id, retry_count, next_retry, last_error)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (outbox_id, peer_id) DO UPDATE SET
            retry_count = EXCLUDED.retry_count,
            next_retry = EXCLUDED.next_retry,
            last_error = EXCLUDED.last_error,
            delivered_at = NULL`

	_, err := s.pool.Exec(s.ctx, query, outboxID, peerID, retryCount, nextRetry, lastError)
	if err != nil {
		return fmt.Errorf("failed to update retry info: %w", err)
	}

	return nil
}

// MarkOutboxFailed marks an activity as failed after max retries
func (s *PostgresStorage) MarkOutboxFailed(outboxID int64, peerID string, lastError string) error {
	query := `UPDATE federation_outbox SET status = 'FAILED', updated_at = NOW() WHERE id = $1`

	if _, err := s.pool.Exec(s.ctx, query, outboxID); err != nil {
		return fmt.Errorf("failed to mark outbox as failed: %w", err)
	}

	// Also update delivery record
	updateDelivery := `
        UPDATE outbox_delivery 
        SET last_error = $3, next_retry = NULL
        WHERE outbox_id = $1 AND peer_id = $2`

	if _, err := s.pool.Exec(s.ctx, updateDelivery, outboxID, peerID, lastError); err != nil {
		return fmt.Errorf("failed to update delivery error: %w", err)
	}

	return nil
}

// ----Federation----//
// LogFederationEvent logs a federation event
func (s *PostgresStorage) LogFederationEvent(serverID string, activityType string, details interface{}) error {
	detailsJSON, err := json.Marshal(details)
	if err != nil {
		return fmt.Errorf("failed to marshal details: %w", err)
	}

	query := `INSERT INTO federation_log (server_id, activity_type, details) VALUES ($1, $2, $3)`

	_, err = s.pool.Exec(s.ctx, query, serverID, activityType, detailsJSON)
	if err != nil {
		return fmt.Errorf("failed to log event: %w", err)
	}

	return nil
}

// UpdatePeerTrustScore updates a peer's trust score
func (s *PostgresStorage) UpdatePeerTrustScore(serverID string, newScore float64) error {
	query := `UPDATE peer_registry SET trust_score = $2, updated_at = NOW() WHERE server_id = $1`

	result, err := s.pool.Exec(s.ctx, query, serverID, newScore)
	if err != nil {
		return fmt.Errorf("failed to update trust score: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("peer %s not found", serverID)
	}

	return nil
}

// GetFailedActivities gets activities that need retry
func (s *PostgresStorage) GetFailedActivities(limit int) ([]*OutboxEntry, error) {
	query := `
        SELECT o.id, o.activity, o.status, o.created_at
        FROM federation_outbox o
        JOIN outbox_delivery d ON o.id = d.outbox_id
        WHERE o.status = 'PROCESSING'
          AND d.delivered_at IS NULL
          AND d.next_retry <= NOW()
        LIMIT $1
        FOR UPDATE SKIP LOCKED`

	rows, err := s.pool.Query(s.ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query failed activities: %w", err)
	}
	defer rows.Close()

	var entries []*OutboxEntry
	for rows.Next() {
		var entry OutboxEntry
		var activityJSON []byte

		if err := rows.Scan(&entry.ID, &activityJSON, &entry.Status, &entry.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan activity: %w", err)
		}

		var activity types.Activity
		if err := json.Unmarshal(activityJSON, &activity); err != nil {
			return nil, fmt.Errorf("failed to unmarshal activity: %w", err)
		}
		entry.Activity = &activity

		entries = append(entries, &entry)
	}

	return entries, nil
}

// CreateFeature creates a new geospatial feature
func (s *PostgresStorage) CreateFeature(geometry interface{}, properties map[string]interface{}) (int64, error) {
	// Convert geometry to GeoJSON string
	geomJSON, err := json.Marshal(geometry)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal geometry: %w", err)
	}

	// Convert properties to JSONB
	propsJSON, err := json.Marshal(properties)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal properties: %w", err)
	}

	query := `
        INSERT INTO geo_features (geom, feature_data, version, created_at)
        VALUES (ST_SetSRID(ST_GeomFromGeoJSON($1), 4326), $2, 1, NOW())
        RETURNING feature_id`

	var featureID int64
	err = s.pool.QueryRow(s.ctx, query, string(geomJSON), propsJSON).Scan(&featureID)
	if err != nil {
		return 0, fmt.Errorf("failed to create feature: %w", err)
	}

	log.Printf("Created feature %d", featureID)
	return featureID, nil
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

// GetPendingOutboxCount returns count of pending outbox activities
func (s *PostgresStorage) GetPendingOutboxCount() (int, error) {
	var count int
	query := `SELECT COUNT(*) FROM federation_outbox WHERE status = 'PENDING'`
	err := s.pool.QueryRow(s.ctx, query).Scan(&count)
	return count, err
}

// GetFeature retrieves a feature by ID
func (s *PostgresStorage) GetFeature(featureID int64) (*types.GeoJSONFeature, error) {
	query := `
        SELECT feature_id, ST_AsGeoJSON(geom) as geom, feature_data, version
        FROM geo_features
        WHERE feature_id = $1`

	var feature types.GeoJSONFeature
	var geomJSON string
	var featureDataJSON []byte

	err := s.pool.QueryRow(s.ctx, query, featureID).Scan(
		&feature.ID, &geomJSON, &featureDataJSON, &feature.Version,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("feature %d not found", featureID)
		}
		return nil, fmt.Errorf("failed to get feature: %w", err)
	}

	feature.Type = "Feature"

	// Parse geometry
	var geom interface{}
	if err := json.Unmarshal([]byte(geomJSON), &geom); err != nil {
		return nil, fmt.Errorf("failed to parse geometry: %w", err)
	}
	feature.Geometry = geom

	// Parse properties
	if err := json.Unmarshal(featureDataJSON, &feature.Properties); err != nil {
		feature.Properties = make(map[string]interface{})
	}

	return &feature, nil
}

// UpdateFeature updates an existing feature
func (s *PostgresStorage) UpdateFeature(featureID int64, geometry interface{}, properties map[string]interface{}, version int) error {
	geomJSON, err := json.Marshal(geometry)
	if err != nil {
		return fmt.Errorf("failed to marshal geometry: %w", err)
	}

	propsJSON, err := json.Marshal(properties)
	if err != nil {
		return fmt.Errorf("failed to marshal properties: %w", err)
	}

	query := `
        UPDATE geo_features 
        SET geom = ST_SetSRID(ST_GeomFromGeoJSON($1), 4326),
            feature_data = $2,
            version = $3,
            last_edited_timestamp = NOW()
        WHERE feature_id = $4 AND version < $3`

	result, err := s.pool.Exec(s.ctx, query, string(geomJSON), propsJSON, version, featureID)
	if err != nil {
		return fmt.Errorf("failed to update feature: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("feature %d not found or version conflict", featureID)
	}

	log.Printf("Updated feature %d to version %d", featureID, version)
	return nil
}

// DeleteFeature deletes a feature
func (s *PostgresStorage) DeleteFeature(featureID int64) error {
	query := `DELETE FROM geo_features WHERE feature_id = $1`

	result, err := s.pool.Exec(s.ctx, query, featureID)
	if err != nil {
		return fmt.Errorf("failed to delete feature: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("feature %d not found", featureID)
	}

	log.Printf("Deleted feature %d", featureID)
	return nil
}

// GetFeatures retrieves features with filtering
func (s *PostgresStorage) GetFeatures(collectionID string, bbox string, limit string, offset string) ([]*types.GeoJSONFeature, int, error) {
	// Parse limit and offset
	limitInt := 10
	if limit != "" {
		if l, err := strconv.Atoi(limit); err == nil && l > 0 {
			limitInt = l
			if limitInt > 100 {
				limitInt = 100 // Max limit
			}
		}
	}

	offsetInt := 0
	if offset != "" {
		if o, err := strconv.Atoi(offset); err == nil && o > 0 {
			offsetInt = o
		}
	}

	// Build query with optional bbox filter
	query := `
        SELECT feature_id, ST_AsGeoJSON(geom) as geom, feature_data, version
        FROM geo_features
        WHERE feature_data->>'collection' = $1 OR $1 = ''
        ORDER BY feature_id
        LIMIT $2 OFFSET $3`

	rows, err := s.pool.Query(s.ctx, query, collectionID, limitInt, offsetInt)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to query features: %w", err)
	}
	defer rows.Close()

	var features []*types.GeoJSONFeature
	for rows.Next() {
		var feature types.GeoJSONFeature
		var geomJSON string
		var featureDataJSON []byte

		if err := rows.Scan(&feature.ID, &geomJSON, &featureDataJSON, &feature.Version); err != nil {
			return nil, 0, fmt.Errorf("failed to scan feature: %w", err)
		}

		feature.Type = "Feature"

		// Parse geometry
		var geom interface{}
		if err := json.Unmarshal([]byte(geomJSON), &geom); err != nil {
			continue
		}
		feature.Geometry = geom

		// Parse properties
		if err := json.Unmarshal(featureDataJSON, &feature.Properties); err != nil {
			feature.Properties = make(map[string]interface{})
		}

		features = append(features, &feature)
	}

	// Get total count
	countQuery := `SELECT COUNT(*) FROM geo_features WHERE feature_data->>'collection' = $1 OR $1 = ''`
	var totalCount int
	s.pool.QueryRow(s.ctx, countQuery, collectionID).Scan(&totalCount)

	return features, totalCount, nil
}

// GetFeaturesByGeometry retrieves features within a geometry
func (s *PostgresStorage) GetFeaturesByGeometry(collectionID string, geometry interface{}, distance float64) ([]*types.GeoJSONFeature, error) {
	geomJSON, err := json.Marshal(geometry)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal geometry: %w", err)
	}

	var query string
	var args []interface{}

	if distance > 0 {
		// DWithin query for radius search
		query = `
            SELECT feature_id, ST_AsGeoJSON(geom) as geom, feature_data, version,
                   ST_Distance(geom, ST_SetSRID(ST_GeomFromGeoJSON($1), 4326)) as distance
            FROM geo_features
            WHERE ST_DWithin(geom, ST_SetSRID(ST_GeomFromGeoJSON($1), 4326), $2)
              AND (feature_data->>'collection' = $3 OR $3 = '')
            ORDER BY distance
            LIMIT 100`
		args = []interface{}{string(geomJSON), distance, collectionID}
	} else {
		// ST_Within query for polygon containment
		query = `
            SELECT feature_id, ST_AsGeoJSON(geom) as geom, feature_data, version
            FROM geo_features
            WHERE ST_Within(geom, ST_SetSRID(ST_GeomFromGeoJSON($1), 4326))
              AND (feature_data->>'collection' = $2 OR $2 = '')
            LIMIT 1000`
		args = []interface{}{string(geomJSON), collectionID}
	}

	rows, err := s.pool.Query(s.ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query features by geometry: %w", err)
	}
	defer rows.Close()

	var features []*types.GeoJSONFeature
	for rows.Next() {
		var feature types.GeoJSONFeature
		var geomJSON string
		var featureDataJSON []byte

		if distance > 0 {
			var dist float64
			if err := rows.Scan(&feature.ID, &geomJSON, &featureDataJSON, &feature.Version, &dist); err != nil {
				return nil, fmt.Errorf("failed to scan feature: %w", err)
			}
			feature.Properties["distance"] = dist
		} else {
			if err := rows.Scan(&feature.ID, &geomJSON, &featureDataJSON, &feature.Version); err != nil {
				return nil, fmt.Errorf("failed to scan feature: %w", err)
			}
		}

		feature.Type = "Feature"

		var geom interface{}
		if err := json.Unmarshal([]byte(geomJSON), &geom); err != nil {
			continue
		}
		feature.Geometry = geom

		if err := json.Unmarshal(featureDataJSON, &feature.Properties); err != nil {
			feature.Properties = make(map[string]interface{})
		}

		features = append(features, &feature)
	}

	return features, nil
}

// Ping checks database connectivity
func (s *PostgresStorage) Ping() error {
	return s.pool.Ping(s.ctx)
}
