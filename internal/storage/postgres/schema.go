package postgres

import (
	"fmt"
	"log"
)

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
