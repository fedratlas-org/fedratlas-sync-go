package postgres

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"

	"github.com/fedratlas-org/fedratlas-sync-go/pkg/types"

	"github.com/jackc/pgx/v5"
)

//FeaturePostgresStorage implementation

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
