package ogc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/fedratlas-org/fedratlas-sync-go/pkg/types"
)

// FeatureRepository implements storage.FeatureRepository using OGC API
type FeatureRepository struct {
	repo *Repository
}

// NewFeatureRepository creates a new OGC feature repository
func NewFeatureRepository(repo *Repository) *FeatureRepository {
	return &FeatureRepository{repo: repo}
}

// CreateFeature creates a new feature via OGC API
func (r *FeatureRepository) CreateFeature(geometry interface{}, properties map[string]interface{}) (int64, error) {
	feature := types.GeoJSONFeature{
		Type:       "Feature",
		Geometry:   geometry,
		Properties: properties,
	}

	body, err := json.Marshal(feature)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal feature: %w", err)
	}

	url := fmt.Sprintf("%s/collections/%s/items", r.repo.baseURL, r.repo.collection)
	resp, err := r.repo.client.Post(url, "application/geo+json", bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("OGC API request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("OGC API returned %d: %s", resp.StatusCode, string(body))
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("failed to parse response: %w", err)
	}

	// Extract ID
	if id, ok := result["id"].(float64); ok {
		return int64(id), nil
	}
	if id, ok := result["id"].(string); ok {
		if idInt, err := strconv.ParseInt(id, 10, 64); err == nil {
			return idInt, nil
		}
	}

	return 0, fmt.Errorf("no ID in response")
}

// GetFeature retrieves a feature via OGC API
func (r *FeatureRepository) GetFeature(featureID int64) (*types.GeoJSONFeature, error) {
	url := fmt.Sprintf("%s/collections/%s/items/%d", r.repo.baseURL, r.repo.collection, featureID)

	resp, err := r.repo.client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("OGC API request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("OGC API returned %d", resp.StatusCode)
	}

	var feature types.GeoJSONFeature
	if err := json.NewDecoder(resp.Body).Decode(&feature); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return &feature, nil
}

// GetFeatures retrieves features via OGC API with filtering
func (r *FeatureRepository) GetFeatures(collectionID string, bbox string, limit string, offset string) ([]*types.GeoJSONFeature, int, error) {
	// Build URL with query parameters
	url := fmt.Sprintf("%s/collections/%s/items", r.repo.baseURL, r.repo.collection)

	params := []string{}
	if bbox != "" {
		params = append(params, "bbox="+bbox)
	}
	if limit != "" {
		params = append(params, "limit="+limit)
	}
	if offset != "" {
		params = append(params, "offset="+offset)
	}

	if len(params) > 0 {
		url += "?"
		for i, p := range params {
			if i > 0 {
				url += "&"
			}
			url += p
		}
	}

	resp, err := r.repo.client.Get(url)
	if err != nil {
		return nil, 0, fmt.Errorf("OGC API request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, 0, fmt.Errorf("OGC API returned %d", resp.StatusCode)
	}

	var response struct {
		Features       []*types.GeoJSONFeature `json:"features"`
		NumberMatched  int                     `json:"numberMatched"`
		NumberReturned int                     `json:"numberReturned"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, 0, fmt.Errorf("failed to parse response: %w", err)
	}

	return response.Features, response.NumberMatched, nil
}

// UpdateFeature updates a feature via OGC API
func (r *FeatureRepository) UpdateFeature(featureID int64, geometry interface{}, properties map[string]interface{}, version int) error {
	feature := types.GeoJSONFeature{
		ID:         featureID,
		Type:       "Feature",
		Geometry:   geometry,
		Properties: properties,
	}

	body, err := json.Marshal(feature)
	if err != nil {
		return fmt.Errorf("failed to marshal feature: %w", err)
	}

	url := fmt.Sprintf("%s/collections/%s/items/%d", r.repo.baseURL, r.repo.collection, featureID)

	req, err := http.NewRequest("PUT", url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/geo+json")

	resp, err := r.repo.client.Do(req)
	if err != nil {
		return fmt.Errorf("OGC API request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("OGC API returned %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// DeleteFeature deletes a feature via OGC API
func (r *FeatureRepository) DeleteFeature(featureID int64) error {
	url := fmt.Sprintf("%s/collections/%s/items/%d", r.repo.baseURL, r.repo.collection, featureID)

	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := r.repo.client.Do(req)
	if err != nil {
		return fmt.Errorf("OGC API request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("OGC API returned %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// GetFeaturesByGeometry retrieves features near a geometry via OGC API
func (r *FeatureRepository) GetFeaturesByGeometry(collectionID string, geometry interface{}, distance float64) ([]*types.GeoJSONFeature, error) {
	// Convert geometry to GeoJSON
	_, err := json.Marshal(geometry)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal geometry: %w", err)
	}

	// OGC API supports spatial queries via bbox or filter
	// This is a simplified implementation
	url := fmt.Sprintf("%s/collections/%s/items?limit=100", r.repo.baseURL, r.repo.collection)

	resp, err := r.repo.client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("OGC API request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("OGC API returned %d", resp.StatusCode)
	}

	var response struct {
		Features []*types.GeoJSONFeature `json:"features"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	// In a real implementation, you'd use the OGC API's spatial filter
	// For now, return all features
	return response.Features, nil
}
