package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"fedratlas-sync/internal/storage"
	"fedratlas-sync/internal/sync"
	"fedratlas-sync/pkg/types"

	"github.com/go-chi/chi/v5"
)

type FeaturesHandler struct {
	engine  *sync.SyncEngine
	storage *storage.PostgresStorage
}

func NewFeaturesHandler(engine *sync.SyncEngine, storage *storage.PostgresStorage) *FeaturesHandler {
	return &FeaturesHandler{
		engine:  engine,
		storage: storage,
	}
}

// ListCollections - GET /fedmap/v1/collections
func (h *FeaturesHandler) ListCollections(w http.ResponseWriter, r *http.Request) {
	collections := []types.Collection{
		{
			ID:          "roads",
			Title:       "Road Network",
			Description: "All road features including highways, streets, and paths",
			Extent: types.Extent{
				Spatial: types.SpatialExtent{
					Bbox: [][]float64{{-180, -90, 180, 90}},
					Crs:  "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
				},
			},
		},
		{
			ID:          "pois",
			Title:       "Points of Interest",
			Description: "Restaurants, shops, landmarks, and other POIs",
			Extent: types.Extent{
				Spatial: types.SpatialExtent{
					Bbox: [][]float64{{-180, -90, 180, 90}},
					Crs:  "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
				},
			},
		},
		{
			ID:          "boundaries",
			Title:       "Administrative Boundaries",
			Description: "Country, state, and city boundaries",
			Extent: types.Extent{
				Spatial: types.SpatialExtent{
					Bbox: [][]float64{{-180, -90, 180, 90}},
					Crs:  "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
				},
			},
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"collections": collections,
	})
}

// CreateFeature - POST /fedmap/v1/collections/{collectionId}/items
func (h *FeaturesHandler) CreateFeature(w http.ResponseWriter, r *http.Request) {
	collectionID := chi.URLParam(r, "collectionId")

	var feature types.GeoJSONFeature
	if err := json.NewDecoder(r.Body).Decode(&feature); err != nil {
		http.Error(w, "Invalid GeoJSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	feature.Type = "Feature"

	// Add collection ID to properties
	if feature.Properties == nil {
		feature.Properties = make(map[string]interface{})
	}
	feature.Properties["collection"] = collectionID
	feature.Properties["created_at"] = time.Now().UTC()

	// Save to database
	featureID, err := h.storage.CreateFeature(feature.Geometry, feature.Properties)
	if err != nil {
		http.Error(w, "Failed to save feature: "+err.Error(), http.StatusInternalServerError)
		return
	}

	feature.ID = featureID

	// Trigger federation sync
	featureData, _ := json.Marshal(feature)
	h.engine.OnFeatureChange(featureID, types.ActivityCreate, featureData, 1)

	w.Header().Set("Content-Type", "application/geo+json")
	w.Header().Set("Location", r.URL.Path+"/"+strconv.FormatInt(featureID, 10))
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(feature)
}

// GetFeature - GET /fedmap/v1/collections/{collectionId}/items/{featureId}
func (h *FeaturesHandler) GetFeature(w http.ResponseWriter, r *http.Request) {
	collectionID := chi.URLParam(r, "collectionId")
	featureIDStr := chi.URLParam(r, "featureId")

	featureID, err := strconv.ParseInt(featureIDStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid feature ID", http.StatusBadRequest)
		return
	}

	feature, err := h.storage.GetFeature(featureID)
	if err != nil {
		http.Error(w, "Feature not found", http.StatusNotFound)
		return
	}

	// Verify feature belongs to requested collection
	if collection, ok := feature.Properties["collection"]; ok && collection != collectionID {
		http.Error(w, "Feature not in this collection", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/geo+json")
	json.NewEncoder(w).Encode(feature)
}

// UpdateFeature - PUT /fedmap/v1/collections/{collectionId}/items/{featureId}
func (h *FeaturesHandler) UpdateFeature(w http.ResponseWriter, r *http.Request) {
	collectionID := chi.URLParam(r, "collectionId")
	featureIDStr := chi.URLParam(r, "featureId")

	featureID, err := strconv.ParseInt(featureIDStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid feature ID", http.StatusBadRequest)
		return
	}

	var feature types.GeoJSONFeature
	if err := json.NewDecoder(r.Body).Decode(&feature); err != nil {
		http.Error(w, "Invalid GeoJSON", http.StatusBadRequest)
		return
	}

	feature.Type = "Feature"
	feature.ID = featureID

	// Get current feature to check version
	current, err := h.storage.GetFeature(featureID)
	if err != nil {
		http.Error(w, "Feature not found", http.StatusNotFound)
		return
	}

	// Preserve collection ID
	if feature.Properties == nil {
		feature.Properties = make(map[string]interface{})
	}
	feature.Properties["collection"] = collectionID
	feature.Properties["updated_at"] = time.Now().UTC()

	newVersion := 1
	if ver, ok := current.Properties["version"]; ok {
		if v, ok := ver.(float64); ok {
			newVersion = int(v) + 1
		}
	}

	// Update database
	err = h.storage.UpdateFeature(featureID, feature.Geometry, feature.Properties, newVersion)
	if err != nil {
		http.Error(w, "Failed to update feature: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Trigger federation sync
	featureData, _ := json.Marshal(feature)
	h.engine.OnFeatureChange(featureID, types.ActivityUpdate, featureData, newVersion)

	w.Header().Set("Content-Type", "application/geo+json")
	json.NewEncoder(w).Encode(feature)
}

// DeleteFeature - DELETE /fedmap/v1/collections/{collectionId}/items/{featureId}
func (h *FeaturesHandler) DeleteFeature(w http.ResponseWriter, r *http.Request) {
	//collectionID := chi.URLParam(r, "collectionId")
	featureIDStr := chi.URLParam(r, "featureId")

	featureID, err := strconv.ParseInt(featureIDStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid feature ID", http.StatusBadRequest)
		return
	}

	// Delete from database
	err = h.storage.DeleteFeature(featureID)
	if err != nil {
		http.Error(w, "Failed to delete feature: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Trigger federation sync
	h.engine.OnFeatureChange(featureID, types.ActivityDelete, nil, 0)

	w.WriteHeader(http.StatusNoContent)
}

// ListFeatures - GET /fedmap/v1/collections/{collectionId}/items
func (h *FeaturesHandler) ListFeatures(w http.ResponseWriter, r *http.Request) {
	collectionID := chi.URLParam(r, "collectionId")

	bbox := r.URL.Query().Get("bbox")
	limit := r.URL.Query().Get("limit")
	offset := r.URL.Query().Get("offset")

	features, totalCount, err := h.storage.GetFeatures(collectionID, bbox, limit, offset)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := types.GeoJSONFeatureCollection{
		Type:     "FeatureCollection",
		Features: features,
	}

	w.Header().Set("Content-Type", "application/geo+json")
	w.Header().Set("X-Total-Count", strconv.Itoa(totalCount))
	json.NewEncoder(w).Encode(response)
}
