package types

import (
	"encoding/json"
	"time"
)

// ========== Activity Types ==========

type ActivityType string

const (
	ActivityCreate ActivityType = "Create"
	ActivityUpdate ActivityType = "Update"
	ActivityDelete ActivityType = "Delete"
)

type Activity struct {
	ID          string          `json:"id"`
	Type        ActivityType    `json:"type"`
	Timestamp   time.Time       `json:"timestamp"`
	ServerID    string          `json:"server_id"`
	FeatureID   int64           `json:"feature_id"`
	FeatureData json.RawMessage `json:"feature_data,omitempty"`
	Version     int             `json:"version"`
	Signature   string          `json:"signature"`
	Checksum    string          `json:"checksum"`
}

type SignedActivity struct {
	Activity    Activity `json:"activity"`
	SenderID    string   `json:"sender_id"`
	PublicKeyID string   `json:"public_key_id"`
}

// ========== Manifest Types ==========

type Manifest struct {
	ProtocolVersion string         `json:"protocol_version"`
	ServerID        string         `json:"server_id"`
	Status          string         `json:"status"`
	PublicKey       string         `json:"public_key"`
	Endpoints       EndpointConfig `json:"endpoints"`
	Datasets        []DatasetInfo  `json:"datasets"`
}

type EndpointConfig struct {
	InboxURL    string `json:"inbox_url"`
	OutboxURL   string `json:"outbox_url"`
	ManifestURL string `json:"manifest_url"`
}

type DatasetInfo struct {
	ID           string    `json:"id"`
	Name         string    `json:"name"`
	Description  string    `json:"description,omitempty"`
	FeatureCount int64     `json:"feature_count"`
	Bounds       []float64 `json:"bounds,omitempty"`
	UpdatedAt    time.Time `json:"updated_at,omitempty"`
}

// ========== Peer Types ==========

type Peer struct {
	ServerID    string    `json:"server_id"`
	PublicKey   string    `json:"public_key"`
	TrustScore  float64   `json:"trust_score"`
	EndpointURL string    `json:"endpoint_url"`
	Status      string    `json:"status"`
	LastSeen    time.Time `json:"last_seen"`
	CreatedAt   time.Time `json:"created_at"`
}

// ========== GeoJSON Types (OGC API Features) ==========

type GeoJSONFeature struct {
	ID         int64                  `json:"id,omitempty"`
	Type       string                 `json:"type"`
	Geometry   interface{}            `json:"geometry"`
	Properties map[string]interface{} `json:"properties"`
	Version    int                    `json:"version,omitempty"`
}

type GeoJSONFeatureCollection struct {
	Type     string            `json:"type"`
	Features []*GeoJSONFeature `json:"features"`
}

// ========== Collection Types ==========

type Collection struct {
	ID          string `json:"id"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Extent      Extent `json:"extent"`
	Links       []Link `json:"links,omitempty"`
}

type Extent struct {
	Spatial SpatialExtent `json:"spatial"`
}

type SpatialExtent struct {
	Bbox [][]float64 `json:"bbox"`
	Crs  string      `json:"crs"`
}

type Link struct {
	Rel  string `json:"rel"`
	Href string `json:"href"`
	Type string `json:"type"`
}

// ========== Sync Message Types ==========

type SyncMessage struct {
	// Identification
	SenderServerID string `json:"sender_server_id"`

	// Content
	FeatureID        int64           `json:"feature_id"`
	ActivityType     string          `json:"activity_type"`
	Version          int             `json:"version"`
	TargetObjectData json.RawMessage `json:"target_object_data"`

	// Metadata
	Timestamp    time.Time `json:"timestamp"`
	CollectionID string    `json:"collection_id,omitempty"`

	// Security
	PayloadChecksum string `json:"payload_checksum"`
	SenderSignature string `json:"sender_signature"`
}

// ========== Query Types ==========

type SpatialQuery struct {
	Geometry interface{} `json:"geometry"`
	Distance float64     `json:"distance,omitempty"`
	Limit    int         `json:"limit,omitempty"`
}
