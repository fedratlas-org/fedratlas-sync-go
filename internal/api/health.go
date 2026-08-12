package api

import (
	"encoding/json"
	"fedratlas-sync/internal/storage"
	"net/http"
	"runtime"
	"time"

	"fedratlas-sync/internal/sync"
)

// HealthHandler handles all health-related endpoints
type HealthHandler struct {
	serverID  string
	engine    *sync.SyncEngine
	storage   storage.Repository
	startTime time.Time
}

// NewHealthHandler creates a new health handler
func NewHealthHandler(serverID string, engine *sync.SyncEngine, storage storage.Repository) *HealthHandler {
	return &HealthHandler{
		serverID:  serverID,
		engine:    engine,
		storage:   storage,
		startTime: time.Now(),
	}
}

// HealthCheck handles GET /health - basic health check
func (h *HealthHandler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	response := map[string]string{
		"status":    "healthy",
		"server_id": h.serverID,
	}

	json.NewEncoder(w).Encode(response)
}

// ReadinessCheck handles GET /ready - detailed readiness check
func (h *HealthHandler) ReadinessCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Check database connectivity
	dbHealthy := true
	if err := h.storage.Ping(); err != nil {
		dbHealthy = false
	}

	// Check sync engine
	engineHealthy := h.engine.IsRunning()

	// Determine overall status
	overallStatus := "ready"
	httpStatus := http.StatusOK

	if !dbHealthy || !engineHealthy {
		overallStatus = "not ready"
		httpStatus = http.StatusServiceUnavailable
	}

	response := map[string]interface{}{
		"status":    overallStatus,
		"server_id": h.serverID,
		"components": map[string]bool{
			"database":    dbHealthy,
			"sync_engine": engineHealthy,
		},
	}

	w.WriteHeader(httpStatus)
	json.NewEncoder(w).Encode(response)
}

// DetailedHealth handles GET /health/detailed - comprehensive health info
func (h *HealthHandler) DetailedHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Get memory stats
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	response := map[string]interface{}{
		"server_id":      h.serverID,
		"status":         "healthy",
		"uptime_seconds": int(time.Since(h.startTime).Seconds()),
		"metrics": map[string]interface{}{
			"active_peers":    len(h.engine.GetPeers()),
			"outbox_pending":  h.engine.GetOutboxCount(),
			"goroutines":      runtime.NumGoroutine(),
			"memory_alloc_mb": memStats.Alloc / 1024 / 1024,
			"memory_total_mb": memStats.TotalAlloc / 1024 / 1024,
		},
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// Ping handles GET /ping - lightweight keep-alive
func (h *HealthHandler) Ping(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("pong"))
}
