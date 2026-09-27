# Fedratlas Sync Engine

A federated map data synchronization engine that enables OGC API-compliant map servers to securely share and synchronize geospatial data across a distributed network.

## 📋 Table of Contents

- [Overview](#-overview)
- [Features](#-features)
- [Architecture](#%EF%B8%8F-architecture)
- [Prerequisites](#-prerequisites)
- [Quick Start](#-quick-start)
- [Configuration](#%EF%B8%8F-configuration)
- [API Reference](#-api-reference)
- [Database Schema](#%EF%B8%8F-database-schema)
- [Integration Guide](#-integration-guide)
- [Deployment](#-deployment)
- [Troubleshooting](#-troubleshooting)
- [Contributing](#-contributing)

---

## 🌟 Overview

**Fedratlas** is a protocol and reference implementation for federated map data sharing. It enables autonomous map servers to:

- **Discover** other servers in the federation
- **Exchange** geospatial data securely
- **Synchronize** changes in real-time
- **Maintain** data ownership and sovereignty

The engine works with **any map backend** (Python, Node.js, Java, Go, etc.) via gRPC or HTTP and supports **OGC API - Features** compliance.

---

## ✨ Features

### Core Federation

| Feature | Description |
|---------|-------------|
| **Peer Discovery** | Automatic server discovery via `/fedmap/v1/manifest` |
| **Secure Communication** | Ed25519 cryptographic signatures |
| **Real-time Sync** | Outbox/Inbox pattern for reliable synchronization |
| **Conflict Resolution** | Version-based conflict handling |
| **Trust Scoring** | Automatic trust score management for peers |

### API Support

| Type | Description |
|------|-------------|
| **HTTP REST** | Full OGC API - Features support |
| **gRPC** | High-performance service-to-service communication |
| **Health Checks** | `/health`, `/ready`, `/ping` endpoints |

### Storage

| Feature | Description |
|---------|-------------|
| **PostgreSQL + PostGIS** | Spatial data storage |
| **Repository Pattern** | Pluggable storage backends |
| **Transaction Support** | ACID-compliant operations |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  HTTP/REST Clients (Browsers, curl)    gRPC Clients (Services)          │
│       │                                       │                         │
│       ▼                                       ▼                         │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │                    FEDRATLAS SYNC ENGINE                          │  │
│  │                                                                   │  │
│  │  ┌───────────────┐    ┌───────────────┐    ┌───────────────────┐  │  │
│  │  │  HTTP Server  │    │  gRPC Server  │    │  Sync Engine      │  │  │
│  │  │  (Port 8080)  │    │  (Port 50051) │    │  • Outbox         │  │  │
│  │  └───────────────┘    └───────────────┘    │  • Inbox          │  │  │
│  │                                            │  • Peers          │  │  │
│  │  ┌───────────────┐    ┌───────────────┐    │  • Signatures     │  │  │
│  │  │  Crypto       │    │  Storage      │    └───────────────────┘  │  │
│  │  │  (Ed25519)    │    │  (PostgreSQL) │                           │  │
│  │  └───────────────┘    └───────────────┘                           │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                                    │                                    │
│                                    ▼                                    │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │  PostgreSQL + PostGIS                                             │  │
│  │  • geo_features  • peer_registry  • federation_outbox             │  │
│  └───────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 📦 Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| **Go** | 1.21+ | Required |
| **PostgreSQL** | 12+ | With PostGIS extension |
| **Docker** | 20+ | Optional |
| **Make** | 4.0+ | Optional |

---

## 🚀 Quick Start

### 1. Run with Docker (Easiest)

```bash
# Start Fedratlas with PostgreSQL
docker run -d \
  --name fedratlas \
  -e FEDRATLAS_SERVER_ID=my-server \
  -e DATABASE_URL=postgres://user:pass@host:5432/fedratlas \
  -p 8080:8080 \
  -p 50051:50051 \
  fedratlas/sync-engine:latest
```

### 2. Run from Source

```bash
# Clone the repository
git clone https://github.com/fedratlas-org/fedratlas-sync-go
cd fedratlas-sync-go

# Install dependencies
go mod download

# Set environment variables
export FEDRATLAS_SERVER_ID=my-server
export DATABASE_URL=postgres://user:pass@localhost:5432/fedratlas

# Run the server
go run cmd/server/main.go
```

### 3. Install as a Package

```bash
# Add to your go.mod
go get github.com/fedratlas-org/fedratlas-sync-go@latest
```

---

## ⚙️ Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `FEDRATLAS_SERVER_ID` | `server-001` | Unique server identifier |
| `DATABASE_URL` | (Required) | PostgreSQL connection string |
| `HTTP_PORT` | `8080` | HTTP API port |
| `GRPC_PORT` | `50051` | gRPC server port |
| `FEDRATLAS_ENABLED` | `true` | Enable/disable federation |

### Example `.env` File

```env
FEDRATLAS_SERVER_ID=nyc-map-server
DATABASE_URL=postgres://fedratlas:password@localhost:5432/fedratlas?sslmode=disable
HTTP_PORT=8080
GRPC_PORT=50051
```

---

## 📡 API Reference

### HTTP Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/ready` | GET | Readiness check |
| `/ping` | GET | Simple ping |
| `/fedmap/v1/manifest` | GET | Server manifest (discovery) |
| `/fedmap/v1/inbox` | POST | Receive federation updates |
| `/fedmap/v1/peers` | GET | List all peers |
| `/fedmap/v1/peers` | POST | Add a new peer |
| `/fedmap/v1/peers/{id}` | GET | Get peer details |
| `/fedmap/v1/peers/{id}/block` | POST | Block a peer |
| `/fedmap/v1/peers/{id}/follow` | POST | Follow a peer |

### OGC API - Features Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/collections` | GET | List all collections |
| `/collections/{id}` | GET | Get collection details |
| `/collections/{id}/items` | GET | Get features |
| `/collections/{id}/items` | POST | Create feature |
| `/collections/{id}/items/{fid}` | GET | Get feature |
| `/collections/{id}/items/{fid}` | PUT | Update feature |
| `/collections/{id}/items/{fid}` | DELETE | Delete feature |

### gRPC Methods

| Method | Description |
|--------|-------------|
| `OnFeatureChange` | Notify engine of data change |
| `AddPeer` | Register a new peer |
| `GetPeers` | Get all connected peers |
| `GetServerID` | Get server identifier |
| `GetManifest` | Get server manifest |

---

## 🗄️ Database Schema

### `geo_features` Table

Stores synchronized geospatial features.

```sql
CREATE TABLE geo_features (
    feature_id BIGSERIAL PRIMARY KEY,
    geom GEOMETRY(Geometry, 4326),
    version INTEGER DEFAULT 1,
    trust_score NUMERIC(3,2) DEFAULT 0.5,
    feature_data JSONB,
    last_edited_timestamp TIMESTAMP DEFAULT NOW(),
    created_by VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW()
);
```

### `peer_registry` Table

Tracks all federated peers.

```sql
CREATE TABLE peer_registry (
    server_id VARCHAR(100) PRIMARY KEY,
    public_key TEXT NOT NULL,
    trust_score NUMERIC(3,2) DEFAULT 0.5,
    endpoint_url TEXT NOT NULL,
    status VARCHAR(20) DEFAULT 'PENDING',
    last_seen TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
```

### `federation_outbox` Table

Queues outgoing federation messages.

```sql
CREATE TABLE federation_outbox (
    id BIGSERIAL PRIMARY KEY,
    activity JSONB NOT NULL,
    status VARCHAR(20) DEFAULT 'PENDING',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
```

---

## 🔗 Integration Guide

### Python Example

```python
import grpc
import fedratlas_pb2
import fedratlas_pb2_grpc

class FedratlasClient:
    def __init__(self, host="localhost:50051"):
        self.channel = grpc.insecure_channel(host)
        self.stub = fedratlas_pb2_grpc.FedratlasServiceStub(self.channel)

    def on_feature_change(self, feature_id, activity_type, feature_data, version):
        request = fedratlas_pb2.FeatureChangeRequest(
            feature_id=feature_id,
            activity_type=activity_type,
            feature_data=feature_data,
            version=version
        )
        return self.stub.OnFeatureChange(request)
```

### Node.js Example

```javascript
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');

class FedratlasClient {
    constructor(host = 'localhost:50051') {
        const packageDef = protoLoader.loadSync('./fedratlas.proto');
        const proto = grpc.loadPackageDefinition(packageDef);
        this.client = new proto.FedratlasService(host, grpc.credentials.createInsecure());
    }

    async onFeatureChange(featureId, activityType, featureData, version) {
        return new Promise((resolve, reject) => {
            this.client.OnFeatureChange({
                feature_id: featureId,
                activity_type: activityType,
                feature_data: featureData,
                version: version
            }, (err, resp) => err ? reject(err) : resolve(resp));
        });
    }
}
```

### Database Trigger (For Syncing to Your Tables)

```sql
CREATE OR REPLACE FUNCTION sync_geo_to_places()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO places (id, name, category, geom, created_at)
    VALUES (
        NEW.feature_id,
        COALESCE(NEW.feature_data->>'name', 'Unnamed'),
        COALESCE(NEW.feature_data->>'category', 'other'),
        NEW.geom,
        NEW.created_at
    )
    ON CONFLICT (id) DO UPDATE SET
        name = COALESCE(EXCLUDED.name, places.name),
        category = COALESCE(EXCLUDED.category, places.category),
        geom = EXCLUDED.geom;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_sync_geo_to_places
AFTER INSERT OR UPDATE ON geo_features
FOR EACH ROW
EXECUTE FUNCTION sync_geo_to_places();
```

---

## 🚢 Deployment

### Docker Compose

```yaml
version: '3.8'

services:
  postgres:
    image: postgis/postgis:16-3.4
    environment:
      POSTGRES_USER: fedratlas
      POSTGRES_PASSWORD: fedratlas
      POSTGRES_DB: fedratlas
    volumes:
      - postgres_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"

  fedratlas:
    image: fedratlas/sync-engine:latest
    environment:
      FEDRATLAS_SERVER_ID: map-server
      DATABASE_URL: postgres://fedratlas:fedratlas@postgres:5432/fedratlas
      HTTP_PORT: 8080
      GRPC_PORT: 50051
    ports:
      - "8080:8080"
      - "50051:50051"
    depends_on:
      - postgres

volumes:
  postgres_data:
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fedratlas
spec:
  replicas: 1
  selector:
    matchLabels:
      app: fedratlas
  template:
    metadata:
      labels:
        app: fedratlas
    spec:
      containers:
      - name: fedratlas
        image: fedratlas/sync-engine:latest
        ports:
        - containerPort: 8080
        - containerPort: 50051
        env:
        - name: FEDRATLAS_SERVER_ID
          value: "map-server"
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: fedratlas-secrets
              key: database-url
```

---

## 🔧 Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| **Connection refused** | Ensure Fedratlas service is running on the correct port |
| **Signature verification fails** | Verify public keys match in peer registry |
| **Foreign key constraint** | Create missing users in trigger function |
| **Data not syncing** | Check `federation_outbox` table for pending activities |
| **Health check fails** | Verify database connection and sync engine status |

### Debug Commands

```bash
# Check health
curl http://localhost:8080/health

# Check peers
curl http://localhost:8080/fedmap/v1/peers

# Check manifest
curl http://localhost:8080/fedmap/v1/manifest

# Check outbox status
psql -c "SELECT status, COUNT(*) FROM federation_outbox GROUP BY status;"

# Check peer registry
psql -c "SELECT server_id, status, trust_score FROM peer_registry;"
```

### Logs

```bash
# Check logs in Docker
docker logs fedratlas

# Check logs in Kubernetes
kubectl logs deployment/fedratlas

# Check logs in systemd
journalctl -u fedratlas -f
```

---

## 🤝 Contributing

We welcome contributions!
### Development

```bash
# Clone the repository
git clone https://github.com/fedratlas-org/fedratlas-sync-go
cd fedratlas-sync-go

# Install dependencies
go mod download

# Run tests
go test ./...

# Build
go build ./cmd/server

# Run with hot reload (requires air)
air
```

---

## 🌐 Resources

| Resource | Link |
|----------|------|
| **GitHub** | https://github.com/fedratlas-org/fedratlas-sync-go |
| **PostGIS** | https://postgis.net |

---

## 🙏 Acknowledgments

- OGC for the API - Features standard
- The ActivityPub and AT Protocol communities for federation inspiration
- All contributors and users of Fedratlas

---

**Built with ❤️ by the Fedratlas Community**
