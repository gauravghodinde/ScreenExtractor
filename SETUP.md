# SceneExtractor — Setup Guide

Complete instructions for running SceneExtractor locally and via Docker.

---

## Prerequisites

| Tool               | Version   | Purpose                          |
|--------------------|-----------|----------------------------------|
| Docker & Compose   | Latest    | Infrastructure services          |
| Go                 | 1.23+     | Orchestrator API                 |
| Python             | 3.10+     | Worker & Search API              |
| Node.js            | 20+       | Next.js frontend                 |
| FFmpeg             | Latest    | Video/subtitle processing        |

---

## Option 1 — Full Docker (Recommended)

Spin up **everything** (infra + app) in containers with a single command.

```bash
cd infra
docker compose up -d --build
```

This starts:
- **PostgreSQL 16** — job metadata store
- **Redis** — job queue
- **MinIO** — S3-compatible object storage (auto-creates `scene-uploads` and `scene-results` buckets)
- **Go Orchestrator** — REST API for uploads and queries
- **Python Worker** — media processing, embedding, and indexing
- **Search API** — FastAPI semantic search endpoint
- **pgAdmin** — database management UI

### Access Points

| Service            | URL                         | Credentials                  |
|--------------------|-----------------------------|------------------------------|
| Frontend           | http://localhost:3000        | —                            |
| Orchestrator API   | http://localhost:8080        | —                            |
| Search API         | http://localhost:8000        | —                            |
| MinIO Console      | http://localhost:9001        | `minioadmin` / `minioadmin`  |
| pgAdmin            | http://localhost:5050        | `admin@example.com` / `admin`|

> [!NOTE]
> The frontend is **not** included in the `infra/docker-compose.yml`. Run it separately (see [Frontend](#3-frontend) below) or add your own service entry.

---

## Option 2 — Hybrid Local Development

Run infrastructure in Docker and application services natively for faster iteration.

### 1. Infrastructure

```bash
cd local
docker compose up -d
```

Services started:

| Service    | Host Port | Internal Port | Notes                        |
|------------|-----------|---------------|------------------------------|
| PostgreSQL | `5433`    | `5432`        | DB: `scene_seeker`           |
| Redis      | `6380`    | `6379`        | In-memory, no persistence    |
| MinIO      | `9000`    | `9000`        | API endpoint                 |
| MinIO UI   | `9001`    | `9001`        | Web console                  |
| pgAdmin    | `5050`    | `80`          | DB admin UI                  |

Wait for health checks to pass:

```bash
docker compose ps   # all services should be "healthy"
```

---

### 2. Go Orchestrator

```bash
cd go

# Install dependencies
go mod download

# Create .env (if not present — copy from local/.env.orchestrator)
cp ../local/.env.orchestrator .env

# Run
go run ./cmd/orchestrator
```

The orchestrator will listen on **http://localhost:8080**.

<details>
<summary><strong>Environment variables</strong> (<code>.env</code>)</summary>

```env
PG_HOST=localhost
PG_PORT=5433
PG_USER=scene_user
PG_PASS=scene_pass
PG_DB=scene_seeker
REDIS_ADDR=localhost:6380
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
BUCKET_UPLOADS=scene-uploads
PORT=8080
WORKER_SEARCH_ADDR=http://localhost:8000
```
</details>

---

### 3. Python Worker

```bash
cd worker

# Create virtual environment
python -m venv venv
source venv/bin/activate        # Linux/macOS
# venv\Scripts\Activate.ps1     # Windows PowerShell

# Install dependencies
pip install -r requirements.txt

# Create .env (if not present)
cp ../local/.env.worker .env

# Run worker
cd src
python -m scene_seeker_worker.worker
```

<details>
<summary><strong>Environment variables</strong> (<code>.env</code>)</summary>

```env
REDIS_ADDR=localhost:6380
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
PG_HOST=localhost
PG_PORT=5433
PG_USER=scene_user
PG_PASS=scene_pass
PG_DB=scene_seeker
S3_BUCKET_UPLOADS=scene-uploads
S3_BUCKET_RESULTS=scene-results
WORKER_GRPC_PORT=50051
```
</details>

> [!IMPORTANT]
> FFmpeg must be installed on your system for the worker to process videos. Install via your package manager:
> ```bash
> # Ubuntu/Debian
> sudo apt install ffmpeg
>
> # macOS
> brew install ffmpeg
> ```

---

### 4. Search API

The search API is a separate FastAPI service built from the same worker codebase:

```bash
cd worker
source venv/bin/activate

cd src
uvicorn scene_seeker_worker.search_api:app --host 0.0.0.0 --port 8000
```

Accessible at **http://localhost:8000**.

---

### 5. Frontend

```bash
cd frontend

# Install dependencies
npm install

# Create .env.local (if not present)
echo 'NEXT_PUBLIC_API_URL=http://localhost:8080' > .env.local

# Run dev server
npm run dev
```

Open **http://localhost:3000**.

---

## Database

The PostgreSQL schema is auto-applied on first boot via the migration file at `infra/migrations/001_create_jobs.sql`.

### Connecting via psql

```bash
docker exec -it local-postgres-1 psql -U scene_user -d scene_seeker
```

### Connecting via pgAdmin

1. Open http://localhost:5050
2. Login: `admin@example.com` / `admin`
3. Add server → Host: `postgres` (or `localhost`), Port: `5432` (or `5433` from host), User: `scene_user`, Password: `scene_pass`

---

## MinIO (Object Storage)

### Web Console

Open http://localhost:9001 — login with `minioadmin` / `minioadmin`.

### CLI (`mc`)

```bash
mc alias set local http://localhost:9000 minioadmin minioadmin
mc ls local/scene-uploads
mc ls local/scene-results
```

---

## API Quick Reference

```bash
# Upload a video
curl -X POST http://localhost:8080/upload \
  -H "Content-Length: $(stat -c%s video.mkv)" \
  --data-binary @video.mkv

# Search for a clip
curl -X POST http://localhost:8080/query_clip \
  -H "Content-Type: application/json" \
  -d '{"query": "hello world", "clip_duration": 15}'
```

---

## Troubleshooting

### Port conflicts

Default host ports are `5433`, `6380`, `9000`, `9001`, `5050`. If any collide, edit the `ports` mapping in the relevant `docker-compose.yml` and update the corresponding `.env` files.

### Worker not processing jobs

```bash
# Verify Redis is reachable
docker exec -it local-redis-1 redis-cli ping     # → PONG

# Check queue length
docker exec -it local-redis-1 redis-cli LLEN scene_jobs
```

### Full reset

```bash
cd local
docker compose down -v          # wipes all data volumes
docker compose up -d            # fresh start

# Also clean build artifacts
rm -rf ../worker/venv ../frontend/node_modules ../frontend/.next
```

---

## Project Structure

```
scenextracter/
├── frontend/          # Next.js 16 + TailwindCSS 4
├── go/                # Go orchestrator (Gin framework)
│   ├── cmd/orchestrator/
│   └── internal/
├── worker/            # Python worker + Search API
│   └── src/scene_seeker_worker/
├── infra/             # Full Docker Compose (all services)
│   ├── docker-compose.yml
│   └── migrations/
├── local/             # Hybrid dev setup (infra-only Docker)
│   ├── docker-compose.yml
│   ├── .env.orchestrator
│   ├── .env.worker
│   └── *.ps1 scripts
└── README.md
```
