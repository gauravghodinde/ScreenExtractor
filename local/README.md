# Run SceneXtracter Locally

This folder contains scripts to run the project locally with Docker only for infrastructure services (PostgreSQL, Redis, MinIO).

## Prerequisites

- **Docker Desktop** - For infrastructure services
- **Go 1.23+** - For orchestrator
- **Python 3.10+** - For worker and search API
- **Node.js 20+** - For frontend

## Quick Start

### Step 1: Start Infrastructure (Required)

Open a terminal and run:

```powershell
cd local
.\1-start-infra.ps1
```

This starts PostgreSQL, Redis, MinIO, and PgAdmin in Docker containers.

### Step 2: Start Application Services

Open **3 separate terminals** and run:

**Terminal 1 - Go Orchestrator:**
```powershell
cd local
.\2-start-orchestrator.ps1
```

**Terminal 2 - Python Worker:**
```powershell
cd local
.\3-start-worker.ps1
```

**Terminal 3 - Next.js Frontend:**
```powershell
cd local
.\4-start-frontend.ps1
```

**Terminal 4 - Search API (Optional):**
```powershell
cd local
.\5-start-search-api.ps1
```

## Access Points

| Service | URL | Credentials |
|---------|-----|-------------|
| **Frontend** | http://localhost:3000 | - |
| **Orchestrator API** | http://localhost:8080 | - |
| **Search API** | http://localhost:8000 | - |
| **MinIO Console** | http://localhost:9001 | minioadmin / minioadmin |
| **PgAdmin** | http://localhost:5050 | admin@example.com / admin |

## Architecture

```
┌─────────────────────────────────────────────────┐
│ Infrastructure (Docker)                         │
│  • PostgreSQL (port 5433)                       │
│  • Redis (port 6380)                            │
│  • MinIO (port 9000)                            │
│  • PgAdmin (port 5050)                          │
└─────────────────────────────────────────────────┘
                     ▲
                     │
┌─────────────────────────────────────────────────┐
│ Application Services (Native)                   │
│  • Go Orchestrator (port 8080)                  │
│  • Python Worker (background)                   │
│  • Search API (port 8000)                       │
│  • Next.js Frontend (port 3000)                 │
└─────────────────────────────────────────────────┘
```

## Environment Variables

Environment variables are stored in:
- `.env.orchestrator` - Go orchestrator configuration
- `.env.worker` - Python worker configuration

These are automatically loaded by the startup scripts.

## Stopping Services

**Stop Infrastructure:**
```powershell
cd local
.\stop-all.ps1
```

**Stop Application Services:**
Press `Ctrl+C` in each terminal running the application services.

## Troubleshooting

### Port Already in Use

If you get port conflicts, edit `docker-compose.yml` and change the port mappings:
```yaml
ports:
  - "5433:5432"  # Change 5433 to another port
```

Then update `.env.orchestrator` and `.env.worker` with the new ports.

### Database Connection Issues

Check if PostgreSQL is running:
```powershell
docker ps
```

Reset the database:
```powershell
cd local
docker-compose down -v
docker-compose up -d
```

### Worker Not Processing Jobs

1. Check Redis connection:
```powershell
docker exec -it local-redis-1 redis-cli -p 6379 ping
```

2. Check the Redis queue:
```powershell
docker exec -it local-redis-1 redis-cli -p 6379 LLEN scene_jobs
```

### Python Dependencies

If the worker fails to start, manually install dependencies:
```powershell
cd worker
python -m venv venv
venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### Go Dependencies

If the orchestrator fails to build:
```powershell
cd go
go mod tidy
go mod download
```

## Development Tips

### Hot Reload

- **Frontend**: Next.js has built-in hot reload
- **Orchestrator**: Restart the script after code changes
- **Worker**: Restart the script after code changes

### Viewing Logs

**Infrastructure logs:**
```powershell
cd local
docker-compose logs -f
```

**Specific service logs:**
```powershell
docker-compose logs -f postgres
docker-compose logs -f redis
docker-compose logs -f minio
```

### Database Access

**Using psql:**
```powershell
docker exec -it local-postgres-1 psql -U scene_user -d scene_seeker
```

**Using PgAdmin:**
1. Go to http://localhost:5050
2. Login with admin@example.com / admin
3. Add server: host=postgres, port=5432, user=scene_user, password=scene_pass

### MinIO Access

**Using MinIO Console:**
1. Go to http://localhost:9001
2. Login with minioadmin / minioadmin
3. Browse buckets: scene-uploads, scene-results

**Using mc CLI:**
```powershell
mc alias set local http://localhost:9000 minioadmin minioadmin
mc ls local/scene-uploads
```

## Clean Start

To completely reset everything:

```powershell
cd local
docker-compose down -v
docker-compose up -d

# Clean worker environment
cd ..\worker
Remove-Item -Recurse -Force venv

# Clean frontend
cd ..\frontend
Remove-Item -Recurse -Force node_modules
Remove-Item -Recurse -Force .next
```

Then start services again from Step 1.
