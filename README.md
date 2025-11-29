# scenextractor

This repository contains two services for the Scene Extractor project:

- `go/` - Orchestrator (Gin, Postgres, MinIO, Redis)
- `worker/` - Python GPU worker (faster-whisper, ffmpeg-python)
- `infra/` - Docker Compose and environment examples

Quick start (development):

1. Copy environment file

```bash
cp infra/.env.example infra/.env
```

2. From `infra/`, bring up the stack:

```bash
docker compose up --build
```

Notes:
- The worker image expects NVIDIA Container Toolkit to be installed on the host to provide GPU access (for `gpus: all`).
- For GPU-enabled PyTorch install, the Dockerfile may need an explicit PyTorch wheel URL matching your CUDA version.
