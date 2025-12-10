# scenextractor

This repository contains two services for the Scene Extractor project:

- `go/` - Orchestrator (Gin, Postgres, MinIO, Redis)
- `worker/` - Python GPU worker (faster-whisper, ffmpeg-python)
- `infra/` - Docker Compose and environment examples

Quick start (development):

1. Copy environment file

```bash
cp infra/.env.example infra/.env
# Orchestrator API

This document describes the HTTP routes exposed by the Orchestrator service implemented in `go/cmd/orchestrator/main.go`. It lists each endpoint, expected request format, responses, important environment variables, Redis queue names, and usage examples.

**Note:** This project expects Redis, Postgres, MinIO (S3-compatible storage), and the Worker service to be available (see `infra/docker-compose.yml`).

**Environment variables**
- `PORT` : port for the orchestrator (default `8080`).
- `REDIS_ADDR` : Redis address (default `redis:6379`).
- `PG_HOST` : Postgres host (default `postgres`).
- `PG_PORT` : Postgres port (default `5432`).
- `PG_USER` : Postgres user (default `scene_user`).
- `PG_PASS` : Postgres password (default `scene_pass`).
- `PG_DB`   : Postgres database (default `scene_seeker`).
- `MINIO_ENDPOINT` : MinIO endpoint (default `minio:9000`). Can include scheme `http://host:9000`.
- `MINIO_ACCESS_KEY` : MinIO access key.
- `MINIO_SECRET_KEY` : MinIO secret key.
- `BUCKET_UPLOADS` : bucket for uploads (default `scene-uploads`).
- `WORKER_SEARCH_ADDR` : URL of worker's search API (default `http://worker:50052`).

Redis queue names used by the orchestrator
- `scene_jobs` : main job queue for scene extraction jobs.
- `queue:transcription` : queue for uploaded videos that require transcription.

Postgres schema expectations
- The orchestrator writes to a `jobs` table. Expected columns (based on usage):
	- `id` (string/UUID)
	- `status` (string), e.g. `queued`, `pending`, `done`.
	- `details` (JSON) containing metadata such as `video_path`, `start`, `end`, `result` (S3 path), etc.
	- `created_at` (timestamp) is used when listing jobs.

Routes

- `GET /healthz`
	- Simple health check. Returns `200` with body `{ "status": "ok" }`.

- `POST /search`
	- Proxies the JSON body to the worker search API (`WORKER_SEARCH_ADDR + /search`).
	- Request: any JSON accepted by the worker search API (commonly `{ "query": "...", "top_k": N }`).
	- Response: returned verbatim from the worker (status code and body).

	Example:
	```bash
	curl -X POST http://localhost:8080/search \
		-H "Content-Type: application/json" \
		-d '{"query":"hello world","top_k":5}'
	```

- `POST /jobs`
	- Create a plain job entry and enqueue it into `scene_jobs`.
	- Required JSON field: `video_path` (string)
	- Optional fields: `start` (float), `end` (float), `query` (string)
	- On success: returns `201 Created` and JSON `{ "id": "<job-id>" }`.

	Example:
	```bash
	curl -X POST http://localhost:8080/jobs \
		-H "Content-Type: application/json" \
		-d '{"video_path":"s3://scene-uploads/myvideo.mp4","start":10,"end":40,"query":"car chase"}'
	```

	Notes:
	- If JSON parsing fails, the handler attempts to parse form-encoded data and then a simple `key:value` fallback.
	- The job gets inserted into Postgres (`jobs` table) and the message is pushed to Redis `scene_jobs` (LPUSH).

- `POST /upload`
	- Stream-upload a video to MinIO and create an associated job for transcription.
	- Required header: `Content-Length` (the handler streams directly to MinIO without buffering when Content-Length is present).
	- Optional header: `X-Filename` to preserve filename extension (or `?filename=` query param).
	- Returns `201 Created` and JSON `{ "id": "<job-id>", "s3_path": "s3://bucket/object" }`.

	Example (uploading a local file):
	```bash
	FILE=./sample.mp4
	SIZE=$(stat -c%s "$FILE")
	curl -X POST http://localhost:8080/upload \
		-H "Content-Length: $SIZE" \
		-H "X-Filename: sample.mp4" \
		--data-binary "@$FILE"
	```

	Behavior:
	- The file is uploaded to `BUCKET_UPLOADS` in MinIO with a generated UUID object name (keeps extension if provided).
	- A `jobs` row is inserted with `status` set to `pending` and the job is pushed to `queue:transcription` in Redis.

- `POST /query_clip`
	- Use the Worker vector search (Chroma-backed) to find a matching subtitle segment, then create a clip-extraction job centered on that segment.
	- Request JSON:
		- `query` (string, required)
		- `top_k` (int, optional, default `5`)
		- `clip_duration` (float seconds, optional, default `20`)
	- On success: returns `202 Accepted` with `{ "job_id": "<new-id>", "clip_start": <start>, "clip_end": <end>, "source_job": "<matched-job-id>" }`.

	Example:
	```bash
	curl -X POST http://localhost:8080/query_clip \
		-H "Content-Type: application/json" \
		-d '{"query":"I love this song","top_k":3,"clip_duration":15}'
	```

	Behavior:
	- The orchestrator calls the worker `/search` endpoint with `{"query": body.Query, "top_k": body.TopK}`.
	- It expects hits with metadata keys `job_id`, `start`, `end`.
	- If a hit is found, the orchestrator looks up the `video_path` of the matched job, computes a clip window around the segment midpoint, inserts a new `jobs` row (status `queued`), and pushes a job message to `scene_jobs`.

- `POST /query_clip_fallback`
	- A lightweight fallback that searches SRT subtitle files stored in MinIO (`scene-results` bucket) for a substring match and creates a clip job from the first matching segment.
	- Request JSON:
		- `query` (string, required)
		- `clip_duration` (float seconds, optional, default `20`)
		- `limit_srts` (int, optional, default `50`) — how many SRT files to scan
	- On success: returns `202 Accepted` similar to `/query_clip`.

	Example:
	```bash
	curl -X POST http://localhost:8080/query_clip_fallback \
		-H "Content-Type: application/json" \
		-d '{"query":"hello there","clip_duration":20}'
	```

	Behavior:
	- Lists objects in `scene-results` bucket, filters `.srt` files, and scans them for a case-insensitive substring.
	- Parses SRT time lines (HH:MM:SS,mmm) to compute segment start/end.
	- Creates and enqueues a clip-extraction job similar to `/query_clip`.

- `GET /jobs`
	- List jobs with pagination and optional status filter.
	- Query parameters:
		- `limit` (int, default `20`, max `100`)
		- `offset` (int, default `0`)
		- `status` (optional string to filter by job status)
	- Returns `200 OK` with JSON `{ "jobs": [ { "id":..., "status":..., "result":..., "created_at":... }, ... ] }`.

	Example:
	```bash
	curl "http://localhost:8080/jobs?limit=10&offset=0&status=done"
	```

- `GET /jobs/:id/result`
	- Return the job's `details->>'result'` S3 path (expected `s3://bucket/key`) streamed through the orchestrator to the client as an attachment.
	- If `result` is missing or job not found, returns `404`.
	- Streams object content with `Content-Type`, `Content-Length`, and `Content-Disposition: attachment; filename="<key>"`.

	Example:
	```bash
	curl -L -o output.mp4 http://localhost:8080/jobs/<job-id>/result
	```

Errors and status codes (high-level)
- `200 OK`: Successful GET/list operations.
- `201 Created`: Resource created (`/jobs`, `/upload`).
- `202 Accepted`: Job queued for processing (`/query_clip`, `/query_clip_fallback`).
- `400 Bad Request`: Invalid/malformed input.
- `404 Not Found`: Resource not found (no search hits or missing job/result).
- `502 Bad Gateway`: Failed to contact worker search API or other upstream service.
- `500 Internal Server Error`: Unexpected server/database/storage error.

Payload examples (job message that is pushed to Redis `scene_jobs`)
```json
{
	"id": "<job-uuid>",
	"video_path": "s3://scene-uploads/<object>",
	"start": 10.0,
	"end": 30.0,
	"query": "car chase"
}
```

Worker interaction
- The orchestrator expects the Worker search API to be reachable at `WORKER_SEARCH_ADDR` and to expose a `/search` endpoint that returns JSON with a structure like:
```json
{
	"results": [
		{ "id": "<seg-id>", "text": "...", "meta": { "job_id": "<job-id>", "start": 12.0, "end": 14.0 }, "distance": 0.123 }
	]
}
```

Quick local run (using `infra/docker-compose.yml`)
- Ensure `.env` or your environment contains Postgres, Redis, MinIO credentials.
- Start services:
```bash
cd infra
docker-compose up -d
```
- Build/run the orchestrator service (in this repo the orchestrator service is prepared under `go/` — the compose file may already include it).

Notes and troubleshooting
- If `/upload` fails with MinIO errors, check `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, and that the bucket `BUCKET_UPLOADS` exists.
- If `/search` calls fail, verify `WORKER_SEARCH_ADDR` and that the Worker container is up.
- For debugging, the orchestrator logs the raw `/jobs` request body and the Redis queue head (up to 5 entries) after enqueuing.

If you want, I can also:
- Add example Postgres `jobs` table migration (SQL) if missing.
- Add a short `examples/` folder with sample `curl` scripts for automated testing.

---
File: `go/cmd/orchestrator/main.go` (routes documented above)
