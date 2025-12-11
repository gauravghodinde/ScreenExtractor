from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import os
import psycopg
import redis
import json
import uuid
import logging
from minio import Minio
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("worker")
# Prefer using local subtitle-based helper functions when available
try:
    from .search_helper import search_subtitles, get_timestamps, get_best_timestamp
    _HAS_SEARCH_HELPER = True
except Exception:
    _HAS_SEARCH_HELPER = False


app = FastAPI()

# MinIO configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET_RESULTS = os.getenv("S3_BUCKET_RESULTS", "scene-results")


class SearchRequest(BaseModel):
    query: str
    top_k: Optional[int] = 5
    job_id: Optional[str] = None


class ClipRequest(BaseModel):
    query: str
    video_path: str
    top_k: Optional[int] = 1
    job_id: Optional[str] = None
    padding: Optional[float] = 0.0


def _connect_redis(addr: str = None) -> redis.Redis:
    addr = addr or os.getenv("REDIS_ADDR", "redis:6379")
    host, port = addr.split(":")
    return redis.Redis(host=host, port=int(port), db=0, decode_responses=True)


def _connect_minio() -> Minio:
    """Create and return MinIO client."""
    host, port = MINIO_ENDPOINT.split(":")
    endpoint = f"{host}:{port}"
    return Minio(endpoint, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)


def _parse_srt_time_to_seconds(time_str: str) -> float:
    """Parse SRT timestamp (HH:MM:SS,mmm) to seconds."""
    try:
        # Format: 00:00:05,120 or 00:00:05.120
        time_str = time_str.replace(',', '.')
        parts = time_str.split(':')
        hours = float(parts[0])
        minutes = float(parts[1])
        seconds = float(parts[2])
        return hours * 3600 + minutes * 60 + seconds
    except Exception:
        return 0.0


def _insert_job_row(job_id: str, status: str = "queued", details: dict = None):
    """Insert an initial job row into Postgres (if available)."""
    dsn = f"host={os.getenv('PG_HOST','postgres')} port={os.getenv('PG_PORT','5432')} user={os.getenv('PG_USER','scene_user')} password={os.getenv('PG_PASS','scene_pass')} dbname={os.getenv('PG_DB','scene_seeker')}"
    try:
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO jobs (id, status, details) VALUES (%s, %s, %s) ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, details = EXCLUDED.details", (job_id, status, json.dumps(details or {})))
                conn.commit()
    except Exception:
        # If Postgres isn't available, fail silently; worker will still update statuses later.
        pass

@app.post("/search")
def search(req: SearchRequest):
    """Search for subtitles using the search helper.
    
    Requires job_id to locate the subtitle index for the specific job.
    """
    if not req.job_id:
        raise HTTPException(status_code=400, detail="job_id is required")
    
    try:
        # Download index files from MinIO for the specific job_id
        minio_client = _connect_minio()
        subs = search_subtitles(
            req.query, 
            top_k=req.top_k,
            minio_client=minio_client,
            minio_bucket=BUCKET_RESULTS,
            job_id=req.job_id
        )
        results = []
        for s in subs:
            # search_subtitles returns dicts with keys like 'start','end','text','score'
            meta = {'job_id': req.job_id}
            logger.info(f"Search result meta: {meta}")
            # include any index if present
            if 'index' in s:
                meta['index'] = s.get('index')
            results.append({
                'id': s.get('index') or None,
                'text': s.get('text'),
                'meta': {'start': s.get('start'), 'end': s.get('end'), **meta},
                'distance': None,
                'score': s.get('score'),
            })
        return {'results': results}
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Index files not found for job")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"search failed: {e}")


@app.post("/clip")
def create_clip(req: ClipRequest):
    """Find timestamps for `req.query`, create a scene job and enqueue it for processing.

    Returns the queued `job_id` and an expected result object name.
    """
    if not req.job_id:
        raise HTTPException(status_code=400, detail="job_id is required")
    
    # Determine start/end using the search helper
    start = None
    end = None
    try:
        # Use MinIO to download index files for the specific job
        minio_client = _connect_minio()
        timestamps = get_timestamps(
            req.query,
            top_k=req.top_k,
            minio_client=minio_client,
            minio_bucket=BUCKET_RESULTS,
            job_id=req.job_id
        )
        if timestamps:
            s, e = timestamps[0]
            start = _parse_srt_time_to_seconds(s) if s else None
            end = _parse_srt_time_to_seconds(e) if e else None
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"timestamp lookup failed: {e}")

    if start is None or end is None:
        raise HTTPException(status_code=404, detail="no timestamp match found for query")

    # apply optional padding
    try:
        pad = float(req.padding or 0.0)
    except Exception:
        pad = 0.0
    start = max(0.0, float(start) - pad)
    end = float(end) + pad

    # create job and enqueue
    job_id = str(uuid.uuid4())
    job = {"id": job_id, "video_path": req.video_path, "start": start, "end": end}

    # insert initial job row (best-effort)
    _insert_job_row(job_id, status="queued", details={"video_path": req.video_path, "start": start, "end": end})

    try:
        r = _connect_redis()
        # use LPUSH so the worker's RPOP consumes in FIFO order with other producers
        r.lpush("scene_jobs", json.dumps(job))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"failed to enqueue job: {e}")

    result_name = f"{job_id}.mp4"
    return {"job_id": job_id, "result": f"s3://{os.getenv('S3_BUCKET_RESULTS','scene-results')}/{result_name}", "start": start, "end": end}
