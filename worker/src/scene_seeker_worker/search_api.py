from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import chromadb
from chromadb.config import Settings
from typing import Optional
import os
import psycopg
import redis
import json
import uuid
from minio import Minio

# Prefer using local subtitle-based helper functions when available
try:
    from .search_helper import search_subtitles, get_timestamps, get_best_timestamp
    _HAS_SEARCH_HELPER = True
except Exception:
    _HAS_SEARCH_HELPER = False


def get_chroma_client(persist_directory: str = "/data/chroma"):
    """Return a chromadb client; try new default constructor first, then
    fall back to the older Settings-based constructor for compatibility.
    """
    # Prefer constructing a client that persists to the configured directory so
    # collections created previously (by re-indexing) are visible. Some chromadb
    # versions accept a Settings object, others use a no-arg Client(); try the
    # Settings path first, then fall back to the default constructor.
    try:
        return chromadb.Client(Settings(chroma_db_impl="duckdb+parquet", persist_directory=persist_directory))
    except Exception:
        try:
            return chromadb.Client()
        except Exception:
            # Last-resort: try Settings again without raising
            return chromadb.Client(Settings(chroma_db_impl="duckdb+parquet", persist_directory=persist_directory))

app = FastAPI()

# Configure chroma client (persist directory inside the worker container)
client = get_chroma_client(persist_directory="/data/chroma")

# Postgres connection info (used as a fallback search when Chroma collection
# is not available). Values are read from the environment to match the
# docker-compose service configuration.
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_USER = os.getenv("PG_USER", "scene_user")
PG_PASS = os.getenv("PG_PASS", "scene_pass")
PG_DB = os.getenv("PG_DB", "scene_seeker")

# MinIO configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET_RESULTS = os.getenv("S3_BUCKET_RESULTS", "scene-results")


def pg_search(query: str, top_k: int = 5):
    """Fallback text search against the transcripts table using Postgres full-text search.

    Returns a list of result dicts with id, text, meta (job_id, start, end) and rank.
    """
    dsn = f"host={PG_HOST} port={PG_PORT} user={PG_USER} password={PG_PASS} dbname={PG_DB}"
    try:
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                # Try full-text search first
                cur.execute(
                    """
                    SELECT id, video_id, start_time, end_time, text,
                           ts_rank(text_tsv, plainto_tsquery('english', %s)) AS rank
                    FROM transcripts
                    WHERE text_tsv @@ plainto_tsquery('english', %s)
                    ORDER BY rank DESC
                    LIMIT %s
                    """,
                    (query, query, top_k),
                )
                rows = cur.fetchall()
                results = []
                for r in rows:
                    results.append({
                        'id': r[0],
                        'text': r[4],
                        'meta': {'job_id': r[1], 'start': float(r[2]) if r[2] is not None else None, 'end': float(r[3]) if r[3] is not None else None},
                        'rank': float(r[5]) if r[5] is not None else None,
                    })
                if results:
                    return results

                # If no full-text matches, fall back to ILIKE substring match
                cur.execute(
                    """
                    SELECT id, video_id, start_time, end_time, text
                    FROM transcripts
                    WHERE text ILIKE %s
                    ORDER BY start_time
                    LIMIT %s
                    """,
                    (f"%{query}%", top_k),
                )
                rows = cur.fetchall()
                for r in rows:
                    results.append({
                        'id': r[0],
                        'text': r[4],
                        'meta': {'job_id': r[1], 'start': float(r[2]) if r[2] is not None else None, 'end': float(r[3]) if r[3] is not None else None},
                        'rank': None,
                    })
                return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"postgres search failed: {e}")


def semantic_fallback_search(query: str, top_k: int = 5, job_id: Optional[str] = None):
    """Compute embeddings for transcripts on-the-fly and return top_k nearest matches.

    This is a fallback when Chroma collection is not available; it's less efficient
    but provides semantic results without requiring Chroma persistence.
    """
    dsn = f"host={PG_HOST} port={PG_PORT} user={PG_USER} password={PG_PASS} dbname={PG_DB}"
    try:
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                if job_id:
                    cur.execute("SELECT id, video_id, start_time, end_time, text FROM transcripts WHERE video_id=%s ORDER BY start_time", (job_id,))
                else:
                    cur.execute("SELECT id, video_id, start_time, end_time, text FROM transcripts ORDER BY start_time")
                rows = cur.fetchall()
                texts = [r[4] for r in rows]
                metas = [{'job_id': r[1], 'start': float(r[2]) if r[2] is not None else None, 'end': float(r[3]) if r[3] is not None else None} for r in rows]
                ids = [r[0] for r in rows]

                if not texts:
                    return []

                # embed all texts and the query
                try:
                    from sentence_transformers import SentenceTransformer
                    import numpy as np
                except Exception as e:
                    raise HTTPException(status_code=500, detail=f"embedding model unavailable: {e}")

                model = SentenceTransformer('all-MiniLM-L6-v2')
                emb_texts = model.encode(texts, convert_to_numpy=True)
                emb_query = model.encode([query], convert_to_numpy=True)[0]

                # cosine similarity
                scores = (emb_texts @ emb_query) / (np.linalg.norm(emb_texts, axis=1) * (np.linalg.norm(emb_query) + 1e-12))
                import heapq
                top_idx = heapq.nlargest(top_k, range(len(scores)), key=lambda i: float(scores[i]))
                results = []
                for i in top_idx:
                    results.append({
                        'id': ids[i],
                        'text': texts[i],
                        'meta': metas[i],
                        'score': float(scores[i]),
                    })
                return results
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"semantic fallback failed: {e}")

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
    # If available, prefer the local subtitle-based search helper which
    # provides timestamped subtitle hits (search_helper.py).
    if _HAS_SEARCH_HELPER and req.job_id:
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
        except FileNotFoundError as e:
            # Index/metadata files not found — fall through to chroma/postgres implementations
            pass
        except Exception as e:
            # If the helper fails, log and fall back to the previous behavior
            raise HTTPException(status_code=500, detail=f"search_helper failed: {e}")

    # Try to use Chroma semantic collection if available. If not, fall back to Postgres full-text search.
    collection = None
    try:
        collection = client.get_collection(name="scene_segments")
    except Exception:
        collection = None

    if collection is None:
        # fallback to Postgres search
        results = pg_search(req.query, top_k=req.top_k)
        return {"results": results}

    # perform query on Chroma; you can filter by metadata.job_id if provided
    # Use query_texts where available for compatibility
    try:
        res = collection.query(query_texts=[req.query], n_results=req.top_k)
    except Exception:
        # try query by embeddings if text query not supported
        try:
            # build embedding via sentence-transformers on demand
            from sentence_transformers import SentenceTransformer
            model = SentenceTransformer('all-MiniLM-L6-v2')
            q_emb = model.encode([req.query], convert_to_numpy=True)[0].tolist()
            res = collection.query(query_embeddings=[q_emb], n_results=req.top_k)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"chroma query failed: {e}")

    # collect hits and optionally filter by job_id
    hits = []
    # Unwrap batch lists if necessary
    ids_batch = res.get('ids')
    docs_batch = res.get('documents')
    metas_batch = res.get('metadatas')
    dists_batch = res.get('distances')
    if isinstance(ids_batch, list) and len(ids_batch) and isinstance(ids_batch[0], list):
        ids_batch = ids_batch[0]
        docs_batch = docs_batch[0] if docs_batch else None
        metas_batch = metas_batch[0] if metas_batch else None
        dists_batch = dists_batch[0] if dists_batch else None

    for i, _id in enumerate(ids_batch):
        meta = metas_batch[i] if metas_batch and i < len(metas_batch) else {}
        if req.job_id and meta.get('job_id') != req.job_id:
            continue
        hits.append({
            'id': _id,
            'text': docs_batch[i] if docs_batch and i < len(docs_batch) else None,
            'meta': meta,
            'distance': dists_batch[i] if dists_batch and i < len(dists_batch) else None,
        })

    return {'results': hits}


@app.post("/clip")
def create_clip(req: ClipRequest):
    """Find timestamps for `req.query`, create a scene job and enqueue it for processing.

    Returns the queued `job_id` and an expected result object name.
    """
    # Determine start/end using the local helper if available
    start = None
    end = None
    try:
        if _HAS_SEARCH_HELPER and req.job_id:
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
        else:
            # Fall back to Postgres search for timestamps
            results = pg_search(req.query, top_k=req.top_k)
            if results:
                first = results[0]
                meta = first.get('meta', {})
                start = meta.get('start')
                end = meta.get('end')
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
