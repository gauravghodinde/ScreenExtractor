import os
import time
import json
import logging
import tempfile
from typing import Any, Dict

import redis
from minio import Minio
from minio.error import S3Error
import psycopg
import ffmpeg

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("worker")

# Configuration (environment)
REDIS_ADDR = os.getenv("REDIS_ADDR", "redis:6379")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET_UPLOADS = os.getenv("S3_BUCKET_UPLOADS", "scene-uploads")
BUCKET_RESULTS = os.getenv("S3_BUCKET_RESULTS", "scene-results")
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_USER = os.getenv("PG_USER", "scene_user")
PG_PASS = os.getenv("PG_PASS", "scene_pass")
PG_DB = os.getenv("PG_DB", "scene_seeker")


def connect_redis(addr: str) -> redis.Redis:
    host, port = addr.split(":")
    return redis.Redis(host=host, port=int(port), db=0, decode_responses=True)


def connect_minio() -> Minio:
    # Minio client expects endpoint without scheme
    host, port = MINIO_ENDPOINT.split(":")
    endpoint = f"{host}:{port}"
    return Minio(endpoint, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)


def connect_pg():
    dsn = f"host={PG_HOST} port={PG_PORT} user={PG_USER} password={PG_PASS} dbname={PG_DB}"
    return psycopg.connect(dsn)


def download_from_minio(minio_client: Minio, bucket: str, object_name: str, dest_path: str) -> None:
    logger.info("downloading %s/%s -> %s", bucket, object_name, dest_path)
    try:
        minio_client.fget_object(bucket, object_name, dest_path)
    except S3Error as e:
        logger.error("minio download error: %s", e)
        raise


def upload_to_minio(minio_client: Minio, bucket: str, object_name: str, src_path: str, content_type: str = "video/mp4") -> None:
    logger.info("uploading %s -> %s/%s", src_path, bucket, object_name)
    try:
        minio_client.fput_object(bucket, object_name, src_path, content_type=content_type)
    except S3Error as e:
        logger.error("minio upload error: %s", e)
        raise


def extract_clip(input_path: str, start: float, end: float, out_path: str) -> None:
    logger.info("extracting clip %s [%s -> %s] -> %s", input_path, start, end, out_path)
    try:
        duration = end - start
        (
            ffmpeg
            .input(input_path, ss=start)
            .output(out_path, t=duration, c="copy")
            .overwrite_output()
            .run(quiet=True)
        )
    except ffmpeg.Error as e:
        logger.error("ffmpeg error: %s", e)
        raise


def update_job_status_pg(conn, job_id: str, status: str, details: Dict[str, Any] = None) -> None:
    # Minimal updater — assumes a jobs table exists with (id text primary key, status text, details jsonb)
    logger.info("updating job %s status=%s", job_id, status)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO jobs (id, status, details) VALUES (%s, %s, %s) ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, details = EXCLUDED.details",
                (job_id, status, json.dumps(details or {})),
            )
            conn.commit()
    except Exception as e:
        logger.exception("failed to update job in postgres: %s", e)
        conn.rollback()


def process_job(job: Dict[str, Any], r: redis.Redis, minio_client: Minio, pg_conn) -> None:
    job_id = job.get("id")
    video_path = job.get("video_path")
    start = float(job.get("start", 0.0))
    end = float(job.get("end", start + 5.0))

    # Expect video_path like: s3://bucket/key
    if not video_path.startswith("s3://"):
        logger.error("unsupported video_path format: %s", video_path)
        update_job_status_pg(pg_conn, job_id, "failed", {"error": "bad path"})
        return

    _, rest = video_path.split("s3://", 1)
    bucket, object_name = rest.split("/", 1)

    with tempfile.TemporaryDirectory() as td:
        local_in = os.path.join(td, "in.mp4")
        local_out = os.path.join(td, "out.mp4")

        try:
            update_job_status_pg(pg_conn, job_id, "downloading")
            download_from_minio(minio_client, bucket, object_name, local_in)

            update_job_status_pg(pg_conn, job_id, "processing")
            extract_clip(local_in, start, end, local_out)

            result_name = f"{job_id}.mp4"
            update_job_status_pg(pg_conn, job_id, "uploading")
            upload_to_minio(minio_client, BUCKET_RESULTS, result_name, local_out)

            update_job_status_pg(pg_conn, job_id, "done", {"result": f"s3://{BUCKET_RESULTS}/{result_name}"})
            logger.info("job %s completed", job_id)
        except Exception as e:
            logger.exception("job %s failed: %s", job_id, e)
            update_job_status_pg(pg_conn, job_id, "failed", {"error": str(e)})


def main():
    r = connect_redis(REDIS_ADDR)
    minio_client = connect_minio()
    pg_conn = None
    try:
        pg_conn = connect_pg()
    except Exception:
        logger.warning("postgres not available at startup; will try when updating jobs")

    logger.info("worker started, polling for jobs...")
    while True:
        try:
            raw = r.rpop("scene_jobs")
            if not raw:
                time.sleep(1)
                continue

            logger.info("popped job: %s", raw)
            try:
                job = json.loads(raw)
            except Exception:
                logger.exception("failed to parse job JSON: %s", raw)
                continue

            # ensure we have a db connection
            if pg_conn is None:
                try:
                    pg_conn = connect_pg()
                except Exception:
                    logger.exception("failed to connect to postgres; marking job as retry")
                    # Requeue job for later
                    r.lpush("scene_jobs", raw)
                    time.sleep(5)
                    continue

            process_job(job, r, minio_client, pg_conn)

        except Exception as e:
            logger.exception("unexpected worker loop error: %s", e)
            time.sleep(2)


if __name__ == '__main__':
    main()
