import time
import json
import logging
import threading
from typing import Any, Dict

from .config import get_redis_client, get_pg_connection
from .pipeline import process_transcription_pipeline, process_clip_pipeline

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("worker")


def update_job_status_pg(
    conn, job_id: str, status: str, details: Dict[str, Any] = None
) -> None:
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


def create_pg_conn_with_retry():
    """Attempt to connect to Postgres, returning None if unavailable."""
    try:
        return get_pg_connection()
    except Exception:
        logger.warning("postgres not available at startup; will try when updating jobs")
        return None


def process_transcription_job(job: Dict[str, Any], pg_conn) -> None:
    job_id = job.get("id")
    video_path = job.get("video_path")
    logger.info("processing transcription job %s for %s", job_id, video_path)

    if not video_path or not video_path.startswith("s3://"):
        logger.error("bad video_path %s", video_path)
        update_job_status_pg(pg_conn, job_id, "failed", {"error": "bad video_path"})
        return

    _, rest = video_path.split("s3://", 1)
    if "/" not in rest:
        logger.error("bad s3 path %s", video_path)
        update_job_status_pg(pg_conn, job_id, "failed", {"error": "bad s3 path"})
        return

    # Callback wrapper to allow the pipeline to update PG without importing psycopg
    def status_callback(status: str, details: Dict[str, Any] = None):
        if details is None:
            details = {}
        update_job_status_pg(pg_conn, job_id, status, details)

    try:
        result_path = process_transcription_pipeline(
            job_id, video_path, status_callback
        )
        if result_path == "no_subtitles":
            update_job_status_pg(
                pg_conn, job_id, "completed", {"result": "no_subtitles"}
            )
        else:
            update_job_status_pg(pg_conn, job_id, "completed", {"result": result_path})
        logger.info("transcription job %s completed", job_id)
    except Exception as e:
        logger.exception("transcription job %s failed: %s", job_id, e)
        update_job_status_pg(pg_conn, job_id, "failed", {"error": str(e)})


def process_job(job: Dict[str, Any], pg_conn) -> None:
    job_id = job.get("id")
    video_path = job.get("video_path")
    start = float(job.get("start", 0.0))
    end = float(job.get("end", start + 5.0))

    if not video_path or not video_path.startswith("s3://"):
        logger.error("unsupported video_path format: %s", video_path)
        update_job_status_pg(pg_conn, job_id, "failed", {"error": "bad path"})
        return

    def status_callback(status: str, details: Dict[str, Any] = None):
        if details is None:
            details = {}
        update_job_status_pg(pg_conn, job_id, status, details)

    try:
        result_path = process_clip_pipeline(
            job_id, video_path, start, end, status_callback
        )
        update_job_status_pg(pg_conn, job_id, "completed", {"result": result_path})
        logger.info("clip job %s completed", job_id)
    except Exception as e:
        logger.exception("job %s failed: %s", job_id, e)
        update_job_status_pg(pg_conn, job_id, "failed", {"error": str(e)})


def transcription_listener(r, pg_conn):
    logger.info(
        "transcription listener started, waiting for jobs on queue:transcription"
    )
    while True:
        try:
            item = r.brpop("queue:transcription", timeout=5)
            if not item:
                time.sleep(1)
                continue
            _, raw = item
            try:
                tjob = json.loads(raw)
            except Exception:
                logger.exception("failed to parse transcription job json: %s", raw)
                continue

            nonlocal_pg = pg_conn
            if nonlocal_pg is None:
                try:
                    nonlocal_pg = get_pg_connection()
                except Exception:
                    logger.exception(
                        "failed to connect to postgres for transcription; requeueing"
                    )
                    r.lpush("queue:transcription", raw)
                    time.sleep(5)
                    continue

            try:
                process_transcription_job(tjob, nonlocal_pg)
            except Exception:
                logger.exception("transcription job failed: %s", tjob.get("id"))
        except Exception as e:
            logger.exception("transcription listener error: %s", e)
            time.sleep(2)


def main():
    r = get_redis_client()
    pg_conn = create_pg_conn_with_retry()

    logger.info("worker started, polling for jobs...")

    t = threading.Thread(target=transcription_listener, args=(r, pg_conn), daemon=True)
    t.start()

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

            if pg_conn is None:
                try:
                    pg_conn = get_pg_connection()
                except Exception:
                    logger.exception(
                        "failed to connect to postgres; marking job as retry"
                    )
                    r.lpush("scene_jobs", raw)
                    time.sleep(5)
                    continue

            process_job(job, pg_conn)

        except Exception as e:
            logger.exception("unexpected worker loop error: %s", e)
            time.sleep(2)


if __name__ == "__main__":
    main()
