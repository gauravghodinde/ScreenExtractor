import os
import redis
import psycopg
from minio import Minio
from dotenv import load_dotenv

env_path = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../local/.env.worker")
)
load_dotenv(env_path)

# Server / General
REDIS_ADDR = os.getenv("REDIS_ADDR", "redis:6379")

# MinIO Config
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET_UPLOADS = os.getenv("S3_BUCKET_UPLOADS", "scene-uploads")
BUCKET_RESULTS = os.getenv("S3_BUCKET_RESULTS", "scene-results")

# Postgres Config
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_USER = os.getenv("PG_USER", "scene_user")
PG_PASS = os.getenv("PG_PASS", "scene_pass")
PG_DB = os.getenv("PG_DB", "scene_seeker")


def get_redis_client() -> redis.Redis:
    """Create and return a Redis client."""
    host, port = REDIS_ADDR.split(":")
    return redis.Redis(host=host, port=int(port), db=0, decode_responses=True)


def get_minio_client() -> Minio:
    """Create and return a MinIO client."""
    host, port = MINIO_ENDPOINT.split(":")
    endpoint = f"{host}:{port}"
    return Minio(
        endpoint, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False
    )


def get_pg_connection():
    """Create and return a psycopg PostgreSQL connection."""
    dsn = f"host={PG_HOST} port={PG_PORT} user={PG_USER} password={PG_PASS} dbname={PG_DB}"
    return psycopg.connect(dsn)
