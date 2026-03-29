import os
import tempfile
import logging
import ffmpeg
import subprocess
from minio import Minio
from minio.error import S3Error

from .config import BUCKET_RESULTS, get_minio_client
from .indexer import index_srt_file

logger = logging.getLogger("worker")


def download_from_minio(
    minio_client: Minio, bucket: str, object_name: str, dest_path: str
) -> None:
    logger.info("downloading %s/%s -> %s", bucket, object_name, dest_path)
    try:
        minio_client.fget_object(bucket, object_name, dest_path)
    except S3Error as e:
        logger.error("minio download error: %s", e)
        raise


def upload_to_minio(
    minio_client: Minio,
    bucket: str,
    object_name: str,
    src_path: str,
    content_type: str = "video/mp4",
) -> None:
    logger.info("uploading %s -> %s/%s", src_path, bucket, object_name)
    try:
        minio_client.fput_object(
            bucket, object_name, src_path, content_type=content_type
        )
    except S3Error as e:
        logger.error("minio upload error: %s", e)
        raise


def extract_clip(input_path: str, start: float, end: float, out_path: str) -> None:
    logger.info("extracting clip %s [%s -> %s] -> %s", input_path, start, end, out_path)
    try:
        duration = end - start
        (
            ffmpeg.input(input_path, ss=start)
            .output(out_path, t=duration, c="copy")
            .overwrite_output()
            .run(quiet=True)
        )
    except ffmpeg.Error as e:
        logger.error("ffmpeg error: %s", e)
        raise


def extract_subtitles(input_path: str, out_srt_path: str) -> None:
    logger.info("extracting subtitles from %s -> %s", input_path, out_srt_path)
    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        input_path,
        "-map",
        "0:s:0",
        out_srt_path,
    ]
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        logger.error(
            "ffmpeg subtitle extraction failed: %s", e.stderr.decode(errors="ignore")
        )
        raise


def process_transcription_pipeline(
    job_id: str, video_path: str, update_status_func
) -> str:
    """
    Downloads video, extracts subtitles, runs indexing, and uploads results to MinIO.
    Returns the MinIO path of the uploaded SRT file on success.
    `update_status_func(status, details)` is a callback to update PG state.
    """
    minio_client = get_minio_client()
    _, rest = video_path.split("s3://", 1)
    bucket, object_name = rest.split("/", 1)

    with tempfile.TemporaryDirectory() as td:
        local_in = os.path.join(td, "in.mkv")
        try:
            update_status_func("downloading", None)
            local_srt = os.path.join(td, f"{job_id}.srt")

            # Direct SRT download if uploaded file is already SRT
            if object_name.lower().endswith(".srt"):
                download_from_minio(minio_client, bucket, object_name, local_srt)
            else:
                download_from_minio(minio_client, bucket, object_name, local_in)
                update_status_func("processing", None)
                try:
                    extract_subtitles(local_in, local_srt)
                except Exception as e:
                    logger.warning(
                        "no subtitles or failed to extract for %s: %s", job_id, e
                    )
                    return "no_subtitles"

            try:
                upload_to_minio(
                    minio_client,
                    BUCKET_RESULTS,
                    f"{job_id}.srt",
                    local_srt,
                    content_type="text/srt",
                )
            except Exception as e:
                logger.warning("failed to upload srt to results: %s", e)

            # Index the SRT
            logger.info("about to index srt for job %s at %s", job_id, local_srt)
            try:
                idx_local = os.path.join(td, "index.pkl")
                meta_local = os.path.join(td, "metadata.pkl")
                index_srt_file(
                    local_srt,
                    index_file=idx_local,
                    metadata_file=meta_local,
                    minio_client=minio_client,
                    minio_bucket=BUCKET_RESULTS,
                    index_object_name="index.pkl",
                    metadata_object_name="metadata.pkl",
                    job_id=job_id,
                )
                logger.info(
                    "indexed SRT and uploaded pickles for transcription job %s", job_id
                )
            except Exception as e:
                logger.exception(
                    "failed to index srt or upload pickles for transcription job %s: %s",
                    job_id,
                    e,
                )

            return f"srt://{bucket}/{object_name}"

        finally:
            if os.path.exists(local_in):
                try:
                    os.remove(local_in)
                except Exception:
                    pass


def process_clip_pipeline(
    job_id: str, video_path: str, start: float, end: float, update_status_func
) -> str:
    """
    Downloads video, extracts a clip, and uploads the clip to MinIO.
    Returns the MinIO path of the uploaded short clip on success.
    """
    minio_client = get_minio_client()
    _, rest = video_path.split("s3://", 1)
    bucket, object_name = rest.split("/", 1)

    with tempfile.TemporaryDirectory() as td:
        local_in = os.path.join(td, "in.mp4")
        local_out = os.path.join(td, "out.mp4")

        update_status_func("downloading", None)
        download_from_minio(minio_client, bucket, object_name, local_in)

        update_status_func("processing", None)
        extract_clip(local_in, start, end, local_out)

        result_name = f"{job_id}.mp4"
        update_status_func("uploading", None)
        upload_to_minio(minio_client, BUCKET_RESULTS, result_name, local_out)

        return f"s3://{BUCKET_RESULTS}/{result_name}"
