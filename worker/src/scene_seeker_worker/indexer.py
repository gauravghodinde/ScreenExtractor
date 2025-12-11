"""
Indexing function to generate index files from SRT files.
"""
import pickle
import os
from typing import Optional
from .srt_parser import SRTParser
from .vector_search import VectorSearchEngine
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("worker")
try:
    # Minio is an optional dependency for remote storage uploads
    from minio import Minio  # type: ignore
except Exception:
    Minio = None


def index_srt_file(
    srt_filepath: str,
    index_file: str = "subtitle_index.pkl",
    metadata_file: str = "subtitle_metadata.pkl",
    minio_client: Optional[object] = None,
    minio_bucket: Optional[str] = None,
    index_object_name: Optional[str] = None,
    metadata_object_name: Optional[str] = None,
    job_id: Optional[str] = None
):
    """
    Index an SRT file and generate pickle files for searching.
    
    Args:
        srt_filepath: Path to the SRT file to index
        index_file: Path where the index pickle file will be saved
        metadata_file: Path where the metadata pickle file will be saved
    
    Returns:
        None
        
    Example:
        index_srt_file("my_video.srt")
        # Creates: subtitle_index.pkl and subtitle_metadata.pkl
    """
    print(f"Indexing SRT file: {srt_filepath}")

    # Try to pull existing index/metadata from MinIO so we can append instead of recreate
    if minio_client and minio_bucket:
        idx_obj = index_object_name or os.path.basename(index_file)
        meta_obj = metadata_object_name or os.path.basename(metadata_file)
        try:
            minio_client.fget_object(minio_bucket, idx_obj, index_file)
            minio_client.fget_object(minio_bucket, meta_obj, metadata_file)
            print("Downloaded existing index and metadata from MinIO for merge")
        except Exception as e:
            print(f"Could not download existing index/metadata from MinIO (continuing with local files): {e}")

    existing_entries = []
    search_engine = VectorSearchEngine()
    loaded_existing_index = False

    # Load existing index/metadata if present to avoid recreating new files
    if os.path.exists(index_file) and os.path.exists(metadata_file):
        try:
            search_engine.load_index(index_file)
            with open(metadata_file, 'rb') as f:
                existing_metadata = pickle.load(f)
            existing_entries = existing_metadata.get('entries', [])
            loaded_existing_index = True
            print(f"Loaded existing index with {len(existing_entries)} entries")
        except Exception as e:
            print(f"Warning: could not load existing index/metadata, will rebuild fresh: {e}")

    # Parse SRT file
    parser = SRTParser(srt_filepath)
    new_entries = parser.parse(jobid=job_id)
    print(f"Found {len(new_entries)} subtitle entries in new SRT")

    # Extract texts for the new entries only
    new_texts = [entry.text for entry in new_entries]

    # Append to existing index if present; otherwise create a new one
    if loaded_existing_index:
        print("Appending new embeddings to existing index...")
        search_engine.append(new_texts)
    else:
        print("Creating new index from scratch...")
        search_engine.index(new_texts)

    # Save index locally first
    search_engine.save_index(index_file)
    print(f"Index saved to {index_file}")

    # Combine metadata entries and track all source files
    combined_entries = existing_entries + new_entries
    srt_files = []
    if loaded_existing_index:
        # Support both legacy 'srt_file' and new list-based 'srt_files'
        if isinstance(existing_metadata, dict):
            if 'srt_files' in existing_metadata:
                srt_files.extend(existing_metadata['srt_files'])
            elif 'srt_file' in existing_metadata:
                srt_files.append(existing_metadata['srt_file'])
    srt_files.append(srt_filepath)

    metadata = {
        'srt_files': srt_files,
        'srt_file': srt_filepath,  # keep legacy key for compatibility
        'entries': combined_entries
    }
    with open(metadata_file, 'wb') as f:
        pickle.dump(metadata, f)
    print(f"Metadata saved to {metadata_file} (total entries: {len(combined_entries)})")

    # If a MinIO client and bucket provided, upload the files
    if minio_client and minio_bucket:
        # determine object names
        idx_obj = index_object_name or os.path.basename(index_file)
        meta_obj = metadata_object_name or os.path.basename(metadata_file)

        logger.info(f"Uploading index {index_file} -> {minio_bucket}/{idx_obj}")
        try:
            # Minio client's fput_object signature: bucket_name, object_name, file_path, content_type
            # some clients accept content_type named parameter; pass only what is common.
            minio_client.fput_object(minio_bucket, idx_obj, index_file, content_type='application/octet-stream')
        except Exception as e:
            print(f"Failed to upload index to MinIO: {e}")

        print(f"Uploading metadata {metadata_file} -> {minio_bucket}/{meta_obj}")
        try:
            minio_client.fput_object(minio_bucket, meta_obj, metadata_file, content_type='application/octet-stream')
        except Exception as e:
            print(f"Failed to upload metadata to MinIO: {e}")

    print("Indexing complete!")


if __name__ == "__main__":
    # Example usage
    index_srt_file("4f7e24aa-1497-4e8f-b8b6-2f978bc5fa2a.srt")
