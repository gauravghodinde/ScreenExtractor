"""
Indexing function to generate index files from SRT files.
"""
import pickle
import os
from typing import Optional
from .srt_parser import SRTParser
from .vector_search import VectorSearchEngine


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
    
    # Parse SRT file
    parser = SRTParser(srt_filepath)
    entries = parser.parse()
    print(f"Found {len(entries)} subtitle entries")
    
    # Extract texts
    texts = [entry.text for entry in entries]
    
    # Create vector embeddings
    print("Creating vector embeddings...")
    search_engine = VectorSearchEngine()
    search_engine.index(texts)

    # Save index locally first
    search_engine.save_index(index_file)
    print(f"Index saved to {index_file}")

    # Save metadata
    metadata = {
        'srt_file': srt_filepath,
        'entries': entries
    }
    with open(metadata_file, 'wb') as f:
        pickle.dump(metadata, f)
    print(f"Metadata saved to {metadata_file}")

    # If a MinIO client and bucket provided, upload the files
    if minio_client and minio_bucket:
        # determine object names
        idx_obj = index_object_name or os.path.basename(index_file)
        meta_obj = metadata_object_name or os.path.basename(metadata_file)

        print(f"Uploading index {index_file} -> {minio_bucket}/{idx_obj}")
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
