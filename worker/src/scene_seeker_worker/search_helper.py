"""
Helper functions for searching subtitles and returning timestamps.
"""
import os
import pickle
import tempfile
from typing import List, Tuple, Dict, Optional
from .vector_search import VectorSearchEngine
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("worker")

def search_subtitles(query: str, index_file: str = "subtitle_index.pkl", 
                     metadata_file: str = "subtitle_metadata.pkl",
                     top_k: int = 5,
                     minio_client: Optional[object] = None,
                     minio_bucket: Optional[str] = None,
                     job_id: Optional[str] = None) -> List[Dict[str, any]]:
    """
    Search subtitles and return timestamps.
    
    Args:
        query: Search query string
        index_file: Path to the saved index file (or object name if using MinIO)
        metadata_file: Path to the saved metadata file (or object name if using MinIO)
        top_k: Number of results to return
        minio_client: Optional MinIO client to download files from remote storage
        minio_bucket: Optional bucket name for MinIO
        job_id: Optional job ID to construct MinIO object names (e.g., {job_id}.pkl)
    
    Returns:
        List of dictionaries with 'start', 'end', 'text', and 'score' keys
        
    Example:
        results = search_subtitles("everybody relax")
        for result in results:
            print(f"{result['start']} --> {result['end']}")
            print(f"Text: {result['text']}")
            print(f"Score: {result['score']}")
    """
    # If MinIO client provided, download files to temp directory
    cleanup_files = False
    if minio_client and minio_bucket:
        cleanup_files = True
        temp_dir = tempfile.mkdtemp()
        
        # Use job_id if provided to construct object names
        if job_id:
            index_obj = f"{job_id}.pkl"
            meta_obj = f"{job_id}_metadata.pkl"
        else:
            index_obj = index_file
            meta_obj = metadata_file
        
        # Download files from MinIO
        local_index = os.path.join(temp_dir, "index.pkl")
        local_meta = os.path.join(temp_dir, "metadata.pkl")
        
        try:
            minio_client.fget_object(minio_bucket, index_obj, local_index)
            minio_client.fget_object(minio_bucket, meta_obj, local_meta)
            index_file = local_index
            metadata_file = local_meta
        except Exception as e:
            # Clean up temp directory on error
            import shutil
            shutil.rmtree(temp_dir, ignore_errors=True)
            raise FileNotFoundError(f"Failed to download index files from MinIO: {e}")
    
    # Check if files exist
    if not os.path.exists(index_file):
        raise FileNotFoundError(f"Index file not found: {index_file}. Run indexing first.")
    
    if not os.path.exists(metadata_file):
        raise FileNotFoundError(f"Metadata file not found: {metadata_file}. Run indexing first.")
    
    # Load the search engine
    search_engine = VectorSearchEngine()
    search_engine.load_index(index_file)
    
    # Load metadata (subtitle entries)
    with open(metadata_file, 'rb') as f:
        metadata = pickle.load(f)
    entries = metadata['entries']
    
    # Perform search
    results = search_engine.search(query, top_k)
    # Format results
    output = []
    for idx, score in results:
        entry = entries[idx]
        logger.info(f'about to index srt for job {job_id}: {entry.jobid}')
        output.append({
            'start': entry.start,
            'end': entry.end,
            'text': entry.text,
            'score': score,
            'index': entry.index
        })
    
    # Clean up temp files if we downloaded from MinIO
    if cleanup_files:
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    return output


def get_timestamps(query: str, index_file: str = "subtitle_index.pkl",
                   metadata_file: str = "subtitle_metadata.pkl",
                   top_k: int = 1,
                   minio_client: Optional[object] = None,
                   minio_bucket: Optional[str] = None,
                   job_id: Optional[str] = None) -> List[Tuple[str, str]]:
    """
    Search subtitles and return only timestamps (start, end).
    
    Args:
        query: Search query string
        index_file: Path to the saved index file
        metadata_file: Path to the saved metadata file
        top_k: Number of results to return
        minio_client: Optional MinIO client
        minio_bucket: Optional bucket name
        job_id: Optional job ID for MinIO object names
    
    Returns:
        List of tuples (start_time, end_time)
        
    Example:
        timestamps = get_timestamps("everybody relax")
        for start, end in timestamps:
            print(f"{start} --> {end}")
    """
    results = search_subtitles(query, index_file, metadata_file, top_k,
                               minio_client=minio_client, minio_bucket=minio_bucket, job_id=job_id)
    return [(r['start'], r['end']) for r in results]


def get_best_timestamp(query: str, index_file: str = "subtitle_index.pkl",
                       metadata_file: str = "subtitle_metadata.pkl",
                       minio_client: Optional[object] = None,
                       minio_bucket: Optional[str] = None,
                       job_id: Optional[str] = None) -> Tuple[str, str]:
    """
    Search subtitles and return the best matching timestamp.
    
    Args:
        query: Search query string
        index_file: Path to the saved index file
        metadata_file: Path to the saved metadata file
        minio_client: Optional MinIO client
        minio_bucket: Optional bucket name
        job_id: Optional job ID for MinIO object names
    
    Returns:
        Tuple of (start_time, end_time)
        
    Example:
        start, end = get_best_timestamp("everybody relax")
        print(f"{start} --> {end}")
    """
    timestamps = get_timestamps(query, index_file, metadata_file, top_k=1,
                                minio_client=minio_client, minio_bucket=minio_bucket, job_id=job_id)
    return timestamps[0] if timestamps else (None, None)


if __name__ == "__main__":
    # Example usage
    query = "everybody relax. This is not even a date."
    
    print("=== Simple Timestamp Search ===\n")
    
    # Get best match
    start, end = get_best_timestamp(query)
    print(f"Best match timestamp: {start} --> {end}")
    
    print("\n" + "="*50 + "\n")
    
    # Get top 3 matches
    print("Top 3 matches:")
    timestamps = get_timestamps(query, top_k=3)
    for i, (start, end) in enumerate(timestamps, 1):
        print(f"{i}. {start} --> {end}")
    
    print("\n" + "="*50 + "\n")
    
    # Get full details
    print("Full search results:")
    results = search_subtitles(query, top_k=3)
    for i, result in enumerate(results, 1):
        print(f"\n{i}. Timestamp: {result['start']} --> {result['end']}")
        print(f"   Score: {result['score']:.4f}")
        print(f"   Text: {result['text']}")
