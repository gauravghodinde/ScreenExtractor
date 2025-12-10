"""
Vector search system for subtitle searching using semantic embeddings.
"""
import numpy as np
from sentence_transformers import SentenceTransformer
from typing import List, Tuple
import pickle
import os


class VectorSearchEngine:
    """Semantic search engine using vector embeddings."""
    
    def __init__(self, model_name: str = 'all-MiniLM-L6-v2'):
        """
        Initialize the search engine with a sentence transformer model.
        
        Args:
            model_name: Name of the sentence-transformers model to use
        """
        self.model = SentenceTransformer(model_name)
        self.embeddings = None
        self.texts = None
    
    def encode_texts(self, texts: List[str]) -> np.ndarray:
        """
        Convert texts to vector embeddings.
        
        Args:
            texts: List of text strings to encode
            
        Returns:
            Numpy array of embeddings
        """
        return self.model.encode(texts, convert_to_numpy=True)
    
    def index(self, texts: List[str]):
        """
        Index texts by creating embeddings.
        
        Args:
            texts: List of text strings to index
        """
        self.texts = texts
        self.embeddings = self.encode_texts(texts)
    
    def search(self, query: str, top_k: int = 5) -> List[Tuple[int, float]]:
        """
        Search for similar texts using semantic similarity.
        
        Args:
            query: Search query string
            top_k: Number of top results to return
            
        Returns:
            List of tuples (index, similarity_score)
        """
        if self.embeddings is None:
            raise ValueError("No texts indexed. Call index() first.")
        
        # Encode the query
        query_embedding = self.model.encode([query], convert_to_numpy=True)[0]
        
        # Compute cosine similarity
        similarities = self._cosine_similarity(query_embedding, self.embeddings)
        
        # Get top k results
        top_indices = np.argsort(similarities)[::-1][:top_k]
        results = [(int(idx), float(similarities[idx])) for idx in top_indices]
        
        return results
    
    @staticmethod
    def _cosine_similarity(query_vec: np.ndarray, embeddings: np.ndarray) -> np.ndarray:
        """
        Compute cosine similarity between query and all embeddings.
        
        Args:
            query_vec: Query vector
            embeddings: Matrix of embedding vectors
            
        Returns:
            Array of similarity scores
        """
        # Normalize vectors
        query_norm = query_vec / np.linalg.norm(query_vec)
        embeddings_norm = embeddings / np.linalg.norm(embeddings, axis=1, keepdims=True)
        
        # Compute dot product
        similarities = np.dot(embeddings_norm, query_norm)
        
        return similarities
    
    def save_index(self, filepath: str):
        """
        Save the indexed embeddings and texts to disk.
        
        Args:
            filepath: Path to save the index
        """
        data = {
            'embeddings': self.embeddings,
            'texts': self.texts
        }
        with open(filepath, 'wb') as f:
            pickle.dump(data, f)
    
    def load_index(self, filepath: str):
        """
        Load indexed embeddings and texts from disk.
        
        Args:
            filepath: Path to load the index from
        """
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Index file not found: {filepath}")
        
        with open(filepath, 'rb') as f:
            data = pickle.load(f)
        
        self.embeddings = data['embeddings']
        self.texts = data['texts']
