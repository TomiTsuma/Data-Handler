"""
Vector Index for semantic search using embeddings.
"""
import os
import json
import numpy as np
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
from dataclasses import dataclass
import hashlib


@dataclass
class VectorDocument:
    """Document with embedding."""
    doc_id: str
    text: str
    embedding: np.ndarray
    metadata: Dict[str, Any]


class VectorIndex:
    """Vector index for semantic search using embeddings."""
    
    def __init__(
        self,
        embedding_dim: int = 384,
        metric: str = "cosine"
    ):
        self.embedding_dim = embedding_dim
        self.metric = metric
        
        # Storage
        self.documents: Dict[str, VectorDocument] = {}
        self.embeddings_matrix: Optional[np.ndarray] = None
        self.doc_ids_list: List[str] = []
    
    def _compute_similarity(
        self,
        query_embedding: np.ndarray,
        doc_embeddings: np.ndarray
    ) -> np.ndarray:
        """Compute similarity between query and document embeddings."""
        if self.metric == "cosine":
            # Normalize
            query_norm = query_embedding / (np.linalg.norm(query_embedding) + 1e-8)
            doc_norms = doc_embeddings / (np.linalg.norm(doc_embeddings, axis=1, keepdims=True) + 1e-8)
            similarities = np.dot(doc_norms, query_norm)
        elif self.metric == "euclidean":
            distances = np.linalg.norm(doc_embeddings - query_embedding, axis=1)
            similarities = 1.0 / (1.0 + distances)
        else:
            raise ValueError(f"Unknown metric: {self.metric}")
        
        return similarities
    
    def add_document(
        self,
        doc_id: str,
        text: str,
        embedding: np.ndarray,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Add a document with its embedding."""
        metadata = metadata or {}
        
        # Validate embedding
        if len(embedding) != self.embedding_dim:
            raise ValueError(f"Embedding dimension mismatch: {len(embedding)} != {self.embedding_dim}")
        
        # Store document
        self.documents[doc_id] = VectorDocument(
            doc_id=doc_id,
            text=text,
            embedding=embedding,
            metadata=metadata
        )
        
        # Update matrix
        self._rebuild_matrix()
    
    def add_documents(
        self,
        doc_ids: List[str],
        texts: List[str],
        embeddings: np.ndarray,
        metadata_list: Optional[List[Dict[str, Any]]] = None
    ) -> None:
        """Add multiple documents with embeddings."""
        metadata_list = metadata_list or [{}] * len(doc_ids)
        
        if embeddings.shape[0] != len(doc_ids):
            raise ValueError("Number of embeddings must match number of documents")
        
        if embeddings.shape[1] != self.embedding_dim:
            raise ValueError(f"Embedding dimension mismatch: {embeddings.shape[1]} != {self.embedding_dim}")
        
        for i, (doc_id, text, embedding, metadata) in enumerate(
            zip(doc_ids, texts, embeddings, metadata_list)
        ):
            self.documents[doc_id] = VectorDocument(
                doc_id=doc_id,
                text=text,
                embedding=embedding,
                metadata=metadata
            )
        
        self._rebuild_matrix()
    
    def _rebuild_matrix(self) -> None:
        """Rebuild the embeddings matrix."""
        if not self.documents:
            self.embeddings_matrix = None
            self.doc_ids_list = []
            return
        
        self.doc_ids_list = list(self.documents.keys())
        self.embeddings_matrix = np.vstack([
            self.documents[doc_id].embedding for doc_id in self.doc_ids_list
        ])
    
    def search(
        self,
        query_embedding: np.ndarray,
        top_k: int = 10,
        filter_metadata: Optional[Dict[str, Any]] = None
    ) -> List[Tuple[str, float, Dict[str, Any]]]:
        """Search for similar documents."""
        if self.embeddings_matrix is None or len(self.doc_ids_list) == 0:
            return []
        
        # Validate query embedding
        if len(query_embedding) != self.embedding_dim:
            raise ValueError(f"Query embedding dimension mismatch: {len(query_embedding)} != {self.embedding_dim}")
        
        # Compute similarities
        similarities = self._compute_similarity(query_embedding, self.embeddings_matrix)
        
        # Sort by similarity
        sorted_indices = np.argsort(similarities)[::-1]
        
        results = []
        for idx in sorted_indices:
            doc_id = self.doc_ids_list[idx]
            score = float(similarities[idx])
            
            if score <= 0:
                continue
            
            doc = self.documents[doc_id]
            
            # Apply metadata filter
            if filter_metadata:
                match = all(
                    doc.metadata.get(k) == v
                    for k, v in filter_metadata.items()
                )
                if not match:
                    continue
            
            results.append((doc_id, score, doc.metadata))
            
            if len(results) >= top_k:
                break
        
        return results
    
    def get_document(self, doc_id: str) -> Optional[VectorDocument]:
        """Get a document by ID."""
        return self.documents.get(doc_id)
    
    def get_embedding(self, doc_id: str) -> Optional[np.ndarray]:
        """Get embedding for a document."""
        doc = self.documents.get(doc_id)
        return doc.embedding if doc else None
    
    def save(self, path: Path) -> None:
        """Save the index to disk."""
        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)
        
        # Save embeddings matrix
        if self.embeddings_matrix is not None:
            np.save(path / 'embeddings.npy', self.embeddings)
        
        # Save document IDs
        with open(path / 'doc_ids.json', 'w') as f:
            json.dump(self.doc_ids_list, f)
        
        # Save metadata and texts
        docs_data = {}
        for doc_id, doc in self.documents.items():
            docs_data[doc_id] = {
                'doc_id': doc.doc_id,
                'text': doc.text,
                'metadata': doc.metadata
            }
        with open(path / 'documents.json', 'w') as f:
            json.dump(docs_data, f)
        
        # Save config
        config = {
            'embedding_dim': self.embedding_dim,
            'metric': self.metric
        }
        with open(path / 'config.json', 'w') as f:
            json.dump(config, f)
    
    def load(self, path: Path) -> None:
        """Load the index from disk."""
        path = Path(path)
        
        # Load config
        with open(path / 'config.json', 'r') as f:
            config = json.load(f)
            self.embedding_dim = config['embedding_dim']
            self.metric = config['metric']
        
        # Load document IDs
        with open(path / 'doc_ids.json', 'r') as f:
            self.doc_ids_list = json.load(f)
        
        # Load embeddings
        embeddings_path = path / 'embeddings.npy'
        if embeddings_path.exists():
            self.embeddings_matrix = np.load(embeddings_path)
        
        # Load documents
        with open(path / 'documents.json', 'r') as f:
            docs_data = json.load(f)
        
        for i, doc_id in enumerate(self.doc_ids_list):
            data = docs_data[doc_id]
            self.documents[doc_id] = VectorDocument(
                doc_id=doc_id,
                text=data['text'],
                embedding=self.embeddings_matrix[i] if self.embeddings_matrix is not None else np.zeros(self.embedding_dim),
                metadata=data['metadata']
            )


class EmbeddingGenerator:
    """Generate embeddings using sentence-transformers or fallback."""
    
    def __init__(self, model_name: str = "all-MiniLM-L6-v2", use_gpu: bool = False):
        self.model_name = model_name
        self.use_gpu = use_gpu
        self.model = None
        self.embedding_dim = None
    
    def _load_model(self):
        """Lazy load the model."""
        if self.model is not None:
            return
        
        try:
            from sentence_transformers import SentenceTransformer
            self.model = SentenceTransformer(self.model_name)
            if self.use_gpu:
                self.model = self.model.to('cuda')
            self.embedding_dim = self.model.get_sentence_embedding_dimension()
        except ImportError:
            print("Warning: sentence-transformers not installed. Using fallback embedding.")
            self.model = None
            self.embedding_dim = 384
    
    def encode(self, texts: List[str]) -> np.ndarray:
        """Generate embeddings for texts."""
        self._load_model()
        
        if self.model is not None:
            embeddings = self.model.encode(texts, convert_to_numpy=True, show_progress_bar=False)
            return embeddings
        else:
            # Fallback: random embeddings (not useful for actual search)
            print("Warning: Using random embeddings. Install sentence-transformers for real embeddings.")
            return np.random.randn(len(texts), self.embedding_dim).astype(np.float32)
    
    def encode_single(self, text: str) -> np.ndarray:
        """Generate embedding for a single text."""
        return self.encode([text])[0]


if __name__ == "__main__":
    # Test
    generator = EmbeddingGenerator()
    
    # Test encoding
    texts = [
        "Machine learning is a subset of artificial intelligence.",
        "Deep learning uses neural networks.",
        "Drug discovery involves molecular docking."
    ]
    
    print("Generating embeddings...")
    embeddings = generator.encode(texts)
    print(f"Embeddings shape: {embeddings.shape}")
    
    # Create index
    index = VectorIndex(embedding_dim=generator.embedding_dim)
    index.add_documents(
        doc_ids=["doc1", "doc2", "doc3"],
        texts=texts,
        embeddings=embeddings
    )
    
    # Search
    query = "What is machine learning?"
    query_embedding = generator.encode_single(query)
    results = index.search(query_embedding, top_k=2)
    
    print(f"\nSearch results for: '{query}'")
    for doc_id, score, metadata in results:
        print(f"  {doc_id}: {score:.4f}")