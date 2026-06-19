"""
Hybrid RAG System combining BM25 and Vector search.
"""
import os
import json
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
from dataclasses import dataclass, asdict
import numpy as np

from .document_processor import DocumentProcessor, DocumentChunk
from .bm25_index import BM25Index, BM25Document
from .vector_index import VectorIndex, EmbeddingGenerator


@dataclass
class SearchResult:
    """Result from hybrid search."""
    chunk_id: str
    document_id: str
    document_path: str
    text: str
    score: float
    bm25_score: float
    vector_score: float
    metadata: Dict[str, Any]


class HybridRAG:
    """Hybrid RAG system combining BM25 and vector search."""
    
    def __init__(
        self,
        data_dir: str = "/home/rhadamanthys/Data-Handler/data/tmp",
        index_dir: str = "/home/rhadamanthys/Data-Handler/rag/index",
        embedding_model: str = "all-MiniLM-L6-v2",
        chunk_size: int = 512,
        chunk_overlap: int = 50,
        bm25_weight: float = 0.5,
        vector_weight: float = 0.5,
        use_gpu: bool = False
    ):
        self.data_dir = Path(data_dir)
        self.index_dir = Path(index_dir)
        self.index_dir.mkdir(parents=True, exist_ok=True)
        
        self.bm25_weight = bm25_weight
        self.vector_weight = vector_weight
        
        # Initialize components
        self.document_processor = DocumentProcessor(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap
        )
        self.bm25_index = BM25Index()
        self.embedding_generator = EmbeddingGenerator(
            model_name=embedding_model,
            use_gpu=use_gpu
        )
        self.vector_index = None  # Initialized after embedding model loads
        
        # Storage
        self.chunks: Dict[str, DocumentChunk] = {}
    
    def build_index(self, force_rebuild: bool = False) -> None:
        """Build the hybrid index from PDF documents."""
        # Check if index already exists
        bm25_path = self.index_dir / "bm25"
        vector_path = self.index_dir / "vector"
        chunks_path = self.index_dir / "chunks.json"
        
        if not force_rebuild and bm25_path.exists() and vector_path.exists() and chunks_path.exists():
            print("Loading existing index...")
            self.load_index()
            return
        
        print("Building new index...")
        
        # Process all PDFs
        print("Processing PDFs...")
        all_chunks = self.document_processor.process_directory(self.data_dir)
        
        if not all_chunks:
            print("No documents found to index.")
            return
        
        print(f"Total chunks: {len(all_chunks)}")
        
        # Store chunks
        for chunk in all_chunks:
            self.chunks[chunk.chunk_id] = chunk
        
        # Build BM25 index
        print("Building BM25 index...")
        bm25_docs = [
            BM25Document(
                doc_id=chunk.chunk_id,
                text=chunk.text,
                metadata={
                    'document_id': chunk.document_id,
                    'document_path': chunk.document_path,
                    'chunk_index': chunk.chunk_index,
                    **chunk.metadata
                }
            )
            for chunk in all_chunks
        ]
        self.bm25_index.add_documents(bm25_docs)
        
        # Build vector index
        print("Generating embeddings...")
        texts = [chunk.text for chunk in all_chunks]
        doc_ids = [chunk.chunk_id for chunk in all_chunks]
        metadata_list = [
            {
                'document_id': chunk.document_id,
                'document_path': chunk.document_path,
                **chunk.metadata
            }
            for chunk in all_chunks
        ]
        
        # Generate embeddings in batches
        batch_size = 100
        all_embeddings = []
        
        for i in range(0, len(texts), batch_size):
            batch_texts = texts[i:i+batch_size]
            print(f"  Embedding batch {i//batch_size + 1}/{(len(texts)-1)//batch_size + 1}")
            batch_embeddings = self.embedding_generator.encode(batch_texts)
            all_embeddings.append(batch_embeddings)
        
        embeddings = np.vstack(all_embeddings)
        
        # Initialize vector index
        self.vector_index = VectorIndex(
            embedding_dim=self.embedding_generator.embedding_dim
        )
        
        print("Building vector index...")
        self.vector_index.add_documents(
            doc_ids=doc_ids,
            texts=texts,
            embeddings=embeddings,
            metadata_list=metadata_list
        )
        
        # Save index
        print("Saving index...")
        self.save_index()
        
        print("Index built successfully!")
    
    def search(
        self,
        query: str,
        top_k: int = 10,
        bm25_weight: Optional[float] = None,
        vector_weight: Optional[float] = None,
        filter_metadata: Optional[Dict[str, Any]] = None,
        return_chunks: bool = True
    ) -> List[SearchResult]:
        """
        Search using hybrid BM25 + vector search.
        
        Args:
            query: Search query
            top_k: Number of results to return
            bm25_weight: Weight for BM25 scores (default: self.bm25_weight)
            vector_weight: Weight for vector scores (default: self.vector_weight)
            filter_metadata: Metadata filters
            return_chunks: Whether to return full chunks
        
        Returns:
            List of SearchResult objects
        """
        bm25_weight = bm25_weight if bm25_weight is not None else self.bm25_weight
        vector_weight = vector_weight if vector_weight is not None else self.vector_weight
        
        # BM25 search
        bm25_results = self.bm25_index.search(
            query=query,
            top_k=top_k * 3,  # Get more results for hybrid
            filter_metadata=filter_metadata
        )
        
        # Vector search
        query_embedding = self.embedding_generator.encode_single(query)
        vector_results = self.vector_index.search(
            query_embedding=query_embedding,
            top_k=top_k * 3,
            filter_metadata=filter_metadata
        )
        
        # Combine scores
        all_doc_ids = set()
        bm25_scores = {}
        vector_scores = {}
        
        for doc_id, score, _ in bm25_results:
            all_doc_ids.add(doc_id)
            bm25_scores[doc_id] = score
        
        for doc_id, score, _ in vector_results:
            all_doc_ids.add(doc_id)
            vector_scores[doc_id] = score
        
        # Normalize scores
        if bm25_scores:
            max_bm25 = max(bm25_scores.values())
            if max_bm25 > 0:
                bm25_scores = {k: v/max_bm25 for k, v in bm25_scores.items()}
        
        if vector_scores:
            max_vector = max(vector_scores.values())
            if max_vector > 0:
                vector_scores = {k: v/max_vector for k, v in vector_scores.items()}
        
        # Combine scores
        combined_scores = {}
        for doc_id in all_doc_ids:
            bm25 = bm25_scores.get(doc_id, 0)
            vector = vector_scores.get(doc_id, 0)
            combined_scores[doc_id] = bm25_weight * bm25 + vector_weight * vector
        
        # Sort and create results
        sorted_doc_ids = sorted(combined_scores.keys(), key=lambda x: combined_scores[x], reverse=True)
        
        results = []
        for doc_id in sorted_doc_ids[:top_k]:
            chunk = self.chunks.get(doc_id)
            if not chunk:
                continue
            
            result = SearchResult(
                chunk_id=doc_id,
                document_id=chunk.document_id,
                document_path=chunk.document_path,
                text=chunk.text if return_chunks else chunk.text[:200] + "...",
                score=combined_scores[doc_id],
                bm25_score=bm25_scores.get(doc_id, 0),
                vector_score=vector_scores.get(doc_id, 0),
                metadata=chunk.metadata
            )
            results.append(result)
        
        return results
    
    def get_context(
        self,
        query: str,
        top_k: int = 5,
        max_context_length: int = 4000
    ) -> str:
        """
        Get context for a query by retrieving and concatenating relevant chunks.
        
        Args:
            query: Search query
            top_k: Number of chunks to retrieve
            max_context_length: Maximum context length in characters
        
        Returns:
            Concatenated context string
        """
        results = self.search(query=query, top_k=top_k, return_chunks=True)
        
        context_parts = []
        current_length = 0
        
        for result in results:
            text = result.text
            if current_length + len(text) > max_context_length:
                break
            
            context_parts.append(f"[Document: {result.document_id}]\n{text}\n")
            current_length += len(text)
        
        return "\n".join(context_parts)
    
    def save_index(self) -> None:
        """Save the index to disk."""
        # Save BM25 index
        self.bm25_index.save(self.index_dir / "bm25")
        
        # Save vector index
        if self.vector_index:
            self.vector_index.save(self.index_dir / "vector")
        
        # Save chunks
        chunks_data = {
            chunk_id: {
                'chunk_id': chunk.chunk_id,
                'document_id': chunk.document_id,
                'document_path': chunk.document_path,
                'text': chunk.text,
                'metadata': chunk.metadata,
                'chunk_index': chunk.chunk_index,
                'total_chunks': chunk.total_chunks
            }
            for chunk_id, chunk in self.chunks.items()
        }
        with open(self.index_dir / "chunks.json", 'w') as f:
            json.dump(chunks_data, f)
        
        # Save config
        config = {
            'bm25_weight': self.bm25_weight,
            'vector_weight': self.vector_weight,
            'embedding_model': self.embedding_generator.model_name,
            'embedding_dim': self.embedding_generator.embedding_dim
        }
        with open(self.index_dir / "config.json", 'w') as f:
            json.dump(config, f)
    
    def load_index(self) -> None:
        """Load the index from disk."""
        # Load config
        with open(self.index_dir / "config.json", 'r') as f:
            config = json.load(f)
            self.bm25_weight = config['bm25_weight']
            self.vector_weight = config['vector_weight']
        
        # Load BM25 index
        self.bm25_index.load(self.index_dir / "bm25")
        
        # Load chunks
        with open(self.index_dir / "chunks.json", 'r') as f:
            chunks_data = json.load(f)
        
        for chunk_id, data in chunks_data.items():
            self.chunks[chunk_id] = DocumentChunk(
                chunk_id=data['chunk_id'],
                document_id=data['document_id'],
                document_path=data['document_path'],
                text=data['text'],
                metadata=data['metadata'],
                chunk_index=data['chunk_index'],
                total_chunks=data['total_chunks']
            )
        
        # Load vector index
        self.vector_index = VectorIndex(
            embedding_dim=config['embedding_dim']
        )
        self.vector_index.load(self.index_dir / "vector")


if __name__ == "__main__":
    # Test
    rag = HybridRAG()
    rag.build_index()
    
    # Search
    query = "What is deep learning for drug discovery?"
    results = rag.search(query, top_k=5)
    
    print(f"\nSearch results for: '{query}'")
    for i, result in enumerate(results):
        print(f"\n{i+1}. {result.chunk_id}")
        print(f"   Score: {result.score:.4f} (BM25: {result.bm25_score:.4f}, Vector: {result.vector_score:.4f})")
        print(f"   Document: {result.document_id}")
        print(f"   Text preview: {result.text[:150]}...")