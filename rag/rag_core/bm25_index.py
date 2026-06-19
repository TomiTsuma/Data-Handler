"""
BM25 Indexer for lexical search.
"""
import os
import json
import pickle
import re
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
from dataclasses import dataclass, asdict
from collections import defaultdict
import math


@dataclass
class BM25Document:
    """Document for BM25 indexing."""
    doc_id: str
    text: str
    metadata: Dict[str, Any]
    tokens: List[str] = None


class BM25Tokenizer:
    """Simple tokenizer for BM25."""
    
    def __init__(self, lowercase: bool = True, min_token_length: int = 2):
        self.lowercase = lowercase
        self.min_token_length = min_token_length
        self.stopwords = self._load_stopwords()
    
    def _load_stopwords(self) -> set:
        """Load common English stopwords."""
        return {
            'a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for', 'from',
            'has', 'he', 'in', 'is', 'it', 'its', 'of', 'on', 'that', 'the',
            'to', 'was', 'were', 'will', 'with', 'the', 'this', 'but', 'they',
            'have', 'had', 'what', 'when', 'where', 'who', 'which', 'why', 'how',
            'all', 'each', 'every', 'both', 'few', 'more', 'most', 'other',
            'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so',
            'than', 'too', 'very', 'can', 'just', 'should', 'now', 'also'
        }
    
    def tokenize(self, text: str) -> List[str]:
        """Tokenize text into words."""
        # Lowercase
        if self.lowercase:
            text = text.lower()
        
        # Split on non-alphanumeric
        tokens = re.findall(r'\b[a-z0-9]+\b', text)
        
        # Filter
        tokens = [
            t for t in tokens
            if len(t) >= self.min_token_length and t not in self.stopwords
        ]
        
        return tokens


class BM25Index:
    """BM25 index for lexical search."""
    
    def __init__(
        self,
        k1: float = 1.5,
        b: float = 0.75,
        tokenizer: Optional[BM25Tokenizer] = None
    ):
        self.k1 = k1
        self.b = b
        self.tokenizer = tokenizer or BM25Tokenizer()
        
        # Index storage
        self.documents: Dict[str, BM25Document] = {}
        self.inverted_index: Dict[str, Dict[str, int]] = defaultdict(dict)
        self.document_lengths: Dict[str, int] = {}
        self.avg_doc_length: float = 0.0
        self.total_documents: int = 0
        self.idf_cache: Dict[str, float] = {}
    
    def _compute_idf(self, term: str) -> float:
        """Compute IDF for a term."""
        if term in self.idf_cache:
            return self.idf_cache[term]
        
        df = len(self.inverted_index.get(term, {}))
        if df == 0:
            idf = 0.0
        else:
            idf = math.log((self.total_documents - df + 0.5) / (df + 0.5) + 1)
        
        self.idf_cache[term] = idf
        return idf
    
    def add_document(self, doc: BM25Document) -> None:
        """Add a document to the index."""
        # Tokenize
        tokens = self.tokenizer.tokenize(doc.text)
        doc.tokens = tokens
        
        # Store document
        self.documents[doc.doc_id] = doc
        self.total_documents += 1
        
        # Update inverted index
        term_freqs = defaultdict(int)
        for token in tokens:
            term_freqs[token] += 1
        
        for term, freq in term_freqs.items():
            self.inverted_index[term][doc.doc_id] = freq
        
        # Update document lengths
        self.document_lengths[doc.doc_id] = len(tokens)
        
        # Clear IDF cache since corpus changed
        self.idf_cache.clear()
    
    def add_documents(self, docs: List[BM25Document]) -> None:
        """Add multiple documents to the index."""
        for doc in docs:
            self.add_document(doc)
        
        # Compute average document length
        if self.document_lengths:
            self.avg_doc_length = sum(self.document_lengths.values()) / len(self.document_lengths)
    
    def search(
        self,
        query: str,
        top_k: int = 10,
        filter_metadata: Optional[Dict[str, Any]] = None
    ) -> List[Tuple[str, float, Dict[str, Any]]]:
        """Search for documents matching the query."""
        # Tokenize query
        query_tokens = self.tokenizer.tokenize(query)
        
        # Score documents
        scores: Dict[str, float] = defaultdict(float)
        
        for token in query_tokens:
            idf = self._compute_idf(token)
            
            if token not in self.inverted_index:
                continue
            
            for doc_id, tf in self.inverted_index[token].items():
                doc_length = self.document_lengths.get(doc_id, 1)
                
                # BM25 score
                numerator = tf * (self.k1 + 1)
                denominator = tf + self.k1 * (1 - self.b + self.b * doc_length / self.avg_doc_length)
                score = idf * numerator / denominator
                
                scores[doc_id] += score
        
        # Sort and filter
        results = []
        for doc_id, score in sorted(scores.items(), key=lambda x: x[1], reverse=True):
            if score <= 0:
                continue
            
            doc = self.documents.get(doc_id)
            if not doc:
                continue
            
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
    
    def get_document(self, doc_id: str) -> Optional[BM25Document]:
        """Get a document by ID."""
        return self.documents.get(doc_id)
    
    def save(self, path: Path) -> None:
        """Save the index to disk."""
        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)
        
        # Save documents
        docs_data = {
            doc_id: {
                'doc_id': doc.doc_id,
                'text': doc.text,
                'metadata': doc.metadata,
                'tokens': doc.tokens
            }
            for doc_id, doc in self.documents.items()
        }
        with open(path / 'documents.json', 'w') as f:
            json.dump(docs_data, f)
        
        # Save inverted index
        with open(path / 'inverted_index.json', 'w') as f:
            json.dump(dict(self.inverted_index), f)
        
        # Save stats
        stats = {
            'k1': self.k1,
            'b': self.b,
            'document_lengths': self.document_lengths,
            'avg_doc_length': self.avg_doc_length,
            'total_documents': self.total_documents
        }
        with open(path / 'stats.json', 'w') as f:
            json.dump(stats, f)
    
    def load(self, path: Path) -> None:
        """Load the index from disk."""
        path = Path(path)
        
        # Load documents
        with open(path / 'documents.json', 'r') as f:
            docs_data = json.load(f)
        
        for doc_id, data in docs_data.items():
            doc = BM25Document(
                doc_id=data['doc_id'],
                text=data['text'],
                metadata=data['metadata'],
                tokens=data.get('tokens')
            )
            self.documents[doc_id] = doc
        
        # Load inverted index
        with open(path / 'inverted_index.json', 'r') as f:
            inverted_data = json.load(f)
            self.inverted_index = defaultdict(dict, inverted_data)
        
        # Load stats
        with open(path / 'stats.json', 'r') as f:
            stats = json.load(f)
            self.k1 = stats['k1']
            self.b = stats['b']
            self.document_lengths = stats['document_lengths']
            self.avg_doc_length = stats['avg_doc_length']
            self.total_documents = stats['total_documents']


if __name__ == "__main__":
    # Test
    index = BM25Index()
    
    # Add test documents
    docs = [
        BM25Document(
            doc_id="doc1",
            text="Machine learning is a subset of artificial intelligence.",
            metadata={"source": "test", "category": "AI"}
        ),
        BM25Document(
            doc_id="doc2",
            text="Deep learning uses neural networks for pattern recognition.",
            metadata={"source": "test", "category": "AI"}
        ),
        BM25Document(
            doc_id="doc3",
            text="Drug discovery uses molecular docking and machine learning.",
            metadata={"source": "test", "category": "drug"}
        ),
    ]
    
    index.add_documents(docs)
    
    # Search
    results = index.search("machine learning")
    print("Search results for 'machine learning':")
    for doc_id, score, metadata in results:
        print(f"  {doc_id}: {score:.4f} - {metadata}")