"""
FastAPI backend for the AI Research Assistant.
"""
import os
from pathlib import Path
from typing import List, Optional
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

from rag_core import HybridRAG, SearchResult

# Initialize FastAPI app
app = FastAPI(
    title="AI Research Assistant API",
    description="Hybrid RAG system for querying PDF research papers",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize RAG system
rag_system = None


def get_rag_system() -> HybridRAG:
    """Get or initialize the RAG system."""
    global rag_system
    if rag_system is None:
        data_dir = os.getenv("RAG_DATA_DIR", "/home/rhadamanthys/Data-Handler/data/tmp")
        index_dir = os.getenv("RAG_INDEX_DIR", "/home/rhadamanthys/Data-Handler/rag/index")
        
        rag_system = HybridRAG(
            data_dir=data_dir,
            index_dir=index_dir,
            embedding_model="all-MiniLM-L6-v2",
            chunk_size=512,
            chunk_overlap=50,
            bm25_weight=0.5,
            vector_weight=0.5,
            use_gpu=False
        )
        
        # Build index if needed
        if not (Path(index_dir) / "bm25").exists():
            print("Building initial index...")
            rag_system.build_index()
        else:
            print("Loading existing index...")
            rag_system.load_index()
    
    return rag_system


# Pydantic models
class SearchRequest(BaseModel):
    query: str
    top_k: int = 10
    bm25_weight: Optional[float] = None
    vector_weight: Optional[float] = None
    filter_metadata: Optional[dict] = None


class SearchResponse(BaseModel):
    query: str
    results: List[dict]
    total_results: int
    search_time_ms: float


class ContextRequest(BaseModel):
    query: str
    top_k: int = 5
    max_context_length: int = 4000


class ContextResponse(BaseModel):
    query: str
    context: str
    sources: List[dict]


# API endpoints
@app.get("/")
async def root():
    """API health check."""
    return {
        "status": "healthy",
        "title": "AI Research Assistant API",
        "version": "1.0.0"
    }


@app.post("/search", response_model=SearchResponse)
async def search(request: SearchRequest):
    """
    Search PDF documents using hybrid BM25 + vector search.
    
    - **query**: Search query
    - **top_k**: Number of results to return (default: 10)
    - **bm25_weight**: Weight for BM25 scores (default: 0.5)
    - **vector_weight**: Weight for vector scores (default: 0.5)
    - **filter_metadata**: Metadata filters (optional)
    """
    import time
    
    start_time = time.time()
    
    try:
        rag = get_rag_system()
        results = rag.search(
            query=request.query,
            top_k=request.top_k,
            bm25_weight=request.bm25_weight,
            vector_weight=request.vector_weight,
            filter_metadata=request.filter_metadata,
            return_chunks=True
        )
        
        # Format results
        formatted_results = []
        for result in results:
            formatted_results.append({
                "chunk_id": result.chunk_id,
                "document_id": result.document_id,
                "document_path": result.document_path,
                "score": round(result.score, 4),
                "bm25_score": round(result.bm25_score, 4),
                "vector_score": round(result.vector_score, 4),
                "metadata": result.metadata,
                "text_preview": result.text[:200] + "..." if len(result.text) > 200 else result.text
            })
        
        search_time = (time.time() - start_time) * 1000
        
        return SearchResponse(
            query=request.query,
            results=formatted_results,
            total_results=len(formatted_results),
            search_time_ms=round(search_time, 2)
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/context", response_model=ContextResponse)
async def get_context(request: ContextRequest):
    """
    Get context for a query by retrieving relevant document chunks.
    
    - **query**: Search query
    - **top_k**: Number of chunks to retrieve (default: 5)
    - **max_context_length**: Maximum context length in characters (default: 4000)
    """
    try:
        rag = get_rag_system()
        context = rag.get_context(
            query=request.query,
            top_k=request.top_k,
            max_context_length=request.max_context_length
        )
        
        # Extract sources
        sources = []
        for chunk_id in set([
            result.document_id 
            for result in rag.search(request.query, top_k=request.top_k, return_chunks=False)
        ]):
            sources.append({
                "document_id": chunk_id,
                "path": f"/home/rhadamanthys/Data-Handler/data/tmp/{chunk_id}"
            })
        
        return ContextResponse(
            query=request.query,
            context=context,
            sources=sources
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats")
async def get_stats():
    """Get system statistics."""
    try:
        rag = get_rag_system()
        
        return {
            "total_documents": len(rag.chunks),
            "bm25_documents": len(rag.bm25_index.documents),
            "vector_documents": len(rag.vector_index.documents) if rag.vector_index else 0,
            "embedding_dim": rag.embedding_generator.embedding_dim,
            "bm25_weight": rag.bm25_weight,
            "vector_weight": rag.vector_weight
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/rebuild-index")
async def rebuild_index():
    """Rebuild the search index from PDF documents."""
    try:
        rag = get_rag_system()
        rag.build_index(force_rebuild=True)
        
        return {
            "status": "success",
            "message": "Index rebuilt successfully"
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "AI Research Assistant API"
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)