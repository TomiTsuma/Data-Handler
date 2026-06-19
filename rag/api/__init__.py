"""
RAG API - FastAPI backend for the AI Research Assistant.
"""

from .main import app, get_rag_system

__all__ = ['app', 'get_rag_system']