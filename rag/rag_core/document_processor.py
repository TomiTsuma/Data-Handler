"""
Document Processor for PDF text extraction and chunking.
"""
import os
import re
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import hashlib

# PDF processing
try:
    import fitz  # PyMuPDF
    HAS_FITZ = True
except ImportError:
    HAS_FITZ = False

try:
    import PyPDF2
    HAS_PYPDF2 = True
except ImportError:
    HAS_PYPDF2 = False


@dataclass
class DocumentChunk:
    """A chunk of text from a document."""
    chunk_id: str
    document_id: str
    document_path: str
    text: str
    metadata: Dict[str, Any]
    chunk_index: int
    total_chunks: int


class DocumentProcessor:
    """Process PDF documents into chunks for RAG."""
    
    def __init__(
        self,
        chunk_size: int = 512,
        chunk_overlap: int = 50,
        min_chunk_size: int = 100
    ):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.min_chunk_size = min_chunk_size
    
    def extract_text_from_pdf(self, pdf_path: Path) -> str:
        """Extract text from a PDF file."""
        text = ""
        
        if HAS_FITZ:
            try:
                with fitz.open(pdf_path) as doc:
                    for page in doc:
                        text += page.get_text() + "\n"
                return text
            except Exception as e:
                print(f"PyMuPDF failed for {pdf_path}: {e}")
        
        if HAS_PYPDF2:
            try:
                with open(pdf_path, 'rb') as f:
                    reader = PyPDF2.PdfReader(f)
                    for page in reader.pages:
                        text += page.extract_text() + "\n"
                return text
            except Exception as e:
                print(f"PyPDF2 failed for {pdf_path}: {e}")
        
        raise ImportError("No PDF library available. Install PyMuPDF or PyPDF2.")
    
    def clean_text(self, text: str) -> str:
        """Clean extracted text."""
        # Remove excessive whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r' {2,}', ' ', text)
        
        # Remove common PDF artifacts
        text = re.sub(r'\x0c', '', text)  # Form feed
        text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', text)  # Control chars
        
        # Normalize unicode
        text = text.encode('utf-8', errors='ignore').decode('utf-8')
        
        return text.strip()
    
    def chunk_text(
        self,
        text: str,
        document_id: str,
        document_path: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> List[DocumentChunk]:
        """Split text into overlapping chunks."""
        metadata = metadata or {}
        chunks = []
        
        # Split on paragraphs first
        paragraphs = text.split('\n\n')
        
        current_chunk = ""
        chunk_index = 0
        
        for para in paragraphs:
            para = para.strip()
            if not para:
                continue
            
            # If paragraph fits, add it
            if len(current_chunk) + len(para) < self.chunk_size:
                current_chunk += para + "\n\n"
            else:
                # Save current chunk if large enough
                if len(current_chunk.strip()) >= self.min_chunk_size:
                    chunk_id = self._generate_chunk_id(document_id, chunk_index)
                    chunks.append(DocumentChunk(
                        chunk_id=chunk_id,
                        document_id=document_id,
                        document_path=document_path,
                        text=current_chunk.strip(),
                        metadata=metadata,
                        chunk_index=chunk_index,
                        total_chunks=-1  # Will update later
                    ))
                    chunk_index += 1
                
                # Start new chunk with overlap
                if len(current_chunk) > self.chunk_overlap:
                    overlap_text = current_chunk[-self.chunk_overlap:]
                    current_chunk = overlap_text + "\n\n" + para + "\n\n"
                else:
                    current_chunk = para + "\n\n"
        
        # Add final chunk
        if len(current_chunk.strip()) >= self.min_chunk_size:
            chunk_id = self._generate_chunk_id(document_id, chunk_index)
            chunks.append(DocumentChunk(
                chunk_id=chunk_id,
                document_id=document_id,
                document_path=document_path,
                text=current_chunk.strip(),
                metadata=metadata,
                chunk_index=chunk_index,
                total_chunks=-1
            ))
        
        # Update total_chunks
        for chunk in chunks:
            chunk.total_chunks = len(chunks)
        
        return chunks
    
    def _generate_chunk_id(self, document_id: str, chunk_index: int) -> str:
        """Generate a unique chunk ID."""
        return f"{document_id}_chunk_{chunk_index}"
    
    def process_document(
        self,
        pdf_path: Path,
        metadata: Optional[Dict[str, Any]] = None
    ) -> List[DocumentChunk]:
        """Process a single PDF document."""
        metadata = metadata or {}
        
        # Generate document ID from filename
        document_id = pdf_path.stem
        metadata['filename'] = pdf_path.name
        metadata['filepath'] = str(pdf_path)
        
        # Extract text
        text = self.extract_text_from_pdf(pdf_path)
        text = self.clean_text(text)
        
        # Chunk
        chunks = self.chunk_text(
            text,
            document_id=document_id,
            document_path=str(pdf_path),
            metadata=metadata
        )
        
        return chunks
    
    def process_directory(
        self,
        directory: Path,
        recursive: bool = True
    ) -> List[DocumentChunk]:
        """Process all PDFs in a directory."""
        all_chunks = []
        
        pattern = "**/*.pdf" if recursive else "*.pdf"
        
        for pdf_path in directory.glob(pattern):
            try:
                print(f"Processing: {pdf_path.name}")
                chunks = self.process_document(pdf_path)
                all_chunks.extend(chunks)
                print(f"  -> {len(chunks)} chunks")
            except Exception as e:
                print(f"Error processing {pdf_path}: {e}")
        
        return all_chunks


if __name__ == "__main__":
    # Test
    processor = DocumentProcessor()
    
    # Process all PDFs in data/tmp
    data_dir = Path("/home/rhadamanthys/Data-Handler/data/tmp")
    chunks = processor.process_directory(data_dir)
    print(f"\nTotal chunks: {len(chunks)}")
    
    if chunks:
        print(f"\nSample chunk:")
        print(f"  ID: {chunks[0].chunk_id}")
        print(f"  Document: {chunks[0].document_id}")
        print(f"  Text preview: {chunks[0].text[:200]}...")