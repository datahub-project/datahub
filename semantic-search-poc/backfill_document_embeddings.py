#!/usr/bin/env python3
"""
Backfill embeddings for document entities with simple text chunking.

This script:
1. Fetches document entities from documentindex_v2_semantic
2. Chunks the document text into ~400 token chunks (using chars/4 estimate)
3. Creates embeddings for each chunk using Cohere API
4. Updates documents with chunked embeddings

Usage:
    python backfill_document_embeddings.py --batch-size 10 --dry-run
    python backfill_document_embeddings.py --batch-size 50
"""

import os
import sys
import time
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from datetime import datetime
import argparse

from dotenv import load_dotenv
from opensearchpy import OpenSearch

from embedding_utils import create_embeddings
from embedding_cache import CachedEmbeddings


@dataclass
class BackfillConfig:
    """Configuration for the backfill process"""
    opensearch_host: str = "localhost"
    opensearch_port: int = 9200
    source_index: str = "documentindex_v2_semantic"
    embedding_provider: str = "bedrock"  # "bedrock" or "cohere"
    bedrock_model: str = "cohere.embed-english-v3"  # Bedrock model ID
    batch_size: int = 10
    dry_run: bool = False
    max_chunk_tokens: int = 400  # Target tokens per chunk
    chars_per_token: int = 4  # Rough estimate: 1 token ≈ 4 characters
    embedding_field: str = "cohere_embed_v3"
    use_cache: bool = True


class DocumentEmbeddingBackfiller:
    """Handles the backfill process for document embeddings"""
    
    def __init__(self, config: BackfillConfig):
        self.config = config
        
        # Initialize OpenSearch client
        self.opensearch = OpenSearch(
            hosts=[{"host": config.opensearch_host, "port": config.opensearch_port}],
            http_compress=True,
            use_ssl=False,
            verify_certs=False,
        )
        
        # Initialize embeddings
        self.embeddings = create_embeddings(use_cache=config.use_cache)
        print(f"✓ Using {'cached' if config.use_cache else 'direct'} embeddings with model: {self.embeddings.model_name}")
        
        self.stats = {
            "processed": 0,
            "updated": 0,
            "total_chunks": 0,
        }
    
    def chunk_text(self, text: str) -> List[str]:
        """
        Chunk text into segments of approximately max_chunk_tokens.
        
        Uses character count / 4 as a rough token estimate.
        Splits on sentence boundaries when possible.
        """
        if not text or not text.strip():
            return []
        
        max_chars = self.config.max_chunk_tokens * self.config.chars_per_token
        
        # If text fits in one chunk, return as-is
        if len(text) <= max_chars:
            return [text]
        
        chunks = []
        current_chunk = ""
        
        # Split on sentences (rough approximation)
        sentences = text.replace(". ", ".|").replace(".\n", ".|").split("|")
        
        for sentence in sentences:
            sentence = sentence.strip()
            if not sentence:
                continue
            
            # If adding this sentence would exceed the limit, save current chunk
            if current_chunk and len(current_chunk) + len(sentence) + 1 > max_chars:
                chunks.append(current_chunk)
                current_chunk = sentence
            else:
                if current_chunk:
                    current_chunk += " " + sentence
                else:
                    current_chunk = sentence
            
            # If a single sentence is too long, split it by characters
            if len(current_chunk) > max_chars:
                # Split the oversized chunk at character boundary
                while len(current_chunk) > max_chars:
                    chunks.append(current_chunk[:max_chars])
                    current_chunk = current_chunk[max_chars:]
        
        # Add the last chunk if there's anything left
        if current_chunk:
            chunks.append(current_chunk)
        
        return chunks
    
    def fetch_documents_without_embeddings(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Fetch document entities that don't have embeddings yet"""
        # Use the parent field for exists check since chunks is nested
        query = {
            "query": {
                "bool": {
                    "must_not": {
                        "exists": {
                            "field": f"embeddings.{self.config.embedding_field}"
                        }
                    }
                }
            },
            "size": limit,
            "_source": True
        }
        
        response = self.opensearch.search(
            index=self.config.source_index,
            body=query
        )
        
        documents = []
        for hit in response["hits"]["hits"]:
            doc = {
                "_id": hit["_id"],
                "_source": hit["_source"]
            }
            documents.append(doc)
        
        print(f"✓ Found {len(documents)} documents without embeddings")
        return documents
    
    def extract_document_text(self, doc_source: Dict[str, Any]) -> str:
        """
        Extract text content from document.
        
        Documents have a 'text' field that contains the main content.
        """
        text = doc_source.get("text", "")
        
        # Fallback to title if no text
        if not text or not text.strip():
            text = doc_source.get("title", "")
        
        return text.strip()
    
    def update_document_with_chunked_embeddings(
        self, 
        doc_id: str, 
        chunks: List[str],
        embeddings: List[List[float]]
    ) -> None:
        """Update document with chunked embedding data"""
        
        if len(chunks) != len(embeddings):
            raise ValueError(f"Chunks ({len(chunks)}) and embeddings ({len(embeddings)}) count mismatch")
        
        # Prepare the chunked embeddings structure
        chunk_data = []
        current_offset = 0
        for i, (chunk_text, embedding) in enumerate(zip(chunks, embeddings)):
            chunk_length = len(chunk_text)
            chunk_data.append({
                "position": i,
                "text": chunk_text,
                "character_offset": current_offset,
                "character_length": chunk_length,
                "token_count": len(chunk_text.split()),  # Rough estimate
                "vector": embedding
            })
            current_offset += chunk_length
        
        # Determine model version string based on provider
        model_version = (
            f"bedrock/{self.config.bedrock_model}" 
            if self.config.embedding_provider == "bedrock" 
            else self.embeddings.model_name
        )
        
        embedding_data = {
            "embeddings": {
                self.config.embedding_field: {
                    "model_version": model_version,
                    "generated_at": datetime.utcnow().isoformat() + "Z",
                    "chunking_strategy": f"sentence_boundary_{self.config.max_chunk_tokens}t",
                    "total_chunks": len(chunks),
                    "total_tokens": sum(c["token_count"] for c in chunk_data),
                    "chunks": chunk_data
                }
            }
        }
        
        if self.config.dry_run:
            print(f"[DRY RUN] Would update document {doc_id}")
            print(f"  Chunks: {len(chunks)}")
            print(f"  Total text length: {sum(len(c) for c in chunks)} chars")
            print(f"  Vector dimensions per chunk: {len(embeddings[0]) if embeddings else 0}")
            return
        
        # Update the document
        print(f"  📝 Updating index={self.config.source_index}, id={doc_id}")
        print(f"  📝 Embedding data has {len(chunk_data)} chunks")
        
        response = self.opensearch.update(
            index=self.config.source_index,
            id=doc_id,
            body={"doc": embedding_data},
            refresh=True  # Force refresh so changes are immediately visible
        )
        
        print(f"  ✓ Update response: {response.get('result', 'unknown')} (version: {response.get('_version', '?')})")
        
        if response.get("result") not in ["updated", "noop"]:
            raise RuntimeError(f"Failed to update document {doc_id}: {response}")
    
    def process_document(self, document: Dict[str, Any]) -> None:
        """Process a single document"""
        doc_id = document["_id"]
        source = document["_source"]
        
        # Extract text
        text = self.extract_document_text(source)
        
        if not text:
            print(f"  ⚠️  Skipping {doc_id} - no text content")
            return
        
        # Chunk the text
        chunks = self.chunk_text(text)
        print(f"  📄 {doc_id}: {len(text)} chars → {len(chunks)} chunks")
        
        if self.config.dry_run:
            print(f"  [DRY RUN] Would create {len(chunks)} embeddings and update document")
            self.stats["processed"] += 1
            self.stats["total_chunks"] += len(chunks)
            return
        
        # Create embeddings for all chunks
        embeddings = self.embeddings.embed_documents(chunks)
        
        if len(embeddings) != len(chunks):
            raise ValueError(f"Expected {len(chunks)} embeddings, got {len(embeddings)}")
        
        # Update document
        self.update_document_with_chunked_embeddings(doc_id, chunks, embeddings)
        
        self.stats["processed"] += 1
        self.stats["updated"] += 1
        self.stats["total_chunks"] += len(chunks)
    
    def run_backfill(self) -> None:
        """Run the complete backfill process"""
        print(f"🚀 Starting document embedding backfill...")
        print(f"   Index: {self.config.source_index}")
        print(f"   Provider: {self.config.embedding_provider}")
        print(f"   Model: {self.embeddings.model_name}")
        print(f"   Batch size: {self.config.batch_size}")
        print(f"   Max chunk tokens: {self.config.max_chunk_tokens}")
        print(f"   Dry run: {self.config.dry_run}")
        
        # Fetch documents that need embeddings
        documents = self.fetch_documents_without_embeddings(limit=1000)
        
        if not documents:
            print("✓ No documents found that need embeddings")
            return
        
        # Process each document
        for i, document in enumerate(documents, 1):
            print(f"\n📝 Processing document {i}/{len(documents)}")
            try:
                self.process_document(document)
                
                # Rate limiting between documents
                if not self.config.dry_run and i % self.config.batch_size == 0:
                    time.sleep(1)
                    
            except Exception as e:
                print(f"  ❌ Error processing document: {e}")
                if not self.config.dry_run:
                    raise
        
        # Print final stats
        print(f"\n📊 Backfill Complete!")
        print(f"   Documents processed: {self.stats['processed']}")
        print(f"   Documents updated: {self.stats['updated']}")
        print(f"   Total chunks created: {self.stats['total_chunks']}")
        print(f"   Avg chunks per document: {self.stats['total_chunks'] / max(1, self.stats['processed']):.1f}")
        
        if self.config.use_cache:
            match self.embeddings:
                case CachedEmbeddings() as cached:
                    stats = cached.get_cache_stats()
                    print(f"   Cache hits: {stats['cache_hits']}")
                    print(f"   Cache misses: {stats['cache_misses']}")
                    cache_hit_rate = stats['cache_hits'] / max(1, stats['cache_hits'] + stats['cache_misses']) * 100
                    print(f"   Cache hit rate: {cache_hit_rate:.1f}%")
                case _:
                    pass


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Backfill document embeddings with chunking")
    parser.add_argument("--batch-size", type=int, default=10, help="Batch size for rate limiting")
    parser.add_argument("--dry-run", action="store_true", help="Run without making changes")
    parser.add_argument("--max-chunk-tokens", type=int, default=400, help="Max tokens per chunk")
    parser.add_argument("--provider", choices=["bedrock", "cohere"], default="bedrock", 
                        help="Embedding provider (default: bedrock)")
    
    args = parser.parse_args()
    
    # Load environment variables
    load_dotenv()
    
    # Create config
    config = BackfillConfig(
        embedding_provider=args.provider,
        bedrock_model=os.getenv("BEDROCK_MODEL", "cohere.embed-english-v3"),
        batch_size=args.batch_size,
        dry_run=args.dry_run,
        max_chunk_tokens=args.max_chunk_tokens
    )
    
    # Validate credentials based on provider (unless dry-run)
    if not args.dry_run:
        if args.provider == "cohere":
            cohere_api_key = os.getenv("COHERE_API_KEY", "")
            if not cohere_api_key:
                print("❌ COHERE_API_KEY not found in environment variables")
                print("   Set it in .env file or export COHERE_API_KEY=your_key")
                sys.exit(1)
        # For bedrock, AWS credentials are handled by boto3 automatically
        # (IAM role, ~/.aws/credentials, or environment variables)
    
    # Run backfill
    backfiller = DocumentEmbeddingBackfiller(config)
    backfiller.run_backfill()


if __name__ == "__main__":
    main()

