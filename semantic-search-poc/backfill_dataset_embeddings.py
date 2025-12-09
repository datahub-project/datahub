#!/usr/bin/env python3
"""
Backfill embeddings for dataset entities using Cohere API and v2 text generation.

This script:
1. Fetches dataset documents from datasetindex_v2_semantic
2. Generates text descriptions using EmbeddingTextGeneratorV2
3. Creates embeddings using Cohere embed-english-v3.0
4. Updates documents with embeddings in the cohere_embed_v3 field

Usage:
    python backfill_dataset_embeddings.py --batch-size 10 --dry-run
    python backfill_dataset_embeddings.py --batch-size 50
"""

import os
import sys
import json
import time
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from datetime import datetime
import argparse

from dotenv import load_dotenv
from opensearchpy import OpenSearch

from embedding_text_generator_v2 import EmbeddingTextGeneratorV2
from embedding_utils import create_embeddings
from embedding_cache import CachedEmbeddings


@dataclass
class BackfillConfig:
    """Configuration for the backfill process"""
    opensearch_host: str = "localhost"
    opensearch_port: int = 9200
    source_index: str = "datasetindex_v2_semantic"
    cohere_model: str = "cohere/embed-english-v3.0"  # LiteLLM format
    batch_size: int = 10
    dry_run: bool = False
    max_text_length: int = 8000  # Cohere limit is ~8192 tokens
    embedding_field: str = "cohere_embed_v3"
    use_cache: bool = True


class DatasetEmbeddingBackfiller:
    """Handles the backfill process for dataset embeddings"""
    
    def __init__(self, config: BackfillConfig):
        self.config = config
        self.text_generator = EmbeddingTextGeneratorV2()
        
        # Initialize OpenSearch client
        self.opensearch = OpenSearch(
            hosts=[{"host": config.opensearch_host, "port": config.opensearch_port}],
            http_compress=True,
            use_ssl=False,
            verify_certs=False,
        )
        
        # Initialize embeddings via shared factory
        self.embeddings = create_embeddings(use_cache=config.use_cache)
        print(f"✓ Using {'cached' if config.use_cache else 'direct'} embeddings with model: {self.embeddings.model_name}")
        
        self.stats = {
            "processed": 0,
            "updated": 0,
        }
    
    def fetch_datasets_without_embeddings(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Fetch dataset documents that don't have embeddings yet"""
        query = {
            "query": {
                "bool": {
                    "must_not": {
                        "exists": {
                            "field": f"embeddings.{self.config.embedding_field}.chunks"
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
        
        print(f"✓ Found {len(documents)} datasets without embeddings")
        return documents
    
    def generate_text_description(self, dataset_doc: Dict[str, Any]) -> str:
        """Generate text description using v2 generator"""
        # Extract the source data
        source = dataset_doc.get("_source", {})
        
        # Use v2 generator to create description
        description = self.text_generator.generate(source)
        
        if not description or not description.strip():
            raise ValueError(f"Empty description generated for document {dataset_doc.get('_id', 'unknown')}")
        
        # Truncate if too long
        if len(description) > self.config.max_text_length:
            description = description[:self.config.max_text_length].rsplit(' ', 1)[0] + "..."
        
        return description
    
    def create_embeddings(self, texts: List[str]) -> List[List[float]]:
        """Create embeddings using LiteLLM wrapper with caching"""
        if not texts:
            raise ValueError("No texts provided for embedding")
        
        embeddings = self.embeddings.embed_documents(texts)
        
        if not embeddings or len(embeddings) != len(texts):
            raise ValueError(f"Expected {len(texts)} embeddings, got {len(embeddings) if embeddings else 0}")
        
        return embeddings
    
    def update_document_with_embeddings(
        self, 
        doc_id: str, 
        text: str, 
        embedding: List[float]
    ) -> None:
        """Update document with embedding data"""
        
        if not embedding:
            raise ValueError(f"Empty embedding for document {doc_id}")
        
        # Prepare the embedding structure
        embedding_data = {
            "embeddings": {
                self.config.embedding_field: {
                    "model_version": self.config.cohere_model,
                    "generated_at": datetime.utcnow().isoformat() + "Z",
                    "chunking_strategy": "single_chunk",
                    "total_chunks": 1,
                    "total_tokens": len(text.split()),  # Rough estimate
                    "chunks": [
                        {
                            "position": 0,
                            "text": text,
                            "character_offset": 0,
                            "character_length": len(text),
                            "token_count": len(text.split()),
                            "vector": embedding
                        }
                    ]
                }
            }
        }
        
        if self.config.dry_run:
            print(f"[DRY RUN] Would update document {doc_id} with embedding")
            print(f"  Text length: {len(text)} chars")
            print(f"  Vector dimensions: {len(embedding)}")
            return
        
        # Update the document
        response = self.opensearch.update(
            index=self.config.source_index,
            id=doc_id,
            body={"doc": embedding_data}
        )
        
        if response.get("result") != "updated":
            raise RuntimeError(f"Failed to update document {doc_id}: {response}")
    
    def process_batch(self, documents: List[Dict[str, Any]]) -> None:
        """Process a batch of documents"""
        print(f"\n📝 Processing batch of {len(documents)} documents...")
        
        # Generate text descriptions
        texts = []
        doc_ids = []
        
        for doc in documents:
            doc_id = doc["_id"]
            print(f"  📄 Generating text for {doc_id}")
            text = self.generate_text_description(doc)
            texts.append(text)
            doc_ids.append(doc_id)
        
        print(f"  ✓ Generated {len(texts)} text descriptions")
        
        # Create embeddings
        print(f"  🔮 Creating embeddings for {len(texts)} texts")
        embeddings = self.create_embeddings(texts)
        print(f"  ✓ Created {len(embeddings)} embeddings")
        
        # Update documents
        print(f"  💾 Updating {len(doc_ids)} documents")
        for doc_id, text, embedding in zip(doc_ids, texts, embeddings):
            self.update_document_with_embeddings(doc_id, text, embedding)
            self.stats["updated"] += 1
            self.stats["processed"] += 1
            print(f"  ✓ Updated {doc_id}")
        
        # Rate limiting - be nice to Cohere API
        if not self.config.dry_run:
            time.sleep(1)
    
    def run_backfill(self) -> None:
        """Run the complete backfill process"""
        print(f"🚀 Starting dataset embedding backfill...")
        print(f"   Index: {self.config.source_index}")
        print(f"   Model: {self.config.cohere_model}")
        print(f"   Batch size: {self.config.batch_size}")
        print(f"   Dry run: {self.config.dry_run}")
        
        # Fetch documents that need embeddings
        documents = self.fetch_datasets_without_embeddings(limit=1000)
        
        if not documents:
            print("No documents found that need embeddings")
            return
        
        # Process in batches
        total_batches = (len(documents) + self.config.batch_size - 1) // self.config.batch_size
        
        for i in range(0, len(documents), self.config.batch_size):
            batch_num = (i // self.config.batch_size) + 1
            batch = documents[i:i + self.config.batch_size]
            
            print(f"\n📦 Batch {batch_num}/{total_batches}")
            self.process_batch(batch)
        
        # Print final stats
        print(f"\n📊 Backfill Complete!")
        print(f"   Processed: {self.stats['processed']}")
        print(f"   Updated: {self.stats['updated']}")
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
    parser = argparse.ArgumentParser(description="Backfill dataset embeddings")
    parser.add_argument("--batch-size", type=int, default=10, help="Batch size for processing")
    parser.add_argument("--dry-run", action="store_true", help="Run without making changes")
    parser.add_argument("--max-docs", type=int, default=None, help="Maximum documents to process")
    
    args = parser.parse_args()
    
    # Load environment variables
    load_dotenv()
    
    # Create config
    config = BackfillConfig(
        cohere_model=os.getenv("COHERE_MODEL", "cohere/embed-english-v3.0"),
        batch_size=args.batch_size,
        dry_run=args.dry_run
    )
    
    # Validate that we have the required API key
    cohere_api_key = os.getenv("COHERE_API_KEY", "")
    if not cohere_api_key:
        print("❌ COHERE_API_KEY not found in environment variables")
        sys.exit(1)
    
    # Run backfill
    backfiller = DatasetEmbeddingBackfiller(config)
    backfiller.run_backfill()


if __name__ == "__main__":
    main()
