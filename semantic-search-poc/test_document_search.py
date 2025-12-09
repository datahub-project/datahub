#!/usr/bin/env python3
"""Quick test script for semantic search on documents."""

import sys
from opensearchpy import OpenSearch
from embedding_utils import create_embeddings

# Query from user
query = sys.argv[1] if len(sys.argv) > 1 else "pet adoption"

print(f"🔍 Searching for: '{query}'")
print()

# Initialize
embeddings = create_embeddings(use_cache=True)
opensearch = OpenSearch(
    hosts=[{"host": "localhost", "port": 9200}],
    use_ssl=False,
    verify_certs=False,
)

# Get query embedding
print("📝 Generating query embedding...")
query_vector = embeddings.embed_query(query)
print(f"✓ Got embedding vector ({len(query_vector)} dimensions)")
print()

# Search using kNN on nested chunks
search_query = {
    "size": 5,
    "query": {
        "nested": {
            "path": "embeddings.cohere_embed_v3.chunks",
            "score_mode": "max",
            "query": {
                "knn": {
                    "embeddings.cohere_embed_v3.chunks.vector": {
                        "vector": query_vector,
                        "k": 5
                    }
                }
            }
        }
    },
    "_source": ["urn", "title", "text", "embeddings.cohere_embed_v3.total_chunks"]
}

print("🔎 Executing semantic search...")
response = opensearch.search(
    index="documentindex_v2_semantic",
    body=search_query
)

# Display results
print(f"✓ Found {response['hits']['total']['value']} results")
print()

for i, hit in enumerate(response["hits"]["hits"], 1):
    source = hit["_source"]
    score = hit["_score"]
    
    print(f"{i}. {source.get('title', 'Untitled')} (score: {score:.4f})")
    print(f"   URN: {source.get('urn', 'N/A')}")
    
    # Show chunks info if available
    chunks_info = source.get("embeddings", {}).get("cohere_embed_v3", {}).get("total_chunks")
    if chunks_info:
        print(f"   Chunks: {chunks_info}")
    
    # Show text preview
    text = source.get("text", "")
    if text:
        preview = text[:200] + "..." if len(text) > 200 else text
        print(f"   Text: {preview}")
    
    print()

