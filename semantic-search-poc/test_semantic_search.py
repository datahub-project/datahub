#!/usr/bin/env python3
"""
Test semantic search functionality with real queries and embeddings.
"""

import json
import requests
from embedding_utils import create_cohere_embeddings, create_cached_embeddings
from dotenv import load_dotenv

load_dotenv()

def test_semantic_search():
    """Test semantic search with real queries"""
    
    # Initialize embeddings with caching
    embeddings = create_cached_embeddings(
        create_cohere_embeddings(model="embed-english-v3.0")
    )
    
    # Test queries
    test_queries = [
        "customer data and user information",
        "financial reports and revenue metrics", 
        "product analytics and sales data",
        "machine learning and AI models",
        "operational databases and system information"
    ]
    
    print("🔍 Testing Semantic Search Functionality\n")
    
    for i, query in enumerate(test_queries, 1):
        print(f"📝 Query {i}: '{query}'")
        
        # Generate embedding for query (using embed_documents with single query)
        query_embeddings = embeddings.embed_documents([query])
        query_embedding = query_embeddings[0]
        print(f"   ✓ Generated query embedding ({len(query_embedding)} dimensions)")
        
        # Search for similar datasets using nested kNN query
        search_payload = {
            "query": {
                "nested": {
                    "path": "embeddings.cohere_embed_v3.chunks",
                    "query": {
                        "knn": {
                            "embeddings.cohere_embed_v3.chunks.vector": {
                                "vector": query_embedding,
                                "k": 3
                            }
                        }
                    }
                }
            },
            "_source": ["name", "platform", "embeddings.cohere_embed_v3.chunks.text"],
            "size": 3
        }
        
        try:
            response = requests.post(
                "http://localhost:9200/datasetindex_v2_semantic/_search",
                headers={"Content-Type": "application/json"},
                json=search_payload,
                timeout=10
            )
            
            if response.status_code == 200:
                results = response.json()
                total_hits = results.get("hits", {}).get("total", {}).get("value", 0)
                hits = results.get("hits", {}).get("hits", [])
                
                print(f"   ✓ Found {len(hits)} similar datasets (total: {total_hits}):")
                
                if len(hits) == 0:
                    print("     No results returned. Let me check if there's an issue...")
                    # Debug: check if there are any documents with embeddings
                    debug_payload = {
                        "query": {"exists": {"field": "embeddings.cohere_embed_v3.chunks.vector"}},
                        "size": 1
                    }
                    debug_response = requests.post(
                        "http://localhost:9200/datasetindex_v2_semantic/_search",
                        headers={"Content-Type": "application/json"},
                        json=debug_payload
                    )
                    if debug_response.status_code == 200:
                        debug_results = debug_response.json()
                        debug_total = debug_results.get("hits", {}).get("total", {}).get("value", 0)
                        print(f"     Debug: {debug_total} documents have embeddings in the index")
                
                for j, hit in enumerate(hits, 1):
                    score = hit["_score"]
                    source = hit["_source"]
                    name = source.get("name", "Unknown")
                    platform = source.get("platform", "Unknown")
                    
                    # Get the text description
                    chunks = source.get("embeddings", {}).get("cohere_embed_v3", {}).get("chunks", [])
                    text = chunks[0].get("text", "No description") if chunks else "No description"
                    
                    print(f"     {j}. Score: {score:.3f}")
                    print(f"        Name: {name}")
                    print(f"        Platform: {platform}")
                    print(f"        Description: {text}")
                    print()
                    
            else:
                print(f"   ❌ Search failed: {response.status_code} - {response.text}")
                if response.status_code == 400:
                    error_details = response.json()
                    print(f"   Error details: {json.dumps(error_details, indent=2)}")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
            
        print("-" * 80)

if __name__ == "__main__":
    test_semantic_search()
