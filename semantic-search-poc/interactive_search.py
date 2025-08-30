#!/usr/bin/env python3
"""
Interactive Semantic Search Tool for DataHub

A command-line interface for exploring datasets, charts, and dashboards
using natural language queries powered by semantic search.

Usage:
    uv run python interactive_search.py
"""

import os
import sys
import json
import time
import math
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import requests
from dotenv import load_dotenv
from embedding_utils import create_embeddings

# Load environment variables
load_dotenv()

@dataclass
class SearchResult:
    """Represents a single search result"""
    score: float  # Raw OpenSearch score (0-1, normalized)
    cosine_similarity: float  # Actual cosine similarity (-1 to 1)
    name: str
    platform: str
    qualified_name: str
    description: str
    entity_type: str
    index_name: str

class InteractiveSemanticSearch:
    """Interactive semantic search interface"""
    
    def __init__(self):
        # Hardcode OpenSearch URL for local testing to prevent SSRF
        self.base_url = "http://localhost:9200"
        
        # Initialize embeddings via shared factory (env-driven provider + caching)
        print("🔧 Initializing embeddings...")
        try:
            self.embeddings = create_embeddings(use_cache=True)
            print("✅ Embeddings initialized successfully")
        except Exception as e:
            print(f"❌ Failed to initialize embeddings: {e}")
            sys.exit(1)
        
        # Available indices
        self.indices = {
            "datasets": "datasetindex_v2_semantic",
            "charts": "chartindex_v2_semantic", 
            "dashboards": "dashboardindex_v2_semantic",
            "all": "datasetindex_v2_semantic,chartindex_v2_semantic,dashboardindex_v2_semantic"
        }
        
        # Verify OpenSearch connection
        self._check_opensearch_connection()
    
    def _check_opensearch_connection(self):
        """Verify OpenSearch is accessible and indices exist"""
        try:
            response = requests.get(f"{self.base_url}/_cluster/health", timeout=5)
            if response.status_code != 200:
                raise Exception(f"OpenSearch health check failed: {response.status_code}")
            
            # Check if semantic indices exist
            for name, index in self.indices.items():
                if name == "all":
                    continue
                response = requests.head(f"{self.base_url}/{index}")
                if response.status_code == 404:
                    print(f"⚠️  Warning: Index '{index}' not found")
                    
        except Exception as e:
            print(f"❌ OpenSearch connection failed: {e}")
            print(f"   Make sure OpenSearch is running at {self.base_url}")
            sys.exit(1)
    
    def _generate_query_embedding(self, query: str) -> List[float]:
        """Generate embedding vector for search query"""
        try:
            embeddings = self.embeddings.embed_documents([query])
            return embeddings[0]
        except Exception as e:
            print(f"❌ Failed to generate embedding: {e}")
            raise
    
    def _execute_search(self, query_vector: List[float], indices: str, k: int = 10, 
                       filters: Optional[Dict] = None) -> List[SearchResult]:
        """Execute semantic search against OpenSearch
        
        Uses optimized query pattern for OpenSearch 2.17+ with pre-filtering support.
        """
        # Build kNN query with optional filters
        knn_query = {
            "embeddings.cohere_embed_v3.chunks.vector": {
                "vector": query_vector,
                "k": k * 2 if not filters else k + 5  # Minimal oversampling with pre-filtering
            }
        }
        
        # Add filters inside kNN for pre-filtering (OpenSearch 2.17+)
        if filters:
            knn_query["embeddings.cohere_embed_v3.chunks.vector"]["filter"] = filters
        
        search_payload = {
            "query": {
                "nested": {
                    "path": "embeddings.cohere_embed_v3.chunks",
                    "score_mode": "max",  # Use best chunk score
                    "query": {
                        "knn": knn_query
                    }
                }
            },
            "_source": [
                "name", 
                "platform", 
                "qualifiedName",
                "embeddings.cohere_embed_v3.chunks.text",
                "typeNames"
            ],
            "size": k,
            "track_total_hits": True  # Get accurate total count
        }
        
        try:
            response = requests.post(
                f"{self.base_url}/{indices}/_search",
                headers={"Content-Type": "application/json"},
                json=search_payload,
                timeout=10
            )
            
            if response.status_code != 200:
                raise Exception(f"Search failed: {response.status_code} - {response.text}")
            
            results = response.json()
            return self._parse_search_results(results)
            
        except Exception as e:
            print(f"❌ Search execution failed: {e}")
            return []
    
    def _parse_search_results(self, results: Dict[str, Any]) -> List[SearchResult]:
        """Parse OpenSearch results into SearchResult objects"""
        search_results = []
        
        hits = results.get("hits", {}).get("hits", [])
        for hit in hits:
            source = hit["_source"]
            
            # Extract description from embeddings
            chunks = source.get("embeddings", {}).get("cohere_embed_v3", {}).get("chunks", [])
            description = chunks[0].get("text", "No description available") if chunks else "No description available"
            
            # Determine entity type
            type_names = source.get("typeNames", [])
            entity_type = type_names[0] if type_names else "unknown"
            
            # Clean up platform name
            platform = source.get("platform", "")
            platform_clean = platform.split(":")[-1] if platform else "unknown"
            
            # OpenSearch with cosinesimil returns normalized scores in range [0, 1]
            # The score is calculated as: (1 + cosine_similarity) / 2
            # To get actual cosine similarity: (2 * score) - 1
            raw_score = hit["_score"]
            cosine_sim = (2 * raw_score) - 1  # Convert to actual cosine similarity [-1, 1]
            
            result = SearchResult(
                score=raw_score,
                cosine_similarity=cosine_sim,
                name=source.get("name", "Unknown"),
                platform=platform_clean,
                qualified_name=source.get("qualifiedName", ""),
                description=description,
                entity_type=entity_type,
                index_name=hit["_index"]
            )
            
            search_results.append(result)
        
        return search_results
    
    def _display_results(self, results: List[SearchResult], query: str):
        """Display search results in a formatted way"""
        if not results:
            print("🔍 No results found for your query.")
            print("   Try different keywords or check if the indices have data.")
            return
        
        print(f"\n🔍 Found {len(results)} results for: '{query}'")
        print("=" * 80)
        
        for i, result in enumerate(results, 1):
            # Determine emoji based on entity type
            emoji = {
                "dataset": "📊",
                "chart": "📈", 
                "dashboard": "📋"
            }.get(result.entity_type, "📄")
            
            print(f"\n{emoji} {i}. {result.name}")
            print(f"   Similarity: {result.cosine_similarity:.1%} | Platform: {result.platform} | Type: {result.entity_type}")
            
            if result.qualified_name and result.qualified_name != result.name:
                print(f"   Location: {result.qualified_name}")
            
            # Wrap description text nicely
            desc_lines = self._wrap_text(result.description, 70)
            for j, line in enumerate(desc_lines):
                prefix = "   Description: " if j == 0 else "                "
                print(f"{prefix}{line}")
        
        print("\n" + "=" * 80)
    
    def _wrap_text(self, text: str, width: int) -> List[str]:
        """Simple text wrapping for better display"""
        if len(text) <= width:
            return [text]
        
        lines = []
        words = text.split()
        current_line = ""
        
        for word in words:
            if len(current_line + " " + word) <= width:
                current_line = current_line + " " + word if current_line else word
            else:
                if current_line:
                    lines.append(current_line)
                current_line = word
        
        if current_line:
            lines.append(current_line)
        
        return lines
    
    def _show_help(self):
        """Display help information"""
        print("\n📚 DataHub Semantic Search - Help")
        print("=" * 50)
        print("Commands:")
        print("  help, h, ?          - Show this help message")
        print("  quit, exit, q       - Exit the application")
        print("  scope <type>        - Change search scope")
        print("  filter <platform>   - Filter by platform (e.g., snowflake, dbt)")
        print("  filter clear        - Clear platform filter")
        print("  stats               - Show index statistics")
        print("  clear               - Clear the screen")
        print("")
        print("Search Scopes:")
        print("  datasets            - Search only datasets")
        print("  charts              - Search only charts")
        print("  dashboards          - Search only dashboards")
        print("  all                 - Search all entity types (default)")
        print("")
        print("Example queries:")
        print("  customer data and user information")
        print("  financial reports and revenue metrics")
        print("  machine learning models and predictions")
        print("  operational databases and system tables")
        print("")
        print("Tips:")
        print("  • Use natural language - describe what you're looking for")
        print("  • Be specific about the domain (finance, customer, ML, etc.)")
        print("  • Results are ranked by semantic similarity")
        print("=" * 50)
    
    def _show_stats(self):
        """Show index statistics"""
        print("\n📊 Index Statistics")
        print("=" * 40)
        
        for name, index in self.indices.items():
            if name == "all":
                continue
                
            try:
                response = requests.get(f"{self.base_url}/{index}/_count")
                if response.status_code == 200:
                    count = response.json().get("count", 0)
                    print(f"  {name.capitalize()}: {count:,} documents")
                else:
                    print(f"  {name.capitalize()}: Index not found")
            except:
                print(f"  {name.capitalize()}: Error retrieving count")
        
        print("=" * 40)
    
    def run(self):
        """Main interactive loop"""
        print("\n🚀 DataHub Interactive Semantic Search")
        print("   Type 'help' for commands or enter a search query")
        print("   Use Ctrl+C or 'quit' to exit\n")
        
        current_scope = "all"
        current_filter = None  # Platform filter
        
        try:
            while True:
                # Show current scope and filter in prompt
                scope_display = current_scope if current_scope != "all" else "all entities"
                filter_display = f" | platform:{current_filter}" if current_filter else ""
                prompt = f"🔍 [{scope_display}{filter_display}] Search> "
                
                try:
                    query = input(prompt).strip()
                except EOFError:
                    print("\nGoodbye! 👋")
                    break
                
                if not query:
                    continue
                
                # Handle commands
                query_lower = query.lower()
                
                if query_lower in ["quit", "exit", "q"]:
                    print("Goodbye! 👋")
                    break
                
                elif query_lower in ["help", "h", "?"]:
                    self._show_help()
                    continue
                
                elif query_lower == "stats":
                    self._show_stats()
                    continue
                
                elif query_lower == "clear":
                    os.system('clear' if os.name == 'posix' else 'cls')
                    continue
                
                elif query_lower.startswith("scope "):
                    scope = query_lower.split(" ", 1)[1]
                    if scope in self.indices:
                        current_scope = scope
                        print(f"✅ Search scope changed to: {scope}")
                    else:
                        print(f"❌ Invalid scope. Available: {', '.join(self.indices.keys())}")
                    continue
                
                elif query_lower.startswith("filter "):
                    filter_cmd = query_lower.split(" ", 1)[1]
                    if filter_cmd == "clear":
                        current_filter = None
                        print("✅ Platform filter cleared")
                    else:
                        # Map common platform names to URNs
                        platform_map = {
                            "snowflake": "urn:li:dataPlatform:snowflake",
                            "dbt": "urn:li:dataPlatform:dbt",
                            "looker": "urn:li:dataPlatform:looker",
                            "bigquery": "urn:li:dataPlatform:bigquery",
                            "postgres": "urn:li:dataPlatform:postgres",
                            "mysql": "urn:li:dataPlatform:mysql",
                            "hive": "urn:li:dataPlatform:hive",
                        }
                        if filter_cmd in platform_map:
                            current_filter = platform_map[filter_cmd]
                            print(f"✅ Platform filter set to: {filter_cmd}")
                        else:
                            print(f"❌ Unknown platform. Available: {', '.join(platform_map.keys())}")
                    continue
                
                # Execute search
                print("🔄 Searching...")
                start_time = time.time()
                
                try:
                    # Generate query embedding
                    query_vector = self._generate_query_embedding(query)
                    
                    # Build filters if platform filter is set
                    filters = None
                    if current_filter:
                        filters = {
                            "term": {"platform.keyword": current_filter}
                        }
                    
                    # Execute search with optional filters
                    results = self._execute_search(
                        query_vector, 
                        self.indices[current_scope],
                        k=10,
                        filters=filters
                    )
                    
                    # Display results
                    search_time = time.time() - start_time
                    self._display_results(results, query)
                    print(f"⏱️  Search completed in {search_time:.2f} seconds")
                    
                except Exception as e:
                    print(f"❌ Search failed: {e}")
                
        except KeyboardInterrupt:
            print("\n\nGoodbye! 👋")

def main():
    """Entry point"""
    try:
        search_app = InteractiveSemanticSearch()
        search_app.run()
    except KeyboardInterrupt:
        print("\n\nGoodbye! 👋")
    except Exception as e:
        print(f"❌ Application error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
