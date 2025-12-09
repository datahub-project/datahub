#!/usr/bin/env python3
"""
Demo script showcasing the interactive semantic search capabilities.
This script runs automated searches to demonstrate the system.
"""

import time
from interactive_search import InteractiveSemanticSearch

def run_demo():
    """Run a demo of semantic search capabilities"""
    print("🎬 DataHub Semantic Search Demo")
    print("=" * 50)
    
    # Initialize search
    search = InteractiveSemanticSearch()
    
    # Demo queries
    demo_queries = [
        "customer data and user information",
        "financial reports and revenue metrics", 
        "machine learning models and AI",
        "sales data and analytics",
        "operational system information"
    ]
    
    for i, query in enumerate(demo_queries, 1):
        print(f"\n🔍 Demo Query {i}: '{query}'")
        print("-" * 60)
        
        # Generate embedding and search
        try:
            query_vector = search._generate_query_embedding(query)
            results = search._execute_search(
                query_vector, 
                search.indices["all"],
                k=5  # Show top 5 for demo
            )
            
            # Display results
            if results:
                for j, result in enumerate(results, 1):
                    emoji = {
                        "dataset": "📊",
                        "chart": "📈", 
                        "dashboard": "📋"
                    }.get(result.entity_type, "📄")
                    
                    print(f"{emoji} {j}. {result.name} (Score: {result.score:.3f})")
                    print(f"   Platform: {result.platform} | Type: {result.entity_type}")
                    
                    # Show shortened description
                    desc = result.description
                    if len(desc) > 100:
                        desc = desc[:100] + "..."
                    print(f"   {desc}")
                    print()
            else:
                print("   No results found")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
        
        # Pause between queries
        time.sleep(1)
    
    print("\n✨ Demo complete! Try the interactive version with:")
    print("   uv run python interactive_search.py")

if __name__ == "__main__":
    run_demo()
