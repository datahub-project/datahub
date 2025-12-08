#!/usr/bin/env python3
"""Test GraphQL semantic search for documents."""

import sys
import json
from datahub.ingestion.graph.client import get_default_graph

# Query from user
query = sys.argv[1] if len(sys.argv) > 1 else "notion documents"

print(f"🔍 Testing GraphQL semantic search for: '{query}'")
print()

# Initialize DataHub graph client (reads config from ~/.datahubenv)
print("📡 Connecting to DataHub...")
graph = get_default_graph()
print(f"✓ Connected to {graph.config.server}")
print()

# GraphQL query for semantic search on documents
graphql_query = """
query semanticSearchDocuments($input: SearchInput!) {
  semanticSearch(input: $input) {
    start
    count
    total
    searchResults {
      entity {
        urn
        type
        ... on Document {
          urn
          info {
            title
            contents {
              text
            }
          }
        }
      }
    }
  }
}
"""

variables = {
    "input": {
        "type": "DOCUMENT",
        "query": query,
        "start": 0,
        "count": 5
    }
}

print("📝 Sending GraphQL request...")
try:
    result = graph.execute_graphql(
        query=graphql_query,
        variables=variables
    )

    if "errors" in result:
        print("❌ GraphQL Errors:")
        for error in result["errors"]:
            print(f"  - {error.get('message', 'Unknown error')}")
            if "extensions" in error:
                print(f"    {json.dumps(error['extensions'], indent=4)}")
        print()

    if "data" in result and result["data"] and result["data"].get("semanticSearch"):
        search_results = result["data"]["semanticSearch"]
        print(f"✓ Found {search_results['total']} results")
        print()

        for i, item in enumerate(search_results["searchResults"], 1):
            entity = item["entity"]
            print(f"{i}. {entity['type']}: {entity['urn']}")
            if entity.get("info"):
                info = entity["info"]
                print(f"   Title: {info.get('title', 'N/A')}")
                contents = info.get("contents", {})
                text = contents.get("text", "") if contents else ""
                if text:
                    preview = text[:100] + "..." if len(text) > 100 else text
                    print(f"   Text: {preview}")
            print()
    else:
        print("Response:")
        print(json.dumps(result, indent=2))

except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()

