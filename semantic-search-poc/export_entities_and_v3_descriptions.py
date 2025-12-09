#!/usr/bin/env python3
"""
Export up to 100 dataset records from OpenSearch into two files:
- entities_raw_100.json: plain JSON entities (as returned by OpenSearch _source)
- entities_v3_descriptions_100.json: JSON array of {name, qualifiedName, description}

This script uses the V3 Hybrid text generator and does NOT depend on generate_full_report.
"""

import json
from typing import Any, Dict, List
from dotenv import load_dotenv

from opensearch_client import OpenSearchClient
from embedding_text_generator_v3 import EmbeddingTextGeneratorV3


def _match_all(client: OpenSearchClient, index: str, size: int) -> List[Dict[str, Any]]:
    try:
        resp = client.client.search(index=index, body={
            "size": size,
            "query": {"match_all": {}},
            "_source": [
                "name", "description", "qualifiedName",
                "tags", "glossaryTerms", "customProperties",
                "fieldPaths", "fieldDescriptions", "platform",
                "origin", "owners", "domains"
            ]
        })
        return [hit["_source"] for hit in resp.get("hits", {}).get("hits", [])]
    except Exception as e:
        print(f"match_all error: {e}")
        return []


def main() -> int:
    load_dotenv()

    client = OpenSearchClient()

    # Try to fetch up to 100 entities with a diversity query first
    entities: List[Dict[str, Any]] = client.get_sample_entities(sample_size=100)

    # If still fewer than 100, fallback to match_all to top up
    if len(entities) < 100:
        needed = 100 - len(entities)
        extra = _match_all(client, index="datasetindex_v2", size=needed)
        # Deduplicate by qualifiedName or name
        seen = set()
        deduped: List[Dict[str, Any]] = []
        def _key(e: Dict[str, Any]) -> str:
            return (e.get("qualifiedName") or e.get("name") or "").lower()
        for e in entities + extra:
            k = _key(e)
            if k and k not in seen:
                deduped.append(e)
                seen.add(k)
        entities = deduped[:100]

    print(f"Collected {len(entities)} entities")

    # Write raw entities
    with open("entities_raw_100.json", "w") as f:
        json.dump(entities, f, indent=2)

    # Generate V3 descriptions
    v3 = EmbeddingTextGeneratorV3()
    outputs: List[Dict[str, Any]] = []
    for e in entities:
        desc = v3.generate(e)
        outputs.append({
            "name": e.get("name"),
            "qualifiedName": e.get("qualifiedName"),
            "description": desc,
        })

    with open("entities_v3_descriptions_100.json", "w") as f:
        json.dump(outputs, f, indent=2)

    print("Wrote entities_raw_100.json and entities_v3_descriptions_100.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


