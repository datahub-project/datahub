import requests
import json

def get_all_docs(index_name):
    """Fetch all documents from an index"""
    response = requests.post(
        f'http://localhost:9200/{index_name}/_search',
        headers={'Content-Type': 'application/json'},
        json={"size": 100, "sort": [{"urn": "asc"}]}
    )
    hits = response.json()['hits']['hits']
    return {hit['_source']['urn']: hit['_source'] for hit in hits}

# Get all docs from both indices
v2_docs = get_all_docs('documentindex_v2')
semantic_docs = get_all_docs('documentindex_v2_semantic')

print(f"V2 index: {len(v2_docs)} documents")
print(f"Semantic index: {len(semantic_docs)} documents")
print()

# Find documents only in one index
v2_only = set(v2_docs.keys()) - set(semantic_docs.keys())
semantic_only = set(semantic_docs.keys()) - set(v2_docs.keys())

if v2_only:
    print(f"Documents ONLY in V2 index ({len(v2_only)}):")
    for urn in v2_only:
        print(f"  - {urn}")
    print()

if semantic_only:
    print(f"Documents ONLY in Semantic index ({len(semantic_only)}):")
    for urn in semantic_only:
        print(f"  - {urn}")
    print()

# Compare fields for documents in both indices
print("=== Field comparison for documents in both indices ===")
mismatches = []

for urn in sorted(set(v2_docs.keys()) & set(semantic_docs.keys())):
    v2_doc = v2_docs[urn]
    semantic_doc = semantic_docs[urn]
    
    # Get fields (excluding embeddings which is semantic-only)
    v2_fields = set(v2_doc.keys())
    semantic_fields = set(semantic_doc.keys()) - {'embeddings'}
    
    # Check for missing fields
    missing_in_semantic = v2_fields - semantic_fields
    missing_in_v2 = semantic_fields - v2_fields
    
    if missing_in_semantic or missing_in_v2:
        mismatches.append({
            'urn': urn,
            'missing_in_semantic': list(missing_in_semantic),
            'missing_in_v2': list(missing_in_v2)
        })

if mismatches:
    print(f"\n⚠️  Found {len(mismatches)} documents with field mismatches:\n")
    for mismatch in mismatches:
        print(f"URN: {mismatch['urn']}")
        if mismatch['missing_in_semantic']:
            print(f"  Missing in semantic: {mismatch['missing_in_semantic']}")
        if mismatch['missing_in_v2']:
            print(f"  Missing in V2: {mismatch['missing_in_v2']}")
        print()
else:
    print("✅ All documents have matching fields!")
    
# Check for value differences in common fields
print("\n=== Checking for value differences ===")
value_diffs = []

for urn in sorted(set(v2_docs.keys()) & set(semantic_docs.keys())):
    v2_doc = v2_docs[urn]
    semantic_doc = semantic_docs[urn]
    
    # Compare common fields
    common_fields = set(v2_doc.keys()) & set(semantic_doc.keys())
    
    for field in common_fields:
        if v2_doc[field] != semantic_doc[field]:
            value_diffs.append({
                'urn': urn,
                'field': field,
                'v2_value': v2_doc[field],
                'semantic_value': semantic_doc[field]
            })

if value_diffs:
    print(f"⚠️  Found {len(value_diffs)} field value differences:\n")
    for diff in value_diffs[:10]:  # Show first 10
        print(f"URN: {diff['urn']}")
        print(f"  Field: {diff['field']}")
        print(f"  V2: {str(diff['v2_value'])[:100]}")
        print(f"  Semantic: {str(diff['semantic_value'])[:100]}")
        print()
else:
    print("✅ All field values match!")
