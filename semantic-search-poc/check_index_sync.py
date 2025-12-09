import json
import requests

def get_all_docs(index_name):
    """Fetch all documents from an index"""
    response = requests.post(
        f'http://localhost:9200/{index_name}/_search',
        headers={'Content-Type': 'application/json'},
        json={'size': 100, 'query': {'match_all': {}}}
    )
    hits = response.json()['hits']['hits']
    return {hit['_source']['urn']: hit['_source'] for hit in hits}

# Fetch all documents from both indices
v2_docs = get_all_docs('documentindex_v2')
semantic_docs = get_all_docs('documentindex_v2_semantic')

print(f"V2 index: {len(v2_docs)} docs")
print(f"Semantic index: {len(semantic_docs)} docs")
print()

# Check for missing documents
missing_in_semantic = set(v2_docs.keys()) - set(semantic_docs.keys())
missing_in_v2 = set(semantic_docs.keys()) - set(v2_docs.keys())

if missing_in_semantic:
    print(f"⚠️  Missing in semantic index ({len(missing_in_semantic)}):")
    for urn in missing_in_semantic:
        print(f"  - {urn}")
    print()

if missing_in_v2:
    print(f"⚠️  Missing in V2 index ({len(missing_in_v2)}):")
    for urn in missing_in_v2:
        print(f"  - {urn}")
    print()

# Compare field sets for common documents
field_mismatches = []
for urn in set(v2_docs.keys()) & set(semantic_docs.keys()):
    v2_fields = set(v2_docs[urn].keys())
    semantic_fields = set(semantic_docs[urn].keys())
    
    # Embeddings is expected to be only in semantic
    v2_only = v2_fields - semantic_fields - {'platform'}
    semantic_only = semantic_fields - v2_fields - {'embeddings'}
    
    if v2_only or semantic_only:
        field_mismatches.append({
            'urn': urn,
            'v2_only': list(v2_only),
            'semantic_only': list(semantic_only)
        })

if field_mismatches:
    print(f"⚠️  Field mismatches found ({len(field_mismatches)} docs):")
    for mismatch in field_mismatches[:10]:  # Show first 10
        print(f"\n  URN: {mismatch['urn']}")
        if mismatch['v2_only']:
            print(f"    Fields only in V2: {mismatch['v2_only']}")
        if mismatch['semantic_only']:
            print(f"    Fields only in Semantic: {mismatch['semantic_only']}")
else:
    print("✅ All common documents have matching fields (except embeddings)")

print(f"\nTotal documents checked: {len(set(v2_docs.keys()) & set(semantic_docs.keys()))}")
