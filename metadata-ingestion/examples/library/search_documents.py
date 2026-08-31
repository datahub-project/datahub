# Inlined from metadata-ingestion/examples/library/search_documents.py
"""Example: Searching documents using the DataHub SDK.

This example demonstrates how to search for documents using the DataHub SDK.
"""

from datahub.sdk import DataHubClient, FilterDsl

# Initialize the client
client = DataHubClient.from_env()

# ============================================================================
# Example 1: Search for all documents
# ============================================================================
# Use get_urns with entity type filter to find documents
document_urns = client.search.get_urns(
    filter=FilterDsl.entity_type("document"),
)

print("All documents:")
for urn in document_urns:
    print(f"  - {urn}")

# ============================================================================
# Example 2: Search with a text query
# ============================================================================
# Search for documents matching "data quality"
document_urns = client.search.get_urns(
    query="data quality",
    filter=FilterDsl.entity_type("document"),
)

print("\nDocuments matching 'data quality':")
for urn in document_urns:
    print(f"  - {urn}")

# ============================================================================
# Example 3: Search within a specific domain
# ============================================================================
document_urns = client.search.get_urns(
    filter=FilterDsl.and_(
        FilterDsl.entity_type("document"),
        FilterDsl.domain("urn:li:domain:engineering"),
    ),
)

print("\nDocuments in engineering domain:")
for urn in document_urns:
    print(f"  - {urn}")

# ============================================================================
# Example 4: Search with tags
# ============================================================================
document_urns = client.search.get_urns(
    filter=FilterDsl.and_(
        FilterDsl.entity_type("document"),
        FilterDsl.tag("urn:li:tag:important"),
    ),
)

print("\nDocuments with 'important' tag:")
for urn in document_urns:
    print(f"  - {urn}")
