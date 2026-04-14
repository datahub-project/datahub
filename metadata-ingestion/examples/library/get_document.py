# Inlined from metadata-ingestion/examples/library/get_document.py
"""Example: Retrieving documents using the DataHub SDK.

This example demonstrates how to get documents and access their properties.
"""

from datahub.metadata.urns import DocumentUrn
from datahub.sdk import DataHubClient

# Initialize the client
client = DataHubClient.from_env()

# ============================================================================
# Example 1: Get a document by URN
# ============================================================================
doc = client.entities.get(DocumentUrn("my-tutorial-doc"))

if doc:
    print(f"Document: {doc.title}")
    print(f"URN: {doc.urn}")
    print(f"Status: {doc.status}")
    print(f"Subtype: {doc.subtype}")
    print(f"\nContents:\n{doc.text}")

    # Check document type (native vs external)
    if doc.is_native:
        print("\nThis is a native document (stored in DataHub)")
    elif doc.is_external:
        print("\nThis is an external document")
        print(f"  External URL: {doc.external_url}")
        print(f"  External ID: {doc.external_id}")

    # Check visibility
    if doc.show_in_global_context:
        print("Visible in global search and sidebar")
    else:
        print("Hidden from global context (accessible only via related assets)")

    # Check related entities
    if doc.related_assets:
        print(f"\nRelated assets: {len(doc.related_assets)}")
        for asset in doc.related_assets:
            print(f"  - {asset}")

    if doc.related_documents:
        print(f"\nRelated documents: {len(doc.related_documents)}")
        for related_doc in doc.related_documents:
            print(f"  - {related_doc}")

    # Check parent document
    if doc.parent_document:
        print(f"\nParent document: {doc.parent_document}")

    # Get custom properties
    if doc.custom_properties:
        print("\nCustom properties:")
        for key, value in doc.custom_properties.items():
            print(f"  {key}: {value}")
else:
    print("Document not found")

# ============================================================================
# Example 2: Check if a document exists
# ============================================================================
doc = client.entities.get(DocumentUrn("might-not-exist"))

if doc is not None:
    print(f"\nDocument exists: {doc.urn}")
else:
    print("\nDocument does not exist")
