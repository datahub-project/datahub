# Inlined from metadata-ingestion/examples/library/update_document.py
"""Example: Updating documents using the DataHub SDK.

This example demonstrates how to retrieve, modify, and update documents.
"""

from datahub.metadata.urns import DocumentUrn
from datahub.sdk import DataHubClient

# Initialize the client
client = DataHubClient.from_env()

# ============================================================================
# Example 1: Retrieve and update a document's content
# ============================================================================
# First, get the existing document from DataHub
doc = client.entities.get(DocumentUrn("my-tutorial-doc"))

if doc:
    # Update the text content
    doc.set_text("# Updated Getting Started Guide\n\nThis is the updated content...")

    # Save changes
    client.entities.upsert(doc)
    print("Document contents updated!")

# ============================================================================
# Example 2: Update document title
# ============================================================================
doc = client.entities.get(DocumentUrn("my-tutorial-doc"))

if doc:
    doc.set_title("Updated Tutorial Title")
    client.entities.upsert(doc)
    print("Document title updated!")

# ============================================================================
# Example 3: Update both contents and title with method chaining
# ============================================================================
doc = client.entities.get(DocumentUrn("my-tutorial-doc"))

if doc:
    # Method chaining for multiple updates
    doc.set_text("# Comprehensive Guide\n\nFully updated content...").set_title(
        "Comprehensive DataHub Guide"
    )
    client.entities.upsert(doc)
    print("Document fully updated!")

# ============================================================================
# Example 4: Update document visibility (global context)
# ============================================================================
doc = client.entities.get(DocumentUrn("my-tutorial-doc"))

if doc:
    # Hide document from global search and sidebar
    # Useful for making documents only accessible via related assets (e.g., for AI agents)
    doc.hide_from_global_context()
    client.entities.upsert(doc)
    print("Document hidden from global context!")

    # Later, show it again in global search/sidebar
    doc.show_in_global_search()
    client.entities.upsert(doc)
    print("Document visible in global context again!")

# ============================================================================
# Example 5: Update related assets and documents
# ============================================================================
doc = client.entities.get(DocumentUrn("my-tutorial-doc"))

if doc:
    # Add related assets - the document becomes accessible from these assets
    doc.add_related_asset("urn:li:dataset:(urn:li:dataPlatform:snowflake,users,PROD)")
    doc.add_related_asset("urn:li:dataset:(urn:li:dataPlatform:snowflake,orders,PROD)")

    # Add a related document
    doc.add_related_document("urn:li:document:related-guide")

    client.entities.upsert(doc)
    print("Related entities updated!")

# ============================================================================
# Example 6: Move a document to a different parent
# ============================================================================
# Documents can be organized hierarchically. Moving a document changes its parent.
doc = client.entities.get(DocumentUrn("child-section"))

if doc:
    # Check current parent
    print(f"Current parent: {doc.parent_document}")

    # Move to a new parent document
    doc.set_parent_document("urn:li:document:new-parent-guide")
    client.entities.upsert(doc)
    print("Document moved to new parent!")

    # Remove from hierarchy (make it a top-level document)
    doc.set_parent_document(None)
    client.entities.upsert(doc)
    print("Document is now a top-level document!")

# ============================================================================
# Example 7: Update document status
# ============================================================================
doc = client.entities.get(DocumentUrn("my-tutorial-doc"))

if doc:
    # Publish the document
    doc.publish()
    client.entities.upsert(doc)
    print("Document published!")

    # Later, unpublish it
    doc.unpublish()
    client.entities.upsert(doc)
    print("Document unpublished!")
