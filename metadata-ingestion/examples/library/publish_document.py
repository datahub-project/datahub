# Inlined from metadata-ingestion/examples/library/publish_document.py
"""Example: Publishing and unpublishing documents using the DataHub SDK.

This example demonstrates how to control document visibility by
publishing or unpublishing documents.
"""

from datahub.metadata.urns import DocumentUrn
from datahub.sdk import DataHubClient, Document

# Initialize the client
client = DataHubClient.from_env()

# ============================================================================
# Example 1: Publish a document
# ============================================================================
# Get the document
doc = client.entities.get(DocumentUrn("my-tutorial-doc"))

if doc:
    # Publish makes the document visible to users
    doc.publish()
    client.entities.upsert(doc)
    print(f"Document published! Status: {doc.status}")

# ============================================================================
# Example 2: Unpublish a document
# ============================================================================
doc = client.entities.get(DocumentUrn("my-tutorial-doc"))

if doc:
    # Unpublish hides the document from general users
    doc.unpublish()
    client.entities.upsert(doc)
    print(f"Document unpublished! Status: {doc.status}")

# ============================================================================
# Example 3: Create a document with specific status
# ============================================================================
# Create as published (default)
published_doc = Document.create_document(
    id="new-published-doc",
    title="New Published Document",
    text="This document is published from the start.",
)
client.entities.upsert(published_doc)
print(f"Created published document: {published_doc.urn}")

# Create as unpublished (work in progress)
unpublished_doc = Document.create_document(
    id="new-unpublished-doc",
    title="Work in Progress Document",
    text="This document is not yet published.",
    status="UNPUBLISHED",
)
client.entities.upsert(unpublished_doc)
print(f"Created unpublished document: {unpublished_doc.urn}")
