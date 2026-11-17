"""Example: Deleting documents using the DataHub SDK.

This example demonstrates how to delete documents from DataHub.
"""

from datahub.errors import ItemNotFoundError
from datahub.metadata.urns import DocumentUrn
from datahub.sdk import DataHubClient

# Initialize the client
client = DataHubClient.from_env()

# ============================================================================
# Example 1: Delete a document by URN
# ============================================================================
doc_urn = DocumentUrn("my-tutorial-doc")

# entities.get() raises ItemNotFoundError rather than returning None, so that is
# how you check for existence.
try:
    client.entities.get(doc_urn)
except ItemNotFoundError:
    raise SystemExit(f"Document not found: {doc_urn}") from None

client.entities.delete(str(doc_urn))
print(f"Document deleted: {doc_urn}")

# ============================================================================
# Example 2: Delete multiple documents
# ============================================================================
# Here a missing document is not an error -- we just skip it and carry on.
doc_ids_to_delete = [
    "doc-1",
    "doc-2",
    "doc-3",
]

for doc_id in doc_ids_to_delete:
    doc_urn = DocumentUrn(doc_id)
    try:
        client.entities.get(doc_urn)
    except ItemNotFoundError:
        print(f"Not found (skipping): {doc_urn}")
        continue

    client.entities.delete(str(doc_urn))
    print(f"Deleted: {doc_urn}")

print("Cleanup complete!")
