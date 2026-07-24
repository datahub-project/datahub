


# Document

## Why Would You Use Documents?

Documents in DataHub are content-indexed resources that can store knowledge, documentation, FAQs, tutorials, and other textual content. They provide a centralized place to manage and search through organizational knowledge, making them accessible to both humans and AI systems.

Documents support rich metadata including:

- **Searchable content** with full-text search capabilities
- **Categorization** via types, domains, and owners
- **Visibility control** to show/hide documents in global search and navigation
- **Relationships** to data assets (datasets, dashboards, charts, etc.)
- **Hierarchical organization** through parent-child relationships

### Types of Documents

DataHub supports two types of documents:

1. **Native Documents**: Created and stored directly in DataHub. Full content is indexed and searchable. Use `Document.create_document()` to create these.

2. **External Documents**: References to documents stored in external systems (Notion, Confluence, Google Docs, etc.). These link to the original content via URL. Use `Document.create_external_document()` to create these.

### Document Visibility

Documents can be configured to:

- **Show in global context** (default): Appear in global search results and the knowledge base sidebar
- **Hide from global context**: Only accessible through related assets. This is useful for:
  - Documentation specific to a single dataset
  - Context documents for AI agents
  - Private notes attached to assets

### Goal Of This Guide

This guide will show you how to:

- Create native and external documents
- Control document visibility
- Link documents to data assets
- Update document contents and metadata
- Publish and unpublish documents
- Delete documents

## Prerequisites

For this tutorial, you need to deploy DataHub Quickstart and ingest sample data.
For detailed steps, please refer to [DataHub Quickstart Guide](/docs/quickstart.md).

## Create Document

### Native Document

Native documents are stored directly in DataHub with full content indexing.



#### GraphQL


```graphql
mutation createDocument {
  createDocument(
    input: {
      id: "my-tutorial-doc"
      contents: {
        text: "# Getting Started with DataHub\n\nThis tutorial will help you get started..."
      }
      title: "DataHub Tutorial"
      subType: "Tutorial"
      state: PUBLISHED
    }
  )
}
```

If you see the following response, the operation was successful:

```json
{
  "data": {
    "createDocument": "urn:li:document:my-tutorial-doc"
  },
  "extensions": {}
}
```



#### Python


```python
from datahub.sdk import DataHubClient, Document

client = DataHubClient.from_env()

# Create a native document
doc = Document.create_document(
    id="getting-started-tutorial",
    title="Getting Started with DataHub",
    text="# Getting Started with DataHub\n\nThis tutorial will help you get started...",
    subtype="Tutorial",
)

client.entities.upsert(doc)
print(f"Created document: {doc.urn}")
```




### External Document

External documents reference content stored in other platforms like Notion or Confluence.



#### GraphQL


```graphql
mutation createExternalDocument {
  createDocument(
    input: {
      id: "notion-handbook"
      contents: { text: "Summary for search indexing..." }
      title: "Engineering Handbook"
      subType: "Reference"
      state: PUBLISHED
    }
  )
}
```



#### Python


```python
from datahub.sdk import DataHubClient, Document

client = DataHubClient.from_env()

# Create an external document (from Notion)
doc = Document.create_external_document(
    id="notion-engineering-handbook",
    title="Engineering Handbook",
    platform="urn:li:dataPlatform:notion",
    external_url="https://notion.so/team/engineering-handbook",
    external_id="notion-page-abc123",
    text="Summary of the handbook for search...",  # Optional
    owners=["urn:li:corpuser:engineering-lead"],
)

client.entities.upsert(doc)
print(f"Created external document: {doc.urn}")
```




### Document Hidden from Global Context

Documents can be hidden from global search and sidebar navigation. They remain accessible through related assets - useful for AI agent context or asset-specific documentation.



#### GraphQL


```graphql
mutation createHiddenDocument {
  createDocument(
    input: {
      id: "dataset-context-doc"
      contents: { text: "Context about the orders dataset for AI agents..." }
      title: "Orders Dataset Context"
      settings: { showInGlobalContext: false }
      relatedAssets: [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,orders,PROD)"
      ]
    }
  )
}
```



#### Python


```python
from datahub.sdk import DataHubClient, Document

client = DataHubClient.from_env()

# Create a document hidden from global context
# Only accessible via the related asset - useful for AI agents
doc = Document.create_document(
    id="orders-dataset-context",
    title="Orders Dataset Context",
    text="# Context for AI Agents\n\nThe orders dataset contains daily summaries...",
    show_in_global_context=False,  # Hidden from global search/sidebar
    related_assets=["urn:li:dataset:(urn:li:dataPlatform:snowflake,orders,PROD)"],
)

client.entities.upsert(doc)
print(f"Created AI-only context document: {doc.urn}")
```




### Document with Full Metadata



#### GraphQL


```graphql
mutation createDocumentWithMetadata {
  createDocument(
    input: {
      id: "faq-data-quality"
      contents: {
        text: "# Data Quality FAQ\n\n## Q: How do we measure data quality?\n\nA: We use..."
      }
      title: "Data Quality FAQ"
      subType: "FAQ"
      state: PUBLISHED
      relatedAssets: [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_table,PROD)"
      ]
      owners: [{ owner: "urn:li:corpuser:john", type: TECHNICAL_OWNER }]
    }
  )
}
```



#### Python


```python
from datahub.sdk import DataHubClient, Document

client = DataHubClient.from_env()

doc = Document.create_document(
    id="faq-data-quality",
    title="Data Quality FAQ",
    text="# Data Quality FAQ\n\n## Q: How do we measure data quality?\n\nA: We use...",
    subtype="FAQ",
    related_assets=["urn:li:dataset:(urn:li:dataPlatform:snowflake,my_table,PROD)"],
    owners=["urn:li:corpuser:john"],
    domain="urn:li:domain:engineering",
    tags=["urn:li:tag:important"],
    custom_properties={"team": "data-platform", "version": "1.0"},
)

client.entities.upsert(doc)
print(f"Created document with metadata: {doc.urn}")
```




## Update Document

Update the contents, title, or visibility of an existing document.



#### GraphQL


```graphql
mutation updateDocumentContents {
  updateDocumentContents(
    input: {
      urn: "urn:li:document:my-tutorial-doc"
      contents: {
        text: "# Updated Getting Started Guide\n\nThis is the updated content..."
      }
    }
  )
}
```

Update the title:

```graphql
mutation updateDocumentTitle {
  updateDocumentContents(
    input: {
      urn: "urn:li:document:my-tutorial-doc"
      title: "Updated Tutorial Title"
    }
  )
}
```

Update visibility settings:

```graphql
mutation updateDocumentSettings {
  updateDocumentSettings(
    input: {
      urn: "urn:li:document:my-tutorial-doc"
      showInGlobalContext: false
    }
  )
}
```



#### Python


```python
# Inlined from /metadata-ingestion/examples/library/update_document.py
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

```




## Search Documents

Search through documents with various filters.



#### GraphQL


```graphql
query searchDocuments {
  searchDocuments(
    input: { query: "data quality", types: ["FAQ"], start: 0, count: 10 }
  ) {
    total
    documents {
      urn
      type
      subType
      info {
        title
        status {
          state
        }
        contents {
          text
        }
      }
    }
  }
}
```



#### Python


```python
# Inlined from /metadata-ingestion/examples/library/search_documents.py
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

```




## Get Document

Retrieve the full contents and metadata of a specific document.



#### GraphQL


```graphql
query getDocument {
  document(urn: "urn:li:document:my-tutorial-doc") {
    urn
    type
    subType
    info {
      title
      source {
        sourceType
        externalUrl
        externalId
      }
      status {
        state
      }
      contents {
        text
      }
      relatedAssets {
        asset {
          urn
        }
      }
      relatedDocuments {
        document {
          urn
        }
      }
      parentDocument {
        document {
          urn
        }
      }
    }
    settings {
      showInGlobalContext
    }
  }
}
```



#### Python


```python
# Inlined from /metadata-ingestion/examples/library/get_document.py
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

```




## Publish/Unpublish Document

Control whether a document is visible to users.



#### GraphQL


Publish a document:

```graphql
mutation publishDocument {
  updateDocumentStatus(
    input: { urn: "urn:li:document:my-tutorial-doc", state: PUBLISHED }
  )
}
```

Unpublish a document:

```graphql
mutation unpublishDocument {
  updateDocumentStatus(
    input: { urn: "urn:li:document:my-tutorial-doc", state: UNPUBLISHED }
  )
}
```



#### Python


```python
# Inlined from /metadata-ingestion/examples/library/publish_document.py
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

```




## Delete Document

Remove a document from DataHub.



#### GraphQL


```graphql
mutation deleteDocument {
  deleteDocument(urn: "urn:li:document:my-tutorial-doc")
}
```



#### Python


```python
# Inlined from /metadata-ingestion/examples/library/delete_document.py
# Inlined from metadata-ingestion/examples/library/delete_document.py
"""Example: Deleting documents using the DataHub SDK.

This example demonstrates how to delete documents from DataHub.
"""

from datahub.metadata.urns import DocumentUrn
from datahub.sdk import DataHubClient

# Initialize the client
client = DataHubClient.from_env()

# ============================================================================
# Example 1: Delete a document by URN
# ============================================================================
doc_urn = DocumentUrn("my-tutorial-doc")

# First check if it exists
doc = client.entities.get(doc_urn)

if doc:
    # Delete the document
    client.entities.delete(str(doc_urn))
    print(f"Document deleted: {doc_urn}")
else:
    print(f"Document not found: {doc_urn}")

# ============================================================================
# Example 2: Delete multiple documents
# ============================================================================
doc_ids_to_delete = [
    "doc-1",
    "doc-2",
    "doc-3",
]

for doc_id in doc_ids_to_delete:
    doc_urn = DocumentUrn(doc_id)
    doc = client.entities.get(doc_urn)
    if doc:
        client.entities.delete(str(doc_urn))
        print(f"Deleted: {doc_urn}")
    else:
        print(f"Not found (skipping): {doc_urn}")

print("Cleanup complete!")

```




## Advanced Operations

### Link Related Assets

Associate a document with data assets. Documents linked to assets can be accessed from those assets even when hidden from global context.



#### GraphQL


```graphql
mutation updateRelatedEntities {
  updateDocumentRelatedEntities(
    input: {
      urn: "urn:li:document:my-doc"
      relatedAssets: [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_table,PROD)"
        "urn:li:dashboard:(looker,dashboard1)"
      ]
      relatedDocuments: ["urn:li:document:related-doc"]
    }
  )
}
```



#### Python


```python
from datahub.sdk import DataHubClient, Document

client = DataHubClient.from_env()

doc = client.entities.get("urn:li:document:my-doc", Document)
if doc:
    # Add related assets
    doc.add_related_asset("urn:li:dataset:(urn:li:dataPlatform:snowflake,my_table,PROD)")
    doc.add_related_asset("urn:li:dashboard:(looker,dashboard1)")

    # Add related documents
    doc.add_related_document("urn:li:document:related-doc")

    client.entities.upsert(doc)
    print("Related entities updated!")
```




### Update Document Sub-Type

Change the sub-type (e.g., "FAQ", "Tutorial", "Runbook") of a document:



#### GraphQL


```graphql
mutation updateDocumentSubType {
  updateDocumentSubType(
    input: { urn: "urn:li:document:my-doc", subType: "Reference" }
  )
}
```



#### Python


```python
from datahub.sdk import DataHubClient, Document

client = DataHubClient.from_env()

doc = client.entities.get("urn:li:document:my-doc", Document)
if doc:
    doc.set_subtype("Reference")
    client.entities.upsert(doc)
    print(f"Sub-type updated: {doc.subtype}")
```




### Move Document

Move a document to a different parent (for hierarchical organization):



#### GraphQL


```graphql
mutation moveDocument {
  moveDocument(
    input: {
      urn: "urn:li:document:child-doc"
      newParent: "urn:li:document:new-parent"
    }
  )
}
```



#### Python


```python
from datahub.sdk import DataHubClient, Document

client = DataHubClient.from_env()

doc = client.entities.get("urn:li:document:child-doc", Document)
if doc:
    # Move to a new parent
    doc.set_parent_document("urn:li:document:new-parent")
    client.entities.upsert(doc)
    print(f"Document moved! New parent: {doc.parent_document}")

    # Or make it a top-level document (no parent)
    doc.set_parent_document(None)
    client.entities.upsert(doc)
    print("Document is now a top-level document!")
```




## End-to-End: Push, Index, and Verify

This workflow covers pushing pre-refined documents, triggering semantic indexing, and confirming
retrieval. By default, DataHub includes a built-in scheduled embedding job that runs every 15
minutes to index new and updated documents. If you don't want to wait for the next scheduled run,
you can trigger it on demand as shown below.

### Step 1: Push Your Documents

Push one or more documents using the Python SDK:

```python
from datahub.sdk import DataHubClient, Document

client = DataHubClient.from_env()

docs = [
    Document.create_document(
        id="orders-dataset-context",
        title="Orders Dataset Context",
        text="# Orders Dataset\n\nThe orders table contains daily order summaries...",
        related_assets=["urn:li:dataset:(urn:li:dataPlatform:snowflake,orders,PROD)"],
        show_in_global_context=False,  # AI agent context doc
    ),
    Document.create_document(
        id="payments-dataset-context",
        title="Payments Dataset Context",
        text="# Payments Dataset\n\nThe payments table tracks transaction records...",
        related_assets=["urn:li:dataset:(urn:li:dataPlatform:snowflake,payments,PROD)"],
        show_in_global_context=False,
    ),
]

for doc in docs:
    client.entities.upsert(doc)
    print(f"Pushed: {doc.urn}")
```

### Step 2: Trigger Semantic Indexing

To trigger the built-in embedding job immediately:

```bash
datahub graphql --query 'mutation {
  createIngestionExecutionRequest(input: {
    ingestionSourceUrn: "urn:li:dataHubIngestionSource:datahub-documents"
  })
}'
```

This kicks off the embedding pipeline, which fetches the new documents, chunks the text, generates
embeddings via your configured provider, and writes `SemanticContent` aspects back to DataHub.

:::note
The `datahub-documents` source uses incremental processing — it tracks content hashes and only
re-embeds documents whose text has changed since the last run.
:::

### Wait for Completion and Validate

The `createIngestionExecutionRequest` mutation returns an execution request URN immediately. Poll
the `executionRequest` query until `result` is non-null, then check the status:

```python
import json
import time

import requests

GMS_URL = "https://your-instance.acryl.io/api/graphql"
TOKEN = "your-token"

POLL_QUERY = """
query getExecutionStatus($urn: String!) {
  executionRequest(urn: $urn) {
    result {
      status
      durationMs
      report
    }
  }
}
"""

def wait_for_indexing(execution_urn: str, poll_interval: int = 10, timeout: int = 300) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = requests.post(
            GMS_URL,
            json={"query": POLL_QUERY, "variables": {"urn": execution_urn}},
            headers={"Authorization": f"Bearer {TOKEN}"},
        )
        result = resp.json()["data"]["executionRequest"]["result"]
        if result is None:
            print("Still running...")
            time.sleep(poll_interval)
            continue

        status = result["status"]
        report = json.loads(result["report"].split("~~~~ Ingestion Report ~~~~")[1].split("~~~~")[0].strip())
        source_report = report["source"]["report"]

        print(f"Status: {status} ({result['durationMs'] / 1000:.1f}s)")
        print(f"  Documents fetched:           {source_report['num_documents_fetched']}")
        print(f"  Documents processed:         {source_report['num_documents_processed']}")
        print(f"  Documents skipped (unchanged): {source_report['num_documents_skipped_unchanged']}")
        print(f"  Embeddings generated:        {source_report['num_embeddings_generated']}")
        print(f"  Embedding failures:          {source_report['num_embedding_failures']}")

        if status != "SUCCESS":
            raise RuntimeError(f"Embedding job failed: {source_report.get('failures')}")
        if source_report["num_embedding_failures"] > 0:
            raise RuntimeError(f"Embedding errors: {source_report['embedding_failures']}")

        return source_report

    raise TimeoutError(f"Embedding job did not complete within {timeout}s")
```

Key fields in the source report to validate:

| Field                             | Meaning                                                        |
| --------------------------------- | -------------------------------------------------------------- |
| `num_documents_processed`         | Documents that were embedded this run                          |
| `num_documents_skipped_unchanged` | Documents skipped due to unchanged content (incremental)       |
| `num_embedding_failures`          | Should be `0` — any value here means some docs weren't indexed |
| `status`                          | `SUCCESS` or `FAILURE` at the top level                        |

### Step 3: Verify Semantic Retrieval

Confirm that your documents are indexed and retrievable with a semantic query:

```bash
# Search documents semantically
datahub search --semantic "what questions can the orders dataset answer?" \
  --filter entity_type=document \
  --table

# Narrow to a specific domain once domains are configured
datahub search --semantic "daily transaction summaries" \
  --filter entity_type=document \
  --filter domain=urn:li:domain:commerce \
  --table
```

If semantic search is not yet configured, check the status first:

```bash
datahub search diagnose
```

## Python SDK Reference

The Document SDK provides the following methods:

### Creation Methods

| Method                                   | Description                                |
| ---------------------------------------- | ------------------------------------------ |
| `Document.create_document(...)`          | Create a native document stored in DataHub |
| `Document.create_external_document(...)` | Create a reference to an external document |

### Content & Metadata

| Method                                 | Description                                |
| -------------------------------------- | ------------------------------------------ |
| `doc.title` / `doc.set_title(...)`     | Get/set the document title                 |
| `doc.text` / `doc.set_text(...)`       | Get/set the document text content          |
| `doc.subtype` / `doc.set_subtype(...)` | Get/set the sub-type (FAQ, Tutorial, etc.) |
| `doc.custom_properties`                | Get the custom properties dictionary       |
| `doc.set_custom_property(key, value)`  | Set a single custom property               |

### Visibility & Lifecycle

| Method                               | Description                               |
| ------------------------------------ | ----------------------------------------- |
| `doc.status` / `doc.set_status(...)` | Get/set PUBLISHED or UNPUBLISHED status   |
| `doc.publish()` / `doc.unpublish()`  | Publish or unpublish the document         |
| `doc.show_in_global_context`         | Check if visible in global search/sidebar |
| `doc.hide_from_global_context()`     | Hide from global context (AI-only access) |
| `doc.show_in_global_search()`        | Show in global context                    |

### Relationships

| Method                                                               | Description                       |
| -------------------------------------------------------------------- | --------------------------------- |
| `doc.related_assets`                                                 | Get list of related asset URNs    |
| `doc.add_related_asset(...)` / `doc.remove_related_asset(...)`       | Add/remove a related asset        |
| `doc.related_documents`                                              | Get list of related document URNs |
| `doc.add_related_document(...)` / `doc.remove_related_document(...)` | Add/remove a related document     |
| `doc.parent_document` / `doc.set_parent_document(...)`               | Get/set parent for hierarchy      |

### Source Information

| Method             | Description                                |
| ------------------ | ------------------------------------------ |
| `doc.is_native`    | Check if this is a native DataHub document |
| `doc.is_external`  | Check if this is an external reference     |
| `doc.external_url` | Get the external URL (external docs only)  |
| `doc.external_id`  | Get the external system ID                 |

### Metadata (via mixins)

| Method                                       | Description        |
| -------------------------------------------- | ------------------ |
| `doc.add_tag(...)` / `doc.set_tags(...)`     | Add tags           |
| `doc.add_owner(...)` / `doc.set_owners(...)` | Add owners         |
| `doc.set_domain(...)`                        | Set the domain     |
| `doc.add_term(...)` / `doc.set_terms(...)`   | Add glossary terms |

For more examples, see:

- [Python SDK Examples](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/library/)
