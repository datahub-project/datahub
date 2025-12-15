import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

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
For detailed steps, please refer to [Datahub Quickstart Guide](/docs/quickstart.md).

## Create Document

### Native Document

Native documents are stored directly in DataHub with full content indexing.

<Tabs>
<TabItem value="graphql" label="GraphQL">

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

</TabItem>
<TabItem value="python" label="Python" default>

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

</TabItem>
</Tabs>

### External Document

External documents reference content stored in other platforms like Notion or Confluence.

<Tabs>
<TabItem value="graphql" label="GraphQL">

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

</TabItem>
<TabItem value="python" label="Python" default>

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

</TabItem>
</Tabs>

### Document Hidden from Global Context

Documents can be hidden from global search and sidebar navigation. They remain accessible through related assets - useful for AI agent context or asset-specific documentation.

<Tabs>
<TabItem value="graphql" label="GraphQL">

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

</TabItem>
<TabItem value="python" label="Python" default>

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

</TabItem>
</Tabs>

### Document with Full Metadata

<Tabs>
<TabItem value="graphql" label="GraphQL">

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

</TabItem>
<TabItem value="python" label="Python" default>

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

</TabItem>
</Tabs>

## Update Document

Update the contents, title, or visibility of an existing document.

<Tabs>
<TabItem value="graphql" label="GraphQL">

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

</TabItem>
<TabItem value="python" label="Python" default>

```python
{{ inline /metadata-ingestion/examples/library/update_document.py show_path_as_comment }}
```

</TabItem>
</Tabs>

## Search Documents

Search through documents with various filters.

<Tabs>
<TabItem value="graphql" label="GraphQL">

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

</TabItem>
<TabItem value="python" label="Python" default>

```python
{{ inline /metadata-ingestion/examples/library/search_documents.py show_path_as_comment }}
```

</TabItem>
</Tabs>

## Get Document

Retrieve the full contents and metadata of a specific document.

<Tabs>
<TabItem value="graphql" label="GraphQL">

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

</TabItem>
<TabItem value="python" label="Python" default>

```python
{{ inline /metadata-ingestion/examples/library/get_document.py show_path_as_comment }}
```

</TabItem>
</Tabs>

## Publish/Unpublish Document

Control whether a document is visible to users.

<Tabs>
<TabItem value="graphql" label="GraphQL">

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

</TabItem>
<TabItem value="python" label="Python" default>

```python
{{ inline /metadata-ingestion/examples/library/publish_document.py show_path_as_comment }}
```

</TabItem>
</Tabs>

## Delete Document

Remove a document from DataHub.

<Tabs>
<TabItem value="graphql" label="GraphQL">

```graphql
mutation deleteDocument {
  deleteDocument(urn: "urn:li:document:my-tutorial-doc")
}
```

</TabItem>
<TabItem value="python" label="Python" default>

```python
{{ inline /metadata-ingestion/examples/library/delete_document.py show_path_as_comment }}
```

</TabItem>
</Tabs>

## Advanced Operations

### Link Related Assets

Associate a document with data assets. Documents linked to assets can be accessed from those assets even when hidden from global context.

<Tabs>
<TabItem value="graphql" label="GraphQL">

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

</TabItem>
<TabItem value="python" label="Python" default>

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

</TabItem>
</Tabs>

### Update Document Sub-Type

Change the sub-type (e.g., "FAQ", "Tutorial", "Runbook") of a document:

<Tabs>
<TabItem value="graphql" label="GraphQL">

```graphql
mutation updateDocumentSubType {
  updateDocumentSubType(
    input: { urn: "urn:li:document:my-doc", subType: "Reference" }
  )
}
```

</TabItem>
<TabItem value="python" label="Python" default>

```python
from datahub.sdk import DataHubClient, Document

client = DataHubClient.from_env()

doc = client.entities.get("urn:li:document:my-doc", Document)
if doc:
    doc.set_subtype("Reference")
    client.entities.upsert(doc)
    print(f"Sub-type updated: {doc.subtype}")
```

</TabItem>
</Tabs>

### Move Document

Move a document to a different parent (for hierarchical organization):

<Tabs>
<TabItem value="graphql" label="GraphQL">

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

</TabItem>
<TabItem value="python" label="Python" default>

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

</TabItem>
</Tabs>

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
