import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Document

## Why Would You Use Documents?

Documents in DataHub are content-indexed resources that can store knowledge, documentation, FAQs, tutorials, and other textual content. They provide a centralized place to manage and search through organizational knowledge, making them accessible to both humans and AI systems.

Documents support rich metadata including:

- **Searchable content** with full-text search capabilities
- **Categorization** via types, domains, and owners
- **AI integration** through publish/unpublish controls
- **Relationships** to other entities (datasets, charts, dashboards)
- **Version control** through parent-child relationships

For more information about documents, refer to the [knowledge.graphql schema](https://github.com/datahub-project/datahub/blob/master/datahub-graphql-core/src/main/resources/knowledge.graphql).

### Goal Of This Guide

This guide will show you how to:

- Create: create a document with content and metadata
- Update: update document contents and titles
- Search: search through documents with filters
- Retrieve: get document contents
- Publish: make documents visible to AI systems
- Delete: remove documents

## Prerequisites

For this tutorial, you need to deploy DataHub Quickstart and ingest sample data.
For detailed steps, please refer to [Datahub Quickstart Guide](/docs/quickstart.md).

## Create Document

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
      status: { state: PUBLISHED }
    }
  ) {
    urn
  }
}
```

If you see the following response, the operation was successful:

```json
{
  "data": {
    "createDocument": {
      "urn": "urn:li:document:my-tutorial-doc"
    }
  },
  "extensions": {}
}
```

You can also create documents with additional metadata like domain and owners:

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
      status: { state: PUBLISHED }
      relatedEntities: {
        assetUrns: [
          "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_table,PROD)"
        ]
      }
      owners: [{ ownerUrn: "urn:li:corpuser:john", type: TECHNICAL_OWNER }]
    }
  ) {
    urn
  }
}
```

</TabItem>
<TabItem value="python" label="Python" default>

```python
{{ inline /metadata-ingestion/examples/library/create_document.py show_path_as_comment }}
```

</TabItem>
</Tabs>

### Expected Outcomes of Creating Document

You can now see your document has been created and is searchable in DataHub.

## Update Document

Update the contents or title of an existing document.

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
mutation updateDocumentRelatedEntities {
  updateDocumentRelatedEntities(
    input: {
      urn: "urn:li:document:my-tutorial-doc"
      title: "Updated Tutorial Title"
    }
  )
}
```

If you see `true` in the response, the operation was successful.

</TabItem>
<TabItem value="python" label="Python" default>

```python
{{ inline /metadata-ingestion/examples/library/update_document.py show_path_as_comment }}
```

</TabItem>
</Tabs>

### Expected Outcomes of Updating Document

The document contents and/or title have been updated in DataHub.

## Search Documents

Search through documents with various filters like type, domain, state, and text query.

<Tabs>
<TabItem value="graphql" label="GraphQL">

```graphql
query searchDocuments {
  searchDocuments(
    input: {
      query: "data quality"
      filters: { types: ["FAQ"], states: [PUBLISHED] }
      start: 0
      count: 10
    }
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

Expected response:

```json
{
  "data": {
    "searchDocuments": {
      "total": 1,
      "documents": [
        {
          "urn": "urn:li:document:faq-data-quality",
          "type": "DOCUMENT",
          "subType": "FAQ",
          "info": {
            "title": "Data Quality FAQ",
            "status": {
              "state": "PUBLISHED"
            },
            "contents": {
              "text": "# Data Quality FAQ\n\n..."
            }
          }
        }
      ]
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

### Expected Outcomes of Searching Documents

You'll receive a list of documents matching your search criteria, with full metadata and content.

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
      description
      status {
        state
      }
      contents {
        text
      }
      created {
        time
        actor
      }
      lastModified {
        time
        actor
      }
    }
  }
}
```

Expected response:

```json
{
  "data": {
    "document": {
      "urn": "urn:li:document:my-tutorial-doc",
      "type": "DOCUMENT",
      "subType": "Tutorial",
      "info": {
        "title": "DataHub Tutorial",
        "status": {
          "state": "PUBLISHED"
        },
        "contents": {
          "text": "# Getting Started with DataHub\n\n..."
        },
        "created": {
          "time": 1234567890000,
          "actor": "urn:li:corpuser:john"
        }
      }
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

### Expected Outcomes of Getting Document

You'll receive the complete document with all metadata and content.

## Publish/Unpublish Document

Control whether a document is visible to AI systems by publishing or unpublishing it.

<Tabs>
<TabItem value="graphql" label="GraphQL">

Publish a document:

```graphql
mutation publishDocument {
  updateDocumentStatus(
    input: {
      urn: "urn:li:document:my-tutorial-doc"
      status: { state: PUBLISHED }
    }
  )
}
```

Unpublish a document:

```graphql
mutation unpublishDocument {
  updateDocumentStatus(
    input: {
      urn: "urn:li:document:my-tutorial-doc"
      status: { state: UNPUBLISHED }
    }
  )
}
```

If you see `true` in the response, the operation was successful.

</TabItem>
<TabItem value="python" label="Python" default>

```python
{{ inline /metadata-ingestion/examples/library/publish_document.py show_path_as_comment }}
```

</TabItem>
</Tabs>

### Expected Outcomes of Publishing/Unpublishing

The document's visibility to AI systems has been updated. Published documents are searchable by AI, while unpublished ones are hidden.

## Delete Document

Remove a document from DataHub.

<Tabs>
<TabItem value="graphql" label="GraphQL">

```graphql
mutation deleteDocument {
  deleteDocument(urn: "urn:li:document:my-tutorial-doc")
}
```

If you see `true` in the response, the document has been deleted successfully.

</TabItem>
<TabItem value="python" label="Python" default>

```python
{{ inline /metadata-ingestion/examples/library/delete_document.py show_path_as_comment }}
```

</TabItem>
</Tabs>

### Expected Outcomes of Deleting Document

The document has been removed from DataHub and will no longer appear in search results.

## Advanced Operations

### Update Document Sub-Type

Change the sub-type (e.g., "FAQ", "Tutorial", "Reference") of a document:

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
from datahub.api.entities.document import Document
from datahub.ingestion.graph.client import get_default_graph

graph = get_default_graph()
doc = Document(graph=graph)

# Update sub-type
result = doc.update_sub_type(
    urn="urn:li:document:my-doc",
    sub_type="Reference"
)
print(f"Sub-type updated: {result}")
```

</TabItem>
</Tabs>

### Link Related Entities

Associate a document with related assets (datasets, dashboards, etc.):

<Tabs>
<TabItem value="graphql" label="GraphQL">

```graphql
mutation updateRelatedEntities {
  updateDocumentRelatedEntities(
    input: {
      urn: "urn:li:document:my-doc"
      relatedEntities: {
        assetUrns: [
          "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_table,PROD)"
          "urn:li:dashboard:(looker,dashboard1)"
        ]
        documentUrns: ["urn:li:document:related-doc"]
      }
    }
  )
}
```

</TabItem>
<TabItem value="python" label="Python" default>

```python
from datahub.api.entities.document import Document
from datahub.ingestion.graph.client import get_default_graph

graph = get_default_graph()
doc = Document(graph=graph)

# Link related entities
result = doc.update_related_entities(
    urn="urn:li:document:my-doc",
    related_asset_urns=[
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_table,PROD)",
        "urn:li:dashboard:(looker,dashboard1)"
    ],
    related_document_urns=["urn:li:document:related-doc"]
)
print(f"Related entities updated: {result}")
```

</TabItem>
</Tabs>

## Python SDK Reference

For complete Python SDK documentation, including all available methods and parameters, see:

- [Document SDK Quickstart](/docs/cli-commands/document.md#python-sdk-usage)
- [Python SDK Examples](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/library/)

## CLI Reference

For command-line operations, see the [Document CLI Reference](/docs/cli-commands/document.md).
