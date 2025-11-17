# DataHub Document Command

The `document` command allows you to interact with Document entities in DataHub. Documents are content-indexed resources that can store knowledge, documentation, FAQs, tutorials, and other textual content. They provide a centralized place to manage and search through organizational knowledge.

## Commands

### create

Create a new document in DataHub.

```shell
datahub document create --text TEXT [OPTIONS]
```

**Options:**

- `--text TEXT` - The text content of the document (required if not using `--file`)
- `--file, -f FILENAME` - Read text content from a file instead of `--text`
- `--title TEXT` - Optional title for the document
- `--id TEXT` - Optional custom ID for the document (defaults to auto-generated UUID)
- `--sub-type TEXT` - Optional sub-type (e.g., "FAQ", "Tutorial", "Reference")
- `--state [published|unpublished]` - Document state (default: UNPUBLISHED)
- `--parent TEXT` - Optional URN of the parent document
- `--related-asset TEXT` - Optional related asset URN (can be specified multiple times)
- `--related-document TEXT` - Optional related document URN (can be specified multiple times)
- `--owner TEXT` - Optional owner in format "urn:type" (e.g., "urn:li:corpuser:user1:TECHNICAL_OWNER")

**Examples:**

```shell
# Create a simple document
datahub document create --text "# My Document\n\nContent here" --title "My Document"

# Create from a file
datahub document create --file README.md --title "README" --sub-type "Reference"

# Create with full metadata
datahub document create \
  --text "# Data Quality FAQ\n\nQ: How do we measure quality?" \
  --title "Data Quality FAQ" \
  --sub-type "FAQ" \
  --state published \
  --owner "urn:li:corpuser:john:TECHNICAL_OWNER" \
  --related-asset "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_table,PROD)"

# Create a published tutorial
datahub document create \
  --file tutorial.md \
  --title "Getting Started with DataHub" \
  --sub-type "Tutorial" \
  --state published
```

### update

Update the contents and/or title of an existing document.

```shell
datahub document update URN [OPTIONS]
```

**Options:**

- `--text TEXT` - The new text content
- `--file, -f FILENAME` - Read new text content from a file
- `--title TEXT` - The new title

**Examples:**

```shell
# Update document text
datahub document update "urn:li:document:abc-123" --text "Updated content"

# Update from a file
datahub document update "urn:li:document:abc-123" --file updated.md

# Update title only
datahub document update "urn:li:document:abc-123" --title "New Title"

# Update both text and title
datahub document update "urn:li:document:abc-123" \
  --text "New content" \
  --title "New Title"
```

### get

Get a document by its URN.

```shell
datahub document get URN [--format FORMAT]
```

**Options:**

- `--format [json|text]` - Output format (default: text)

**Examples:**

```shell
# Get document in text format (default)
datahub document get "urn:li:document:abc-123"

# Get document in JSON format
datahub document get "urn:li:document:abc-123" --format json
```

**Sample Output (text format):**

```
URN: urn:li:document:abc-123
Type: DOCUMENT
Sub-Type: Tutorial
Title: Getting Started
Status: PUBLISHED
Created: 2024-01-15 10:30:00

Contents:
# Getting Started

Welcome to this tutorial...
```

### search

Search for documents.

```shell
datahub document search [OPTIONS]
```

**Options:**

- `--query TEXT` - Search query string
- `--type TEXT` - Filter by document sub-type (can be specified multiple times)
- `--state [published|unpublished]` - Filter by state (can be specified multiple times)
- `--domain TEXT` - Filter by domain URN (can be specified multiple times)
- `--owner TEXT` - Filter by owner URN (can be specified multiple times)
- `--start INTEGER` - Starting offset (default: 0)
- `--count INTEGER` - Number of results (default: 10)
- `--format [json|text|table]` - Output format (default: table)

**Examples:**

```shell
# Search all documents
datahub document search

# Search with query
datahub document search --query "data quality"

# Search by type
datahub document search --type "FAQ" --type "Tutorial"

# Search published documents only
datahub document search --state published

# Search by domain
datahub document search --domain "urn:li:domain:engineering"

# Search by owner
datahub document search --owner "urn:li:corpuser:john"

# Complex search with pagination
datahub document search \
  --query "machine learning" \
  --type "Tutorial" \
  --state published \
  --count 20 \
  --format json

# Search in table format (default)
datahub document search --query "API" --format table
```

**Sample Output (table format):**

```
Found 3 document(s)

URN                          Title              Type      Status
---------------------------  -----------------  --------  -----------
urn:li:document:doc1         API Guide          Tutorial  PUBLISHED
urn:li:document:doc2         API FAQ            FAQ       PUBLISHED
urn:li:document:doc3         API Reference      Reference UNPUBLISHED
```

### list

List documents (alias for search with default parameters).

```shell
datahub document list [OPTIONS]
```

**Options:**

- `--state [published|unpublished]` - Filter by state (can be specified multiple times)
- `--count INTEGER` - Number of results (default: 100)
- `--format [json|text|table]` - Output format (default: table)

**Examples:**

```shell
# List all documents
datahub document list

# List published documents only
datahub document list --state published

# List with custom count
datahub document list --count 50
```

### publish

Publish a document (make it visible to AI systems).

```shell
datahub document publish URN
```

**Example:**

```shell
datahub document publish "urn:li:document:abc-123"
```

### unpublish

Unpublish a document (make it not visible to AI systems).

```shell
datahub document unpublish URN
```

**Example:**

```shell
datahub document unpublish "urn:li:document:abc-123"
```

### delete

Delete a document.

```shell
datahub document delete URN [--force]
```

**Options:**

- `--force` - Skip confirmation prompt

**Examples:**

```shell
# Delete with confirmation
datahub document delete "urn:li:document:abc-123"

# Delete without confirmation
datahub document delete "urn:li:document:abc-123" --force
```

## Python SDK Usage

The Document CLI is built on top of the Python SDK, which provides programmatic access to all document operations.

### Installation

```shell
pip install 'acryl-datahub[datahub-rest]'
```

### Basic Usage

```python
from datahub.api.entities.document import Document
from datahub.ingestion.graph.client import get_default_graph

# Initialize the graph client
graph = get_default_graph()

# Create a Document API instance
doc_api = Document(graph=graph)

# Create a document
urn = doc_api.create(
    text="# My Document\n\nContent here",
    title="My Document",
    sub_type="Tutorial",
    state="PUBLISHED"
)
print(f"Created: {urn}")

# Update a document
doc_api.update(
    urn=urn,
    text="# Updated Document\n\nNew content"
)

# Search documents
results = doc_api.search(
    query="tutorial",
    types=["Tutorial"],
    states=["PUBLISHED"]
)
print(f"Found {results['total']} documents")

# Get a document
document = doc_api.get(urn=urn)
print(document['info']['title'])

# Publish/unpublish
doc_api.publish(urn=urn)
doc_api.unpublish(urn=urn)

# Delete a document
doc_api.delete(urn=urn)
```

### Advanced Operations

```python
# Create with full metadata
urn = doc_api.create(
    text="# Data Quality Guide",
    title="Data Quality Guide",
    id="dq-guide-2024",
    sub_type="Reference",
    state="PUBLISHED",
    domain_urn="urn:li:domain:engineering",
    related_asset_urns=[
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_table,PROD)"
    ],
    related_document_urns=["urn:li:document:related-doc"],
    owners=[
        {
            "urn": "urn:li:corpuser:john",
            "type": "TECHNICAL_OWNER"
        }
    ]
)

# Update sub-type
doc_api.update_sub_type(
    urn=urn,
    sub_type="Tutorial"
)

# Update related entities
doc_api.update_related_entities(
    urn=urn,
    related_asset_urns=[
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,my_table,PROD)"
    ]
)

# Move document (change parent)
doc_api.move(
    urn=urn,
    parent_urn="urn:li:document:parent-doc"
)

# Check existence
if doc_api.exists(urn=urn):
    print("Document exists")
```

### Error Handling

```python
from datahub.api.entities.document import (
    Document,
    DocumentValidationError,
    DocumentOperationError,
    DocumentNotFoundError
)

try:
    doc_api = Document(graph=graph)
    urn = doc_api.create(text="", title="Empty")
except DocumentValidationError as e:
    print(f"Validation error: {e}")
except DocumentOperationError as e:
    print(f"Operation failed: {e}")
except DocumentNotFoundError as e:
    print(f"Document not found: {e}")
```

## Document Structure

Documents in DataHub have the following key properties:

### Core Fields

- **URN** - Unique identifier (format: `urn:li:document:{id}`)
- **Type** - Always "DOCUMENT"
- **Sub-Type** - Optional categorization (e.g., "FAQ", "Tutorial", "Reference")
- **Title** - Display name for the document
- **Contents** - Text content (supports markdown)
- **Status** - State field controlling visibility:
  - `PUBLISHED` - Visible to AI systems
  - `UNPUBLISHED` - Hidden from AI systems

### Metadata

- **Description** - Optional description
- **Domain** - Associated domain URN
- **Owners** - List of owners with ownership types
- **Created/Modified** - Timestamps and actors
- **Parent** - Optional parent document URN for hierarchies

### Relationships

- **Related Assets** - URNs of related datasets, dashboards, etc.
- **Related Documents** - URNs of related documents

## Use Cases

### Documentation Hub

Create a centralized documentation repository:

```shell
# Create main documentation
datahub document create \
  --file docs/index.md \
  --title "Documentation Home" \
  --state published

# Create category documents
datahub document create \
  --file docs/getting-started.md \
  --title "Getting Started" \
  --sub-type "Tutorial" \
  --state published

datahub document create \
  --file docs/faq.md \
  --title "Frequently Asked Questions" \
  --sub-type "FAQ" \
  --state published
```

### Knowledge Base for AI

Build a knowledge base that AI systems can query:

```shell
# Create and publish knowledge documents
for file in knowledge/*.md; do
  datahub document create \
    --file "$file" \
    --title "$(basename $file .md)" \
    --sub-type "Reference" \
    --state published
done

# Search published documents
datahub document search --state published --format json
```

### Asset Documentation

Link documentation to data assets:

```shell
# Create documentation for a dataset
datahub document create \
  --file dataset-guide.md \
  --title "Sales Dataset Guide" \
  --sub-type "Reference" \
  --related-asset "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales,PROD)" \
  --owner "urn:li:corpuser:data-team:TECHNICAL_OWNER" \
  --state published
```

## Configuration

The Document CLI uses the standard DataHub CLI configuration. Configure your DataHub connection:

```shell
# Set DataHub GMS endpoint
export DATAHUB_GMS_URL=http://localhost:8080

# Or use a config file
datahub init
```

For more information on configuration, see the [DataHub CLI documentation](/docs/cli).

## See Also

- [Document API Tutorial](/docs/api/tutorials/documents.md) - Comprehensive API guide with examples
- [GraphQL API Reference](https://github.com/datahub-project/datahub/blob/master/datahub-graphql-core/src/main/resources/knowledge.graphql) - GraphQL schema for documents
- [Python SDK Examples](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion/examples/library) - More code examples
