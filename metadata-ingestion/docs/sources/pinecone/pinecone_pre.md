### Overview

The `pinecone` module ingests metadata from Pinecone vector database into DataHub. It extracts index configurations, namespace statistics, and infers schemas from vector metadata fields.

### Prerequisites

Before running ingestion, ensure you have a valid Pinecone API key with read access to your indexes.

#### Steps to Get the Required Information

1. Log in to the [Pinecone Console](https://app.pinecone.io/).
2. Navigate to **API Keys** in the left sidebar.
3. Copy an existing API key or create a new one with read permissions.

:::note

Schema inference samples vectors from each namespace to build a schema. This requires that your vectors have metadata fields attached. If vectors have no metadata, schema inference is skipped gracefully.

:::
