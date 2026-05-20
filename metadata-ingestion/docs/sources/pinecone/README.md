## Overview

Pinecone is a managed vector database platform used to store, index, and query high-dimensional vector embeddings for AI and machine learning applications. Learn more in the [official Pinecone documentation](https://docs.pinecone.io/).

The DataHub integration for Pinecone extracts metadata about indexes, namespaces, and vector collections, including inferred schemas from vector metadata fields.

## Concept Mapping

| Source Concept    | DataHub Concept                | Notes                                                |
| ----------------- | ------------------------------ | ---------------------------------------------------- |
| Pinecone Account  | Platform Instance              | Organizes assets within the platform context.        |
| Index             | Container (PINECONE_INDEX)     | Top-level organizational unit storing vectors.       |
| Namespace         | Container (PINECONE_NAMESPACE) | Logical partition within an index.                   |
| Vector Collection | Dataset                        | Represents the collection of vectors in a namespace. |
| Metadata Fields   | SchemaField                    | Inferred from sampled vector metadata.               |
