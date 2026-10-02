### Overview

The DataHub Entity Embeddings source makes catalog entities findable through semantic search. It reads entities from DataHub, turns their searchable metadata into text, and stores embeddings in their `semanticContent` aspect. Document entities are left to the [DataHub Documents](datahub-documents.md) source.

Entity types and their searchable fields come from the server, so enabling a new entity type for semantic search needs no change to this source. See Capabilities for how entities are processed.

### Prerequisites

#### 1. DataHub Server Configuration

Semantic search must be enabled on the server, with an embedding provider configured and the entity types to embed listed in `ELASTICSEARCH_SEMANTIC_SEARCH_ENTITIES`. See the [Semantic Search Configuration Guide](../../../how-to/semantic-search-configuration.md).

#### 2. The `semanticContent` Aspect

Embeddings are stored in the `semanticContent` aspect. The base entity registry declares it for `document`, `service`, `api`, `repository`, `aiAgent` and `agentSkill`. To embed any other type, such as datasets or glossary terms, add the aspect with an [entity registry plugin](https://docs.datahub.com/docs/metadata-models-custom), loaded by GMS and the consumers:

```yaml
# plugins/models/semantic-search/1.0.0/entity-registry.yaml
id: semantic-search
entities:
  - name: dataset
    aspects:
      - semanticContent
  - name: glossaryTerm
    aspects:
      - semanticContent
```

#### 3. Privileges

The token used by the source needs:

- **Manage System Operations**, to read the entity registry (`/openapi/v1/registry/models/entity/specifications`).
- Read access to the entities to embed, and permission to write their `semanticContent` aspect.

#### 4. Embedding Provider Credentials

The same credentials as the `datahub-documents` source, for example AWS credentials with `bedrock:InvokeModel` when using AWS Bedrock.
