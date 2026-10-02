### Overview

The DataHub Entity Embeddings source makes catalog entities findable through semantic search. It reads entities from DataHub, turns their searchable metadata into text, and stores embeddings in their `semanticContent` aspect. Document entities are left to the [DataHub Documents](datahub-documents.md) source.

#### How it works

1. Reads the entity types enabled for semantic search from the server, and the searchable fields of each type from the server's entity registry.
2. Scrolls each type, fetching only the aspects that hold searchable fields. `platform_pattern` filters entities by platform.
3. Renders those fields as markdown: names, descriptions, tags, glossary terms, owners, domains, schema fields, custom properties and so on. Referenced entities are resolved to their names.
4. Embeds the text with the server's embedding configuration, the same one `datahub-documents` uses, and emits `semanticContent` through the pipeline's sink. An entity without indexable text gets a skip marker instead.

Because types and fields come from the server, enabling a new entity type for semantic search needs no change to this source.

#### Incremental processing

Stateful ingestion, enabled by default, stores a hash of each entity's text and embedding configuration, so only new or changed entities are embedded again. `max_entities_per_run` and `time_budget_seconds` bound a run; the next run continues where it stopped. Entities that fail are retried on the next run.

### Prerequisites

#### 1. DataHub Server Configuration

Semantic search must be enabled on the server, with an embedding provider configured and the entity types to embed listed in `ELASTICSEARCH_SEMANTIC_SEARCH_ENTITIES`. See the [Semantic Search Configuration Guide](../../../how-to/semantic-search-configuration.md).

#### 2. The `semanticContent` Aspect

Embeddings are stored in the `semanticContent` aspect. The base entity registry declares it only for `document` and a few AI-related entity types, so other types need it added with an [entity registry plugin](https://docs.datahub.com/docs/metadata-models-custom), loaded by GMS and the consumers:

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
