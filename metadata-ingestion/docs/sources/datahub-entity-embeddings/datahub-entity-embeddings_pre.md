### Overview

The DataHub Entity Embeddings source makes catalog entities findable through semantic search. It reads entities from DataHub, turns their searchable metadata into text, and stores embeddings in their `semanticContent` aspect. Document entities are left to the [DataHub Documents](datahub-documents.md) source.

Entity types and their searchable fields come from the server, so enabling a new entity type for semantic search needs no change to this source. See Capabilities for how entities are processed.

### Prerequisites

#### 1. DataHub Server Configuration

Semantic search must be enabled on the server with an embedding provider configured. See the [Semantic Search Configuration Guide](../../../how-to/semantic-search-configuration.md).

`ELASTICSEARCH_SEMANTIC_SEARCH_ENTITIES` is the list of entity types this source embeds. Add the types you want next to `document`, and set the same value on GMS, the MAE consumer (if it runs standalone) and the `system-update` job, which creates the semantic index of each type:

```bash
ELASTICSEARCH_SEMANTIC_SEARCH_ENTITIES=document,dataset,glossaryTerm
```

The source reads this list from the server at the start of each run, so adding a type later only needs this change and the plugin entry below; the recipe stays the same.

#### 2. The `semanticContent` Aspect

Embeddings are stored in the `semanticContent` aspect. The base entity registry declares it for `document`, `service`, `api`, `repository`, `aiAgent` and `agentSkill`. Every other type in `ELASTICSEARCH_SEMANTIC_SEARCH_ENTITIES`, such as `dataset` or `glossaryTerm`, needs it added with an [entity registry plugin](https://docs.datahub.com/docs/metadata-models-custom). Without it, the source skips the type and reports why. The plugin needs no code, only a YAML file:

```yaml
id: semantic-search
entities:
  - name: dataset
    aspects:
      - semanticContent
  - name: glossaryTerm
    aspects:
      - semanticContent
```

Mount it as a file whose path is the plugin version, `/etc/datahub/plugins/models/semantic-search/1.0.0`, rather than as `entity-registry.yaml` inside that directory. A directory is loaded as a plugin with its own model classes, so a YAML-only plugin cannot see core aspects and fails with `Aspect semanticContent does not exist`. In Kubernetes, use a ConfigMap with `subPath`:

```yaml
volumeMounts:
  - name: semantic-registry-plugin
    mountPath: /etc/datahub/plugins/models/semantic-search/1.0.0
    subPath: entity-registry.yaml
    readOnly: true
```

Mount it on every process that loads the entity registry: GMS, the MAE and MCE consumers if they run standalone, and the `system-update` job. GMS reports the plugin under `models` in `/config`, with `loadResult: SUCCESS`.

#### 3. Privileges

The token used by the source needs:

- **Manage System Operations**, to read the entity registry (`/openapi/v1/registry/models/entity/specifications`).
- Read access to the entities to embed, and permission to write their `semanticContent` aspect.

#### 4. Embedding Provider Credentials

The same credentials as the `datahub-documents` source, for example AWS credentials with `bedrock:InvokeModel` when using AWS Bedrock.
