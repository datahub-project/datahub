## Overview

DataHub Entity Embeddings generates semantic search embeddings for the catalog entities already stored in DataHub, such as datasets, dashboards, glossary terms and domains: the entity types enabled for semantic search on the server, except operational search groups (for example `timeseries`), which are opt-in through `search_groups`. It complements the [DataHub Documents](datahub-documents.md) source, which owns Document entities.

For each entity, it renders the searchable fields declared in the server's entity registry as text, embeds it with the server's embedding configuration, and writes the result to the entity's `semanticContent` aspect. Unchanged entities are skipped on later runs.

## Concept Mapping

| Source Concept                                   | DataHub Concept      | Notes                                                                     |
| ------------------------------------------------ | -------------------- | ------------------------------------------------------------------------- |
| Entity enabled for semantic search               | Eligible entity type | From `semanticSearchConfig.enabledEntities`, filtered by `search_groups`. |
| Searchable fields (name, description, tags, ...) | Embedded text chunks | Taken from the entity registry's `@Searchable` annotations.               |
| Embedding vectors                                | `semanticContent`    | Written under the embedding model key from the server's configuration.    |
