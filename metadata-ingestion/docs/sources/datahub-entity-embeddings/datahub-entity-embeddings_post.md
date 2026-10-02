### Capabilities

- **Entity types from the server.** By default (`entity_types: [auto]`) the source embeds every type in the server's `semanticSearchConfig.enabledEntities`, except `document`. A type is skipped, and the reason is recorded in the report, when it is not in the entity registry, has no `semanticContent` aspect, has no searchable text fields, or belongs to a search group outside `search_groups`.
- **Platform filtering.** `platform_pattern` matches the platform from the `dataPlatformInstance` aspect, or else the platform in the entity's URN. Entities without a platform, such as tags, domains and glossary terms, are not filtered.
- **Incremental runs.** Only new or changed entities are embedded again. Changing the embedding model or the chunking settings re-embeds everything.
- **Bounded runs.** `max_entities_per_run` caps the entities embedded per run, and `time_budget_seconds` caps the run time; both stop a run cleanly with its state committed. `max_consecutive_failures` aborts a run when the embedding provider keeps failing. `index_delay_seconds` spaces out writes to GMS.

#### How it works

1. Reads the entity types enabled for semantic search from the server, and the searchable fields of each type from the server's entity registry.
2. Scrolls each type, fetching the aspects that hold searchable fields, plus `siblings` (when `text.include_siblings` is on) and `dataPlatformInstance` for platform filtering. `platform_pattern` filters entities by platform.
3. Renders those fields as markdown: names, descriptions, tags, glossary terms, domains, schema fields and so on, plus custom properties when `text.include_custom_properties` is on. Referenced tags, terms, domains and similar entities are resolved to their names; owners and lineage are left out.
4. Embeds the text with the server's embedding configuration, the same one `datahub-documents` uses, and emits `semanticContent` through the pipeline's sink. An entity without indexable text gets a skip marker instead.

#### Search groups

On registries that assign entity types to search groups, only types in `search_groups` (default `primary`) are embedded; operational groups such as `timeseries` or `query` must be added explicitly. Types without a search group, which is the case for every type in the base registry, are not filtered by `search_groups`.

#### Incremental processing

Stateful ingestion, enabled by default, stores a hash of each entity's text and embedding configuration, so only new or changed entities are embedded again. When `max_entities_per_run` or `time_budget_seconds` stops a run, the next run continues where it stopped. Entities that fail, including during a forced re-embed, are retried on the next run.

### Limitations

- An entity that is later excluded, by entity type or by `platform_pattern`, keeps its last embeddings.
- Referenced entities and siblings are read one at a time, which adds requests on large catalogs.
- Entities whose text is empty or yields no chunks get a skip marker instead of embeddings, like in `datahub-documents`.
- `max_entities_per_run` counts embedded entities only; unchanged and filtered entities are still scanned, so it does not bound the run time on its own.

### Troubleshooting

- **"Missing privilege"**: the token cannot read the entity registry. Grant it the Manage System Operations privilege.
- **"Semantic search disabled"**: semantic search is not enabled on the server. See the [Semantic Search Configuration Guide](../../../how-to/semantic-search-configuration.md).
- **"No embedding model"**: no embedding provider and model could be resolved from either the server's semantic search configuration or the recipe's `embedding` section. Check both: an `embedding` section in the recipe takes precedence and is validated against the server's configuration unless `embedding.allow_local_embedding_config` is set.
- **"Entity type skipped"**: the report lists each skipped type with its reason. To embed a type that lacks the `semanticContent` aspect, add the aspect with an entity registry plugin (see Prerequisites).
- **"Too many consecutive embedding failures"**: the embedding provider is failing, for example because of credentials, quota or an unknown model. Entities that failed are retried on the next run.
