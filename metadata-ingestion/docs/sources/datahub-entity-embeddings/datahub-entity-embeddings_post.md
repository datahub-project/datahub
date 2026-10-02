### Capabilities

- **Entity types from the server.** By default (`entity_types: [auto]`) the source embeds every type in the server's `semanticSearchConfig.enabledEntities`, except `document`. A type is skipped, and the reason is recorded in the report, when it is not in the entity registry, has no `semanticContent` aspect, has no searchable text fields, or belongs to a search group outside `search_groups`.
- **Platform filtering.** `platform_pattern` matches the platform from the `dataPlatformInstance` aspect, or else the platform in the entity's URN. Entities without a platform, such as tags, domains and glossary terms, are not filtered.
- **Incremental runs.** Only new or changed entities are embedded again. Changing the embedding model or the chunking settings re-embeds everything.
- **Bounded runs.** `max_entities_per_run` and `time_budget_seconds` stop a run cleanly with its state committed. `max_consecutive_failures` aborts a run when the embedding provider keeps failing. `index_delay_seconds` spaces out writes to GMS.

### Limitations

- An entity that is later excluded, by entity type or by `platform_pattern`, keeps its last embeddings.
- Referenced entities and siblings are read one at a time, which adds requests on large catalogs.
- Entities whose text is empty or yields no chunks get a skip marker instead of embeddings, like in `datahub-documents`.

### Troubleshooting

- **"Missing privilege"**: the token cannot read the entity registry. Grant it the Manage System Operations privilege.
- **"Semantic search disabled"** or **"No embedding model"**: semantic search or the embedding provider is not configured on the server. See the [Semantic Search Configuration Guide](../../../how-to/semantic-search-configuration.md).
- **"Entity type skipped"**: the report lists each skipped type with its reason. To embed a type that lacks the `semanticContent` aspect, add the aspect with an entity registry plugin (see Prerequisites).
- **"Too many consecutive embedding failures"**: the embedding provider is failing, for example because of credentials, quota or an unknown model. Entities that failed are retried on the next run.
