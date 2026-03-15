### Capabilities

#### Database and Platform Mapping

Metabase databases are mapped to a DataHub platform based on the engine field returned by [`/api/database`](https://www.metabase.com/docs/latest/api-documentation.html#database). Override with `engine_platform_map`:

```yaml
engine_platform_map:
  athena: glue
```

DataHub determines the database name from the same API response. Override with `database_alias_map`:

```yaml
database_alias_map:
  postgres: my_custom_db_name
```

#### Platform Instance Mapping

When multiple instances of the same platform exist in DataHub (for example, two ClickHouse clusters), map Metabase database IDs to platform instances with `database_id_to_instance_map`:

```yaml
database_id_to_instance_map:
  "42": platform_instance_in_datahub
```

The key must be a string, not an integer.

If `database_id_to_instance_map` is not set, `platform_instance_map` is used as a fallback. If neither is set, platform instance is omitted from dataset URNs.

#### Column-Level Lineage

Column-level lineage is extracted for **models built with the visual query builder (MBQL)**. It is not available for:

- Native SQL questions or models (SQL is parsed for table-level lineage only)
- MBQL questions (as opposed to models) — `result_metadata.field_ref` is only reliable on saved models

#### Filtering Collections

To exclude collections owned by other users:

```yaml
exclude_other_user_collections: true
```

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

- Column-level lineage requires the card to be saved as a **Model** (type `"model"`). Regular questions do not expose reliable `field_ref` data.
- Template variables in native SQL queries (`{{variable}}`, `[[optional clause]]`) are stripped before parsing, so lineage based on dynamic table references may be incomplete.
- Circular card references are cut off at depth 5; deeply nested card chains beyond that limit will not have lineage extracted.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.

- **Auth failure**: ensure the API key or username/password is correct and the account has access to the relevant collections.
- **Missing lineage**: check that `extract_models: true` is set and that the cards are saved as Models in Metabase.
- **Unknown platform warnings**: add an entry to `engine_platform_map` for the unrecognised engine name.
