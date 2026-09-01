### Capabilities

#### Lineage

The connector extracts lineage across all Metabase query types:

##### Native SQL

SQL queries are parsed with DataHub's SQLGlot-based parser to extract table references, including those inside `JOIN` clauses and subqueries. Metabase template variables (`{{variable}}`, `[[WHERE ...]]`) are stripped before parsing.

##### Query Builder (MBQL)

Questions and models built with Metabase's visual query builder store their logic as MBQL — a structured JSON representation. The connector resolves MBQL to upstream database tables and, for **models**, also produces column-level lineage:

- **Table-level**: The `source-table` field and all `joins[].source-table` entries are resolved to DataHub dataset URNs, covering multi-table join scenarios.
- **Column-level** (models only): `result_metadata[].field_ref` records which MBQL expression produced each output column. The connector resolves these refs to upstream field URNs via `/api/field/{id}`:
  - `["field", id, ...]` — direct pass-through column
  - `["expression", name]` — calculated column; traces back through `query.expressions`
  - `["aggregation", index]` — metric column; traces back through `query.aggregation`. `COUNT(*)` with no explicit field fans-in all resolved upstream columns (matching Tableau lineage behaviour).

##### Nested Queries

Charts or models that reference other cards (`source-table: "card__456"`) are recursively resolved to their ultimate source tables (max depth: 5, to guard against circular references).

##### Dashboard Lineage

Table dependencies from all charts in a dashboard are rolled up into direct table-to-dashboard lineage edges, deduplicating tables referenced by multiple charts.

#### Collection Tags

Metabase Collections are mapped to DataHub tags:

- Tag format: `metabase_collection_{sanitized_name}` (e.g. "Sales & Marketing" → `metabase_collection_sales_marketing`)
- Tags are applied to dashboards, charts, and models within that collection
- Disable with `extract_collections_as_tags: false`

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

#### Filtering Collections

To exclude collections owned by other users:

```yaml
exclude_other_user_collections: true
```

### Limitations

- Column-level lineage is only available for cards saved as a **Model** (type `"model"`). Regular MBQL questions do not expose reliable `field_ref` data.
- Template variables in native SQL queries (`{{variable}}`, `[[optional clause]]`) are stripped before parsing, so lineage based on dynamic table references may be incomplete.
- Circular card references are cut off at depth 5; deeply nested card chains beyond that limit will not have lineage extracted.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.

- **Auth failure**: ensure the API key or username/password is correct and the account has access to the relevant collections.
- **Missing lineage**: check that `extract_models: true` is set and that the cards are saved as Models in Metabase.
- **Unknown platform warnings**: add an entry to `engine_platform_map` for the unrecognised engine name.
