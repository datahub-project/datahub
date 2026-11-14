### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Optional View Lineage

View lineage is disabled by default. Metadata ingestion and profiling work without changing the SQLGlot runtime.

To extract table-level and column-level lineage from view definitions:

1. Install `acryl-datahub[ydb]`, which includes `ydb-sqlglot-plugin`.
2. Set `include_view_lineage: true` in your YDB source configuration. Column-level lineage is enabled by `include_view_column_lineage` when view lineage is enabled.
3. Start the ingestion process with the pure-Python SQLGlot runtime:

   ```bash
   DATAHUB_SQLGLOT_DISABLE_C=1 datahub ingest -c ydb_recipe.yml
   ```

Set the environment variable before the Python process starts. Adding it to the recipe or changing it after SQLGlot has been imported is insufficient. The flag affects all SQLGlot dialects in that process; configure it for the YDB ingestion process rather than for unrelated ingestion jobs.

The YDB plugin cannot use SQLGlot's compiled C extensions. If you explicitly enable lineage without a compatible runtime, configuration validation fails with instructions to select pure-Python SQLGlot or disable lineage. The plugin documents this restriction in its [installation guide](https://github.com/ydb-platform/ydb-sqlglot-plugin#installation).

### Limitations

- **No schema layer.** YDB organizes tables in a directory tree rather than schemas. Directory paths are part of the table name (for example, `jaffle_shop/customers`), and a connection is bound to a single database, so ingestion targets one `database` at a time.
- **No foreign keys.** YDB does not expose foreign-key constraints, so no foreign-key lineage is emitted.
- **View lineage depends on SQL parsing.** Unsupported YQL constructs may prevent lineage extraction for a view. Review `sql_aggregator.views_parse_failures` in the ingestion report when an expected dependency is missing.

### Troubleshooting

If ingestion fails, validate connectivity to the endpoint, the `database` path, and read permissions first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
