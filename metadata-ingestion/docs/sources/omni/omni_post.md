### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Physical table lineage

Omni Views reference physical warehouse tables via `sql_table_name` in model YAML. The connector resolves each reference to a DataHub dataset URN using the `connection_to_platform` mapping. If `normalize_snowflake_names: true` (default), database, schema, and table name components are uppercased to match the casing used by the DataHub Snowflake connector.

#### Column-level lineage

When `include_column_lineage: true` (default), the connector emits `FineGrainedLineage` entries by parsing `sql` expressions in model YAML and matching field references to known view columns. This enables precise field-level impact analysis across the full chain:

```
physical_table.column → semantic_view.field → dashboard_tile.field
```

#### Schema metadata

For each Omni Semantic View, the connector emits a `SchemaMetadata` aspect containing one `SchemaField` per dimension and measure defined in model YAML:

- **Dimensions**: emitted with inferred native type (string, date, timestamp, number, boolean)
- **Measures**: emitted with aggregation type and native type `NUMBER`
- Field descriptions are extracted from the YAML `description` attribute when present

#### Model and document filtering

Use `model_pattern` and `document_pattern` to restrict ingestion to specific models or dashboards:

```yaml
model_pattern:
  allow:
    - "^prod-.*"
  deny:
    - ".*-dev$"

document_pattern:
  allow:
    - ".*"
```

### Limitations

- Access Filters, User Attributes, and Cache schedules are not yet ingested.
- Column lineage is limited to fields that appear in model YAML `sql` expressions; complex or fully derived expressions may not fully resolve.
- Large organizations with many models may approach Omni API rate limits; the connector will automatically retry on 429 responses with exponential backoff.
- True end-to-end integration tests require a live Omni environment; the test suite uses deterministic mock API responses.

### Troubleshooting

If ingestion fails, validate credentials, permissions, and connectivity first. Then review the ingestion report and logs for source-specific errors.

#### Performance and rate limiting

Ingestion performance is primarily limited by the [Omni API rate limits](https://docs.omni.co/api/rate-limits) (default: 60 requests/minute). The connector automatically handles rate limiting via server-side 429 responses with exponential backoff retry. For large Omni instances with thousands of models, expect ingestion to take several hours.

Check the logs for retry warnings (logged by tenacity) to understand if rate limiting or server errors are affecting performance. Frequent 429 retries indicate the connector is saturating the API rate limit and working as efficiently as possible.

Common issues:

| Symptom                                          | Likely Cause                                          | Resolution                                                                    |
| ------------------------------------------------ | ----------------------------------------------------- | ----------------------------------------------------------------------------- |
| `403 Forbidden` on `/v1/connections`             | API key lacks connection read scope                   | Ingestion continues with config fallbacks; physical lineage may be incomplete |
| Physical tables not linked to warehouse entities | `connection_to_platform` not configured               | Add connection mapping for each Omni connection ID                            |
| Snowflake URN mismatch                           | Case mismatch between Omni and DataHub Snowflake URNs | Ensure `normalize_snowflake_names: true` (default)                            |
| Column lineage empty                             | View YAML has no `sql` expressions                    | Expected for views using direct `sql_table_name` without field-level SQL      |
| Slow ingestion performance                       | Omni API rate limiting (60 req/min default)           | Expected for large instances; check logs for retry warnings                   |
