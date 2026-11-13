### Capabilities

#### Lineage

When `extract_lineage` is enabled, the connector parses each table's M/Power Query partition
expression (including `Value.NativeQuery` native SQL) to build upstream table lineage. Use
`server_to_platform_instance` to map the servers named in those expressions to the DataHub platform
instance and environment of the native connector so the URNs line up and entities stitch together.

Query partitions often store Spark SQL (`FROM table`) instead of M. For those, the connector reads
`Catalog` and `Schema` (or `Database`) from the partition's Databricks connection string and
qualifies the table as `catalog.schema.table`. This includes a DSN connection that carries only
`DSN`, `Catalog`, and `Schema`. The connection string itself is not kept, because it can contain a
secret. A reference that is already `catalog.schema.table` is left unchanged. If the connection has
no catalog, or a bare table has no schema, that upstream is not emitted. The connector emits the
upstream reference and does not create the Databricks dataset. A table that has not been ingested
shows as a hidden edge until a Databricks ingestion creates it.

With `extract_column_level_lineage` enabled, the connector additionally emits intra-model
column-level lineage for DAX measures and calculated columns, derived from
`DISCOVER_CALC_DEPENDENCY`.

#### Semantic model

The catalog is a semantic model entity. Its summary lists the tables, and the active relationships
between them. Each table is a dataset with subtype `Semantic Model Dataset`; its columns and
measures stay on that dataset as schema fields.

Each measure that is not hidden is also a metric of the semantic model. The metric carries the
DAX expression and is linked to its table. When column-level lineage is enabled, it is also
linked to the other tables its DAX reads. `ingest_hidden_metrics.enabled` (default false) also
emits measures that are hidden, or that sit on a hidden table. With
`ingest_hidden_metrics.tag_hidden_metrics`, those metrics get the tag named by
`ingest_hidden_metrics.hidden_metric_tag` (default `hidden_metric`). Visible metrics are not
tagged. A column that is not hidden, and whose table is not hidden, is a dimension of the
model. A date/time column is flagged as a time dimension. Metric
ids are the measure names, with `,`, `(` and `)` replaced by `_`. DAX is recorded with the
`OTHER` expression dialect, as the semantic model has no DAX dialect. A table excluded by
`table_pattern` is left off the model, and a relationship that names it is not emitted. Inactive
relationships are not emitted. The TMSL
definition, when `extract_model_definition` is enabled, is the semantic model's native definition.

#### Power BI URN alignment

Set `platform: powerbi` (requires `server` to be a Power BI Premium `powerbi://api.powerbi.com/v1.0/myorg/<workspace>` endpoint) to make the emitted URNs mirror the Power BI connector's table naming
(`<dataset>.<table>`). This lets an AAS-backed Power BI semantic model merge with the entities
produced by the Power BI connector instead of appearing as a separate platform.

`convert_urns_to_lowercase` lowercases the semantic model id and the table dataset names inside their URNs. It defaults to false, so a model keeps the catalog casing Analysis Services returns. Power BI lineage lowercases upstream dataset URNs by default, so set `convert_urns_to_lowercase: true` here when the table URNs must match that lineage. The displayed names are unchanged. Do not change this after the first ingestion: it re-keys every dataset and the semantic model.

### Limitations

- Only tabular models are supported. Multidimensional (MDX) cubes are out of scope.
- Security role descriptions are surfaced as model custom properties; row-level security filters are not ingested.

### Troubleshooting

#### Authentication failures

Confirm the identity is in a model role with the Administrator permission (or is a server
administrator) and that the endpoint
region in `server` matches the token scope. For service principals, verify `tenant_id`,
`client_id`, and `client_secret` are all set.
