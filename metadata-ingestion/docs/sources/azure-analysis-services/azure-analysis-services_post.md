### Capabilities

#### Lineage

When `extract_lineage` is enabled, the connector parses each table's M/Power Query partition
expression (including `Value.NativeQuery` native SQL) to build upstream table lineage. Use
`server_to_platform_instance` to map the servers named in those expressions to the DataHub platform
instance and environment of the native connector so the URNs line up and entities stitch together.

With `extract_column_level_lineage` enabled, the connector additionally emits intra-model
column-level lineage for DAX measures and calculated columns, derived from
`DISCOVER_CALC_DEPENDENCY`.

#### Power BI URN alignment

Set `platform: powerbi` (requires `server` to be a Power BI Premium `powerbi://api.powerbi.com/v1.0/myorg/<workspace>` endpoint) to make the emitted URNs mirror the Power BI connector's table naming
(`<dataset>.<table>`). This lets an AAS-backed Power BI semantic model merge with the entities
produced by the Power BI connector instead of appearing as a separate platform.

`convert_urns_to_lowercase` lowercases the model and table dataset names inside their URNs. It defaults to false, so a model keeps the catalog casing Analysis Services returns. Power BI lineage lowercases upstream dataset URNs by default, so set `convert_urns_to_lowercase: true` here when the cube URN must match that lineage. The displayed dataset name is unchanged. Do not change this after the first ingestion: it re-keys every dataset.

### Limitations

- Only tabular models are supported. Multidimensional (MDX) cubes are out of scope.
- Security role descriptions are surfaced as model custom properties; row-level security filters are not ingested.

### Troubleshooting

#### Authentication failures

Confirm the identity is in a model role with the Administrator permission (or is a server
administrator) and that the endpoint
region in `server` matches the token scope. For service principals, verify `tenant_id`,
`client_id`, and `client_secret` are all set.
