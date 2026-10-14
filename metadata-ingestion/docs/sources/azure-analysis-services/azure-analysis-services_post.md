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

Set `platform: powerbi` to make the emitted URNs mirror the Power BI connector's table naming
(`<dataset>.<table>`). This lets an AAS-backed Power BI semantic model merge with the entities
produced by the Power BI connector instead of appearing as a separate platform.

### Limitations

- Only tabular models are supported. Multidimensional (MDX) cubes are out of scope.
- Row-level security expressions are surfaced as role custom properties, not evaluated.

### Troubleshooting

#### Authentication failures

Confirm the identity is a member of a model role (or a server administrator) and that the endpoint
region in `server` matches the token scope. For service principals, verify `tenant_id`,
`client_id`, and `client_secret` are all set.
