### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features. This module emits containers, tables/views, and schema fields; extracts foreign-key relationships; extracts table- and column-level lineage for views by parsing their SQL definitions; emits approximate row counts from `systables.nrows`; and supports stateful deletion detection.

### Limitations

This module does not support:

- **Column profiling** — no row sampling, null counts, or other column-level statistics; only approximate row counts from `systables.nrows` are emitted.
- **Stored procedures** — SPL routines are not ingested as DataJobs.
- **Usage / query-log lineage** — view lineage is derived only from parsing view SQL definitions, not from query logs or runtime usage.

#### Extended type mapping

Informix extended types (`JSON`, `BSON`, time series, and spatial types such as `ST_Geometry`) are not in the base `syscolumns.coltype` map and fall back to an unknown/null DataHub type. The native type name is still preserved for display.

### Troubleshooting

If ingestion fails, first confirm the JDBC driver is resolvable (see Prerequisites) and that the connecting user has `SELECT` on the system catalog tables. Then review ingestion logs for connection or query errors.
