### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features. This module emits containers, tables/views, and schema fields, and supports stateful deletion detection.

### Limitations

This v1 module does not support:

- **Lineage** — no table/view or query-based lineage is extracted.
- **Profiling** — no row sampling or column statistics.
- **Stored procedures** — SPL routines are not ingested as DataJobs.

#### Extended type mapping

Informix extended types (`JSON`, `BSON`, time series, and spatial types such as `ST_Geometry`) are not in the base `syscolumns.coltype` map and fall back to an unknown/null DataHub type. The native type name is still preserved for display.

### Troubleshooting

If ingestion fails, first confirm the JDBC driver is resolvable (see Prerequisites) and that the connecting user has `SELECT` on the system catalog tables. Then review ingestion logs for connection or query errors.
