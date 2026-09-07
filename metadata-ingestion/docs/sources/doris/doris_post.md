### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Migration from MySQL Connector

If you were previously ingesting Doris using the MySQL connector, switch to the dedicated Doris connector for better support:

**Configuration changes:**

- Change `type: mysql` → `type: doris`
- Change port: `3306` → `9030`

**Important:** Dataset URNs will change from `platform:mysql` to `platform:doris`. This creates new entities in DataHub. Enable stateful ingestion with `remove_stale_metadata: true` to automatically clean up old MySQL-based entities.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

#### Cross-catalog view lineage

One run ingests one catalog, so a view that reads from a table in a different Doris catalog gets no lineage for that view — the reference cannot be resolved without pointing the edge at a same-named table in the ingested catalog. Those views are counted in the ingestion report under a `View lineage skipped for cross-catalog reference` warning. Ingest each catalog with its own recipe to catalog both sides.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.

#### Unknown database when using an Iceberg (or other external) catalog

If logs show `Unknown database 'db_ods'` (or similar) after databases were discovered, the source is reconnecting without the catalog name. Set `catalog: your_catalog` (or `database: your_catalog.db_ods`) so connections use `catalog.database`. See **Multi-Catalog** under Prerequisites.

#### "Table reflected without keys or comment"

Doris rejects `SHOW CREATE TABLE` for some objects, most commonly async materialized views. Those tables are still ingested — columns and types come from `DESCRIBE` instead — but primary keys, foreign keys and the table comment are unavailable. The warning context names each affected table along with the error Doris returned.

This is expected on any instance that has async materialized views, and needs no action. Investigate only if the count is higher than the number of async materialized views you have.

#### "Table reflected without keys or comment after an unexpected error"

The same degradation as above, but triggered by an error Doris is not known to raise for this — most often a missing grant on the table, or a connection dropped mid-reflection. The table is still ingested from `DESCRIBE`, so columns are complete, but keys, foreign keys and the table comment are missing.

Check that the ingestion account can run `SHOW CREATE TABLE` on the tables named in the warning context. Unlike the warning above, this one is worth acting on.

#### "Doris column types unavailable"

`DESCRIBE` failed on a table that otherwise reflected normally, so its column types fall back to what the MySQL protocol reports. Columns and names are correct, but Doris-specific types such as `HLL`, `BITMAP`, `VARIANT` and `LARGEINT` appear as their closest MySQL equivalent rather than the real type.

Confirm the ingestion account can run `DESCRIBE` on the tables named in the warning context.
