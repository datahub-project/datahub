### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Table and Column Descriptions

Table and column comments are read from `information_schema.TABLES` and `information_schema.COLUMNS`, with one query each per database (two when an external catalog needs the retry below), and ingested as dataset and field descriptions. They are not taken from `SHOW CREATE TABLE`: Doris writes comments there in double quotes, which the MySQL DDL parser does not read.

External catalogs, for example Paimon, Hive and Iceberg, are covered the same way on Doris 2.1 and later, where each catalog has its own `information_schema` ([apache/doris#28919](https://github.com/apache/doris/pull/28919)). On older versions `information_schema` has no rows for an external catalog's databases: those tables are ingested without descriptions, and the report shows a "Doris table and column comments not found" warning for each such database. Upgrading Doris fixes it; no grant does.

When the global `show_full_dbname_in_info_schema_db` is enabled, an external catalog's rows are keyed by `catalog.database`. The source then retries with that name when the bare one returns nothing, and uses the qualified name first for the catalog's remaining databases.

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

#### Truncated column comments

Doris truncates `information_schema.COLUMNS.COLUMN_COMMENT` to `column_comment_length_limit` characters when that FE configuration is greater than zero. It defaults to `-1`, which means no limit; raise it or set it back to `-1` if long column descriptions arrive cut short.

#### External catalog table comments

Doris maps the Paimon table comment only from 3.1 onwards ([apache/doris#53405](https://github.com/apache/doris/pull/53405)), so Paimon tables on Doris 2.1 and 3.0 are ingested with column descriptions but no table description.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.

#### Unknown database when using an Iceberg (or other external) catalog

If logs show `Unknown database 'db_ods'` (or similar) after databases were discovered, the source is reconnecting without the catalog name. Set `catalog: your_catalog` (or `database: your_catalog.db_ods`) so connections use `catalog.database`. See **Multi-Catalog** under Prerequisites.

#### "Table reflected without keys"

Doris rejects `SHOW CREATE TABLE` for some objects, most commonly async materialized views. Those tables are still ingested — columns and types come from `DESCRIBE` instead — but primary keys and foreign keys are unavailable. Table and column comments are still read from `information_schema`. The warning context names each affected table along with the error Doris returned.

This is expected on any instance that has async materialized views, and needs no action. Investigate only if the count is higher than the number of async materialized views you have.

#### "Table reflected without keys after an unexpected error"

The same degradation as above, but triggered by an error Doris is not known to raise for this — most often a missing grant on the table. The table is still ingested from `DESCRIBE`, so its columns are complete, but keys and foreign keys are missing.

Check that the ingestion account can run `SHOW CREATE TABLE` on the tables named in the warning context. Unlike the warning above, this one is worth acting on.

#### "Doris column types unavailable"

`DESCRIBE` failed on a table that otherwise reflected normally, so its column types fall back to what the MySQL protocol reports. Columns and names are correct, but Doris-specific types such as `HLL`, `BITMAP`, `VARIANT` and `LARGEINT` appear as their closest MySQL equivalent rather than the real type.

Confirm the ingestion account can run `DESCRIBE` on the tables named in the warning context.

#### "Doris table and column comments unavailable"

The `information_schema` comment lookup failed for the database named in the warning context, so some or all of its tables and columns were ingested without descriptions. This clears the descriptions an earlier run ingested from Doris; descriptions edited in DataHub are kept. Everything else about those tables is unaffected.

Confirm the ingestion account can query `information_schema.TABLES` and `information_schema.COLUMNS`. On a large external catalog the lookup can also outrun the connection's read timeout (`options.connect_args.read_timeout`), so raise it if the error in the warning context is a timeout.

#### "Lost the Doris connection while reflecting a table"

The connection dropped while the table or view named in the failure context was being read, so it was skipped rather than ingested with an empty or partial schema, and stale-entity removal is skipped for this run so it is not deleted. The source then reconnects, so the tables after it are still ingested.

Check FE availability and any proxy or load-balancer idle timeout between the ingestion executor and Doris, then rerun.
