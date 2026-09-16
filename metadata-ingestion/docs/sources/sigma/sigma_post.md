### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Connection record overrides (`connection_to_platform_map`)

All four warehouse lineage paths — DM element → warehouse table, DM customSQL, workbook
customSQL, and workbook chart entity-level BFS — resolve the target DataHub platform and URN
coordinates from the Sigma connection record. `connection_to_platform_map` lets you override
those coordinates per connection.

**env / platform_instance / convert_urns_to_lowercase**

For multi-environment or multi-instance setups, specify the exact env and platform_instance per
Sigma connectionId so emitted lineage edges point to the correct warehouse connector run:

```yml
connection_to_platform_map:
  # Key is the Sigma connectionId (UUID from /v2/connections).
  "4b39cdcd-5a58-4ff6-af0d-8409ff880a23":
    env: PROD
    platform_instance: prod-snowflake
    # Set to false only if the Snowflake connector was run with
    # convert_urns_to_lowercase: false (non-default).
    convert_urns_to_lowercase: true
```

**Warehouses that omit database or schema from the Sigma connection record** (e.g. Redshift):
Sigma's `/v2/connections` API does not return `database` or `schema` fields for all warehouse
types. When those fields are absent, lineage URNs may be under-qualified and will not match
what your warehouse connector emitted. Use `default_database` and `default_schema` to supply
the missing values:

```yml
connection_to_platform_map:
  "a1b2c3d4-0000-0000-0000-000000000001":
    env: PROD
    default_database: my_redshift_db # expands `schema.table` → `my_redshift_db.schema.table`
    default_schema: public # expands bare `table` → `public.table` (SQL parser only)
```

`default_database` applies to all four lineage paths (DM element, DM customSQL, workbook
customSQL, and workbook chart entity-level BFS). `default_schema` is consumed only by the
SQL parser (DM customSQL and workbook customSQL paths) — the entity-level BFS and DM element
paths derive the schema from the `/files` path and do not use this field. The `env` and
`platform_instance` fields are also consumed by the customSQL parsers, so URNs minted from
customSQL definitions match the warehouse connector's env/instance for that connection.

#### Data Model customSQL element lineage

Data Model elements backed by a customSQL source emit warehouse `UpstreamLineage` and column-level `FineGrainedLineage` automatically — no additional configuration is required beyond a valid connection in the Sigma connection registry for most platforms. **Note:** for Redshift and other warehouses where Sigma's connection record omits `database`/`schema`, set `default_database` and `default_schema` in `connection_to_platform_map` — see [Connection record overrides](#connection-record-overrides-connection_to_platform_map) above.

For elements with explicit column lists in their SQL (`SELECT col_a, col_b FROM ...`), column lineage is derived directly from the SQL by the parser (confidence score 0.2). For elements using `SELECT *`, column lineage is inferred from Sigma's formula metadata (`[Custom SQL/COL]` refs on each element column); these entries carry a confidence score of 0.1 — lower than SQL-parsed lineage — because they rely on formula-derived inference rather than direct SQL analysis.

The following report counters are available for operational visibility:

| Counter                                     | Meaning                                                                                        |
| ------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `dm_customsql_aggregator_invocations`       | SQL definitions successfully registered for parsing                                            |
| `dm_customsql_aggregator_invocation_errors` | Registration failures (non-zero indicates an internal error)                                   |
| `dm_customsql_skipped`                      | Elements skipped before parsing (missing definition, unknown connection, unsupported platform) |
| `dm_customsql_parse_failed`                 | Definitions the SQL parser could not interpret (syntax errors, unsupported features, etc.)     |
| `dm_customsql_upstream_emitted`             | Entity-level `UpstreamLineage` aspects emitted                                                 |
| `dm_customsql_column_lineage_emitted`       | Elements with at least one column lineage entry emitted                                        |
| `dm_customsql_fgl_downstream_unmapped`      | Individual FGL downstream fields dropped (SQL column name not found in Sigma formula metadata) |

#### Workbook customSQL chart lineage

When `extract_lineage: true` (default), workbook chart elements whose data source is a customSQL definition
emit warehouse `UpstreamLineage` and column-level `FineGrainedLineage` via the SQL parser — no additional
configuration is required beyond a valid connection in the Sigma connection registry for most platforms.
**Note:** for Redshift and other warehouses where Sigma's connection record omits `database`/`schema`,
set `default_database` and `default_schema` in `connection_to_platform_map` — see
[Connection record overrides](#connection-record-overrides-connection_to_platform_map) above.

The connector reads the workbook-level lineage graph (`/v2/workbooks/{id}/lineage`) to find `type=customSQL`
entries, parses each SQL definition, and registers the results with the `SqlParsingAggregator`.
Column-level lineage is emitted for chart columns whose formula resolves to a named SQL column
(`[CustomSQLName/col]` pattern). Columns using `SELECT *` sources carry lower-confidence (0.1) inferred lineage.

The following report counters are available for operational visibility:

| Counter                                           | Meaning                                                                                        |
| ------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `workbook_customsql_aggregator_invocations`       | SQL definitions successfully registered for parsing                                            |
| `workbook_customsql_aggregator_invocation_errors` | Registration failures (non-zero indicates an internal error)                                   |
| `workbook_customsql_skipped`                      | Entries skipped before parsing (missing definition, unknown connection, unsupported platform)  |
| `workbook_customsql_parse_failed`                 | Definitions the SQL parser could not interpret                                                 |
| `workbook_customsql_upstream_emitted`             | Entity-level `UpstreamLineage` aspects emitted                                                 |
| `workbook_customsql_column_lineage_emitted`       | Charts with at least one column lineage entry emitted                                          |
| `workbook_customsql_fgl_downstream_unmapped`      | Individual FGL downstream fields dropped (SQL column name not found in Sigma formula metadata) |

#### Workbook chart entity-level warehouse upstream

When `extract_lineage: true` (default), workbook chart elements that pull **directly** from a
warehouse table (no Data Model, no customSQL, no Sigma Dataset in between) emit an entity-level
`chartInfo.inputs` edge to the warehouse Dataset.

Column-level lineage for these charts is handled separately by the
[chart inputFields warehouse qualification](#workbook-chart-inputfields-warehouse-column-level-qualification)
path; this feature adds the missing entity-level edge.

No additional configuration is required for most platforms. For Redshift connections where the
Sigma connection record omits `database`/`schema`, set `default_database` in
`connection_to_platform_map` — see
[Connection record overrides](#connection-record-overrides-connection_to_platform_map) above.

| Counter                                | Meaning                                                                                                           |
| -------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `chart_warehouse_upstream_emitted`     | Entity-level chart→warehouse edges emitted (post-dedup)                                                           |
| `chart_warehouse_table_name_unmatched` | Table not found in workbook warehouse index; edge not emitted                                                     |
| `chart_warehouse_table_node_skipped`   | Lineage node missing `name` field or has unexpected ID format; skipped                                            |
| `chart_warehouse_table_name_ambiguous` | Table name matched multiple warehouse URNs; edge skipped — set `default_database` in `connection_to_platform_map` |

#### Sigma Dataset -> warehouse table lineage

Sigma ended support for datasets as a data source on 2026-09-15. A workbook element reading through
a Sigma Dataset still answers `/v2/workbooks/{id}/elements/{id}/query` with HTTP 200, but the body
no longer carries SQL — and that SQL was the only place the dataset's warehouse table was named.

When an element's SQL names no warehouse table, the connector resolves the dataset's warehouse table structurally
instead: `/v2/datasets/{id}/sources` names the table by inode, and `/v2/connections/paths/{inodeId}`
resolves that inode to a `connectionId` plus a path. The URN is then built through the connection
registry, so per-connection `default_database`, `env`, `platform_instance` and
`convert_urns_to_lowercase` all apply, exactly as for Data Model element and chart warehouse edges.
Note `convert_urns_to_lowercase` only changes the default for platforms DataHub lower-cases
(Snowflake); set it explicitly to force lower-casing on any other platform.
No `chart_sources_platform_mapping` entry is needed.

Note the trigger is "the element's SQL named no warehouse table", which is wider than
"the element has no SQL": the SQL parser only runs when a `chart_sources_platform_mapping`
entry matches the element's path, so a recipe without one takes this route even against a
tenant still serving SQL. Those charts previously got no Sigma Dataset lineage at all.

This route is a **stopgap**. It depends on the deprecated dataset API, so it will stop returning
anything once Sigma removes those endpoints; migrate datasets to Data Models to keep lineage.
A 410 from `/sources` latches the endpoint as removed at once. A 404 or 409 is ambiguous —
Sigma answers `409 inode_archived`, not 404, for a dataset it cannot resolve — so the connector
re-asks for a dataset whose `/sources` already succeeded this run. If that now fails too the
endpoint is latched and warned about once; otherwise it is reported as an info for that one
dataset (most likely deleted or re-permissioned after the listing). With nothing having
succeeded yet, repeated not-founds escalate to a warning.

**If you set `env` or `platform_instance` in `chart_sources_platform_mapping`, copy them into
`connection_to_platform_map`.** Before the deprecation these edges came from the SQL parser, which
read that mapping. This route resolves through the connection registry and does not, so without a
`connection_to_platform_map` entry for the connection the URN uses this recipe's `env` and no
platform instance — pointing at a URN your warehouse connector may never have produced. The
connector warns when it detects this.

The credential needs permission to read `/v2/connections/paths/{inodeId}`; without it each table
produces a `connection_path_lookup_failed` warning and no warehouse upstream.

Known limitations:

- Only `type: table` sources resolve. Datasets backed by a CSV upload, by another dataset, or by
  custom SQL have no warehouse table to point at and are counted under
  `dataset_warehouse_no_table_sources`.
- A dataset excluded by `workspace_pattern` is absent from the dataset listing, so its
  `datasetId` is unknown and no lookup can be made. Widen the pattern to recover that lineage.
  This is reported as an info, not a warning, since excluding a workspace is usually deliberate.
- A Sigma Dataset whose warehouse table cannot be resolved gets no warehouse
  `upstreamLineage`, and the chart does not list it under `chartInfo.inputs` — matching the
  pre-deprecation behaviour, where the dataset appeared as a chart input only when its
  warehouse table was identified.
- Only datasets referenced by an ingested workbook chart get warehouse lineage. The route itself
  does not need an element, but it is driven from chart resolution, so a dataset read only by a
  Data Model, or by nothing at all (`migrationStatus: not-required`), is not resolved. This matches
  the pre-deprecation SQL route, which was also per element.
- On a tenant still serving SQL, a dataset reachable both through a matching
  `chart_sources_platform_mapping` (SQL) and through this route can get either spelling of the URN
  depending on which element is processed first. Set `connection_to_platform_map` to make the two
  agree.
- A 2-segment path is read as `[SCHEMA, TABLE]` with the database taken from the connection.
  That is correct for Redshift-style connections and mirrors the existing `/files` path
  handling, but it is unverified elsewhere. **On a genuinely two-level platform such as
  MySQL the path is `[DB, TABLE]`, so do not set `default_database` for that connection**:
  leaving it unset emits `DB.TABLE`, which is the correct MySQL URN, whereas setting it
  produces `<default_database>.DB.TABLE`, which matches nothing. The
  "default_database not configured" warning does not know the difference yet, so ignore it
  for two-level connections and check a few edges if your warehouse reports two-part paths.

| Counter                                    | Meaning                                                                                |
| ------------------------------------------ | -------------------------------------------------------------------------------------- |
| `dataset_warehouse_upstream_from_inode`    | Sigma Datasets whose warehouse table(s) were recovered via this route                  |
| `dataset_warehouse_table_entry_incomplete` | A source entry named a table but was unusable (not an object, or no inodeId)           |
| `dataset_warehouse_no_table_sources`       | Datasets whose sources held no `type: table` entry (CSV, dataset-on-dataset, SQL)      |
| `dataset_warehouse_unknown_connection`     | Table's `connectionId` not in the registry, or platform unmappable                     |
| `dataset_warehouse_unlisted_dataset`       | Dataset absent from `/v2/datasets` (usually `workspace_pattern`); no lookup made       |
| `dataset_sources_lookup_failed`            | `/datasets/{id}/sources` returned non-200, raised, or was not a JSON list              |
| `dataset_sources_lookup_rate_limited`      | Subset of the above: 429 after retries                                                 |
| `dataset_sources_endpoint_removed`         | Endpoint concluded removed: a 410, or a 404/409 that a re-probe confirms; once per run |
| `connection_path_lookup_failed`            | `/connections/paths/{inodeId}` failed or returned an unusable body                     |
| `connection_path_lookup_rate_limited`      | Subset of the above: 429 after retries                                                 |
| `dataset_sources_not_found`                | Subset of `lookup_failed`: 404 for one dataset (deleted after the listing)             |
| `dataset_sources_skipped_endpoint_gone`    | Datasets skipped with no request once the endpoint was latched as removed              |
| `datasets_listing_failed`                  | `/v2/datasets` could not be listed, so no dataset lineage resolves this run            |
| `datasets_dropped_missing_file_metadata`   | Datasets in the listing dropped because their `/files` metadata was missing            |

Each Sigma Dataset also carries its Sigma-reported `migrationStatus` as a `datasetProperties`
custom property — `not-migrated`, `migrated`, or `not-required` (referenced by nothing, so
migration is optional). Use it to find the datasets that still need moving to Data Models. The
property is omitted on tenants that do not report it.

#### Workbook chart inputFields warehouse column-level qualification

When `extract_lineage: true` (default), the connector qualifies chart column `InputFields` to
warehouse Dataset URNs. For each chart column whose formula references a warehouse table (e.g.,
`[TABLE/col]`), the connector resolves the short table name to a fully-qualified warehouse Dataset
URN via a two-level index: first the per-element SQL-parser index, then the workbook-level index
from `/v2/workbooks/{id}/lineage`. The resolved URN is written into `schemaFieldUrn` on each
`InputField` entry.

| Counter                                                     | Meaning                                                              |
| ----------------------------------------------------------- | -------------------------------------------------------------------- |
| `chart_input_fields_warehouse_qualified`                    | Individual column fields successfully qualified to a warehouse URN   |
| `chart_input_fields_warehouse_qualified_via_workbook_index` | Subset qualified via the workbook-level index (not per-element SQL)  |
| `chart_input_fields_warehouse_index_lookup_failed`          | Workbook-level lineage fetch failed; column qualification incomplete |
| `chart_input_fields_warehouse_table_lookup_failed`          | `/files/{inodeId}` call failed for a workbook-level table entry      |
| `chart_input_fields_warehouse_path_unparseable`             | `/files` path did not match expected format                          |
| `chart_input_fields_warehouse_unknown_connection`           | ConnectionId not in registry or platform unmappable                  |

#### Data Model element -> warehouse table lineage

When `ingest_data_models: true` and `extract_lineage: true` (both default), the connector also emits entity-level `UpstreamLineage` from each Sigma Data Model element to the warehouse table it is sourced from.
Resolution uses Sigma's `/v2/dataModels/{id}/lineage` (`type=table` entries) and `/v2/files/{inodeId}` to construct the fully-qualified `<DB>/<SCHEMA>/<TABLE>` identifier from the path and table name fields (path = `Connection Root/<DB>/<SCHEMA>` for most platforms; `Connection Root/<SCHEMA>` for Redshift), then maps the Sigma connection to a DataHub platform via the connection registry.

**Supported platforms**: All Sigma connection types in `SIGMA_TYPE_TO_DATAHUB_PLATFORM_MAP` (Snowflake, BigQuery, Redshift, Databricks, Postgres, MySQL, Athena, Spark, Trino, Presto, Synapse/MSSQL).
Identifier casing is preserved as Sigma reports it, which matches the warehouse catalog for most platforms.
Snowflake is the only platform that requires a case bridge (Snowflake's catalog uses uppercase identifiers, but the DataHub Snowflake connector lowercases them by default).

**Matching URNs to your warehouse connector**: The emitted URNs use the Sigma recipe's `env` and
`platform_instance=None` by default. For multi-environment or multi-instance setups, or for
Redshift connections where the Sigma connection record omits `database`/`schema`, see
[Connection record overrides](#connection-record-overrides-connection_to_platform_map) above.

**Counters to monitor** (visible in the ingestion report):

| Counter                                       | Meaning                                             |
| --------------------------------------------- | --------------------------------------------------- |
| `dm_element_warehouse_upstream_emitted`       | Warehouse lineage edges successfully emitted        |
| `dm_element_warehouse_unknown_connection`     | ConnectionId not in registry or platform unmappable |
| `dm_element_warehouse_table_lookup_failed`    | `/files/{inodeId}` call failed                      |
| `dm_element_warehouse_path_unparseable`       | `/files` path did not match expected format         |
| `dm_element_warehouse_table_entry_incomplete` | Lineage entry missing inodeId or connectionId       |

##### Chart source platform mapping

`chart_sources_platform_mapping` is the legacy fallback for the workbook-chart SQL parser path.
It fires whenever a workbook element exposes an `element.query` (regardless of whether the element
is backed by a Sigma Dataset, a DM element, customSQL, or inline SQL). The SQL-bearing endpoint
does not return a `connectionId`, so the platform, env, and default database/schema cannot be
auto-resolved. You declare them explicitly, scoped to a workbook path prefix or the `"*"` wildcard.

**Prefer `connection_to_platform_map`** for warehouse connections (Snowflake, Redshift, BigQuery,
etc.) — it auto-resolves the platform from the connection record and covers all lineage paths that
have a `connectionId` on hand (DM warehouse table lineage, DM and workbook customSQL parsing,
chart inputFields qualification). Use `chart_sources_platform_mapping` only when the chart's SQL
parser path fires and you cannot reach the connection via `connection_to_platform_map`.

Note that since Sigma's dataset deprecation, an element backed by a Sigma Dataset usually exposes
no `element.query` at all, so the SQL parser path does not fire and this mapping has no effect on
it. That lineage comes from
[Sigma Dataset -> warehouse table lineage](#sigma-dataset---warehouse-table-lineage) instead, which
resolves the connection itself and needs no mapping.

##### Example - For just one specific chart's external upstream data sources

```yml
chart_sources_platform_mapping:
  "workspace_name/workbook_name/chart_name_1":
    data_source_platform: snowflake
    platform_instance: new_instance
    env: PROD

  "workspace_name/folder_name/workbook_name/chart_name_2":
    data_source_platform: postgres
    platform_instance: cloud_instance
    env: DEV
```

##### Example - For all charts within one specific workbook

```yml
chart_sources_platform_mapping:
  "workspace_name/workbook_name_1":
    data_source_platform: snowflake
    platform_instance: new_instance
    env: PROD

  "workspace_name/folder_name/workbook_name_2":
    data_source_platform: snowflake
    platform_instance: new_instance
    env: PROD
```

##### Example - For all workbooks charts within one specific workspace

```yml
chart_sources_platform_mapping:
  "workspace_name":
    data_source_platform: snowflake
    platform_instance: new_instance
    env: PROD
```

##### Example - All workbooks use the same connection

```yml
chart_sources_platform_mapping:
  "*":
    data_source_platform: snowflake
    platform_instance: new_instance
    env: PROD
```

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
