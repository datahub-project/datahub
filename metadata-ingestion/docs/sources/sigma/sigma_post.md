### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Data Model customSQL element lineage

Data Model elements backed by a customSQL source emit warehouse `UpstreamLineage` and column-level `FineGrainedLineage` automatically — no additional configuration is required beyond a valid connection in the Sigma connection registry.

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

#### Data Model element -> warehouse table lineage

When `ingest_data_models: true` and `extract_lineage: true` (both default), the connector also emits entity-level `UpstreamLineage` from each Sigma Data Model element to the warehouse table it is sourced from.
Resolution uses Sigma's `/v2/dataModels/{id}/lineage` (`type=table` entries) and `/v2/files/{inodeId}` to recover the fully-qualified table path (`<DB>/<SCHEMA>/<TABLE>`), then maps the Sigma connection to a DataHub platform via the connection registry.

**Supported platforms**: All Sigma connection types in `SIGMA_TYPE_TO_DATAHUB_PLATFORM_MAP` (Snowflake, BigQuery, Redshift, Databricks, Postgres, MySQL, Athena, Spark, Trino, Presto, Synapse/MSSQL).
Identifier casing is preserved as Sigma reports it, which matches the warehouse catalog for most platforms.
Snowflake is the only platform that requires a case bridge (Snowflake's catalog uses uppercase identifiers, but the DataHub Snowflake connector lowercases them by default).

**Matching URNs to your warehouse connector**: The emitted URNs use the Sigma recipe's `env` and `platform_instance=None` by default.
This is correct for single-environment, single-instance setups.
For multi-environment or multi-instance setups, use `connection_to_platform_map` to specify the exact env and platform_instance per Sigma connectionId:

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

If a Snowflake source recipe sets `convert_urns_to_lowercase: false`, the warehouse connector emits upper-cased URNs and the edges produced here will dangle unless you set `convert_urns_to_lowercase: false` in the matching `connection_to_platform_map` entry.

**Counters to monitor** (visible in the ingestion report):

| Counter                                       | Meaning                                             |
| --------------------------------------------- | --------------------------------------------------- |
| `dm_element_warehouse_upstream_emitted`       | Warehouse lineage edges successfully emitted        |
| `dm_element_warehouse_unknown_connection`     | ConnectionId not in registry or platform unmappable |
| `dm_element_warehouse_table_lookup_failed`    | `/files/{inodeId}` call failed                      |
| `dm_element_warehouse_path_unparseable`       | `/files` path did not match expected format         |
| `dm_element_warehouse_table_entry_incomplete` | Lineage entry missing inodeId or connectionId       |

##### Chart source platform mapping

If you want to provide platform details(platform name, platform instance and env) for chart's all external upstream data sources, then you can use `chart_sources_platform_mapping` as below:

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
