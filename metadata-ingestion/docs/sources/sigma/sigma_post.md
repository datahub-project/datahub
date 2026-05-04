### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

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

##### Data Model element -> warehouse table lineage

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

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
