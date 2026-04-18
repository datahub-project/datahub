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

### Capabilities

#### Column-level lineage

Column-level lineage (CLL) is extracted from Sigma's formula metadata when the
`extract_column_lineage` config option is enabled (default: `true`).

The connector parses the `[Source/Column]` formula syntax used by Sigma elements
and resolves each reference to an upstream column URN. Two resolution paths are
supported:

- **Cross-element references** — `[ElementName/col]` where `ElementName` is
  another element in the same workbook. Emits an `InputField` edge from the
  upstream chart column to the downstream chart column.
- **Warehouse table references** — `[TABLE_NAME/col]` where `TABLE_NAME` matches
  a warehouse table found in the element's SQL lineage. Emits an `InputField`
  edge from the upstream dataset column to the downstream chart column.

This path is non-SQL and does not re-introduce the OOM risk from the disabled
SQL-parsing CLL path (see `generate_column_lineage=False`).

**Known limitation:** When a workbook element is backed by a Sigma Data Model,
its passthrough columns reference the element's own name in the formula, not the
Data Model name. DataHub therefore cannot trace CLL from the workbook element
through to the Data Model's source columns. This is a Sigma API gap; coarse-grained
lineage (workbook → warehouse) is still emitted via the existing SQL-parse path.
Data Model CLL (via `FineGrainedLineage` on the Data Model Dataset) will be added
in a follow-up PR after Ticket 2 (Data Models) ships.

**Requirements:** Requires Sigma API Jan-2026 or later (`/workbooks/{id}/columns`
endpoint). On older tenants the connector logs a one-time warning and skips CLL
gracefully without falling back to SQL parsing.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
