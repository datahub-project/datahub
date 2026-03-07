### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

This connector ingests Excel worksheet datasets into DataHub. Workbooks (Excel files) can be ingested from the local filesystem, from S3 buckets, or from Azure Blob Storage. An asterisk (`*`) can be used in place of a directory or as part of a file name to match multiple directories or files with a single path specification.

:::tip
By default, this connector will ingest all worksheets in a workbook (an Excel file). To filter worksheets use the `worksheet_pattern` config option, or to only ingest the active worksheet use the `active_sheet_only` config option.
:::

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
