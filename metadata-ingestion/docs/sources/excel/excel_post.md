### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

This connector ingests Excel worksheet datasets into DataHub. Workbooks (Excel files) can be ingested from the local filesystem, from S3 buckets, or from Azure Blob Storage. An asterisk (`*`) can be used in place of a directory or as part of a file name to match multiple directories or files with a single path specification.

:::tip
By default, this connector will ingest all worksheets in a workbook (an Excel file). To filter worksheets use the `worksheet_pattern` config option, or to only ingest the active worksheet use the `active_sheet_only` config option.
:::

#### Starter Recipes

Check out the following recipes to get started with ingestion.

For general pointers on writing and running a recipe, see our [main recipe guide](https://docs.datahub.com/docs/metadata-ingestion#recipes).

#### S3

```yaml
source:
  type: excel
  config:
    path_list:
      - "s3://bucket/data/excel/*/*.xlsx"
    aws_config:
      aws_access_key_id: AKIAIOSFODNN7EXAMPLE
      aws_secret_access_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
      aws_region: us-east-1
    profiling:
      enabled: false
```

#### Azure Blob Storage

```yaml
source:
  type: excel
  config:
    path_list:
      - "https://storageaccountname.blob.core.windows.net/abs-data/excel/*/*.xlsx"
    azure_config:
      account_name: storageaccountname
      sas_token: sv=2022-11-02&ss=b&srt=sco&sp=rwdlacx&se=2025-06-07T21:00:00Z&st=2025-05-07T13:00:00Z&spr=https&sig=a1B2c3D4%3D
      container_name: abs-data
    profiling:
      enabled: false
```

#### Local Files

```yaml
source:
  type: excel
  config:
    path_list:
      - "/data/path/reporting/excel/*.xlsx"
    profiling:
      enabled: false
```

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
