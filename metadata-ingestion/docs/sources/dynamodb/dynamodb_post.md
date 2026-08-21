### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Using `schema_sampling_size` config

By default, the connector samples 100 items from each table to infer the schema. You can adjust this using the `schema_sampling_size` configuration option if you need more comprehensive schema coverage:

```yml
# Sample 500 items instead of default 100
schema_sampling_size: 500
```

#### Using `include_table_item` config

If there are items that have most representative fields of the table, users could use the `include_table_item` option to provide a list of primary keys of the table in dynamodb format. We include these items in addition to the items sampled based on `schema_sampling_size` (default 100) when we scan the table.

Take [AWS DynamoDB Developer Guide Example tables and data](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/AppendixSampleTables.html) as an example, if a account has a table `Reply` in the `us-west-2` region with composite primary key `Id` and `ReplyDateTime`, users can use `include_table_item` to include 2 items as following:

Example:

```yml
# The table name should be in the format of region.table_name
# The primary keys should be in the DynamoDB format
include_table_item:
  us-west-2.Reply:
    [
      {
        "ReplyDateTime": { "S": "2015-09-22T19:58:22.947Z" },
        "Id": { "S": "Amazon DynamoDB#DynamoDB Thread 1" },
      },
      {
        "ReplyDateTime": { "S": "2015-10-05T19:58:22.947Z" },
        "Id": { "S": "Amazon DynamoDB#DynamoDB Thread 2" },
      },
    ]
```

#### DynamoDB Export to S3 lineage

Set `include_s3_export_lineage: true` to discover existing [DynamoDB Export to S3](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataExport.HowItWorks.html) jobs and emit COPY lineage from each DynamoDB table to its S3 destination (`s3://bucket/prefix`).

By default this also emits column-level lineage (`include_s3_export_column_lineage: true`) using identity mapping from the inferred DynamoDB schema field paths onto the S3 dataset. Set `include_s3_export_column_lineage: false` for table-level edges only.

The connector only calls `ListExports` and `DescribeExport`. It does not create exports. AWS retains export task metadata for about 90 days, so older exports will not appear until a newer export exists for that destination.

```yml
include_s3_export_lineage: true
# include_s3_export_column_lineage: false
```

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

S3 export lineage is limited to DynamoDB's native Export to S3 feature (DynamoDB JSON or Amazon Ion — not Parquet or Iceberg). Glue/Spark jobs that convert exports to Parquet or Iceberg are discovered by the [Glue source](https://docs.datahub.com/docs/generated/ingestion/sources/glue) when job scripts use `connection_type: dynamodb`.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.

If S3 export lineage is missing, confirm `include_s3_export_lineage` is enabled, the IAM principal can call `dynamodb:ListExports` and `dynamodb:DescribeExport`, and a COMPLETED export exists for the table within the last 90 days.
