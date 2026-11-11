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

By default this emits table-level COPY edges only. Set `include_s3_export_column_lineage: true` to also emit column-level lineage inferred from the DynamoDB table's schema. When the recipe has a DataHub connection and the S3 export dataset already exists in DataHub, only fields that match its schema (case-insensitive) are linked. Without a connection, or when the dataset is not in DataHub, the inferred DynamoDB field paths are used as-is. If the schema cannot be read, only table-level lineage is emitted. Leave it off unless you know the S3 export layout matches those field paths — native Export to S3 writes DynamoDB JSON or Amazon Ion, which often does not. For a robust column-level story, ingest the downstream Glue/Iceberg tables (via the [Glue source](https://docs.datahub.com/docs/generated/ingestion/sources/glue)), which resolves column lineage against real schemas.

The connector only calls `ListExports` and `DescribeExport`. It does not create exports. AWS retains export task metadata for about 90 days, so exports older than that stop being returned. `max_exports_per_table` (default 50) bounds how many completed exports are described per table, since a frequently-exported table accumulates many export records that nearly all resolve to the same destination. AWS does not document the order `ListExports` returns, so when a table exceeds this limit, which exports are kept is not guaranteed; the connector reports truncation, as a warning when the described exports already reach more than one destination.

```yml
include_s3_export_lineage: true
# include_s3_export_column_lineage: true
# max_exports_per_table: 50
# s3_export_platform_instance: my-s3-instance
```

Lineage is written as a patch, so upstreams that other sources write to the same S3 dataset are kept. The connector only adds edges and never removes them: an export destination that ages out, column edges written before `include_s3_export_column_lineage` was turned off, and inferred column edges from a run without a DataHub connection all stay until you remove them.

**Matching the S3 URNs.** Two things must line up for these edges to join the S3 datasets you ingest separately:

- **Destination path.** The emitted S3 URN is the export's configured destination (`s3://bucket/prefix`). DynamoDB actually writes the data one level deeper, under `s3://bucket/prefix/AWSDynamoDB/{exportId}/data/…`. If your S3 source ingests with a `path_spec` pointed at that deeper data path, point it at the `s3://bucket/prefix` level (or add a matching `path_spec`) so the URNs join.
- **Platform instance.** The S3 URN carries no `platform_instance` by default. If your S3 source ingests the export bucket with a `platform_instance`, set `s3_export_platform_instance` to the same value, otherwise these edges point at an S3 URN that does not exist.

When the recipe has a DataHub connection, the ingestion report lists export destinations that no other source has ingested (the dataset has no `datasetProperties`) under `s3_export_destinations_not_in_datahub`. Those edges land on an S3 entity that only carries lineage, which usually means one of the two settings above does not match your S3 source. The same list appears as an info entry in the ingestion run's structured logs. It also includes destinations the S3 source has not ingested yet, for example when the S3 recipe runs after this one. A soft-deleted S3 dataset still has `datasetProperties`, so it is not listed.

**Exports started by Glue jobs.** The [Glue DynamoDB export connector](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-connect-dynamodb-home.html) (`dynamodb.export: "ddb"`) requests a regular DynamoDB export on every job run, so those exports appear in `ListExports` and get COPY edges like any other. If the job sets neither `dynamodb.s3.bucket` nor `dynamodb.s3.prefix`, the export goes to the job's temporary directory (`--TempDir`), so the lineage points at that temporary path, and a scheduled job's exports also count against `max_exports_per_table`. Set `dynamodb.s3.bucket` and `dynamodb.s3.prefix` on those jobs to give the exports a stable destination.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

S3 export lineage is limited to DynamoDB's native Export to S3 feature (DynamoDB JSON or Amazon Ion — not Parquet or Iceberg). Glue/Spark jobs that convert exports to Parquet or Iceberg are discovered by the [Glue source](https://docs.datahub.com/docs/generated/ingestion/sources/glue) when job scripts use `connection_type: dynamodb`.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.

If S3 export lineage is missing, confirm `include_s3_export_lineage` is enabled, the IAM principal can call `dynamodb:ListExports` and `dynamodb:DescribeExport`, and a COMPLETED export exists for the table within the last 90 days.
