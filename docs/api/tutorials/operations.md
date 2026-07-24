


# Operations

## Why Would You Use Operations APIs?

The Operations APIs allow you to report operational changes that were made to a given Dataset or Table using the 'Operation' concept.
These operations may be viewed on the Dataset Profile (e.g. as last modified time), accessed via the DataHub GraphQL API, or
used as inputs to DataHub Cloud [Freshness Assertions](/docs/managed-datahub/observe/freshness-assertions.md).

### Goal Of This Guide

This guide will show you how to report and query Operations for a Dataset.

## Supported Sources

Some ingestion sources can automatically capture operations from native audit logs, query history, table history, or object
timestamps. The table below lists sources that emit dataset-level DataHub `operation` aspects during ingestion.

| Source | Notes |
| ------ | ----- |
| [ABS Data Lake](../../generated/ingestion/sources/abs.md) | Enabled by default as UPDATE operations from blob timestamps. |
| [BigQuery](../../generated/ingestion/sources/bigquery.md) | Enabled by default via usage extraction, can be disabled via `usage.include_operational_stats`. |
| [ClickHouse `clickhouse`](../../generated/ingestion/sources/clickhouse.md) | Optionally enabled via `include_query_log_operations`. |
| [Databricks](../../generated/ingestion/sources/databricks.md) | Enabled by default via usage extraction, can be disabled via `include_operational_stats`. |
| [Delta Lake](../../generated/ingestion/sources/delta-lake.md) | Enabled by default from Delta table history. |
| [Dremio](../../generated/ingestion/sources/dremio.md) | Optionally enabled via `include_query_lineage`; generated from Dremio job history. |
| [Fabric OneLake](../../generated/ingestion/sources/fabric-onelake.md) | Optionally enabled via `usage.include_usage_statistics` and `usage.include_operational_stats`. |
| [Glue](../../generated/ingestion/sources/glue.md) | Enabled by default from Glue table created and last modified timestamps. |
| [Oracle](../../generated/ingestion/sources/oracle.md) | Optionally enabled via `include_query_usage` and `include_operational_stats`. |
| [Redshift](../../generated/ingestion/sources/redshift.md) | Optionally enabled via `include_usage_statistics`; controlled by `include_operational_stats`. |
| [S3 / Local Files](../../generated/ingestion/sources/s3.md) | Enabled by default as UPDATE operations from object timestamps. |
| [Salesforce](../../generated/ingestion/sources/salesforce.md) | Enabled by default from Salesforce object created and last modified timestamps. |
| [SAP HANA](../../generated/ingestion/sources/hana.md) | Derived from observed queries when `include_operational_stats` and `include_query_usage` are enabled. |
| [Snowflake](../../generated/ingestion/sources/snowflake.md) | Enabled by default, can be disabled via configuration `include_operational_stats`. |
| [SQL Queries](../../generated/ingestion/sources/sql-queries.md) | Parsed from non-SELECT SQL queries. |
| [Teradata](../../generated/ingestion/sources/teradata.md) | Optionally enabled via `include_usage_statistics`; controlled by `usage.include_operational_stats`. |


## Prerequisites

For this tutorial, you need to deploy DataHub Quickstart and ingest sample data.
For detailed steps, please refer to [DataHub Quickstart Guide](/docs/quickstart.md).

:::note
Before reporting operations for a dataset, you need to ensure the targeted dataset is already present in DataHub.
:::

## Report Operations

You can use report dataset operations to DataHub using the following APIs.



#### GraphQL


```graphql
mutation reportOperation {
  reportOperation(
    input: {
      urn: "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)"
      operationType: INSERT
      sourceType: DATA_PROCESS
    }
  )
}
```

Where supported operation types include

- `INSERT`
- `UPDATE`
- `DELETE`
- `CREATE`
- `ALTER`
- `DROP`
- `CUSTOM`

If you want to report an operation that happened at a specific time, you can also optionally provide
the `timestampMillis` field. If not provided, the current server time will be used as the operation time.

If you see the following response, the operation was successful:

```json
{
  "data": {
    "reportOperation": true
  },
  "extensions": {}
}
```




#### Python


```python
# Inlined from /metadata-ingestion/examples/library/dataset_report_operation.py
from datahub.api.graphql import Operation

DATAHUB_HOST = "https//:org.acryl.io/gms"
DATAHUB_TOKEN = "<your-datahub-access-token"

dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)"

operation_client = Operation(
    datahub_host=DATAHUB_HOST,
    datahub_token=DATAHUB_TOKEN,
)

operation_type = "INSERT"
source_type = "DATA_PROCESS"  # Source of the operation (data platform or DAG task)

# Report a change operation for the Dataset.
operation_client.report_operation(
    urn=dataset_urn, operation_type=operation_type, source_type=source_type
)

```




## Read Operations

You can use read dataset operations to DataHub using the following APIs.



#### GraphQL


```graphql
query dataset {
    dataset(urn: "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)") {
        operations(
            limit: 10, filter: [], startTimeMillis: <start-timestamp-ms>, endTimeMillis: <end-timestamp-ms>
        ) {
            timestampMillis
            operationType
            sourceType
        }
    }
}
```

Where startTimeMillis and endTimeMillis are optional. By default, operations are sorted by time descending.

If you see the following response, the operation was successful:

```json
{
  "data": {
    "dataset": {
      "operations": [
        {
          "timestampMillis": 1231232332,
          "operationType": "INSERT",
          "sourceType": "DATA_PROCESS"
        }
      ]
    }
  },
  "extensions": {}
}
```




#### Python


```python
# Inlined from /metadata-ingestion/examples/library/dataset_read_operations.py
from datahub.api.graphql import Operation

DATAHUB_HOST = "https//:org.acryl.io/gms"
DATAHUB_TOKEN = "<your-datahub-access-token"

dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)"

operation_client = Operation(
    datahub_host=DATAHUB_HOST,
    datahub_token=DATAHUB_TOKEN,
)

# Query for changes to the Dataset.
operations = operation_client.query_operations(
    urn=dataset_urn,
    # limit=5,
    # start_time_millis=<timestamp>,
    # end_time_millis=<timestamo>
)

```




### Expected Outcomes of Reporting Operations

Reported Operations will appear when displaying the Last Updated time for a Dataset on their DataHub Profile.
They will also be used when selecting the `DataHub Operation` source type under the **Advanced** settings of a Freshness
Assertion.
