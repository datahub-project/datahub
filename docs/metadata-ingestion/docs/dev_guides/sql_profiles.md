
# SQL Profiling

SQL Profiling collects table level and column level statistics.
The SQL-based profiler does not run alone, but rather can be enabled for other SQL-based sources.
Enabling profiling will slow down ingestion runs.

:::caution

Running profiling against many tables or over many rows can run up significant costs.
While we've done our best to limit the expensiveness of the queries the profiler runs, you
should be prudent about the set of tables profiling is enabled on or the frequency
of the profiling runs.

:::

## Capabilities

Extracts:

- Row and column counts for each table
- For each column, if applicable:
  - null counts and proportions
  - distinct counts and proportions
  - minimum, maximum, mean, median, standard deviation, some quantile values
  - histograms or frequencies of unique values

## Supported Sources

| Source | Notes |
| ------ | ----- |
| [ABS Data Lake](../../../docs/generated/ingestion/sources/abs.md) | Optionally enabled via configuration. |
| [Apache Doris](../../../docs/generated/ingestion/sources/doris.md) | Optionally enabled via configuration. |
| [Athena](../../../docs/generated/ingestion/sources/athena.md) | Optionally enabled via configuration. Profiling uses sql queries on whole table which can be expensive operation. |
| [BigID](../../../docs/generated/ingestion/sources/bigid.md) | Column-level profiles from BigID columnProfile data. |
| [BigQuery](../../../docs/generated/ingestion/sources/bigquery.md) | Optionally enabled via configuration. |
| [ClickHouse `clickhouse-usage`](../../../docs/generated/ingestion/sources/clickhouse.md) | Optionally enabled via configuration. |
| [ClickHouse `clickhouse`](../../../docs/generated/ingestion/sources/clickhouse.md) | Optionally enabled via configuration. |
| [CockroachDB](../../../docs/generated/ingestion/sources/cockroachdb.md) | Optionally enabled via configuration. |
| [Databricks](../../../docs/generated/ingestion/sources/databricks.md) | Supported via the `profiling.enabled` config. |
| [Dremio](../../../docs/generated/ingestion/sources/dremio.md) | Optionally enabled via configuration. |
| [Excel](../../../docs/generated/ingestion/sources/excel.md) | Optionally enabled via configuration. |
| [Google Cloud Storage](../../../docs/generated/ingestion/sources/gcs.md) | Optionally enabled via configuration. |
| [IBM Db2](../../../docs/generated/ingestion/sources/db2.md) | Optionally enabled via configuration. |
| [Iceberg](../../../docs/generated/ingestion/sources/iceberg.md) | Optionally enabled via configuration. |
| [Informix](../../../docs/generated/ingestion/sources/informix.md) | Row counts only, via systables.nrows. |
| [Kafka](../../../docs/generated/ingestion/sources/kafka.md) | Optionally enabled via configuration `profiling.enabled`. |
| [MariaDB](../../../docs/generated/ingestion/sources/mariadb.md) | Optionally enabled via configuration. |
| [Microsoft SQL Server](../../../docs/generated/ingestion/sources/mssql.md) | Optionally enabled via configuration. |
| [MySQL](../../../docs/generated/ingestion/sources/mysql.md) | Optionally enabled via configuration. |
| [Postgres](../../../docs/generated/ingestion/sources/postgres.md) | Optionally enabled via configuration. |
| [PowerBI](../../../docs/generated/ingestion/sources/powerbi.md) | Optionally enabled via configuration profiling.enabled. |
| [Presto](../../../docs/generated/ingestion/sources/presto.md) | Optionally enabled via configuration. |
| [Redshift](../../../docs/generated/ingestion/sources/redshift.md) | Optionally enabled via configuration. |
| [S3 / Local Files](../../../docs/generated/ingestion/sources/s3.md) | Optionally enabled via configuration. |
| [Salesforce](../../../docs/generated/ingestion/sources/salesforce.md) | Only table level profiling is supported via `profiling.enabled` config field. |
| [SAP HANA](../../../docs/generated/ingestion/sources/hana.md) | Optionally enabled via configuration. |
| [Snowflake](../../../docs/generated/ingestion/sources/snowflake.md) | Optionally enabled via configuration `profiling.enabled`. |
| [SQLAlchemy](../../../docs/generated/ingestion/sources/sqlalchemy.md) | Optionally enabled via configuration. |
| [StarRocks](../../../docs/generated/ingestion/sources/starrocks.md) | Optionally enabled via configuration. |
| [Teradata](../../../docs/generated/ingestion/sources/teradata.md) | Optionally enabled via configuration. |
| [TiDB](../../../docs/generated/ingestion/sources/tidb.md) | Optionally enabled via configuration. |
| [TimescaleDB](../../../docs/generated/ingestion/sources/timescaledb.md) | Optionally enabled via configuration. |
| [Trino `trino`](../../../docs/generated/ingestion/sources/trino.md) | Optionally enabled via configuration. |
| [Vertica](../../../docs/generated/ingestion/sources/vertica.md) | Optionally enabled via configuration. |


## Profiler Implementation

DataHub uses a SQLAlchemy-based profiler for all SQL sources. It runs profiling queries directly against your SQL source's existing SQLAlchemy connection and emits the table- and column-level statistics listed under [Capabilities](#capabilities). No additional dependencies are required beyond the SQL connector itself.

No configuration is required to use it — any SQL source with profiling enabled will use the SQLAlchemy profiler automatically:

```yaml
source:
  config:
    profiling:
      enabled: true
```

:::note

The legacy Great Expectations profiler (`profiling.method: ge`) has been removed. SQLAlchemy is now the only SQL profiler; the `profiling.method` option no longer has any effect and can be dropped from recipes.

:::
