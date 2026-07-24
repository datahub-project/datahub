
# Automatic Lineage Extraction

DataHub supports **[automatic table- and column-level lineage detection](#automatic-lineage-extraction-support)** from BigQuery, Snowflake, dbt, Looker, PowerBI, and 20+ modern data tools. 
For data tools with limited native lineage tracking, [**DataHub's SQL Parser**](../../lineage/sql_parsing.md) detects lineage with 97-99% accuracy, ensuring teams will have high quality lineage graphs across all corners of their data stack.

## Types of Lineage Connections

Types of lineage connections supported in DataHub and the example codes are as follows.

* [Dataset to Dataset](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/library/lineage_dataset_add.py)
* [DataJob to DataFlow](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/library/lineage_job_dataflow.py)
* [DataJob to Dataset](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/library/lineage_dataset_job_dataset.py)
* [Chart to Dashboard](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/library/lineage_chart_dashboard.py)
* [Chart to Dataset](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/examples/library/lineage_dataset_chart.py)

## Automatic Lineage Extraction Support

This is a summary of automatic lineage extraction support in our data source. Please refer to the **Important Capabilities** table in the source documentation. Note that even if the source does not support automatic extraction, you can still add lineage manually using our API & SDKs.

| Source | Table-Level Lineage | Column-Level Lineage | Related Configs |
| ---------- | ------ | ----- |----- |
| [ABS Data Lake](../../generated/ingestion/sources/abs.md) | ❌ | ❌ | |
| [Aerospike](../../generated/ingestion/sources/aerospike.md) | ❌ | ❌ | |
| [Airbyte](../../generated/ingestion/sources/airbyte.md) | ✅ | ✅ | - incremental_lineage<br />- extract_column_level_lineage|
| [Amazon Kinesis Data Streams](../../generated/ingestion/sources/kinesis.md) | ✅ | ❌ | - include_table_lineage|
| [Apache Doris](../../generated/ingestion/sources/doris.md) | ✅ | ✅ | - incremental_lineage<br />- include_table_location_lineage<br />- include_view_lineage<br />- include_view_column_lineage|
| [Athena](../../generated/ingestion/sources/athena.md) | ✅ | ✅ | - incremental_lineage<br />- include_table_location_lineage<br />- include_view_lineage<br />- include_view_column_lineage|
| [Azure Data Factory](../../generated/ingestion/sources/azure-data-factory.md) | ✅ | ✅ | |
| [BigID](../../generated/ingestion/sources/bigid.md) | ❌ | ❌ | |
| [BigQuery](../../generated/ingestion/sources/bigquery.md) | ✅ | ✅ | - enable_stateful_lineage_ingestion<br />- incremental_lineage<br />- include_table_location_lineage<br />- include_view_lineage<br />- include_view_column_lineage<br />- gcs_lineage_config<br />- lineage_use_sql_parser<br />- lineage_sql_parser_use_raw_names<br />- extract_column_lineage<br />- extract_lineage_from_catalog<br />- include_table_lineage<br />- include_column_lineage_with_gcs<br />- upstream_lineage_in_report|
| [Business Glossary](../../generated/ingestion/sources/business-glossary.md) | ❌ | ❌ | |
| [Cassandra](../../generated/ingestion/sources/cassandra.md) | ❌ | ❌ | |
| [ClickHouse `clickhouse`](../../generated/ingestion/sources/clickhouse.md) | ✅ | ✅ | - incremental_lineage<br />- include_table_location_lineage<br />- include_view_lineage<br />- include_view_column_lineage<br />- include_table_lineage<br />- include_query_log_lineage|
| [ClickHouse `clickhouse-usage`](../../generated/ingestion/sources/clickhouse.md) | ❌ | ❌ | |
| [CockroachDB](../../generated/ingestion/sources/cockroachdb.md) | ✅ | ✅ | - incremental_lineage<br />- include_table_location_lineage<br />- include_view_lineage<br />- include_view_column_lineage<br />- include_query_lineage|
| [Confluence](../../generated/ingestion/sources/confluence.md) | ❌ | ❌ | |
| [CSV Enricher](../../generated/ingestion/sources/csv-enricher.md) | ❌ | ❌ | |
| [Cube](../../generated/ingestion/sources/cube.md) | ✅ | ✅ | - incremental_lineage<br />- include_lineage<br />- include_column_lineage<br />- parse_sql_for_lineage<br />- convert_lineage_urns_to_lowercase|
| [Databricks](../../generated/ingestion/sources/databricks.md) | ✅ | ✅ | - incremental_lineage<br />- include_table_location_lineage<br />- include_view_lineage<br />- include_view_column_lineage<br />- include_table_lineage<br />- include_external_lineage<br />- include_column_lineage<br />- include_federation_lineage<br />- lineage_data_source<br />- ignore_start_time_lineage<br />- column_lineage_column_limit<br />- skip_sqlglot_when_system_table_lineage_missing|
| [DataHub](../../generated/ingestion/sources/datahub.md) | ❌ | ❌ | |
| [DataHubApply](../../generated/ingestion/sources/datahubapply.md) | ❌ | ❌ | |
| [DataHubDebug](../../generated/ingestion/sources/datahubdebug.md) | ❌ | ❌ | |
| [DataHubDocuments](../../generated/ingestion/sources/datahub-documents.md) | ❌ | ❌ | |
| [DataHubGc](../../generated/ingestion/sources/datahubgc.md) | ❌ | ❌ | |
| [dbt `dbt`](../../generated/ingestion/sources/dbt.md) | ✅ | ✅ | - incremental_lineage<br />- prefer_sql_parser_lineage<br />- skip_sources_in_lineage<br />- skip_missing_upstreams_in_lineage<br />- include_column_lineage|
| [dbt `dbt-cloud`](../../generated/ingestion/sources/dbt.md) | ✅ | ✅ | - incremental_lineage<br />- prefer_sql_parser_lineage<br />- skip_sources_in_lineage<br />- skip_missing_upstreams_in_lineage<br />- include_column_lineage|
| [Delta Lake](../../generated/ingestion/sources/delta-lake.md) | ❌ | ❌ | |
| [dlt](../../generated/ingestion/sources/dlt.md) | ✅ | ❌ | - include_lineage|
| [Dremio](../../generated/ingestion/sources/dremio.md) | ✅ | ✅ | - include_query_lineage<br />- incremental_lineage|
| [Druid](../../generated/ingestion/sources/druid.md) | ✅ | ✅ | - incremental_lineage<br />- include_table_location_lineage<br />- include_view_lineage<br />- include_view_column_lineage|
| [Elasticsearch](../../generated/ingestion/sources/elasticsearch.md) | ❌ | ❌ | |
| [Excel](../../generated/ingestion/sources/excel.md) | ❌ | ❌ | |
| [Fabric Data Factory](../../generated/ingestion/sources/fabric-data-factory.md) | ✅ | ❌ | - include_lineage|
| [Fabric OneLake](../../generated/ingestion/sources/fabric-onelake.md) | ❌ | ✅ | |
| [Feast](../../generated/ingestion/sources/feast.md) | ✅ | ❌ | |
| [File Based Lineage](../../generated/ingestion/sources/file-based-lineage.md) | ✅ | ✅ | |
| [Fivetran](../../generated/ingestion/sources/fivetran.md) | ❌ | ✅ | - include_column_lineage<br />- max_table_lineage_per_connector<br />- max_column_lineage_per_connector|
| [Flink](../../generated/ingestion/sources/flink.md) | ✅ | ❌ | - include_lineage|
| [GitHub](../../generated/ingestion/sources/github.md) | ❌ | ❌ | |
| [Glue](../../generated/ingestion/sources/glue.md) | ✅ | ✅ | - emit_storage_lineage<br />- glue_storage_lineage_direction<br />- include_column_lineage<br />- include_view_lineage|
| [Google Cloud Knowledge Catalog (Dataplex)](../../generated/ingestion/sources/dataplex.md) | ✅ | ❌ | - enable_stateful_lineage_ingestion<br />- include_lineage<br />- lineage_locations<br />- lineage_max_retries<br />- lineage_retry_backoff_multiplier<br />- max_workers_lineage|
| [Google Cloud Storage](../../generated/ingestion/sources/gcs.md) | ❌ | ❌ | |
| [Grafana](../../generated/ingestion/sources/grafana.md) | ✅ | ✅ | - include_lineage<br />- include_column_lineage|
| [Hex](../../generated/ingestion/sources/hex.md) | ✅ | ✅ | - include_lineage<br />- use_queried_tables_lineage|
| [Hive](../../generated/ingestion/sources/hive.md) | ✅ | ✅ | - emit_storage_lineage<br />- hive_storage_lineage_direction<br />- include_column_lineage<br />- incremental_lineage<br />- include_view_lineage<br />- include_view_column_lineage|
| [Hive Metastore `hive-metastore`](../../generated/ingestion/sources/hive-metastore.md) | ✅ | ✅ | - incremental_lineage<br />- emit_storage_lineage<br />- hive_storage_lineage_direction<br />- include_column_lineage<br />- include_table_location_lineage<br />- include_view_lineage<br />- include_view_column_lineage|
| [Hive Metastore `presto-on-hive`](../../generated/ingestion/sources/hive-metastore.md) | ✅ | ✅ | - incremental_lineage<br />- emit_storage_lineage<br />- hive_storage_lineage_direction<br />- include_column_lineage<br />- include_table_location_lineage<br />- include_view_lineage<br />- include_view_column_lineage|
| [IBM Db2](../../generated/ingestion/sources/db2.md) | ✅ | ✅ | - incremental_lineage<br />- include_table_location_lineage<br />- include_view_lineage<br />- include_view_column_lineage|
| [Informatica](../../generated/ingestion/sources/informatica.md) | ✅ | ❌ | - extract_lineage|
| [Kafka](../../generated/ingestion/sources/kafka.md) | ❌ | ❌ | |
| [Kafka Connect](../../generated/ingestion/sources/kafka-connect.md) | ✅ | ❌ | - convert_lineage_urns_to_lowercase<br />- schema_resolver_finegrained_lineage|
| [Looker `looker`](../../generated/ingestion/sources/looker.md) | ✅ | ✅ | - extract_column_level_lineage|
| [Looker `lookml`](../../generated/ingestion/sources/looker.md) | ✅ | ✅ | - extract_column_level_lineage<br />- use_api_for_view_lineage<br />- use_api_cache_for_view_lineage<br />- allow_partial_lineage_results|
| [MariaDB](../../generated/ingestion/sources/mariadb.md) | ✅ | ✅ | - incremental_lineage<br />- include_table_location_lineage<br />- include_view_lineage<br />- include_view_column_lineage|
| [Matillion](../../generated/ingestion/sources/matillion-dpc.md) | ✅ | ✅ | - parse_sql_for_lineage<br />- lineage_platform_mapping|
| [Metabase](../../generated/ingestion/sources/metabase.md) | ✅ | ❌ | |
| [Metadata File](../../generated/ingestion/sources/metadata-file.md) | ❌ | ❌ | |
| [Microsoft SQL Server](../../generated/ingestion/sources/mssql.md) | ✅ | ✅ | - incremental_lineage<br />- include_table_location_lineage<br />- include_view_lineage<br />- include_view_column_lineage<br />- include_lineage<br />- include_query_lineage|
| [MicroStrategy](../../generated/ingestion/sources/microstrategy.md) | ✅ | ✅ | - extract_lineage<br />- extract_model_lineage<br />- extract_warehouse_lineage<br />- extract_report_sql_lineage<br />- warehouse_lineage_sql_timeout_seconds|
| [MLflow](../../generated/ingestion/sources/mlflow.md) | ❌ | ❌ | |
| [Mode](../../generated/ingestion/sources/mode.md) | ✅ | ✅ | |
| [MongoDB](../../generated/ingestion/sources/mongodb.md) | ❌ | ❌ | |
| [MySQL](../../generated/ingestion/sources/mysql.md) | ✅ | ✅ | - incremental_lineage<br />- include_table_location_lineage<br />- include_view_lineage<br />- include_view_column_lineage|
| [Neo4j](../../generated/ingestion/sources/neo4j.md) | ❌ | ❌ | |
| [NiFi](../../generated/ingestion/sources/nifi.md) | ✅ | ❌ | - incremental_lineage|
| [Notion](../../generated/ingestion/sources/notion.md) | ❌ | ❌ | |
| [Okta](../../generated/ingestion/sources/okta.md) | ❌ | ❌ | |
| [Omni](../../generated/ingestion/sources/omni.md) | ✅ | ✅ | - include_column_lineage|
| [Open Data Contract Standard](../../generated/ingestion/sources/odcs.md) | ❌ | ❌ | |
| [Oracle](../../generated/ingestion/sources/oracle.md) | ✅ | ✅ | - incremental_lineage<br />- include_table_location_lineage<br />- include_view_lineage<br />- include_view_column_lineage<br />- include_lineage|
| [Pinecone](../../generated/ingestion/sources/pinecone.md) | ❌ | ❌ | |
| [Postgres](../../generated/ingestion/sources/postgres.md) | ✅ | ✅ | - incremental_lineage<br />- include_table_location_lineage<br />- include_view_lineage<br />- include_view_column_lineage<br />- include_query_lineage|
| [PowerBI](../../generated/ingestion/sources/powerbi.md) | ✅ | ✅ | - incremental_lineage<br />- extract_lineage<br />- convert_lineage_urns_to_lowercase<br />- enable_advance_lineage_sql_construct<br />- extract_column_level_lineage|
| [PowerBI Report Server](../../generated/ingestion/sources/powerbi-report-server.md) | ❌ | ❌ | |
| [Preset](../../generated/ingestion/sources/preset.md) | ✅ | ❌ | |
| [Presto](../../generated/ingestion/sources/presto.md) | ✅ | ✅ | - incremental_lineage<br />- include_table_location_lineage<br />- include_view_lineage<br />- include_view_column_lineage<br />- ingest_lineage_to_connectors<br />- include_column_lineage|
| [Qlik Sense](../../generated/ingestion/sources/qlik-sense.md) | ✅ | ✅ | |
| [QuickSight](../../generated/ingestion/sources/quicksight.md) | ✅ | ✅ | - extract_lineage<br />- include_column_lineage|
| [RDF](../../generated/ingestion/sources/rdf.md) | ❌ | ❌ | |
| [Redash](../../generated/ingestion/sources/redash.md) | ✅ | ❌ | |
| [Redshift](../../generated/ingestion/sources/redshift.md) | ✅ | ✅ | - enable_stateful_lineage_ingestion<br />- incremental_lineage<br />- s3_lineage_config<br />- include_table_location_lineage<br />- include_view_lineage<br />- include_view_column_lineage<br />- lineage_generate_queries<br />- include_table_lineage<br />- include_copy_lineage<br />- include_share_lineage<br />- include_unload_lineage<br />- include_table_rename_lineage<br />- table_lineage_mode<br />- extract_column_level_lineage<br />- resolve_temp_table_in_lineage|
| [S3 / Local Files](../../generated/ingestion/sources/s3.md) | ❌ | ❌ | |
| [SageMaker](../../generated/ingestion/sources/sagemaker.md) | ✅ | ❌ | |
| [Salesforce](../../generated/ingestion/sources/salesforce.md) | ✅ | ❌ | |
| [SAP Analytics Cloud](../../generated/ingestion/sources/sac.md) | ✅ | ❌ | - incremental_lineage|
| [SAP HANA](../../generated/ingestion/sources/hana.md) | ✅ | ✅ | - incremental_lineage<br />- include_table_location_lineage<br />- include_view_lineage<br />- include_view_column_lineage|
| [Sigma](../../generated/ingestion/sources/sigma.md) | ✅ | ✅ | - extract_lineage<br />- workbook_lineage_pattern|
| [Slack](../../generated/ingestion/sources/slack.md) | ❌ | ❌ | |
| [SnapLogic](../../generated/ingestion/sources/snaplogic.md) | ✅ | ✅ | - enable_stateful_lineage_ingestion|
| [Snowflake](../../generated/ingestion/sources/snowflake.md) | ✅ | ✅ | - incremental_lineage<br />- include_table_location_lineage<br />- include_view_lineage<br />- include_view_column_lineage<br />- enable_stateful_lineage_ingestion<br />- include_table_lineage<br />- ignore_start_time_lineage<br />- upstream_lineage_in_report<br />- include_column_lineage|
| [Snowplow](../../generated/ingestion/sources/snowplow.md) | ✅ | ❌ | - warehouse_lineage|
| [SQL Queries](../../generated/ingestion/sources/sql-queries.md) | ✅ | ✅ | - incremental_lineage|
| [StarRocks](../../generated/ingestion/sources/starrocks.md) | ✅ | ✅ | - incremental_lineage<br />- include_table_location_lineage<br />- include_view_lineage<br />- include_view_column_lineage|
| [Superset](../../generated/ingestion/sources/superset.md) | ✅ | ❌ | |
| [Tableau](../../generated/ingestion/sources/tableau.md) | ✅ | ✅ | - extract_column_level_lineage<br />- lineage_overrides<br />- extract_lineage_from_unsupported_custom_sql_queries<br />- force_extraction_of_lineage_from_custom_sql_queries<br />- initial_sql_as_lineage|
| [Teradata](../../generated/ingestion/sources/teradata.md) | ✅ | ✅ | - incremental_lineage<br />- include_table_location_lineage<br />- include_view_lineage<br />- include_view_column_lineage<br />- include_table_lineage<br />- include_historical_lineage<br />- lineage_fetch_stall_warning_seconds<br />- lineage_fetch_batch_size<br />- lineage_slow_query_log_seconds|
| [ThoughtSpot](../../generated/ingestion/sources/thoughtspot.md) | ✅ | ✅ | - include_external_lineage|
| [TiDB](../../generated/ingestion/sources/tidb.md) | ✅ | ✅ | - incremental_lineage<br />- include_table_location_lineage<br />- include_view_lineage<br />- include_view_column_lineage|
| [TimescaleDB](../../generated/ingestion/sources/timescaledb.md) | ✅ | ✅ | - incremental_lineage<br />- include_table_location_lineage<br />- include_view_lineage<br />- include_view_column_lineage<br />- include_query_lineage|
| [Trino `trino`](../../generated/ingestion/sources/trino.md) | ✅ | ✅ | - incremental_lineage<br />- include_table_location_lineage<br />- include_view_lineage<br />- include_view_column_lineage<br />- ingest_lineage_to_connectors<br />- include_column_lineage|
| [Trino `starburst-trino-usage`](../../generated/ingestion/sources/trino.md) | ❌ | ❌ | |
| [Vertex AI](../../generated/ingestion/sources/vertexai.md) | ✅ | ❌ | - incremental_lineage<br />- use_ml_metadata_for_lineage|
| [Vertica](../../generated/ingestion/sources/vertica.md) | ✅ | ✅ | - incremental_lineage<br />- include_table_location_lineage<br />- include_view_lineage<br />- include_view_column_lineage<br />- include_projection_lineage|

        
## SQL Parser Lineage Extraction

If you're using a different database system for which we don't support column-level lineage out of the box, but you do have a database query log available, 
we have a SQL queries connector that generates column-level lineage and detailed table usage statistics from the query log.

If these does not suit your needs, you can use the new `DataHubGraph.parse_sql_lineage()` method in our SDK. (See the source code [here](/docs/python-sdk/clients/graph-client))

For more information, refer to the [Extracting Column-Level Lineage from SQL](https://blog.datahubproject.io/extracting-column-level-lineage-from-sql-779b8ce17567) 


:::tip Our Roadmap
We're actively working on expanding lineage support for new data sources.
Visit our [Official Roadmap](https://feature-requests.datahubproject.io/roadmap) for upcoming updates!
:::

## References

- [DataHub Basics: Lineage 101](https://www.youtube.com/watch?v=rONGpsndzRw&t=1s)
- [DataHub November 2022 Town Hall](https://www.youtube.com/watch?v=BlCLhG8lGoY&t=1s) - Including Manual Lineage Demo
- [Data in Context: Lineage Explorer in DataHub](https://blog.datahubproject.io/data-in-context-lineage-explorer-in-datahub-a53a9a476dc4)
- [Harnessing the Power of Data Lineage with DataHub](https://blog.datahubproject.io/harnessing-the-power-of-data-lineage-with-datahub-ad086358dec4)
- [Data Lineage: What It Is And Why It Matters](https://blog.datahubproject.io/data-lineage-what-it-is-and-why-it-matters-1a8d9846f0bd)
                        


:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [DataHub repository](https://github.com/datahub-project/datahub) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
