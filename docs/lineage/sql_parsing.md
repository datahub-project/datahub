---
title: SQL Parsing
---

# The DataHub SQL Parser

Many data platforms are built on top of SQL, which means deeply understanding SQL queries is critical for understanding column-level lineage, usage, and more.

DataHub's SQL parser is built on top of [sqlglot](https://github.com/tobymao/sqlglot) and adds a number of additional features to improve the accuracy of SQL parsing.

In our benchmarks, the DataHub SQL parser generates lineage with 97-99% accuracy and outperforms other SQL parsers by a wide margin.

We've published a blog post on some of the technical details of the parser: [Extracting Column Lineage from SQL Queries](https://blog.datahubproject.io/extracting-column-level-lineage-from-sql-779b8ce17567).

## Built-in SQL Parsing Support

If you're using a tool that DataHub already [integrates with](https://datahubproject.io/integrations), check the documentation for that specific integration.
Most of our integrations, including Snowflake, BigQuery, Redshift, dbt, Looker, PowerBI, Airflow, etc, use the SQL parser to generate column-level lineage and usage statistics.

If you’re using a different database system for which we don’t support column-level lineage out of the box, but you do have a database query log available, the [SQL queries](../generated/ingestion/sources/sql-queries.md) connector can generate column-level lineage and table/column usage statistics from the query log.

## SDK Support

Our SDK provides a [`DataHubGraph.parse_sql_lineage()`](../../python-sdk/clients.md#datahub.ingestion.graph.client.DataHubGraph.parse_sql_lineage) method for programmatically parsing SQL queries.

The resulting object contains a `sql_parsing_result.debug_info.confidence_score` field, which is a 0-1 value indicating the confidence of the parser.

There are also a number of utilities in the `datahub.sql_parsing` module. The `SqlParsingAggregator` is particularly useful, as it can also resolve lineage across temp tables and table renames/swaps.
Note that these utilities are not officially part of the DataHub SDK and hence do not have the same level of stability and support as the rest of the SDK.

## Capabilities

### Supported

- Table-level lineage for `SELECT`, `CREATE`, `INSERT`, `UPDATE`, `DELETE`, and `MERGE` statements
- Column-level lineage for `SELECT` (including `SELECT INTO`), `CREATE VIEW`, `CREATE TABLE AS SELECT` (CTAS), `INSERT`, and `UPDATE` statements
- Subqueries
- CTEs
- `UNION ALL` constructs - will merge lineage across the clauses of the `UNION`
- `SELECT *` and similar expressions will automatically be expanded with the table schemas registered in DataHub. This includes support for platform instances.
- Automatic handling for systems where table and column names are case insensitive. Generally requires that `convert_urns_to_lowercase` is enabled when the corresponding table schemas were ingested into DataHub.
  - Specifically, we'll do fuzzy matching against the table names and schemas to resolve the correct URNs. We do not support having multiple tables/columns that only differ in casing.
- For BigQuery, sharded table suffixes will automatically be normalized. For example, `proj.dataset.table_20230616` will be normalized to `proj.dataset.table_yyyymmdd`. This matches the behavior of our BigQuery ingestion connector, and hence will result in lineage linking up correctly.

### Not supported

- Scalar `UDFs` - We will generate lineage pointing at the columns that are inputs to the UDF, but will not be able to understand the UDF itself.
- Tabular `UDFs`
- `json_extract` and similar functions
- `UNNEST` - We will do a best-effort job, but cannot reliably generate column-level lineage in the presence of `UNNEST` constructs.
- Structs - We will do a best-effort attempt to resolve struct subfields, but it is not guaranteed. This will only impact column-level lineage.
- Snowflake's multi-table inserts
- Multi-statement SQL / SQL scripting

### Limitations

- We only support the 20+ SQL dialects supported by the underlying [sqlglot](https://github.com/tobymao/sqlglot) library.
- There's a few SQL syntaxes that we don't support yet, but intend to support in the future.
  - `INSERT INTO (col1_new, col2_new) SELECT col1_old, col2_old FROM ...`. We only support `INSERT INTO` statements that either (1) don't specify a column list, or (2) specify a column list that matches the columns in the `SELECT` clause.
  - `MERGE INTO` statements - We don't generate column-level lineage for these.
- In cases where the table schema information in DataHub is outdated or otherwise incorrect, we may not be able to generate accurate column-level lineage.
- We trip over BigQuery queries that use the `_partitiontime` and `_partitiondate` pseudo-columns with a table name prefix e.g. `my_table._partitiontime` fails. However, unqualified references like `_partitiontime` and `_partitiondate` will be fine.
- We do not consider columns referenced in `WHERE`, `GROUP BY`, `ORDER BY`, etc. clauses to be part of lineage. For example, `SELECT col1, col2 FROM upstream_table WHERE col3 = 3` will not generate any lineage related to `col3`.
