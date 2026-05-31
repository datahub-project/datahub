## Overview

Glue is a data platform used to store and query analytical or operational data. Learn more in the [official Glue documentation](https://aws.amazon.com/glue/).

The DataHub integration for Glue covers core metadata entities such as datasets/tables/views, schema fields, and containers. It also captures table- and column-level lineage and stateful deletion detection.

:::tip
If you also have files in S3 that you'd like to ingest, we recommend you use Glue's built-in data catalog. See here for a quick guide on how to set up a crawler on Glue and ingest the outputs with DataHub.
:::

## Concept Mapping

| Source Concept       | DataHub Concept                                           | Notes              |
| -------------------- | --------------------------------------------------------- | ------------------ |
| `"glue"`             | [Data Platform](../../metamodel/entities/dataPlatform.md) |                    |
| Glue Database        | [Container](../../metamodel/entities/container.md)        | Subtype `Database` |
| Glue Table           | [Dataset](../../metamodel/entities/dataset.md)            | Subtype `Table`    |
| Glue Job             | [Data Flow](../../metamodel/entities/dataFlow.md)         |                    |
| Glue Job Transform   | [Data Job](../../metamodel/entities/dataJob.md)           |                    |
| Glue Job Data source | [Dataset](../../metamodel/entities/dataset.md)            |                    |
| Glue Job Data sink   | [Dataset](../../metamodel/entities/dataset.md)            |                    |

### Compatibility

To capture lineage across Glue jobs and databases, a requirements must be met – otherwise the AWS API is unable to report any lineage. The job must be created in Glue Studio with the "Generate classic script" option turned on (this option can be accessed in the "Script" tab). Any custom scripts that do not have the proper annotations will not have reported lineage.

### JDBC Lineage

DataHub extracts upstream lineage for Glue job nodes that read from JDBC databases. Two node styles are supported:

#### Named Glue Connections (Visual Editor)

Glue Studio's visual editor stores connection references as `connection_options.connectionName`. DataHub calls the `GetConnection` API to resolve the connection and determine the platform and database.

Supported connection types:

| Glue `ConnectionType` | DataHub Platform                 |
| --------------------- | -------------------------------- |
| `JDBC`                | Parsed from JDBC URL (see below) |
| `POSTGRESQL`          | `postgres`                       |
| `MYSQL`               | `mysql`                          |
| `REDSHIFT`            | `redshift`                       |
| `ORACLE`              | `oracle`                         |
| `SQLSERVER`           | `mssql`                          |

The table is read from `connection_options.dbtable`. If `dbtable` is absent, DataHub falls back to parsing `connection_options.query` (see [SQL Query Lineage](#sql-query-lineage) below).

#### Inline JDBC Nodes (Script Style)

Script-style nodes set `connection_type` to the database protocol and pass the JDBC URL inline via `connection_options.url`. Supported protocols:

| `connection_type` | DataHub Platform | Default schema |
| ----------------- | ---------------- | -------------- |
| `postgresql`      | `postgres`       | `public`       |
| `mysql`           | `mysql`          | —              |
| `mariadb`         | `mysql`          | —              |
| `redshift`        | `redshift`       | `public`       |
| `oracle`          | `oracle`         | —              |
| `sqlserver`       | `mssql`          | `dbo`          |

Example job script args that DataHub can parse:

```python
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="postgresql",
    connection_options={
        "url": "jdbc:postgresql://myhost:5432/mydb",
        "dbtable": "public.orders",
        # or: "query": "SELECT * FROM public.orders WHERE region = 'US'"
    },
)
```

#### Dataset Name Construction

Given a `dbtable` value and the resolved `(platform, database)`:

- `dbtable = "schema.table"` → `database.schema.table`
- `dbtable = "table"` (no schema) → `database.<default_schema>.table` if the platform has a default schema, otherwise `database.table`

#### SQL Query Lineage

When `dbtable` is absent and `connection_options.query` is set, DataHub uses [sqlglot](https://github.com/tobymao/sqlglot) to extract table references from the SQL string.

**Supported:** Single-table queries, JOINs, CTEs, subqueries — all referenced tables are emitted as upstream datasets.

```sql
-- All three tables become upstream lineage inputs
SELECT o.id, c.name, p.price
FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN products p ON o.product_id = p.id
```

**Not supported:** Queries that fail to parse, or queries with no table references (e.g. `SELECT 1`). These produce a warning and the node is skipped.

> **Note:** `query`-based lineage reflects the tables referenced in the SQL at ingestion time. Dynamic SQL, parameterized queries, or queries built at runtime cannot be statically analyzed.
