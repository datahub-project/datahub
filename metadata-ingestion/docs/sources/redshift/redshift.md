# Amazon Redshift

For context on getting started with ingestion, check out our [metadata ingestion guide](../../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[redshift]'`.

## Capabilities

This plugin extracts the following:

- Metadata for databases, schemas, views and tables
- Column types associated with each table
- Table, row, and column statistics via optional SQL profiling
- Table lineage
- Usage statistics

## Concept mapping

| Source Concept | DataHub Concept                                           | Notes              |
| -------------- | --------------------------------------------------------- | ------------------ |
| `"redshift"`   | [Data Platform](../../metamodel/entities/dataPlatform.md) |                    |
| Database       | [Container](../../metamodel/entities/container.md)        | Subtype `Database` |
| Schema         | [Container](../../metamodel/entities/container.md)        | Subtype `Schema`   |
| Table          | [Dataset](../../metamodel/entities/dataset.md)            | Subtype `Table`    |
| View           | [Dataset](../../metamodel/entities/dataset.md)            | Subtype `View`     |

## Ingestion of multiple redshift databases, namespaces

- If multiple databases are present in the Redshift namespace (or provisioned cluster),
  you would need to set up a separate ingestion per database.

- Ingestion recipes of all databases in a particular redshift namespace should use same platform instance.

- If you've multiple redshift namespaces that you want to ingest within DataHub, it is highly recommended that
  you specify a platform_instance equivalent to namespace in recipe. It can be same as namespace id or other
  human readable name however it should be unique across all your redshift namespaces.

## Lineage

There are multiple lineage collector implementations as Redshift does not support table lineage out of the box.

### stl_scan_based

The stl_scan based collector uses Redshift's [stl_insert](https://docs.aws.amazon.com/redshift/latest/dg/r_STL_INSERT.html) and [stl_scan](https://docs.aws.amazon.com/redshift/latest/dg/r_STL_SCAN.html) system tables to
discover lineage between tables.

**Pros:**

- Fast
- Reliable

**Cons:**

- Does not work with Spectrum/external tables because those scans do not show up in stl_scan table.
- If a table is depending on a view then the view won't be listed as dependency. Instead the table will be connected with the view's dependencies.

### sql_based

The sql_based based collector uses Redshift's [stl_insert](https://docs.aws.amazon.com/redshift/latest/dg/r_STL_INSERT.html) to discover all the insert queries
and uses sql parsing to discover the dependencies.

**Pros:**

- Works with Spectrum tables
- Views are connected properly if a table depends on it

**Cons:**

- Slow.
- Less reliable as the query parser can fail on certain queries

### mixed

Using both collector above and first applying the sql based and then the stl_scan based one.

**Pros:**

- Works with Spectrum tables
- Views are connected properly if a table depends on it
- A bit more reliable than the sql_based one only

**Cons:**

- Slow
- May be incorrect at times as the query parser can fail on certain queries

:::note

The redshift stl redshift tables which are used for getting data lineage retain at most seven days of log history, and sometimes closer to 2-5 days. This means you cannot extract lineage from queries issued outside that window.

:::

## Data Sharing Lineage

This is enabled by default, can be disabled via setting `include_share_lineage: False`

It is mandatory to run redshift ingestion of datashare producer namespace at least once so that lineage
shows up correctly after datashare consumer namespace is ingested.

## Profiling

Profiling runs sql queries on the redshift cluster to get statistics about the tables. To be able to do that, the user needs to have read access to the tables that should be profiled.

If you don't want to grant read access to the tables you can enable table level profiling which will get table statistics without reading the data.

```yaml
profiling:
  profile_table_level_only: true
```

## Configuration

For detailed configuration options, see the [recipe configuration](redshift_recipe.yml).

For prerequisites and permissions setup, see the [prerequisites guide](redshift_pre.md).
