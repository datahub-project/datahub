---
sidebar_position: 30
title: MySQL
slug: /generated/ingestion/sources/mysql
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/mysql.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# MySQL

![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)

### Important Capabilities

| Capability                                                                                                 | Status | Notes                                   |
| ---------------------------------------------------------------------------------------------------------- | ------ | --------------------------------------- |
| [Data Profiling](../../../../metadata-ingestion/docs/dev_guides/sql_profiles.md)                           | ✅     | Optionally enabled via configuration    |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅     | Enabled via stateful ingestion          |
| [Domains](../../../domains.md)                                                                             | ✅     | Supported via the `domain` config field |
| [Platform Instance](../../../platform-instances.md)                                                        | ✅     | Enabled by default                      |

This plugin extracts the following:

Metadata for databases, schemas, and tables
Column types and schema associated with each table
Table, row, and column statistics via optional SQL profiling

### Prerequisites

In order to execute this source the user credentials needs the following privileges

- `grant select on DATABASE.* to 'USERNAME'@'%'`
- `grant show view on DATABASE.* to 'USERNAME'@'%'`

`select` is required to see the table structure as well as for profiling.

### CLI based Ingestion

#### Install the Plugin

```shell
pip install 'acryl-datahub[mysql]'
```

### Starter Recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).

```yaml
source:
  type: mysql
  config:
    # Coordinates
    host_port: localhost:3306
    database: dbname

    # Credentials
    username: root
    password: example

    # If you need to use SSL with MySQL:
    # options:
    #   connect_args:
    #     ssl_ca: "path_to/server-ca.pem"
    #     ssl_cert: "path_to/client-cert.pem"
    #     ssl_key: "path_to/client-key.pem"

sink:
  # sink configs
```

### Config Details

<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.

<div className='config-table'>

| Field                                                                                                                                                                                                                                        | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| <div className="path-line"><span className="path-main">database</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                 | database (catalog)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| <div className="path-line"><span className="path-main">database_alias</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                           | [Deprecated] Alias to apply to database when ingesting.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-main">host_port</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                | MySQL host URL. <div className="default-line default-line-with-docs">Default: <span className="default-value">localhost:3306</span></div>                                                                                                                                                                                                                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-main">include_table_location_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                          | If the source supports it, include table lineage to the underlying storage location. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                              |
| <div className="path-line"><span className="path-main">include_tables</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                          | Whether tables should be ingested. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                                |
| <div className="path-line"><span className="path-main">include_views</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                           | Whether views should be ingested. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                                 |
| <div className="path-line"><span className="path-main">options</span></div> <div className="type-name-line"><span className="type-name">object</span></div>                                                                                  | Any options specified here will be passed to [SQLAlchemy.create_engine](https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine) as kwargs.                                                                                                                                                                                                                                                                                                                                                                                  |
| <div className="path-line"><span className="path-main">password</span></div> <div className="type-name-line"><span className="type-name">string(password)</span></div>                                                                       | password                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                        | The instance of the platform that all assets produced by this recipe belong to                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| <div className="path-line"><span className="path-main">scheme</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                   | <div className="default-line ">Default: <span className="default-value">mysql+pymysql</span></div>                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| <div className="path-line"><span className="path-main">sqlalchemy_uri</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                           | URI of database to connect to. See https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls. Takes precedence over other connection parameters.                                                                                                                                                                                                                                                                                                                                                                                          |
| <div className="path-line"><span className="path-main">username</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                 | username                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                      | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div>                                                                                                                                                                                                                                                                                                                                                              |
| <div className="path-line"><span className="path-main">database_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                                               | Regex patterns for databases to filter in ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div>                                                                                                                                                                                                                                                              |
| <div className="path-line"><span className="path-prefix">database_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                       |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| <div className="path-line"><span className="path-prefix">database_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                        |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| <div className="path-line"><span className="path-prefix">database_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                        | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-main">domain</span></div> <div className="type-name-line"><span className="type-name">map(str,AllowDenyPattern)</span></div>                                                                | A class to store allow deny regexes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                           |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                            |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                            | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-main">profile_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                                                | Regex patterns to filter tables (or specific columns) for profiling during ingestion. Note that only tables allowed by the `table_pattern` will be considered. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div>                                                                                                                                                    |
| <div className="path-line"><span className="path-prefix">profile_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                        |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| <div className="path-line"><span className="path-prefix">profile_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                         |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| <div className="path-line"><span className="path-prefix">profile_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                         | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-main">schema_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                                                 | Deprecated in favour of database_pattern. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div>                                                                                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-prefix">schema_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                         |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| <div className="path-line"><span className="path-prefix">schema_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                          |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| <div className="path-line"><span className="path-prefix">schema_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                          | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-main">table_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                                                  | Regex patterns for tables to filter in ingestion. Specify regex to match the entire table name in database.schema.table format. e.g. to match all tables starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.\*' <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div>                                               |
| <div className="path-line"><span className="path-prefix">table_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                          |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| <div className="path-line"><span className="path-prefix">table_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                           |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| <div className="path-line"><span className="path-prefix">table_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                           | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-main">view_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                                                   | Regex patterns for views to filter in ingestion. Note: Defaults to table_pattern if not specified. Specify regex to match the entire view name in database.schema.view format. e.g. to match all views starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.\*' <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div> |
| <div className="path-line"><span className="path-prefix">view_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                           |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| <div className="path-line"><span className="path-prefix">view_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                            |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| <div className="path-line"><span className="path-prefix">view_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                            | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-main">profiling</span></div> <div className="type-name-line"><span className="type-name">GEProfilingConfig</span></div>                                                                     | <div className="default-line ">Default: <span className="default-value">&#123;&#x27;enabled&#x27;: False, &#x27;limit&#x27;: None, &#x27;offset&#x27;: None, ...</span></div>                                                                                                                                                                                                                                                                                                                                                                     |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">catch_exceptions</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                         | <div className="default-line ">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                  | Whether profiling should be done. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                                                                                                |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">field_sample_values_limit</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                | Upper limit for number of sample values to collect for all columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">20</span></div>                                                                                                                                                                                                                                                                                                                                                                 |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_distinct_count</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>             | Whether to profile for the number of distinct values for each column. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                             |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_distinct_value_frequencies</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for distinct value frequencies. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_histogram</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                  | Whether to profile for the histogram for numeric fields. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_max_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                  | Whether to profile for the max value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                          |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_mean_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                 | Whether to profile for the mean value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_median_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>               | Whether to profile for the median value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_min_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                  | Whether to profile for the min value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                          |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_null_count</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                 | Whether to profile for the number of nulls for each column. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_quantiles</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                  | Whether to profile for the quantiles of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_sample_values</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>              | Whether to profile for the sample values for all columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_stddev_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>               | Whether to profile for the standard deviation of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                 |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">limit</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                                    | Max number of documents to profile. By default, profiles all documents.                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">max_number_of_fields_to_profile</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>          | A positive integer that specifies the maximum number of columns to profile for any table. `None` implies all columns. The cost of profiling goes up significantly as the number of columns to profile goes up.                                                                                                                                                                                                                                                                                                                                    |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">max_workers</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                              | Number of worker threads to use for profiling. Set to 1 to disable. <div className="default-line default-line-with-docs">Default: <span className="default-value">80</span></div>                                                                                                                                                                                                                                                                                                                                                                 |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">offset</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                                   | Offset in documents to profile. By default, uses no offset.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">partition_datetime</span></div> <div className="type-name-line"><span className="type-name">string(date-time)</span></div>             | For partitioned datasets profile only the partition which matches the datetime or profile the latest one if not set. Only Bigquery supports this.                                                                                                                                                                                                                                                                                                                                                                                                 |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">partition_profiling_enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>              | <div className="default-line ">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_if_updated_since_days</span></div> <div className="type-name-line"><span className="type-name">number</span></div>             | Profile table only if it has been updated since these many number of days. If set to `null`, no constraint of last modified time for tables to profile. Supported only in `snowflake` and `BigQuery`.                                                                                                                                                                                                                                                                                                                                             |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_level_only</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                 | Whether to perform profiling at table-level only, or include column-level profiling as well. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                                     |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_row_count_estimate_only</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>    | Use an approximate query for row count. This will be much faster but slightly less accurate. Only supported for Postgres and MySQL. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                              |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_row_limit</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                  | Profile tables only if their row count is less then specified count. If set to `null`, no limit on the row count of tables to profile. Supported only in `snowflake` and `BigQuery` <div className="default-line default-line-with-docs">Default: <span className="default-value">5000000</span></div>                                                                                                                                                                                                                                            |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_size_limit</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                 | Profile tables only if their size is less then specified GBs. If set to `null`, no limit on the size of tables to profile. Supported only in `snowflake` and `BigQuery` <div className="default-line default-line-with-docs">Default: <span className="default-value">5</span></div>                                                                                                                                                                                                                                                              |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">query_combiner_enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                   | _This feature is still experimental and can be disabled if it causes issues._ Reduces the total number of queries issued and speeds up profiling by dynamically combining SQL queries where possible. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                             |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">report_dropped_profiles</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                  | Whether to report datasets or dataset columns which were not profiled. Set to `True` for debugging purposes. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                     |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">turn_off_expensive_profiling_metrics</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>     | Whether to turn off expensive profiling or not. This turns off profiling for quantiles, distinct_value_frequencies, histogram & sample_values. This also limits maximum number of fields being profiled to 10. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                   |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">StatefulStaleMetadataRemovalConfig</span></div>                                           | Base specialized config for Stateful Ingestion with stale metadata removal capability.                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                         | The type of the ingestion state provider registered with datahub. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                                                                |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>           | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                      |

</div>
</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.

```javascript
{
  "title": "MySQLConfig",
  "description": "Base configuration class for stateful ingestion for source configs to inherit from.",
  "type": "object",
  "properties": {
    "env": {
      "title": "Env",
      "description": "The environment that all assets produced by this connector belong to",
      "default": "PROD",
      "type": "string"
    },
    "platform_instance": {
      "title": "Platform Instance",
      "description": "The instance of the platform that all assets produced by this recipe belong to",
      "type": "string"
    },
    "stateful_ingestion": {
      "$ref": "#/definitions/StatefulStaleMetadataRemovalConfig"
    },
    "options": {
      "title": "Options",
      "description": "Any options specified here will be passed to [SQLAlchemy.create_engine](https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine) as kwargs.",
      "type": "object"
    },
    "schema_pattern": {
      "title": "Schema Pattern",
      "description": "Deprecated in favour of database_pattern.",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "allOf": [
        {
          "$ref": "#/definitions/AllowDenyPattern"
        }
      ]
    },
    "table_pattern": {
      "title": "Table Pattern",
      "description": "Regex patterns for tables to filter in ingestion. Specify regex to match the entire table name in database.schema.table format. e.g. to match all tables starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "allOf": [
        {
          "$ref": "#/definitions/AllowDenyPattern"
        }
      ]
    },
    "view_pattern": {
      "title": "View Pattern",
      "description": "Regex patterns for views to filter in ingestion. Note: Defaults to table_pattern if not specified. Specify regex to match the entire view name in database.schema.view format. e.g. to match all views starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "allOf": [
        {
          "$ref": "#/definitions/AllowDenyPattern"
        }
      ]
    },
    "profile_pattern": {
      "title": "Profile Pattern",
      "description": "Regex patterns to filter tables (or specific columns) for profiling during ingestion. Note that only tables allowed by the `table_pattern` will be considered.",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "allOf": [
        {
          "$ref": "#/definitions/AllowDenyPattern"
        }
      ]
    },
    "domain": {
      "title": "Domain",
      "description": "Attach domains to databases, schemas or tables during ingestion using regex patterns. Domain key can be a guid like *urn:li:domain:ec428203-ce86-4db3-985d-5a8ee6df32ba* or a string like \"Marketing\".) If you provide strings, then datahub will attempt to resolve this name to a guid, and will error out if this fails. There can be multiple domain keys specified.",
      "default": {},
      "type": "object",
      "additionalProperties": {
        "$ref": "#/definitions/AllowDenyPattern"
      }
    },
    "include_views": {
      "title": "Include Views",
      "description": "Whether views should be ingested.",
      "default": true,
      "type": "boolean"
    },
    "include_tables": {
      "title": "Include Tables",
      "description": "Whether tables should be ingested.",
      "default": true,
      "type": "boolean"
    },
    "include_table_location_lineage": {
      "title": "Include Table Location Lineage",
      "description": "If the source supports it, include table lineage to the underlying storage location.",
      "default": true,
      "type": "boolean"
    },
    "profiling": {
      "title": "Profiling",
      "default": {
        "enabled": false,
        "limit": null,
        "offset": null,
        "report_dropped_profiles": false,
        "turn_off_expensive_profiling_metrics": false,
        "profile_table_level_only": false,
        "include_field_null_count": true,
        "include_field_distinct_count": true,
        "include_field_min_value": true,
        "include_field_max_value": true,
        "include_field_mean_value": true,
        "include_field_median_value": true,
        "include_field_stddev_value": true,
        "include_field_quantiles": false,
        "include_field_distinct_value_frequencies": false,
        "include_field_histogram": false,
        "include_field_sample_values": true,
        "field_sample_values_limit": 20,
        "max_number_of_fields_to_profile": null,
        "profile_if_updated_since_days": null,
        "profile_table_size_limit": 5,
        "profile_table_row_limit": 5000000,
        "profile_table_row_count_estimate_only": false,
        "max_workers": 80,
        "query_combiner_enabled": true,
        "catch_exceptions": true,
        "partition_profiling_enabled": true,
        "partition_datetime": null
      },
      "allOf": [
        {
          "$ref": "#/definitions/GEProfilingConfig"
        }
      ]
    },
    "username": {
      "title": "Username",
      "description": "username",
      "type": "string"
    },
    "password": {
      "title": "Password",
      "description": "password",
      "type": "string",
      "writeOnly": true,
      "format": "password"
    },
    "host_port": {
      "title": "Host Port",
      "description": "MySQL host URL.",
      "default": "localhost:3306",
      "type": "string"
    },
    "database": {
      "title": "Database",
      "description": "database (catalog)",
      "type": "string"
    },
    "database_alias": {
      "title": "Database Alias",
      "description": "[Deprecated] Alias to apply to database when ingesting.",
      "type": "string"
    },
    "scheme": {
      "title": "Scheme",
      "default": "mysql+pymysql",
      "type": "string"
    },
    "sqlalchemy_uri": {
      "title": "Sqlalchemy Uri",
      "description": "URI of database to connect to. See https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls. Takes precedence over other connection parameters.",
      "type": "string"
    },
    "database_pattern": {
      "title": "Database Pattern",
      "description": "Regex patterns for databases to filter in ingestion.",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "allOf": [
        {
          "$ref": "#/definitions/AllowDenyPattern"
        }
      ]
    }
  },
  "additionalProperties": false,
  "definitions": {
    "DynamicTypedStateProviderConfig": {
      "title": "DynamicTypedStateProviderConfig",
      "type": "object",
      "properties": {
        "type": {
          "title": "Type",
          "description": "The type of the state provider to use. For DataHub use `datahub`",
          "type": "string"
        },
        "config": {
          "title": "Config",
          "description": "The configuration required for initializing the state provider. Default: The datahub_api config if set at pipeline level. Otherwise, the default DatahubClientConfig. See the defaults (https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/graph/client.py#L19)."
        }
      },
      "required": [
        "type"
      ],
      "additionalProperties": false
    },
    "StatefulStaleMetadataRemovalConfig": {
      "title": "StatefulStaleMetadataRemovalConfig",
      "description": "Base specialized config for Stateful Ingestion with stale metadata removal capability.",
      "type": "object",
      "properties": {
        "enabled": {
          "title": "Enabled",
          "description": "The type of the ingestion state provider registered with datahub.",
          "default": false,
          "type": "boolean"
        },
        "remove_stale_metadata": {
          "title": "Remove Stale Metadata",
          "description": "Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled.",
          "default": true,
          "type": "boolean"
        }
      },
      "additionalProperties": false
    },
    "AllowDenyPattern": {
      "title": "AllowDenyPattern",
      "description": "A class to store allow deny regexes",
      "type": "object",
      "properties": {
        "allow": {
          "title": "Allow",
          "description": "List of regex patterns to include in ingestion",
          "default": [
            ".*"
          ],
          "type": "array",
          "items": {
            "type": "string"
          }
        },
        "deny": {
          "title": "Deny",
          "description": "List of regex patterns to exclude from ingestion.",
          "default": [],
          "type": "array",
          "items": {
            "type": "string"
          }
        },
        "ignoreCase": {
          "title": "Ignorecase",
          "description": "Whether to ignore case sensitivity during pattern matching.",
          "default": true,
          "type": "boolean"
        }
      },
      "additionalProperties": false
    },
    "GEProfilingConfig": {
      "title": "GEProfilingConfig",
      "type": "object",
      "properties": {
        "enabled": {
          "title": "Enabled",
          "description": "Whether profiling should be done.",
          "default": false,
          "type": "boolean"
        },
        "limit": {
          "title": "Limit",
          "description": "Max number of documents to profile. By default, profiles all documents.",
          "type": "integer"
        },
        "offset": {
          "title": "Offset",
          "description": "Offset in documents to profile. By default, uses no offset.",
          "type": "integer"
        },
        "report_dropped_profiles": {
          "title": "Report Dropped Profiles",
          "description": "Whether to report datasets or dataset columns which were not profiled. Set to `True` for debugging purposes.",
          "default": false,
          "type": "boolean"
        },
        "turn_off_expensive_profiling_metrics": {
          "title": "Turn Off Expensive Profiling Metrics",
          "description": "Whether to turn off expensive profiling or not. This turns off profiling for quantiles, distinct_value_frequencies, histogram & sample_values. This also limits maximum number of fields being profiled to 10.",
          "default": false,
          "type": "boolean"
        },
        "profile_table_level_only": {
          "title": "Profile Table Level Only",
          "description": "Whether to perform profiling at table-level only, or include column-level profiling as well.",
          "default": false,
          "type": "boolean"
        },
        "include_field_null_count": {
          "title": "Include Field Null Count",
          "description": "Whether to profile for the number of nulls for each column.",
          "default": true,
          "type": "boolean"
        },
        "include_field_distinct_count": {
          "title": "Include Field Distinct Count",
          "description": "Whether to profile for the number of distinct values for each column.",
          "default": true,
          "type": "boolean"
        },
        "include_field_min_value": {
          "title": "Include Field Min Value",
          "description": "Whether to profile for the min value of numeric columns.",
          "default": true,
          "type": "boolean"
        },
        "include_field_max_value": {
          "title": "Include Field Max Value",
          "description": "Whether to profile for the max value of numeric columns.",
          "default": true,
          "type": "boolean"
        },
        "include_field_mean_value": {
          "title": "Include Field Mean Value",
          "description": "Whether to profile for the mean value of numeric columns.",
          "default": true,
          "type": "boolean"
        },
        "include_field_median_value": {
          "title": "Include Field Median Value",
          "description": "Whether to profile for the median value of numeric columns.",
          "default": true,
          "type": "boolean"
        },
        "include_field_stddev_value": {
          "title": "Include Field Stddev Value",
          "description": "Whether to profile for the standard deviation of numeric columns.",
          "default": true,
          "type": "boolean"
        },
        "include_field_quantiles": {
          "title": "Include Field Quantiles",
          "description": "Whether to profile for the quantiles of numeric columns.",
          "default": false,
          "type": "boolean"
        },
        "include_field_distinct_value_frequencies": {
          "title": "Include Field Distinct Value Frequencies",
          "description": "Whether to profile for distinct value frequencies.",
          "default": false,
          "type": "boolean"
        },
        "include_field_histogram": {
          "title": "Include Field Histogram",
          "description": "Whether to profile for the histogram for numeric fields.",
          "default": false,
          "type": "boolean"
        },
        "include_field_sample_values": {
          "title": "Include Field Sample Values",
          "description": "Whether to profile for the sample values for all columns.",
          "default": true,
          "type": "boolean"
        },
        "field_sample_values_limit": {
          "title": "Field Sample Values Limit",
          "description": "Upper limit for number of sample values to collect for all columns.",
          "default": 20,
          "type": "integer"
        },
        "max_number_of_fields_to_profile": {
          "title": "Max Number Of Fields To Profile",
          "description": "A positive integer that specifies the maximum number of columns to profile for any table. `None` implies all columns. The cost of profiling goes up significantly as the number of columns to profile goes up.",
          "exclusiveMinimum": 0,
          "type": "integer"
        },
        "profile_if_updated_since_days": {
          "title": "Profile If Updated Since Days",
          "description": "Profile table only if it has been updated since these many number of days. If set to `null`, no constraint of last modified time for tables to profile. Supported only in `snowflake` and `BigQuery`.",
          "exclusiveMinimum": 0,
          "type": "number"
        },
        "profile_table_size_limit": {
          "title": "Profile Table Size Limit",
          "description": "Profile tables only if their size is less then specified GBs. If set to `null`, no limit on the size of tables to profile. Supported only in `snowflake` and `BigQuery`",
          "default": 5,
          "type": "integer"
        },
        "profile_table_row_limit": {
          "title": "Profile Table Row Limit",
          "description": "Profile tables only if their row count is less then specified count. If set to `null`, no limit on the row count of tables to profile. Supported only in `snowflake` and `BigQuery`",
          "default": 5000000,
          "type": "integer"
        },
        "profile_table_row_count_estimate_only": {
          "title": "Profile Table Row Count Estimate Only",
          "description": "Use an approximate query for row count. This will be much faster but slightly less accurate. Only supported for Postgres and MySQL. ",
          "default": false,
          "type": "boolean"
        },
        "max_workers": {
          "title": "Max Workers",
          "description": "Number of worker threads to use for profiling. Set to 1 to disable.",
          "default": 80,
          "type": "integer"
        },
        "query_combiner_enabled": {
          "title": "Query Combiner Enabled",
          "description": "*This feature is still experimental and can be disabled if it causes issues.* Reduces the total number of queries issued and speeds up profiling by dynamically combining SQL queries where possible.",
          "default": true,
          "type": "boolean"
        },
        "catch_exceptions": {
          "title": "Catch Exceptions",
          "default": true,
          "type": "boolean"
        },
        "partition_profiling_enabled": {
          "title": "Partition Profiling Enabled",
          "default": true,
          "type": "boolean"
        },
        "partition_datetime": {
          "title": "Partition Datetime",
          "description": "For partitioned datasets profile only the partition which matches the datetime or profile the latest one if not set. Only Bigquery supports this.",
          "type": "string",
          "format": "date-time"
        }
      },
      "additionalProperties": false
    }
  }
}
```

</TabItem>
</Tabs>

### Code Coordinates

- Class Name: `datahub.ingestion.source.sql.mysql.MySQLSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/sql/mysql.py)

<h2>Questions</h2>

If you've got any questions on configuring ingestion for MySQL, feel free to ping us on [our Slack](https://slack.datahubproject.io).
