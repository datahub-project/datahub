---
sidebar_position: 50
title: Oracle
slug: /generated/ingestion/sources/oracle
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/oracle.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Oracle
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Enabled by default. Supported for types - Database, Schema. |
| [Classification](../../../../metadata-ingestion/docs/dev_guides/classification.md) | ✅ | Optionally enabled via `classification.enabled`. |
| Column-level Lineage | ✅ | Enabled by default to get lineage for views via `include_view_column_lineage`. Supported for types - View. |
| Descriptions | ✅ | Enabled by default. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| [Domains](../../../domains.md) | ✅ | Enabled by default. |
| Schema Metadata | ✅ | Enabled by default. |
| Table-Level Lineage | ✅ | Enabled by default to get lineage for views via `include_view_lineage`. Supported for types - View. |
| Test Connection | ✅ | Enabled by default. |


This plugin extracts the following:

- Metadata for databases, schemas, and tables
- Column types associated with each table
- Table, row, and column statistics via optional SQL profiling

Using the Oracle source requires that you've also installed the correct drivers; see the [cx_Oracle docs](https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html). The easiest one is the [Oracle Instant Client](https://www.oracle.com/database/technologies/instant-client.html).

### Prerequisites

#### Data Dictionary Mode/Views

The Oracle ingestion source supports two modes for extracting metadata information (see `data_dictionary_mode` option): `ALL` and `DBA`. In the `ALL` mode, the SQLAlchemy backend queries `ALL_` data dictionary views to extract metadata information. In the `DBA` mode, the Oracle ingestion source directly queries `DBA_` data dictionary views to extract metadata information. `ALL_` views only provide information accessible to the user used for ingestion while `DBA_` views provide information for the entire database (that is, all schema objects in the database).

The following table contains a brief description of what each data dictionary view is used for:

| Data Dictionary View                               | What's it used for?                                   |
| -------------------------------------------------- | ----------------------------------------------------- |
| `ALL_TABLES` or `DBA_TABLES`                       | Get list of all relational tables in the database     |
| `ALL_VIEWS` or `DBA_VIEWS`                         | Get list of all views in the database                 |
| `ALL_TAB_COMMENTS` or `DBA_TAB_COMMENTS`           | Get comments on tables and views                      |
| `ALL_TAB_COLS` or `DBA_TAB_COLS`                   | Get description of the columns of tables and views    |
| `ALL_COL_COMMENTS` or `DBA_COL_COMMENTS`           | Get comments on the columns of tables and views       |
| `ALL_TAB_IDENTITY_COLS` or `DBA_TAB_IDENTITY_COLS` | Get table identity columns                            |
| `ALL_CONSTRAINTS` or `DBA_CONSTRAINTS`             | Get constraint definitions on tables                  |
| `ALL_CONS_COLUMNS` or `DBA_CONS_COLUMNS`           | Get list of columns that are specified in constraints |
| `ALL_USERS` or `DBA_USERS`                         | Get all schema names                                  |

#### Data Dictionary Views accessible information and required privileges

- `ALL_` views display all the information accessible to the user used for ingestion, including information from the user's schema as well as information from objects in other schemas, if the user has access to those objects by way of grants of privileges or roles.
- `DBA_` views display all relevant information in the entire database. They can be queried only by users with the `SYSDBA` system privilege or `SELECT ANY DICTIONARY` privilege, or `SELECT_CATALOG_ROLE` role, or by users with direct privileges granted to them.

### CLI based Ingestion

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: oracle
  config:
    # Coordinates
    host_port: localhost:5432
    database: dbname

    # Credentials
    username: user
    password: pass

    # Options
    service_name: svc # omit database if using this option

sink:
  # sink configs
```

### Config Details
<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">host_port</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | host URL  |
| <div className="path-line"><span className="path-main">add_database_name_to_urn</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Add oracle database name to urn, default urn is schema.table <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to convert dataset urns to lowercase. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">data_dictionary_mode</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The data dictionary views mode, to extract information about schema objects ('ALL' and 'DBA' views are supported). (https://docs.oracle.com/cd/E11882_01/nav/catalog_views.htm) <div className="default-line default-line-with-docs">Default: <span className="default-value">ALL</span></div> |
| <div className="path-line"><span className="path-main">database</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | If using, omit `service_name`. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">enable_thick_mode</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Connection defaults to thin mode. Set to True to enable thick mode. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_table_location_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If the source supports it, include table lineage to the underlying storage location. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_tables</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether tables should be ingested. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_view_column_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Populates column-level lineage for  view->view and table->view lineage using DataHub's sql parser. Requires `include_view_lineage` to be enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_view_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Populates view->view and table->view lineage using DataHub's sql parser. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_views</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether views should be ingested. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">incremental_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, emits lineage as incremental to existing lineage already in DataHub. When disabled, re-states lineage on each run. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">options</span></div> <div className="type-name-line"><span className="type-name">object</span></div> | Any options specified here will be passed to [SQLAlchemy.create_engine](https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine) as kwargs. To set connection arguments in the URL, specify them under `connect_args`.  |
| <div className="path-line"><span className="path-main">password</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | password <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">scheme</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Will be set automatically to default value. <div className="default-line default-line-with-docs">Default: <span className="default-value">oracle</span></div> |
| <div className="path-line"><span className="path-main">service_name</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Oracle service name. If using, omit `database`. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">sqlalchemy_uri</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | URI of database to connect to. See https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls. Takes precedence over other connection parameters. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">thick_mode_lib_dir</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | If using thick mode on Windows or Mac, set thick_mode_lib_dir to the oracle client libraries path. On Linux, this value is ignored, as ldconfig or LD_LIBRARY_PATH will define the location. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">use_file_backed_cache</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to use a file backed cache for the view definitions. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">username</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | username <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">domain</span></div> <div className="type-name-line"><span className="type-name">map(str,AllowDenyPattern)</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to include in ingestion <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;.&#42;&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">domain.`key`.allow.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to exclude from ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">domain.`key`.deny.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">profile_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">profile_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">schema_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">schema_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">table_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">table_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">view_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">view_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">classification</span></div> <div className="type-name-line"><span className="type-name">ClassificationConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether classification should be used to auto-detect glossary terms <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">info_type_to_term</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div> |   |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">max_workers</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of worker processes to use for classification. Set to 1 to disable. <div className="default-line default-line-with-docs">Default: <span className="default-value">14</span></div> |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">sample_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of sample values used for classification. <div className="default-line default-line-with-docs">Default: <span className="default-value">100</span></div> |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">classifiers</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Classifiers to use to auto-detect glossary terms. If more than one classifier, infotype predictions from the classifier defined later in sequence take precedance. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#123;&#x27;type&#x27;: &#x27;datahub&#x27;, &#x27;config&#x27;: None&#125;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">classification.classifiers.</span><span className="path-main">DynamicTypedClassifierConfig</span></div> <div className="type-name-line"><span className="type-name">DynamicTypedClassifierConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">classification.classifiers.DynamicTypedClassifierConfig.</span><span className="path-main">type</span>&nbsp;<abbr title="Required if DynamicTypedClassifierConfig is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | The type of the classifier to use. For DataHub,  use `datahub`  |
| <div className="path-line"><span className="path-prefix">classification.classifiers.DynamicTypedClassifierConfig.</span><span className="path-main">config</span></div> <div className="type-name-line"><span className="type-name">One of object, null</span></div> | The configuration required for initializing the classifier. If not specified, uses defaults for classifer type. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">column_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">classification.column_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">table_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">classification.table_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">profiling</span></div> <div className="type-name-line"><span className="type-name">GEProfilingConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">catch_exceptions</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> |  <div className="default-line ">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether profiling should be done. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">field_sample_values_limit</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Upper limit for number of sample values to collect for all columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">20</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_distinct_count</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the number of distinct values for each column. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_distinct_value_frequencies</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for distinct value frequencies. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_histogram</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the histogram for numeric fields. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_max_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the max value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_mean_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the mean value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_median_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the median value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_min_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the min value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_null_count</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the number of nulls for each column. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_quantiles</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the quantiles of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_sample_values</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the sample values for all columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_stddev_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile for the standard deviation of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">limit</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Max number of documents to profile. By default, profiles all documents. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">max_number_of_fields_to_profile</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | A positive integer that specifies the maximum number of columns to profile for any table. `None` implies all columns. The cost of profiling goes up significantly as the number of columns to profile goes up. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">max_workers</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of worker threads to use for profiling. Set to 1 to disable. <div className="default-line default-line-with-docs">Default: <span className="default-value">70</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">offset</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Offset in documents to profile. By default, uses no offset. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">partition_datetime</span></div> <div className="type-name-line"><span className="type-name">One of string(date-time), null</span></div> | If specified, profile only the partition which matches this datetime. If not specified, profile the latest partition. Only Bigquery supports this. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">partition_profiling_enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile partitioned tables. Only BigQuery and Aws Athena supports this. If enabled, latest partition data is used for profiling. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_external_tables</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile external tables. Only Snowflake and Redshift supports this. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_if_updated_since_days</span></div> <div className="type-name-line"><span className="type-name">One of number, null</span></div> | Profile table only if it has been updated since these many number of days. If set to `null`, no constraint of last modified time for tables to profile. Supported only in `snowflake` and `BigQuery`. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_nested_fields</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile complex types like structs, arrays and maps.  <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_level_only</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to perform profiling at table-level only, or include column-level profiling as well. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_row_count_estimate_only</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Use an approximate query for row count. This will be much faster but slightly less accurate. Only supported for Postgres and MySQL.  <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_row_limit</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Profile tables only if their row count is less than specified count. If set to `null`, no limit on the row count of tables to profile. Supported only in `Snowflake`, `BigQuery`. Supported for `Oracle` based on gathered stats. <div className="default-line default-line-with-docs">Default: <span className="default-value">5000000</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_size_limit</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Profile tables only if their size is less than specified GBs. If set to `null`, no limit on the size of tables to profile. Supported only in `Snowflake`, `BigQuery` and `Databricks`. Supported for `Oracle` based on calculated size from gathered stats. <div className="default-line default-line-with-docs">Default: <span className="default-value">5</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">query_combiner_enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | *This feature is still experimental and can be disabled if it causes issues.* Reduces the total number of queries issued and speeds up profiling by dynamically combining SQL queries where possible. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">report_dropped_profiles</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to report datasets or dataset columns which were not profiled. Set to `True` for debugging purposes. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">sample_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of rows to be sampled from table for column level profiling.Applicable only if `use_sampling` is set to True. <div className="default-line default-line-with-docs">Default: <span className="default-value">10000</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">turn_off_expensive_profiling_metrics</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to turn off expensive profiling or not. This turns off profiling for quantiles, distinct_value_frequencies, histogram & sample_values. This also limits maximum number of fields being profiled to 10. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">use_sampling</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile column level stats on sample of table. Only BigQuery and Snowflake support this. If enabled, profiling is done on rows sampled from table. Sampling is not done for smaller tables.  <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">operation_config</span></div> <div className="type-name-line"><span className="type-name">OperationConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">lower_freq_profile_enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to do profiling at lower freq or not. This does not do any scheduling just adds additional checks to when not to run profiling. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">profile_date_of_month</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Number between 1 to 31 for date of month (both inclusive). If not specified, defaults to Nothing and this field does not take affect. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">profile_day_of_week</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Number between 0 to 6 for day of week (both inclusive). 0 is Monday and 6 is Sunday. If not specified, defaults to Nothing and this field does not take affect. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">tags_to_ignore_sampling</span></div> <div className="type-name-line"><span className="type-name">One of array, null</span></div> | Fixed list of tags to ignore sampling. If not specified, tables will be sampled based on `use_sampling`. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.tags_to_ignore_sampling.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> |  <div className="default-line ">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">fail_safe_threshold</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'. <div className="default-line default-line-with-docs">Default: <span className="default-value">75.0</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |

</div>


</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.


```javascript
{
  "$defs": {
    "AllowDenyPattern": {
      "additionalProperties": false,
      "description": "A class to store allow deny regexes",
      "properties": {
        "allow": {
          "default": [
            ".*"
          ],
          "description": "List of regex patterns to include in ingestion",
          "items": {
            "type": "string"
          },
          "title": "Allow",
          "type": "array"
        },
        "deny": {
          "default": [],
          "description": "List of regex patterns to exclude from ingestion.",
          "items": {
            "type": "string"
          },
          "title": "Deny",
          "type": "array"
        },
        "ignoreCase": {
          "anyOf": [
            {
              "type": "boolean"
            },
            {
              "type": "null"
            }
          ],
          "default": true,
          "description": "Whether to ignore case sensitivity during pattern matching.",
          "title": "Ignorecase"
        }
      },
      "title": "AllowDenyPattern",
      "type": "object"
    },
    "ClassificationConfig": {
      "additionalProperties": false,
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether classification should be used to auto-detect glossary terms",
          "title": "Enabled",
          "type": "boolean"
        },
        "sample_size": {
          "default": 100,
          "description": "Number of sample values used for classification.",
          "title": "Sample Size",
          "type": "integer"
        },
        "max_workers": {
          "default": 14,
          "description": "Number of worker processes to use for classification. Set to 1 to disable.",
          "title": "Max Workers",
          "type": "integer"
        },
        "table_pattern": {
          "$ref": "#/$defs/AllowDenyPattern",
          "default": {
            "allow": [
              ".*"
            ],
            "deny": [],
            "ignoreCase": true
          },
          "description": "Regex patterns to filter tables for classification. This is used in combination with other patterns in parent config. Specify regex to match the entire table name in `database.schema.table` format. e.g. to match all tables starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'"
        },
        "column_pattern": {
          "$ref": "#/$defs/AllowDenyPattern",
          "default": {
            "allow": [
              ".*"
            ],
            "deny": [],
            "ignoreCase": true
          },
          "description": "Regex patterns to filter columns for classification. This is used in combination with other patterns in parent config. Specify regex to match the column name in `database.schema.table.column` format."
        },
        "info_type_to_term": {
          "additionalProperties": {
            "type": "string"
          },
          "default": {},
          "description": "Optional mapping to provide glossary term identifier for info type",
          "title": "Info Type To Term",
          "type": "object"
        },
        "classifiers": {
          "default": [
            {
              "type": "datahub",
              "config": null
            }
          ],
          "description": "Classifiers to use to auto-detect glossary terms. If more than one classifier, infotype predictions from the classifier defined later in sequence take precedance.",
          "items": {
            "$ref": "#/$defs/DynamicTypedClassifierConfig"
          },
          "title": "Classifiers",
          "type": "array"
        }
      },
      "title": "ClassificationConfig",
      "type": "object"
    },
    "DynamicTypedClassifierConfig": {
      "additionalProperties": false,
      "properties": {
        "type": {
          "description": "The type of the classifier to use. For DataHub,  use `datahub`",
          "title": "Type",
          "type": "string"
        },
        "config": {
          "anyOf": [
            {},
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The configuration required for initializing the classifier. If not specified, uses defaults for classifer type.",
          "title": "Config"
        }
      },
      "required": [
        "type"
      ],
      "title": "DynamicTypedClassifierConfig",
      "type": "object"
    },
    "GEProfilingConfig": {
      "additionalProperties": false,
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether profiling should be done.",
          "title": "Enabled",
          "type": "boolean"
        },
        "operation_config": {
          "$ref": "#/$defs/OperationConfig",
          "description": "Experimental feature. To specify operation configs."
        },
        "limit": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Max number of documents to profile. By default, profiles all documents.",
          "title": "Limit"
        },
        "offset": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Offset in documents to profile. By default, uses no offset.",
          "title": "Offset"
        },
        "profile_table_level_only": {
          "default": false,
          "description": "Whether to perform profiling at table-level only, or include column-level profiling as well.",
          "title": "Profile Table Level Only",
          "type": "boolean"
        },
        "include_field_null_count": {
          "default": true,
          "description": "Whether to profile for the number of nulls for each column.",
          "title": "Include Field Null Count",
          "type": "boolean"
        },
        "include_field_distinct_count": {
          "default": true,
          "description": "Whether to profile for the number of distinct values for each column.",
          "title": "Include Field Distinct Count",
          "type": "boolean"
        },
        "include_field_min_value": {
          "default": true,
          "description": "Whether to profile for the min value of numeric columns.",
          "title": "Include Field Min Value",
          "type": "boolean"
        },
        "include_field_max_value": {
          "default": true,
          "description": "Whether to profile for the max value of numeric columns.",
          "title": "Include Field Max Value",
          "type": "boolean"
        },
        "include_field_mean_value": {
          "default": true,
          "description": "Whether to profile for the mean value of numeric columns.",
          "title": "Include Field Mean Value",
          "type": "boolean"
        },
        "include_field_median_value": {
          "default": true,
          "description": "Whether to profile for the median value of numeric columns.",
          "title": "Include Field Median Value",
          "type": "boolean"
        },
        "include_field_stddev_value": {
          "default": true,
          "description": "Whether to profile for the standard deviation of numeric columns.",
          "title": "Include Field Stddev Value",
          "type": "boolean"
        },
        "include_field_quantiles": {
          "default": false,
          "description": "Whether to profile for the quantiles of numeric columns.",
          "title": "Include Field Quantiles",
          "type": "boolean"
        },
        "include_field_distinct_value_frequencies": {
          "default": false,
          "description": "Whether to profile for distinct value frequencies.",
          "title": "Include Field Distinct Value Frequencies",
          "type": "boolean"
        },
        "include_field_histogram": {
          "default": false,
          "description": "Whether to profile for the histogram for numeric fields.",
          "title": "Include Field Histogram",
          "type": "boolean"
        },
        "include_field_sample_values": {
          "default": true,
          "description": "Whether to profile for the sample values for all columns.",
          "title": "Include Field Sample Values",
          "type": "boolean"
        },
        "max_workers": {
          "default": 70,
          "description": "Number of worker threads to use for profiling. Set to 1 to disable.",
          "title": "Max Workers",
          "type": "integer"
        },
        "report_dropped_profiles": {
          "default": false,
          "description": "Whether to report datasets or dataset columns which were not profiled. Set to `True` for debugging purposes.",
          "title": "Report Dropped Profiles",
          "type": "boolean"
        },
        "turn_off_expensive_profiling_metrics": {
          "default": false,
          "description": "Whether to turn off expensive profiling or not. This turns off profiling for quantiles, distinct_value_frequencies, histogram & sample_values. This also limits maximum number of fields being profiled to 10.",
          "title": "Turn Off Expensive Profiling Metrics",
          "type": "boolean"
        },
        "field_sample_values_limit": {
          "default": 20,
          "description": "Upper limit for number of sample values to collect for all columns.",
          "title": "Field Sample Values Limit",
          "type": "integer"
        },
        "max_number_of_fields_to_profile": {
          "anyOf": [
            {
              "exclusiveMinimum": 0,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "A positive integer that specifies the maximum number of columns to profile for any table. `None` implies all columns. The cost of profiling goes up significantly as the number of columns to profile goes up.",
          "title": "Max Number Of Fields To Profile"
        },
        "profile_if_updated_since_days": {
          "anyOf": [
            {
              "exclusiveMinimum": 0,
              "type": "number"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Profile table only if it has been updated since these many number of days. If set to `null`, no constraint of last modified time for tables to profile. Supported only in `snowflake` and `BigQuery`.",
          "schema_extra": {
            "supported_sources": [
              "snowflake",
              "bigquery"
            ]
          },
          "title": "Profile If Updated Since Days"
        },
        "profile_table_size_limit": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": 5,
          "description": "Profile tables only if their size is less than specified GBs. If set to `null`, no limit on the size of tables to profile. Supported only in `Snowflake`, `BigQuery` and `Databricks`. Supported for `Oracle` based on calculated size from gathered stats.",
          "schema_extra": {
            "supported_sources": [
              "snowflake",
              "bigquery",
              "unity-catalog",
              "oracle"
            ]
          },
          "title": "Profile Table Size Limit"
        },
        "profile_table_row_limit": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": 5000000,
          "description": "Profile tables only if their row count is less than specified count. If set to `null`, no limit on the row count of tables to profile. Supported only in `Snowflake`, `BigQuery`. Supported for `Oracle` based on gathered stats.",
          "schema_extra": {
            "supported_sources": [
              "snowflake",
              "bigquery",
              "oracle"
            ]
          },
          "title": "Profile Table Row Limit"
        },
        "profile_table_row_count_estimate_only": {
          "default": false,
          "description": "Use an approximate query for row count. This will be much faster but slightly less accurate. Only supported for Postgres and MySQL. ",
          "schema_extra": {
            "supported_sources": [
              "postgres",
              "mysql"
            ]
          },
          "title": "Profile Table Row Count Estimate Only",
          "type": "boolean"
        },
        "query_combiner_enabled": {
          "default": true,
          "description": "*This feature is still experimental and can be disabled if it causes issues.* Reduces the total number of queries issued and speeds up profiling by dynamically combining SQL queries where possible.",
          "title": "Query Combiner Enabled",
          "type": "boolean"
        },
        "catch_exceptions": {
          "default": true,
          "description": "",
          "title": "Catch Exceptions",
          "type": "boolean"
        },
        "partition_profiling_enabled": {
          "default": true,
          "description": "Whether to profile partitioned tables. Only BigQuery and Aws Athena supports this. If enabled, latest partition data is used for profiling.",
          "schema_extra": {
            "supported_sources": [
              "athena",
              "bigquery"
            ]
          },
          "title": "Partition Profiling Enabled",
          "type": "boolean"
        },
        "partition_datetime": {
          "anyOf": [
            {
              "format": "date-time",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "If specified, profile only the partition which matches this datetime. If not specified, profile the latest partition. Only Bigquery supports this.",
          "schema_extra": {
            "supported_sources": [
              "bigquery"
            ]
          },
          "title": "Partition Datetime"
        },
        "use_sampling": {
          "default": true,
          "description": "Whether to profile column level stats on sample of table. Only BigQuery and Snowflake support this. If enabled, profiling is done on rows sampled from table. Sampling is not done for smaller tables. ",
          "schema_extra": {
            "supported_sources": [
              "bigquery",
              "snowflake"
            ]
          },
          "title": "Use Sampling",
          "type": "boolean"
        },
        "sample_size": {
          "default": 10000,
          "description": "Number of rows to be sampled from table for column level profiling.Applicable only if `use_sampling` is set to True.",
          "schema_extra": {
            "supported_sources": [
              "bigquery",
              "snowflake"
            ]
          },
          "title": "Sample Size",
          "type": "integer"
        },
        "profile_external_tables": {
          "default": false,
          "description": "Whether to profile external tables. Only Snowflake and Redshift supports this.",
          "schema_extra": {
            "supported_sources": [
              "redshift",
              "snowflake"
            ]
          },
          "title": "Profile External Tables",
          "type": "boolean"
        },
        "tags_to_ignore_sampling": {
          "anyOf": [
            {
              "items": {
                "type": "string"
              },
              "type": "array"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Fixed list of tags to ignore sampling. If not specified, tables will be sampled based on `use_sampling`.",
          "title": "Tags To Ignore Sampling"
        },
        "profile_nested_fields": {
          "default": false,
          "description": "Whether to profile complex types like structs, arrays and maps. ",
          "title": "Profile Nested Fields",
          "type": "boolean"
        }
      },
      "title": "GEProfilingConfig",
      "type": "object"
    },
    "OperationConfig": {
      "additionalProperties": false,
      "properties": {
        "lower_freq_profile_enabled": {
          "default": false,
          "description": "Whether to do profiling at lower freq or not. This does not do any scheduling just adds additional checks to when not to run profiling.",
          "title": "Lower Freq Profile Enabled",
          "type": "boolean"
        },
        "profile_day_of_week": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Number between 0 to 6 for day of week (both inclusive). 0 is Monday and 6 is Sunday. If not specified, defaults to Nothing and this field does not take affect.",
          "title": "Profile Day Of Week"
        },
        "profile_date_of_month": {
          "anyOf": [
            {
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Number between 1 to 31 for date of month (both inclusive). If not specified, defaults to Nothing and this field does not take affect.",
          "title": "Profile Date Of Month"
        }
      },
      "title": "OperationConfig",
      "type": "object"
    },
    "StatefulStaleMetadataRemovalConfig": {
      "additionalProperties": false,
      "description": "Base specialized config for Stateful Ingestion with stale metadata removal capability.",
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False",
          "title": "Enabled",
          "type": "boolean"
        },
        "remove_stale_metadata": {
          "default": true,
          "description": "Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled.",
          "title": "Remove Stale Metadata",
          "type": "boolean"
        },
        "fail_safe_threshold": {
          "default": 75.0,
          "description": "Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'.",
          "maximum": 100.0,
          "minimum": 0.0,
          "title": "Fail Safe Threshold",
          "type": "number"
        }
      },
      "title": "StatefulStaleMetadataRemovalConfig",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
    "schema_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for schemas to filter in ingestion. Specify regex to only match the schema name. e.g. to match all tables in schema analytics, use the regex 'analytics'"
    },
    "table_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for tables to filter in ingestion. Specify regex to match the entire table name in database.schema.table format. e.g. to match all tables starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'"
    },
    "view_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for views to filter in ingestion. Note: Defaults to table_pattern if not specified. Specify regex to match the entire view name in database.schema.view format. e.g. to match all views starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'"
    },
    "classification": {
      "$ref": "#/$defs/ClassificationConfig",
      "default": {
        "enabled": false,
        "sample_size": 100,
        "max_workers": 14,
        "table_pattern": {
          "allow": [
            ".*"
          ],
          "deny": [],
          "ignoreCase": true
        },
        "column_pattern": {
          "allow": [
            ".*"
          ],
          "deny": [],
          "ignoreCase": true
        },
        "info_type_to_term": {},
        "classifiers": [
          {
            "config": null,
            "type": "datahub"
          }
        ]
      },
      "description": "For details, refer to [Classification](../../../../metadata-ingestion/docs/dev_guides/classification.md)."
    },
    "incremental_lineage": {
      "default": false,
      "description": "When enabled, emits lineage as incremental to existing lineage already in DataHub. When disabled, re-states lineage on each run.",
      "title": "Incremental Lineage",
      "type": "boolean"
    },
    "convert_urns_to_lowercase": {
      "default": false,
      "description": "Whether to convert dataset urns to lowercase.",
      "title": "Convert Urns To Lowercase",
      "type": "boolean"
    },
    "env": {
      "default": "PROD",
      "description": "The environment that all assets produced by this connector belong to",
      "title": "Env",
      "type": "string"
    },
    "platform_instance": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details.",
      "title": "Platform Instance"
    },
    "stateful_ingestion": {
      "anyOf": [
        {
          "$ref": "#/$defs/StatefulStaleMetadataRemovalConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null
    },
    "options": {
      "additionalProperties": true,
      "description": "Any options specified here will be passed to [SQLAlchemy.create_engine](https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine) as kwargs. To set connection arguments in the URL, specify them under `connect_args`.",
      "title": "Options",
      "type": "object"
    },
    "profile_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns to filter tables (or specific columns) for profiling during ingestion. Note that only tables allowed by the `table_pattern` will be considered."
    },
    "domain": {
      "additionalProperties": {
        "$ref": "#/$defs/AllowDenyPattern"
      },
      "default": {},
      "description": "Attach domains to databases, schemas or tables during ingestion using regex patterns. Domain key can be a guid like *urn:li:domain:ec428203-ce86-4db3-985d-5a8ee6df32ba* or a string like \"Marketing\".) If you provide strings, then datahub will attempt to resolve this name to a guid, and will error out if this fails. There can be multiple domain keys specified.",
      "title": "Domain",
      "type": "object"
    },
    "include_views": {
      "default": true,
      "description": "Whether views should be ingested.",
      "title": "Include Views",
      "type": "boolean"
    },
    "include_tables": {
      "default": true,
      "description": "Whether tables should be ingested.",
      "title": "Include Tables",
      "type": "boolean"
    },
    "include_table_location_lineage": {
      "default": true,
      "description": "If the source supports it, include table lineage to the underlying storage location.",
      "title": "Include Table Location Lineage",
      "type": "boolean"
    },
    "include_view_lineage": {
      "default": true,
      "description": "Populates view->view and table->view lineage using DataHub's sql parser.",
      "title": "Include View Lineage",
      "type": "boolean"
    },
    "include_view_column_lineage": {
      "default": true,
      "description": "Populates column-level lineage for  view->view and table->view lineage using DataHub's sql parser. Requires `include_view_lineage` to be enabled.",
      "title": "Include View Column Lineage",
      "type": "boolean"
    },
    "use_file_backed_cache": {
      "default": true,
      "description": "Whether to use a file backed cache for the view definitions.",
      "title": "Use File Backed Cache",
      "type": "boolean"
    },
    "profiling": {
      "$ref": "#/$defs/GEProfilingConfig",
      "default": {
        "enabled": false,
        "operation_config": {
          "lower_freq_profile_enabled": false,
          "profile_date_of_month": null,
          "profile_day_of_week": null
        },
        "limit": null,
        "offset": null,
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
        "max_workers": 70,
        "report_dropped_profiles": false,
        "turn_off_expensive_profiling_metrics": false,
        "field_sample_values_limit": 20,
        "max_number_of_fields_to_profile": null,
        "profile_if_updated_since_days": null,
        "profile_table_size_limit": 5,
        "profile_table_row_limit": 5000000,
        "profile_table_row_count_estimate_only": false,
        "query_combiner_enabled": true,
        "catch_exceptions": true,
        "partition_profiling_enabled": true,
        "partition_datetime": null,
        "use_sampling": true,
        "sample_size": 10000,
        "profile_external_tables": false,
        "tags_to_ignore_sampling": null,
        "profile_nested_fields": false
      }
    },
    "username": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "username",
      "title": "Username"
    },
    "password": {
      "anyOf": [
        {
          "format": "password",
          "type": "string",
          "writeOnly": true
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "password",
      "title": "Password"
    },
    "host_port": {
      "description": "host URL",
      "title": "Host Port",
      "type": "string"
    },
    "database": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "If using, omit `service_name`.",
      "title": "Database"
    },
    "scheme": {
      "default": "oracle",
      "description": "Will be set automatically to default value.",
      "title": "Scheme",
      "type": "string"
    },
    "sqlalchemy_uri": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "URI of database to connect to. See https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls. Takes precedence over other connection parameters.",
      "title": "Sqlalchemy Uri"
    },
    "service_name": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Oracle service name. If using, omit `database`.",
      "title": "Service Name"
    },
    "add_database_name_to_urn": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "default": false,
      "description": "Add oracle database name to urn, default urn is schema.table",
      "title": "Add Database Name To Urn"
    },
    "data_dictionary_mode": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": "ALL",
      "description": "The data dictionary views mode, to extract information about schema objects ('ALL' and 'DBA' views are supported). (https://docs.oracle.com/cd/E11882_01/nav/catalog_views.htm)",
      "title": "Data Dictionary Mode"
    },
    "enable_thick_mode": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "default": false,
      "description": "Connection defaults to thin mode. Set to True to enable thick mode.",
      "title": "Enable Thick Mode"
    },
    "thick_mode_lib_dir": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "If using thick mode on Windows or Mac, set thick_mode_lib_dir to the oracle client libraries path. On Linux, this value is ignored, as ldconfig or LD_LIBRARY_PATH will define the location.",
      "title": "Thick Mode Lib Dir"
    }
  },
  "required": [
    "host_port"
  ],
  "title": "OracleConfig",
  "type": "object"
}
```


</TabItem>
</Tabs>

As a SQL-based service, the Oracle integration is also supported by our SQL profiler. See here for more details on configuration.

### Code Coordinates
- Class Name: `datahub.ingestion.source.sql.oracle.OracleSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/sql/oracle.py)


<h2>Questions</h2>

If you've got any questions on configuring ingestion for Oracle, feel free to ping us on [our Slack](https://datahub.com/slack).
