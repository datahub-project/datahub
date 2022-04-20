# Snowflake

To get all metadata from Snowflake you need to use two plugins `snowflake` and `snowflake-usage`. Both of them are described in this page. These will require 2 separate recipes. We understand this is not ideal and we plan to make this easier in the future.

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## `snowflake`
### Setup

To install this plugin, run `pip install 'acryl-datahub[snowflake]'`.

### Prerequisites

In order to execute this source, your Snowflake user will need to have specific privileges granted to it for reading metadata
from your warehouse. 

You can use the `provision_role` block in the recipe to grant the requires roles. 

If your system admins prefer running the commands themselves then they can follow this guide to create a DataHub-specific role, assign it the required privileges, and assign it to a new DataHub user by executing the following Snowflake commands from a user with the `ACCOUNTADMIN` role or `MANAGE GRANTS` privilege.

```sql
create or replace role datahub_role;

// Grant access to a warehouse to run queries to view metadata
grant operate, usage on warehouse "<your-warehouse>" to role datahub_role;

// Grant access to view database and schema in which your tables/views exist
grant usage on DATABASE "<your-database>" to role datahub_role;
grant usage on all schemas in database "<your-database>" to role datahub_role;
grant usage on future schemas in database "<your-database>" to role datahub_role;

// If you are NOT using Snowflake Profiling feature: Grant references privileges to your tables and views 
grant references on all tables in database "<your-database>" to role datahub_role;
grant references on future tables in database "<your-database>" to role datahub_role;
grant references on all external tables in database "<your-database>" to role datahub_role;
grant references on future external tables in database "<your-database>" to role datahub_role;
grant references on all views in database "<your-database>" to role datahub_role;
grant references on future views in database "<your-database>" to role datahub_role;

// If you ARE using Snowflake Profiling feature: Grant select privileges to your tables and views 
grant select on all tables in database "<your-database>" to role datahub_role;
grant select on future tables in database "<your-database>" to role datahub_role;
grant select on all external tables in database "<your-database>" to role datahub_role;
grant select on future external tables in database "<your-database>" to role datahub_role;
grant select on all views in database "<your-database>" to role datahub_role;
grant select on future views in database "<your-database>" to role datahub_role;

// Create a new DataHub user and assign the DataHub role to it 
create user datahub_user display_name = 'DataHub' password='' default_role = datahub_role default_warehouse = '<your-warehouse>';

// Grant the datahub_role to the new DataHub user. 
grant role datahub_role to user datahub_user;
```

The details of each granted privilege can be viewed in [snowflake docs](https://docs.snowflake.com/en/user-guide/security-access-control-privileges.html). A summarization of each privilege, and why it is required for this connector: 
- `operate` is required on warehouse to execute queries 
- `usage` is required for us to run queries using the warehouse
- `usage` on `database` and `schema` are required because without it tables and views inside them are not accessible. If an admin does the required grants on `table` but misses the grants on `schema` or the `database` in which the table/view exists then we will not be able to get metadata for the table/view.
- If metadata is required only on some schemas then you can grant the usage privilieges only on a particular schema like
```sql
grant usage on schema "<your-database>"."<your-schema>" to role datahub_role;
```
- To get the lineage and usage data we need access to the default `snowflake` database

This represents the bare minimum privileges required to extract databases, schemas, views, tables from Snowflake. 

If you plan to enable extraction of table lineage, via the `include_table_lineage` config flag, you'll need to grant additional privileges. See [snowflake usage prerequisites](#prerequisites-1) as the same privilege is required for this purpose too.


### Capabilities

This plugin extracts the following:

- Metadata for databases, schemas, views and tables
- Column types associated with each table
- Table, row, and column statistics via optional [SQL profiling](./sql_profiles.md)
- Table lineage 
  - On Snowflake standard edition we can get
    - table -> view lineage
    - s3 -> table lineage
  - On Snowflake Enterprise edition in addition to the above from Snowflake Standard edition we can get (Please see [caveats](#caveats-1))
    - table -> table lineage
    - view -> table lineage

:::tip

You can also get fine-grained usage statistics for Snowflake using the `snowflake-usage` source described [below](#snowflake-usage-plugin).

:::

| Capability        | Status | Details                                  | 
|-------------------|--------|------------------------------------------|
| Platform Instance | ✔️     | [link](../../docs/platform-instances.md) |
| Data Containers   | ✔️     |                                          |
| Data Domains      | ✔️     | [link](../../docs/domains.md)            |

### Caveats

The [caveats](#caveats-1) mentioned for `snowflake-usage` apply to `snowflake` too.

### Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: snowflake
  config:

    provision_role: # Optional
      enabled: false
      dry_run: true
      run_ingestion: false
      admin_username: "${SNOWFLAKE_ADMIN_USER}"
      admin_password: "${SNOWFLAKE_ADMIN_PASS}"

    # Coordinates
    host_port: account_name
    warehouse: "COMPUTE_WH"

    # Credentials
    username: "${SNOWFLAKE_USER}"
    password: "${SNOWFLAKE_PASS}"
    role: "datahub_role"

    database_pattern:
      allow:
      - "^ACCOUNTING_DB$"
      - "^MARKETING_DB$"
    schema_pattern:
      deny:
      - "information_schema.*"
    table_pattern:
      allow:
      # If you want to ingest only few tables with name revenue and revenue
      - ".*revenue"
      - ".*sales"
    profiling:
      enabled: true
    profile_pattern:
      allow:
        - 'ACCOUNTING_DB.*.*'
        - 'MARKETING_DB.*.*'
      deny:
        - '.*information_schema.*'

sink:
  # sink configs
```

### Config details

Like all SQL-based sources, the Snowflake integration supports:
- Stale Metadata Deletion: See [here](./stateful_ingestion.md) for more details on configuration.
- SQL Profiling: See [here](./sql_profiles.md) for more details on configuration.

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                          | Required | Default                                                                    | Description                                                                                                                                                                             |
|--------------------------------|----------|----------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `authentication_type`          |          | `"DEFAULT_AUTHENTICATOR"`                                                  | The type of authenticator to use when connecting to Snowflake. Supports `"DEFAULT_AUTHENTICATOR"`, `"EXTERNAL_BROWSER_AUTHENTICATOR"` and `"KEY_PAIR_AUTHENTICATOR"`.                   |
| `username`                     |          |                                                                            | Snowflake username.                                                                                                                                                                     |
| `password`                     |          |                                                                            | Snowflake password.                                                                                                                                                                     |
| `private_key_path`             |          |                                                                            | The path to the private key if using key pair authentication. See: https://docs.snowflake.com/en/user-guide/key-pair-auth.html                                                          |
| `private_key_password`         |          |                                                                            | Password for your private key if using key pair authentication.                                                                                                                         |
| `host_port`                    | ✅        |                                                                            | Snowflake host URL.                                                                                                                                                                     |
| `warehouse`                    |          |                                                                            | Snowflake warehouse.                                                                                                                                                                    |
| `role`                         |          |                                                                            | Snowflake role.                                                                                                                                                                         |
| `sqlalchemy_uri`               |          |                                                                            | URI of database to connect to. See https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls. Takes precedence over other connection parameters. |
| `env`                          |          | `"PROD"`                                                                   | Environment to use in namespace when constructing URNs.                                                                                                                                 |
| `platform_instance`            |          | None                                                                       | The Platform instance to use while constructing URNs.                                                                                                                                   |
| `options.<option>`             |          |                                                                            | Any options specified here will be passed to SQLAlchemy's `create_engine` as kwargs.<br />See https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine for details. |
| `database_pattern.allow`       |          |                                                                            | List of regex patterns for databases to include in ingestion.                                                                                                                           |
| `database_pattern.deny`        |          | `"^UTIL_DB$" `<br />`"^SNOWFLAKE$"`<br />`"^SNOWFLAKE_SAMPLE_DATA$"`       | List of regex patterns for databases to exclude from ingestion.                                                                                                                         |
| `database_pattern.ignoreCase`  |          | `True`                                                                     | Whether to ignore case sensitivity during pattern matching.                                                                                                                             |
| `table_pattern.allow`          |          |                                                                            | List of regex patterns for tables to include in ingestion.                                                                                                                              |
| `table_pattern.deny`           |          |                                                                            | List of regex patterns for tables to exclude from ingestion.                                                                                                                            |
| `table_pattern.ignoreCase`     |          | `True`                                                                     | Whether to ignore case sensitivity during pattern matching.                                                                                                                             |
| `schema_pattern.allow`         |          |                                                                            | List of regex patterns for schemas to include in ingestion.                                                                                                                             |
| `schema_pattern.deny`          |          |                                                                            | List of regex patterns for schemas to exclude from ingestion.                                                                                                                           |
| `schema_pattern.ignoreCase`    |          | `True`                                                                     | Whether to ignore case sensitivity during pattern matching.                                                                                                                             |
| `view_pattern.allow`           |          |                                                                            | List of regex patterns for views to include in ingestion.                                                                                                                               |
| `view_pattern.deny`            |          |                                                                            | List of regex patterns for views to exclude from ingestion.                                                                                                                             |
| `view_pattern.ignoreCase`      |          | `True`                                                                     | Whether to ignore case sensitivity during pattern matching.                                                                                                                             |
| `include_tables`               |          | `True`                                                                     | Whether tables should be ingested.                                                                                                                                                      |
| `include_views`                |          | `True`                                                                     | Whether views should be ingested.                                                                                                                                                       |
| `include_table_lineage`        |          | `True`                                                                     | If enabled, populates the snowflake table-to-table and s3-to-snowflake table lineage. Requires appropriate grants given to the role.                                                                |
| `include_view_lineage`         |          | `True`                                                                     | If enabled, populates the snowflake view->table and table->view lineages (no view->view lineage yet). Requires appropriate grants given to the role, and `include_table_lineage` to be `True`.     |
| `bucket_duration`              |          | `"DAY"`                                                                    | Duration to bucket lineage data extraction by. Can be `"DAY"` or `"HOUR"`.                                                                                                              |
| `upstream_lineage_in_report`               |           | `False`  | Whether to report upstream lineage in the report. This should be marked as `True` in case someone is debugging lineage ingestion issues |
| `ignore_start_time_lineage`             |           | `False`     | Whether to ignore `start_time` and read all data for lineage. It is meant to be used for initial ingestion |
| `start_time`                   |          | Start of last full day in UTC (or hour, depending on `bucket_duration`)    | Earliest time of lineage data to consider. For the bootstrap run, set it as far back in time as possible.                                                                               |
| `end_time`                     |          | End of last full day in UTC (or hour, depending on `bucket_duration`)      | Latest time of lineage data to consider.                                                                                                                                                |
| `profiling`                    |          | See the defaults for [profiling config](./sql_profiles.md#Config-details). | See [profiling config](./sql_profiles.md#Config-details).                                                                                                                               |
| `domain.domain_key.allow`      |          |                                                                            | List of regex patterns for tables/schemas to set domain_key domain key (domain_key can be any string like `sales`. There can be multiple domain key specified.                          |
| `domain.domain_key.deny`       |          |                                                                            | List of regex patterns for tables/schemas to not assign domain_key. There can be multiple domain key specified.                                                                         |
| `domain.domain_key.ignoreCase` |          | `True`                                                                     | Whether to ignore case sensitivity during pattern matching.There can be multiple domain key specified.                                                                                  |
| `provision_role.enabled`                |          | `False` | Whether provisioning of Snowflake role (used for ingestion) is enabled or not |
| `provision_role.dry_run`                |          | `False` | If `provision_role` is enabled, whether to dry run the sql commands for system admins to see what sql grant commands would be run without actually running the grant commands |
| `provision_role.drop_role_if_exists`    |          | `False` | Useful during testing to ensure you have a clean slate role. Not recommended for production use cases |
| `provision_role.run_ingestion`         |          | `False`  | If system admins wish to skip actual ingestion of metadata during testing of the provisioning of `role` |
| `provision_role.admin_role`             |          | `accountadmin` | The Snowflake role of admin user used for provisioning of the role specified by `role` config. System admins can audit the open source code and decide to use a different role |
| `provision_role.admin_username`         |  ✅       |          | The username to be used for provisioning of role |
| `provision_role.admin_password`         |  ✅       |          | The password to be used for provisioning of role |


## `snowflake-usage`

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

### Setup

To install this plugin, run `pip install 'acryl-datahub[snowflake-usage]'`.

### Prerequisites 

In order to execute the `snowflake-usage` source, your Snowflake user will need to have specific privileges granted to it. Specifically, you'll need to grant access to the [Account Usage](https://docs.snowflake.com/en/sql-reference/account-usage.html) system tables, using which the DataHub source extracts information. Assuming you've followed the steps outlined in `snowflake` plugin to create a DataHub-specific User & Role, you'll simply need to execute the following commands in Snowflake. This will require a user with the `ACCOUNTADMIN` role (or a role granted the IMPORT SHARES global privilege). Please see [Snowflake docs for more details](https://docs.snowflake.com/en/user-guide/data-share-consumers.html).

```sql
grant imported privileges on database snowflake to role datahub_role;
```

### Capabilities

This plugin extracts the following:

- Statistics on queries issued and tables and columns accessed (excludes views)
- Aggregation of these statistics into buckets, by day or hour granularity


:::note

This source only does usage statistics. To get the tables, views, and schemas in your Snowflake warehouse, ingest using the `snowflake` source described above.

:::

### Caveats
- Some of the features are only available in the Snowflake Enterprise Edition. This docs has notes mentioning where this applies.
- The underlying Snowflake views that we use to get metadata have a [latency of 45 minutes to 3 hours](https://docs.snowflake.com/en/sql-reference/account-usage.html#differences-between-account-usage-and-information-schema). So we would not be able to get very recent metadata in some cases like queries you ran within that time period etc..
- If there is any [incident going on for Snowflake](https://status.snowflake.com/) we will not be able to get the metadata until that incident is resolved.

### Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: snowflake-usage
  config:
    # Coordinates
    host_port: account_name
    warehouse: "COMPUTE_WH"

    # Credentials
    username: "${SNOWFLAKE_USER}"
    password: "${SNOWFLAKE_PASS}"
    role: "datahub_role"

    # Options
    top_n_queries: 10
    email_domain: mycompany.com

    database_pattern:
      allow:
      - "^ACCOUNTING_DB$"
      - "^MARKETING_DB$"
    schema_pattern:
      deny:
      - "information_schema.*"

sink:
  # sink configs
```

### Config details

Snowflake integration also supports prevention of redundant reruns for the same data. See [here](./stateful_ingestion.md) for more details on configuration.

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                           | Required | Default                                                             | Description                                                                      |
|---------------------------------|----------|---------------------------------------------------------------------|----------------------------------------------------------------------------------|
| `username`                      |          |                                                                     | Snowflake username.                                                              |
| `password`                      |          |                                                                     | Snowflake password.                                                              |
| `host_port`                     | ✅       |                                                                     | Snowflake host URL.                                                              |
| `warehouse`                     |          |                                                                     | Snowflake warehouse.                                                             |
| `role`                          |          |                                                                     | Snowflake role.                                                                  |
| `env`                           |          | `"PROD"`                                                            | Environment to use in namespace when constructing URNs.                          |
| `bucket_duration`               |          | `"DAY"`                                                             | Duration to bucket usage events by. Can be `"DAY"` or `"HOUR"`.                  |
| `email_domain`                  |          |                                                                     | Email domain of your organisation so users can be displayed on UI appropriately. |
| `start_time`                    |          | Last full day in UTC (or hour, depending on `bucket_duration`)      | Earliest date of usage logs to consider.                                         |
| `end_time`                      |          | Last full day in UTC (or hour, depending on `bucket_duration`)      | Latest date of usage logs to consider.                                           |
| `top_n_queries`                 |          | `10`                                                                | Number of top queries to save to each table.                                     |
| `include_operational_stats`     |          | `true`                                                              | Whether to display operational stats.                                            |
| `database_pattern.allow`        |          |                                                                     | List of regex patterns for databases to include in ingestion.                    |
| `database_pattern.deny`         |          | `"^UTIL_DB$" `<br />`"^SNOWFLAKE$"`<br />`"^SNOWFLAKE_SAMPLE_DATA$"`| List of regex patterns for databases to exclude from ingestion.                  |
| `database_pattern.ignoreCase`   |          | `True`                                                              | Whether to ignore case sensitivity during pattern matching.                      |
| `schema_pattern.allow`          |          |                                                                     | List of regex patterns for schemas to include in ingestion.                      |
| `schema_pattern.deny`           |          |                                                                     | List of regex patterns for schemas to exclude from ingestion.                    |
| `schema_pattern.ignoreCase`     |          | `True`                                                              | Whether to ignore case sensitivity during pattern matching.                      |
| `view_pattern`                  |          |                                                                     | Allow/deny patterns for views in snowflake dataset names.                        |
| `table_pattern`                 |          |                                                                     | Allow/deny patterns for tables in snowflake dataset names.                       |
| `user_email_pattern.allow`      |          | *                                                                   | List of regex patterns for user emails to include in usage.                      |
| `user_email_pattern.deny`       |          |                                                                     | List of regex patterns for user emails to exclude from usage.                    |
| `user_email_pattern.ignoreCase` |          | `True`                                                              | Whether to ignore case sensitivity during pattern matching.                      |
| `format_sql_queries`            |          | `False`                                                             | Whether to format sql queries                                                    |

:::caution

User's without email address will be ignored from usage if you don't set `email_domain` property.

:::


## Compatibility

Coming soon!

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
