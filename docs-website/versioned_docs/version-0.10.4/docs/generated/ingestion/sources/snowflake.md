---
sidebar_position: 46
title: Snowflake
slug: /generated/ingestion/sources/snowflake
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/snowflake.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Snowflake

### Snowflake Ingestion through the UI

The following video shows you how to ingest Snowflake metadata through the UI.

<div style={{ position: "relative", paddingBottom: "56.25%", height: 0 }}>
  <iframe
    src="https://www.loom.com/embed/15d0401caa1c4aa483afef1d351760db"
    frameBorder={0}
    webkitallowfullscreen=""
    mozallowfullscreen=""
    allowFullScreen=""
    style={{
      position: "absolute",
      top: 0,
      left: 0,
      width: "100%",
      height: "100%"
    }}
  />
</div>

Read on if you are interested in ingesting Snowflake metadata using the **datahub** cli, or want to learn about all the configuration parameters that are supported by the connectors.
![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)

### Important Capabilities

| Capability                                                                                                 | Status | Notes                                                                                                    |
| ---------------------------------------------------------------------------------------------------------- | ------ | -------------------------------------------------------------------------------------------------------- |
| Asset Containers                                                                                           | ✅     | Enabled by default                                                                                       |
| Column-level Lineage                                                                                       | ✅     | Enabled by default, can be disabled via configuration `include_column_lineage`                           |
| [Data Profiling](../../../../metadata-ingestion/docs/dev_guides/sql_profiles.md)                           | ✅     | Optionally enabled via configuration `profiling.enabled`                                                 |
| Dataset Usage                                                                                              | ✅     | Enabled by default, can be disabled via configuration `include_usage_stats`                              |
| Descriptions                                                                                               | ✅     | Enabled by default                                                                                       |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅     | Optionally enabled via `stateful_ingestion.remove_stale_metadata`                                        |
| [Domains](../../../domains.md)                                                                             | ✅     | Supported via the `domain` config field                                                                  |
| Extract Tags                                                                                               | ✅     | Optionally enabled via `extract_tags`                                                                    |
| [Platform Instance](../../../platform-instances.md)                                                        | ✅     | Enabled by default                                                                                       |
| Schema Metadata                                                                                            | ✅     | Enabled by default                                                                                       |
| Table-Level Lineage                                                                                        | ✅     | Enabled by default, can be disabled via configuration `include_table_lineage` and `include_view_lineage` |

### Prerequisites

In order to execute this source, your Snowflake user will need to have specific privileges granted to it for reading metadata
from your warehouse.

Snowflake system admin can follow this guide to create a DataHub-specific role, assign it the required privileges, and assign it to a new DataHub user by executing the following Snowflake commands from a user with the `ACCOUNTADMIN` role or `MANAGE GRANTS` privilege.

```sql
create or replace role datahub_role;

// Grant access to a warehouse to run queries to view metadata
grant operate, usage on warehouse "<your-warehouse>" to role datahub_role;

// Grant access to view database and schema in which your tables/views exist
grant usage on DATABASE "<your-database>" to role datahub_role;
grant usage on all schemas in database "<your-database>" to role datahub_role;
grant usage on future schemas in database "<your-database>" to role datahub_role;

// If you are NOT using Snowflake Profiling or Classification feature: Grant references privileges to your tables and views
grant references on all tables in database "<your-database>" to role datahub_role;
grant references on future tables in database "<your-database>" to role datahub_role;
grant references on all external tables in database "<your-database>" to role datahub_role;
grant references on future external tables in database "<your-database>" to role datahub_role;
grant references on all views in database "<your-database>" to role datahub_role;
grant references on future views in database "<your-database>" to role datahub_role;

// If you ARE using Snowflake Profiling or Classification feature: Grant select privileges to your tables
grant select on all tables in database "<your-database>" to role datahub_role;
grant select on future tables in database "<your-database>" to role datahub_role;
grant select on all external tables in database "<your-database>" to role datahub_role;
grant select on future external tables in database "<your-database>" to role datahub_role;

// Create a new DataHub user and assign the DataHub role to it
create user datahub_user display_name = 'DataHub' password='' default_role = datahub_role default_warehouse = '<your-warehouse>';

// Grant the datahub_role to the new DataHub user.
grant role datahub_role to user datahub_user;

// Optional - required if extracting lineage, usage or tags (without lineage)
grant imported privileges on database snowflake to role datahub_role;
```

The details of each granted privilege can be viewed in [snowflake docs](https://docs.snowflake.com/en/user-guide/security-access-control-privileges.html). A summarization of each privilege, and why it is required for this connector:

- `operate` is required only to start the warehouse.
  If the warehouse is already running during ingestion or has auto-resume enabled,
  this permission is not required.
- `usage` is required for us to run queries using the warehouse
- `usage` on `database` and `schema` are required because without it tables and views inside them are not accessible. If an admin does the required grants on `table` but misses the grants on `schema` or the `database` in which the table/view exists then we will not be able to get metadata for the table/view.
- If metadata is required only on some schemas then you can grant the usage privilieges only on a particular schema like

```sql
grant usage on schema "<your-database>"."<your-schema>" to role datahub_role;
```

This represents the bare minimum privileges required to extract databases, schemas, views, tables from Snowflake.

If you plan to enable extraction of table lineage, via the `include_table_lineage` config flag, extraction of usage statistics, via the `include_usage_stats` config, or extraction of tags (without lineage), via the `extract_tags` config, you'll also need to grant access to the [Account Usage](https://docs.snowflake.com/en/sql-reference/account-usage.html) system tables, using which the DataHub source extracts information. This can be done by granting access to the `snowflake` database.

```sql
grant imported privileges on database snowflake to role datahub_role;
```

### Authentication

Authentication is most simply done via a Snowflake user and password.

Alternatively, other authentication methods are supported via the `authentication_type` config option.

#### Okta OAuth

To set up Okta OAuth authentication, roughly follow the four steps in [this guide](https://docs.snowflake.com/en/user-guide/oauth-okta).

Pass in the following values, as described in the article, for your recipe's `oauth_config`:

- `provider`: okta
- `client_id`: `<OAUTH_CLIENT_ID>`
- `client_secret`: `<OAUTH_CLIENT_SECRET>`
- `authority_url`: `<OKTA_OAUTH_TOKEN_ENDPOINT>`
- `scopes`: The list of your _Okta_ scopes, i.e. with the `session:role:` prefix

Datahub only supports two OAuth grant types: `client_credentials` and `password`.
The steps slightly differ based on which you decide to use.

##### Client Credentials Grant Type (Simpler)

- When creating an Okta App Integration, choose type `API Services`
  - Ensure client authentication method is `Client secret`
  - Note your `Client ID`
- Create a Snowflake user to correspond to your newly created Okta client credentials
  - _Ensure the user's `Login Name` matches your Okta application's `Client ID`_
  - Ensure the user has been granted your datahub role

##### Password Grant Type

- When creating an Okta App Integration, choose type `OIDC` -> `Native Application`
  - Add Grant Type `Resource Owner Password`
  - Ensure client authentication method is `Client secret`
- Create an Okta user to sign into, noting the `Username` and `Password`
- Create a Snowflake user to correspond to your newly created Okta client credentials
  - _Ensure the user's `Login Name` matches your Okta user's `Username` (likely an email)_
  - Ensure the user has been granted your datahub role
- When running ingestion, provide the required `oauth_config` fields,
  including `client_id` and `client_secret`, plus your Okta user's `Username` and `Password`
  - Note: the `username` and `password` config options are not nested under `oauth_config`

### Caveats

- Some of the features are only available in the Snowflake Enterprise Edition. This doc has notes mentioning where this applies.
- The underlying Snowflake views that we use to get metadata have a [latency of 45 minutes to 3 hours](https://docs.snowflake.com/en/sql-reference/account-usage.html#differences-between-account-usage-and-information-schema). So we would not be able to get very recent metadata in some cases like queries you ran within that time period etc. This is applicable particularly for lineage, usage and tags (without lineage) extraction.
- If there is any [incident going on for Snowflake](https://status.snowflake.com/) we will not be able to get the metadata until that incident is resolved.

### CLI based Ingestion

#### Install the Plugin

```shell
pip install 'acryl-datahub[snowflake]'
```

### Starter Recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).

```yaml
source:
  type: snowflake
  config:
    # This option is recommended to be used to ingest all lineage
    ignore_start_time_lineage: true

    # Coordinates
    account_id: "abc48144"
    warehouse: "COMPUTE_WH"

    # Credentials
    username: "${SNOWFLAKE_USER}"
    password: "${SNOWFLAKE_PASS}"
    role: "datahub_role"

    # (Optional) Uncomment and update this section to filter ingested datasets
    # database_pattern:
    #   allow:
    #     - "^ACCOUNTING_DB$"
    #     - "^MARKETING_DB$"

    profiling:
      # Change to false to disable profiling
      enabled: true
      # This option is recommended to reduce profiling time and costs.
      turn_off_expensive_profiling_metrics: true

    # (Optional) Uncomment and update this section to filter profiled tables
    # profile_pattern:
    #   allow:
    #   - "ACCOUNTING_DB.*.*"
    #   - "MARKETING_DB.*.*"
# Default sink is datahub-rest and doesn't need to be configured
# See https://datahubproject.io/docs/metadata-ingestion/sink_docs/datahub for customization options
```

### Config Details

<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.

<div className='config-table'>

| Field                                                                                                                                                                                                                                                                                | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| <div className="path-line"><span className="path-main">account_id</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                 | Snowflake account identifier. e.g. xy12345, xy12345.us-east-2.aws, xy12345.us-central1.gcp, xy12345.central-us.azure, xy12345.us-west-2.privatelink. Refer [Account Identifiers](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#format-2-legacy-account-locator-in-a-region) for more details.                                                                                                                                                                                                                                                    |
| <div className="path-line"><span className="path-main">apply_view_usage_to_tables</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                      | Whether to apply view's usage to its base tables. If set to True, usage is applied to base tables only. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                                                  |
| <div className="path-line"><span className="path-main">authentication_type</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                              | The type of authenticator to use when connecting to Snowflake. Supports "DEFAULT_AUTHENTICATOR", "OAUTH_AUTHENTICATOR", "EXTERNAL_BROWSER_AUTHENTICATOR" and "KEY_PAIR_AUTHENTICATOR". <div className="default-line default-line-with-docs">Default: <span className="default-value">DEFAULT_AUTHENTICATOR</span></div>                                                                                                                                                                                                                                                   |
| <div className="path-line"><span className="path-main">bucket_duration</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div>                                                                                                                    | Size of the time window to aggregate usage stats. <div className="default-line default-line-with-docs">Default: <span className="default-value">DAY</span></div>                                                                                                                                                                                                                                                                                                                                                                                                          |
| <div className="path-line"><span className="path-main">connect_args</span></div> <div className="type-name-line"><span className="type-name">object</span></div>                                                                                                                     | Connect args to pass to Snowflake SqlAlchemy driver                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                       | <div className="default-line ">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| <div className="path-line"><span className="path-main">email_domain</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                                     | Email domain of your organisation so users can be displayed on UI appropriately.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| <div className="path-line"><span className="path-main">end_time</span></div> <div className="type-name-line"><span className="type-name">string(date-time)</span></div>                                                                                                              | Latest date of usage to consider. Default: Current time in UTC                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| <div className="path-line"><span className="path-main">extract_tags</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div>                                                                                                                       | Optional. Allowed values are `without_lineage`, `with_lineage`, and `skip` (default). `without_lineage` only extracts tags that have been applied directly to the given entity. `with_lineage` extracts both directly applied and propagated tags, but will be significantly slower. See the [Snowflake documentation](https://docs.snowflake.com/en/user-guide/object-tagging.html#tag-lineage) for information about tag lineage/propagation. <div className="default-line default-line-with-docs">Default: <span className="default-value">skip</span></div>           |
| <div className="path-line"><span className="path-main">format_sql_queries</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                              | Whether to format sql queries <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                                                                                                                            |
| <div className="path-line"><span className="path-main">ignore_start_time_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                       | <div className="default-line ">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| <div className="path-line"><span className="path-main">include_column_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                          | Populates table->table and view->table column lineage. Requires appropriate grants given to the role and the Snowflake Enterprise Edition or above. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-main">include_external_url</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                            | Whether to populate Snowsight url for Snowflake Objects <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                                   |
| <div className="path-line"><span className="path-main">include_operational_stats</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                       | Whether to display operational stats. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                                                     |
| <div className="path-line"><span className="path-main">include_read_operational_stats</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                  | Whether to report read operational stats. Experimental. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                                                                                                  |
| <div className="path-line"><span className="path-main">include_table_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                           | If enabled, populates the snowflake table-to-table and s3-to-snowflake table lineage. Requires appropriate grants given to the role and Snowflake Enterprise Edition or above. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                            |
| <div className="path-line"><span className="path-main">include_table_location_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                  | If the source supports it, include table lineage to the underlying storage location. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">include_tables</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                                  | Whether tables should be ingested. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-main">include_technical_schema</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                        | If enabled, populates the snowflake technical schema and descriptions. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                    |
| <div className="path-line"><span className="path-main">include_top_n_queries</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                           | Whether to ingest the top_n_queries. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">include_usage_stats</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                             | If enabled, populates the snowflake usage statistics. Requires appropriate grants given to the role. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">include_view_column_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                     | Populates view->view and table->view column lineage. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                                                                                                     |
| <div className="path-line"><span className="path-main">include_view_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                            | If enabled, populates the snowflake view->table and table->view lineages. Requires appropriate grants given to the role, and include_table_lineage to be True. view->table lineage requires Snowflake Enterprise Edition or above. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-main">include_views</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                                   | Whether views should be ingested. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-main">match_fully_qualified_names</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                     | Whether `schema_pattern` is matched against fully qualified schema name `<catalog>.<schema>`. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                                                            |
| <div className="path-line"><span className="path-main">options</span></div> <div className="type-name-line"><span className="type-name">object</span></div>                                                                                                                          | Any options specified here will be passed to [SQLAlchemy.create_engine](https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine) as kwargs.                                                                                                                                                                                                                                                                                                                                                                                                          |
| <div className="path-line"><span className="path-main">password</span></div> <div className="type-name-line"><span className="type-name">string(password)</span></div>                                                                                                               | Snowflake password.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                                | The instance of the platform that all assets produced by this recipe belong to                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| <div className="path-line"><span className="path-main">private_key</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                                      | Private key in a form of '-----BEGIN PRIVATE KEY-----\nprivate-key\n-----END PRIVATE KEY-----\n' if using key pair authentication. Encrypted version of private key will be in a form of '-----BEGIN ENCRYPTED PRIVATE KEY-----\nencrypted-private-key\n-----END ECNCRYPTED PRIVATE KEY-----\n' See: https://docs.snowflake.com/en/user-guide/key-pair-auth.html                                                                                                                                                                                                          |
| <div className="path-line"><span className="path-main">private_key_password</span></div> <div className="type-name-line"><span className="type-name">string(password)</span></div>                                                                                                   | Password for your private key. Required if using key pair authentication with encrypted private key.                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">private_key_path</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                                 | The path to the private key if using key pair authentication. Ignored if `private_key` is set. See: https://docs.snowflake.com/en/user-guide/key-pair-auth.html                                                                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-main">role</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                                             | Snowflake role.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-main">scheme</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                                           | <div className="default-line ">Default: <span className="default-value">snowflake</span></div>                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| <div className="path-line"><span className="path-main">start_time</span></div> <div className="type-name-line"><span className="type-name">string(date-time)</span></div>                                                                                                            | Earliest date of usage to consider. Default: Last full day in UTC (or hour, depending on `bucket_duration`)                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-main">store_last_profiling_timestamps</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                 | Enable storing last profile timestamp in store. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                                                                                                          |
| <div className="path-line"><span className="path-main">store_last_usage_extraction_timestamp</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                           | Enable checking last usage timestamp in store. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                                            |
| <div className="path-line"><span className="path-main">temporary_tables_pattern</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                                                                  |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-main">top_n_queries</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                                                                                                                   | Number of top queries to save to each table. <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div>                                                                                                                                                                                                                                                                                                                                                                                                                |
| <div className="path-line"><span className="path-main">upstream_lineage_in_report</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                      | <div className="default-line ">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| <div className="path-line"><span className="path-main">use_legacy_lineage_method</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                                       | Whether to use the legacy lineage computation method. By default, uses new optimised lineage extraction method that requires less ingestion process memory. Table-to-view and view-to-view column-level lineage are not supported with the legacy method. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                |
| <div className="path-line"><span className="path-main">username</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                                         | Snowflake username.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-main">validate_upstreams_against_patterns</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                                             | Whether to validate upstream snowflake tables against allow-deny patterns <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                 |
| <div className="path-line"><span className="path-main">warehouse</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                                        | Snowflake warehouse.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                                                                                              | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div>                                                                                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-main">classification</span></div> <div className="type-name-line"><span className="type-name">ClassificationConfig</span></div>                                                                                                     | For details, refer [Classification](../../../../metadata-ingestion/docs/dev_guides/classification.md). <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;enabled&#x27;: False, &#x27;sample_size&#x27;: 100, &#x27;table_patt...</span></div>                                                                                                                                                                                                                                                                     |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                     | Whether classification should be used to auto-detect glossary terms <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">info_type_to_term</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div>                                                   |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">sample_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                                                                 | Number of sample values used for classification. <div className="default-line default-line-with-docs">Default: <span className="default-value">100</span></div>                                                                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">classifiers</span></div> <div className="type-name-line"><span className="type-name">array(object)</span></div>                                                           |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">classification.classifiers.</span><span className="path-main">type</span>&nbsp;<abbr title="Required if classifiers is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | The type of the classifier to use. For DataHub, use `datahub`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| <div className="path-line"><span className="path-prefix">classification.classifiers.</span><span className="path-main">config</span></div> <div className="type-name-line"><span className="type-name">object</span></div>                                                           | The configuration required for initializing the classifier. If not specified, uses defaults for classifer type.                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">column_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                                     | Regex patterns to filter columns for classification. This is used in combination with other patterns in parent config. Specify regex to match the column name in `database.schema.table.column` format. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div>                                                                                                                                   |
| <div className="path-line"><span className="path-prefix">classification.column_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                  |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">classification.column_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                   |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">classification.column_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                   | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">table_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                                      | Regex patterns to filter tables for classification. This is used in combination with other patterns in parent config. Specify regex to match the entire table name in `database.schema.table` format. e.g. to match all tables starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.\*' <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div> |
| <div className="path-line"><span className="path-prefix">classification.table_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                   |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">classification.table_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                    |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">classification.table_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                    | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-main">database_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                                                                                       | <div className="default-line ">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#x27;^UTIL_DB$&#x27;, &#x27;^SNOWFLAK...</span></div>                                                                                                                                                                                                                                                                                                                                                                 |
| <div className="path-line"><span className="path-prefix">database_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                               |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">database_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                                |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">database_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-main">domain</span></div> <div className="type-name-line"><span className="type-name">map(str,AllowDenyPattern)</span></div>                                                                                                        | A class to store allow deny regexes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                                   |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                                    |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                    | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-main">oauth_config</span></div> <div className="type-name-line"><span className="type-name">OAuthConfiguration</span></div>                                                                                                         | oauth configuration - https://docs.snowflake.com/en/user-guide/python-connector-example.html#connecting-with-oauth                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-prefix">oauth_config.</span><span className="path-main">authority_url</span>&nbsp;<abbr title="Required if oauth_config is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>     | Authority url of your identity provider                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| <div className="path-line"><span className="path-prefix">oauth_config.</span><span className="path-main">client_id</span>&nbsp;<abbr title="Required if oauth_config is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div>         | client id of your registered application                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| <div className="path-line"><span className="path-prefix">oauth_config.</span><span className="path-main">provider</span>&nbsp;<abbr title="Required if oauth_config is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">Enum</span></div>            | Identity provider for oauth.Supported providers are microsoft and okta.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| <div className="path-line"><span className="path-prefix">oauth_config.</span><span className="path-main">client_secret</span></div> <div className="type-name-line"><span className="type-name">string(password)</span></div>                                                        | client secret of the application if use_certificate = false                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-prefix">oauth_config.</span><span className="path-main">encoded_oauth_private_key</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                      | base64 encoded private key content if use_certificate = true                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| <div className="path-line"><span className="path-prefix">oauth_config.</span><span className="path-main">encoded_oauth_public_key</span></div> <div className="type-name-line"><span className="type-name">string</span></div>                                                       | base64 encoded certificate content if use_certificate = true                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| <div className="path-line"><span className="path-prefix">oauth_config.</span><span className="path-main">scopes</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                                  |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">oauth_config.</span><span className="path-main">use_certificate</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                               | Do you want to use certificate and private key to authenticate using oauth <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-main">profile_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                                                                                        | Regex patterns to filter tables (or specific columns) for profiling during ingestion. Note that only tables allowed by the `table_pattern` will be considered. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div>                                                                                                                                                                            |
| <div className="path-line"><span className="path-prefix">profile_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                                |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">profile_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                                 |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">profile_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                 | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-main">schema_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                                                                                         | Regex patterns for schemas to filter in ingestion. Specify regex to only match the schema name. e.g. to match all tables in schema analytics, use the regex 'analytics' <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div>                                                                                                                                                                   |
| <div className="path-line"><span className="path-prefix">schema_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                                 |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">schema_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                                  |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">schema_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                  | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-main">table_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                                                                                          | Regex patterns for tables to filter in ingestion. Specify regex to match the entire table name in database.schema.table format. e.g. to match all tables starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.\*' <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div>                                                                       |
| <div className="path-line"><span className="path-prefix">table_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                                  |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">table_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                                   |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">table_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                   | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-main">tag_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                                                                                            | List of regex patterns for tags to include in ingestion. Only used if `extract_tags` is enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div>                                                                                                                                                                                                                                          |
| <div className="path-line"><span className="path-prefix">tag_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                                    |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">tag_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                                     |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">tag_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                     | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-main">user_email_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                                                                                     | regex patterns for user emails to filter in usage. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div>                                                                                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-prefix">user_email_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                             |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">user_email_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                              |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">user_email_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                              | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-main">view_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div>                                                                                                           | Regex patterns for views to filter in ingestion. Note: Defaults to table_pattern if not specified. Specify regex to match the entire view name in database.schema.view format. e.g. to match all views starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.\*' <div className="default-line default-line-with-docs">Default: <span className="default-value">&#123;&#x27;allow&#x27;: &#91;&#x27;.\*&#x27;&#93;, &#x27;deny&#x27;: &#91;&#93;, &#x27;ignoreCase&#x27;: True&#125;</span></div>                         |
| <div className="path-line"><span className="path-prefix">view_pattern.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                                   |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">view_pattern.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array(string)</span></div>                                                                    |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-prefix">view_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                    | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-main">profiling</span></div> <div className="type-name-line"><span className="type-name">GEProfilingConfig</span></div>                                                                                                             | <div className="default-line ">Default: <span className="default-value">&#123;&#x27;enabled&#x27;: False, &#x27;limit&#x27;: None, &#x27;offset&#x27;: None, ...</span></div>                                                                                                                                                                                                                                                                                                                                                                                             |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">catch_exceptions</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                 | <div className="default-line ">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                          | Whether profiling should be done. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">field_sample_values_limit</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                                                        | Upper limit for number of sample values to collect for all columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">20</span></div>                                                                                                                                                                                                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_distinct_count</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                     | Whether to profile for the number of distinct values for each column. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                     |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_distinct_value_frequencies</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                         | Whether to profile for distinct value frequencies. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                                                                                                       |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_histogram</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                          | Whether to profile for the histogram for numeric fields. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                                                                                                 |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_max_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                          | Whether to profile for the max value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                                  |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_mean_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                         | Whether to profile for the mean value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                                 |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_median_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                       | Whether to profile for the median value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_min_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                          | Whether to profile for the min value of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                                  |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_null_count</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                         | Whether to profile for the number of nulls for each column. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_quantiles</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                          | Whether to profile for the quantiles of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                                                                                                 |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_sample_values</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                      | Whether to profile for the sample values for all columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                                 |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">include_field_stddev_value</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                       | Whether to profile for the standard deviation of numeric columns. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">limit</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                                                                            | Max number of documents to profile. By default, profiles all documents.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">max_number_of_fields_to_profile</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                                                  | A positive integer that specifies the maximum number of columns to profile for any table. `None` implies all columns. The cost of profiling goes up significantly as the number of columns to profile goes up.                                                                                                                                                                                                                                                                                                                                                            |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">max_workers</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                                                                      | Number of worker threads to use for profiling. Set to 1 to disable. <div className="default-line default-line-with-docs">Default: <span className="default-value">80</span></div>                                                                                                                                                                                                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">offset</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                                                                           | Offset in documents to profile. By default, uses no offset.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">partition_datetime</span></div> <div className="type-name-line"><span className="type-name">string(date-time)</span></div>                                                     | For partitioned datasets profile only the partition which matches the datetime or profile the latest one if not set. Only Bigquery supports this.                                                                                                                                                                                                                                                                                                                                                                                                                         |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">partition_profiling_enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                      | <div className="default-line ">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_if_updated_since_days</span></div> <div className="type-name-line"><span className="type-name">number</span></div>                                                     | Profile table only if it has been updated since these many number of days. If set to `null`, no constraint of last modified time for tables to profile. Supported only in `snowflake` and `BigQuery`.                                                                                                                                                                                                                                                                                                                                                                     |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_level_only</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                         | Whether to perform profiling at table-level only, or include column-level profiling as well. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                                                             |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_row_count_estimate_only</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                            | Use an approximate query for row count. This will be much faster but slightly less accurate. Only supported for Postgres and MySQL. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_row_limit</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                                                          | Profile tables only if their row count is less then specified count. If set to `null`, no limit on the row count of tables to profile. Supported only in `snowflake` and `BigQuery` <div className="default-line default-line-with-docs">Default: <span className="default-value">5000000</span></div>                                                                                                                                                                                                                                                                    |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_size_limit</span></div> <div className="type-name-line"><span className="type-name">integer</span></div>                                                         | Profile tables only if their size is less then specified GBs. If set to `null`, no limit on the size of tables to profile. Supported only in `snowflake` and `BigQuery` <div className="default-line default-line-with-docs">Default: <span className="default-value">5</span></div>                                                                                                                                                                                                                                                                                      |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">query_combiner_enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                           | _This feature is still experimental and can be disabled if it causes issues._ Reduces the total number of queries issued and speeds up profiling by dynamically combining SQL queries where possible. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                     |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">report_dropped_profiles</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                          | Whether to report datasets or dataset columns which were not profiled. Set to `True` for debugging purposes. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                                             |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">turn_off_expensive_profiling_metrics</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                             | Whether to turn off expensive profiling or not. This turns off profiling for quantiles, distinct_value_frequencies, histogram & sample_values. This also limits maximum number of fields being profiled to 10. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                           |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">StatefulStaleMetadataRemovalConfig</span></div>                                                                                   | Base specialized config for Stateful Ingestion with stale metadata removal capability.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                                 | The type of the ingestion state provider registered with datahub. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div>                                                                                                                                                                                                                                                                                                                                                                                        |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div>                                                   | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div>                                                                                                                                                                                                                                                                                                                              |

</div>
</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.

```javascript
{
  "title": "SnowflakeV2Config",
  "description": "Base configuration class for stateful ingestion for source configs to inherit from.",
  "type": "object",
  "properties": {
    "classification": {
      "title": "Classification",
      "description": "For details, refer [Classification](../../../../metadata-ingestion/docs/dev_guides/classification.md).",
      "default": {
        "enabled": false,
        "sample_size": 100,
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
            "type": "datahub",
            "config": null
          }
        ]
      },
      "allOf": [
        {
          "$ref": "#/definitions/ClassificationConfig"
        }
      ]
    },
    "store_last_profiling_timestamps": {
      "title": "Store Last Profiling Timestamps",
      "description": "Enable storing last profile timestamp in store.",
      "default": false,
      "type": "boolean"
    },
    "bucket_duration": {
      "description": "Size of the time window to aggregate usage stats.",
      "default": "DAY",
      "allOf": [
        {
          "$ref": "#/definitions/BucketDuration"
        }
      ]
    },
    "end_time": {
      "title": "End Time",
      "description": "Latest date of usage to consider. Default: Current time in UTC",
      "type": "string",
      "format": "date-time"
    },
    "start_time": {
      "title": "Start Time",
      "description": "Earliest date of usage to consider. Default: Last full day in UTC (or hour, depending on `bucket_duration`)",
      "type": "string",
      "format": "date-time"
    },
    "store_last_usage_extraction_timestamp": {
      "title": "Store Last Usage Extraction Timestamp",
      "description": "Enable checking last usage timestamp in store.",
      "default": true,
      "type": "boolean"
    },
    "top_n_queries": {
      "title": "Top N Queries",
      "description": "Number of top queries to save to each table.",
      "default": 10,
      "exclusiveMinimum": 0,
      "type": "integer"
    },
    "user_email_pattern": {
      "title": "User Email Pattern",
      "description": "regex patterns for user emails to filter in usage.",
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
    "include_operational_stats": {
      "title": "Include Operational Stats",
      "description": "Whether to display operational stats.",
      "default": true,
      "type": "boolean"
    },
    "include_read_operational_stats": {
      "title": "Include Read Operational Stats",
      "description": "Whether to report read operational stats. Experimental.",
      "default": false,
      "type": "boolean"
    },
    "format_sql_queries": {
      "title": "Format Sql Queries",
      "description": "Whether to format sql queries",
      "default": false,
      "type": "boolean"
    },
    "include_top_n_queries": {
      "title": "Include Top N Queries",
      "description": "Whether to ingest the top_n_queries.",
      "default": true,
      "type": "boolean"
    },
    "email_domain": {
      "title": "Email Domain",
      "description": "Email domain of your organisation so users can be displayed on UI appropriately.",
      "type": "string"
    },
    "apply_view_usage_to_tables": {
      "title": "Apply View Usage To Tables",
      "description": "Whether to apply view's usage to its base tables. If set to True, usage is applied to base tables only.",
      "default": false,
      "type": "boolean"
    },
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
      "description": "Regex patterns for schemas to filter in ingestion. Specify regex to only match the schema name. e.g. to match all tables in schema analytics, use the regex 'analytics'",
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
    "scheme": {
      "title": "Scheme",
      "default": "snowflake",
      "type": "string"
    },
    "username": {
      "title": "Username",
      "description": "Snowflake username.",
      "type": "string"
    },
    "password": {
      "title": "Password",
      "description": "Snowflake password.",
      "type": "string",
      "writeOnly": true,
      "format": "password"
    },
    "private_key": {
      "title": "Private Key",
      "description": "Private key in a form of '-----BEGIN PRIVATE KEY-----\\nprivate-key\\n-----END PRIVATE KEY-----\\n' if using key pair authentication. Encrypted version of private key will be in a form of '-----BEGIN ENCRYPTED PRIVATE KEY-----\\nencrypted-private-key\\n-----END ECNCRYPTED PRIVATE KEY-----\\n' See: https://docs.snowflake.com/en/user-guide/key-pair-auth.html",
      "type": "string"
    },
    "private_key_path": {
      "title": "Private Key Path",
      "description": "The path to the private key if using key pair authentication. Ignored if `private_key` is set. See: https://docs.snowflake.com/en/user-guide/key-pair-auth.html",
      "type": "string"
    },
    "private_key_password": {
      "title": "Private Key Password",
      "description": "Password for your private key. Required if using key pair authentication with encrypted private key.",
      "type": "string",
      "writeOnly": true,
      "format": "password"
    },
    "oauth_config": {
      "title": "Oauth Config",
      "description": "oauth configuration - https://docs.snowflake.com/en/user-guide/python-connector-example.html#connecting-with-oauth",
      "allOf": [
        {
          "$ref": "#/definitions/OAuthConfiguration"
        }
      ]
    },
    "authentication_type": {
      "title": "Authentication Type",
      "description": "The type of authenticator to use when connecting to Snowflake. Supports \"DEFAULT_AUTHENTICATOR\", \"OAUTH_AUTHENTICATOR\", \"EXTERNAL_BROWSER_AUTHENTICATOR\" and \"KEY_PAIR_AUTHENTICATOR\".",
      "default": "DEFAULT_AUTHENTICATOR",
      "type": "string"
    },
    "account_id": {
      "title": "Account Id",
      "description": "Snowflake account identifier. e.g. xy12345,  xy12345.us-east-2.aws, xy12345.us-central1.gcp, xy12345.central-us.azure, xy12345.us-west-2.privatelink. Refer [Account Identifiers](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#format-2-legacy-account-locator-in-a-region) for more details.",
      "type": "string"
    },
    "warehouse": {
      "title": "Warehouse",
      "description": "Snowflake warehouse.",
      "type": "string"
    },
    "role": {
      "title": "Role",
      "description": "Snowflake role.",
      "type": "string"
    },
    "include_table_lineage": {
      "title": "Include Table Lineage",
      "description": "If enabled, populates the snowflake table-to-table and s3-to-snowflake table lineage. Requires appropriate grants given to the role and Snowflake Enterprise Edition or above.",
      "default": true,
      "type": "boolean"
    },
    "include_view_lineage": {
      "title": "Include View Lineage",
      "description": "If enabled, populates the snowflake view->table and table->view lineages. Requires appropriate grants given to the role, and include_table_lineage to be True. view->table lineage requires Snowflake Enterprise Edition or above.",
      "default": true,
      "type": "boolean"
    },
    "connect_args": {
      "title": "Connect Args",
      "description": "Connect args to pass to Snowflake SqlAlchemy driver",
      "type": "object"
    },
    "database_pattern": {
      "title": "Database Pattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [
          "^UTIL_DB$",
          "^SNOWFLAKE$",
          "^SNOWFLAKE_SAMPLE_DATA$"
        ],
        "ignoreCase": true
      },
      "allOf": [
        {
          "$ref": "#/definitions/AllowDenyPattern"
        }
      ]
    },
    "ignore_start_time_lineage": {
      "title": "Ignore Start Time Lineage",
      "default": false,
      "type": "boolean"
    },
    "upstream_lineage_in_report": {
      "title": "Upstream Lineage In Report",
      "default": false,
      "type": "boolean"
    },
    "convert_urns_to_lowercase": {
      "title": "Convert Urns To Lowercase",
      "default": true,
      "type": "boolean"
    },
    "include_usage_stats": {
      "title": "Include Usage Stats",
      "description": "If enabled, populates the snowflake usage statistics. Requires appropriate grants given to the role.",
      "default": true,
      "type": "boolean"
    },
    "include_technical_schema": {
      "title": "Include Technical Schema",
      "description": "If enabled, populates the snowflake technical schema and descriptions.",
      "default": true,
      "type": "boolean"
    },
    "include_column_lineage": {
      "title": "Include Column Lineage",
      "description": "Populates table->table and view->table column lineage. Requires appropriate grants given to the role and the Snowflake Enterprise Edition or above.",
      "default": true,
      "type": "boolean"
    },
    "include_view_column_lineage": {
      "title": "Include View Column Lineage",
      "description": "Populates view->view and table->view column lineage.",
      "default": false,
      "type": "boolean"
    },
    "extract_tags": {
      "description": "Optional. Allowed values are `without_lineage`, `with_lineage`, and `skip` (default). `without_lineage` only extracts tags that have been applied directly to the given entity. `with_lineage` extracts both directly applied and propagated tags, but will be significantly slower. See the [Snowflake documentation](https://docs.snowflake.com/en/user-guide/object-tagging.html#tag-lineage) for information about tag lineage/propagation. ",
      "default": "skip",
      "allOf": [
        {
          "$ref": "#/definitions/TagOption"
        }
      ]
    },
    "include_external_url": {
      "title": "Include External Url",
      "description": "Whether to populate Snowsight url for Snowflake Objects",
      "default": true,
      "type": "boolean"
    },
    "match_fully_qualified_names": {
      "title": "Match Fully Qualified Names",
      "description": "Whether `schema_pattern` is matched against fully qualified schema name `<catalog>.<schema>`.",
      "default": false,
      "type": "boolean"
    },
    "use_legacy_lineage_method": {
      "title": "Use Legacy Lineage Method",
      "description": "Whether to use the legacy lineage computation method. By default, uses new optimised lineage extraction method that requires less ingestion process memory. Table-to-view and view-to-view column-level lineage are not supported with the legacy method.",
      "default": false,
      "type": "boolean"
    },
    "validate_upstreams_against_patterns": {
      "title": "Validate Upstreams Against Patterns",
      "description": "Whether to validate upstream snowflake tables against allow-deny patterns",
      "default": true,
      "type": "boolean"
    },
    "tag_pattern": {
      "title": "Tag Pattern",
      "description": "List of regex patterns for tags to include in ingestion. Only used if `extract_tags` is enabled.",
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
    "temporary_tables_pattern": {
      "title": "Temporary Tables Pattern",
      "description": "[Advanced] Regex patterns for temporary tables to filter in lineage ingestion. Specify regex to match the entire table name in database.schema.table format. Defaults are to set in such a way to ignore the temporary staging tables created by known ETL tools. Not used if `use_legacy_lineage_method=True`",
      "default": [
        ".*\\.FIVETRAN_.*_STAGING\\..*",
        ".*__DBT_TMP$",
        ".*\\.SEGMENT_[a-f0-9]{8}[-_][a-f0-9]{4}[-_][a-f0-9]{4}[-_][a-f0-9]{4}[-_][a-f0-9]{12}",
        ".*\\.STAGING_.*_[a-f0-9]{8}[-_][a-f0-9]{4}[-_][a-f0-9]{4}[-_][a-f0-9]{4}[-_][a-f0-9]{12}"
      ],
      "type": "array",
      "items": {
        "type": "string"
      }
    }
  },
  "required": [
    "account_id"
  ],
  "additionalProperties": false,
  "definitions": {
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
    "DynamicTypedClassifierConfig": {
      "title": "DynamicTypedClassifierConfig",
      "type": "object",
      "properties": {
        "type": {
          "title": "Type",
          "description": "The type of the classifier to use. For DataHub,  use `datahub`",
          "type": "string"
        },
        "config": {
          "title": "Config",
          "description": "The configuration required for initializing the classifier. If not specified, uses defaults for classifer type."
        }
      },
      "required": [
        "type"
      ],
      "additionalProperties": false
    },
    "ClassificationConfig": {
      "title": "ClassificationConfig",
      "type": "object",
      "properties": {
        "enabled": {
          "title": "Enabled",
          "description": "Whether classification should be used to auto-detect glossary terms",
          "default": false,
          "type": "boolean"
        },
        "sample_size": {
          "title": "Sample Size",
          "description": "Number of sample values used for classification.",
          "default": 100,
          "type": "integer"
        },
        "table_pattern": {
          "title": "Table Pattern",
          "description": "Regex patterns to filter tables for classification. This is used in combination with other patterns in parent config. Specify regex to match the entire table name in `database.schema.table` format. e.g. to match all tables starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'",
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
        "column_pattern": {
          "title": "Column Pattern",
          "description": "Regex patterns to filter columns for classification. This is used in combination with other patterns in parent config. Specify regex to match the column name in `database.schema.table.column` format.",
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
        "info_type_to_term": {
          "title": "Info Type To Term",
          "description": "Optional mapping to provide glossary term identifier for info type",
          "default": {},
          "type": "object",
          "additionalProperties": {
            "type": "string"
          }
        },
        "classifiers": {
          "title": "Classifiers",
          "description": "Classifiers to use to auto-detect glossary terms. If more than one classifier, infotype predictions from the classifier defined later in sequence take precedance.",
          "default": [
            {
              "type": "datahub",
              "config": null
            }
          ],
          "type": "array",
          "items": {
            "$ref": "#/definitions/DynamicTypedClassifierConfig"
          }
        }
      },
      "additionalProperties": false
    },
    "BucketDuration": {
      "title": "BucketDuration",
      "description": "An enumeration.",
      "enum": [
        "DAY",
        "HOUR"
      ],
      "type": "string"
    },
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
    },
    "OAuthIdentityProvider": {
      "title": "OAuthIdentityProvider",
      "description": "An enumeration.",
      "enum": [
        "microsoft",
        "okta"
      ]
    },
    "OAuthConfiguration": {
      "title": "OAuthConfiguration",
      "type": "object",
      "properties": {
        "provider": {
          "description": "Identity provider for oauth.Supported providers are microsoft and okta.",
          "allOf": [
            {
              "$ref": "#/definitions/OAuthIdentityProvider"
            }
          ]
        },
        "authority_url": {
          "title": "Authority Url",
          "description": "Authority url of your identity provider",
          "type": "string"
        },
        "client_id": {
          "title": "Client Id",
          "description": "client id of your registered application",
          "type": "string"
        },
        "scopes": {
          "title": "Scopes",
          "description": "scopes required to connect to snowflake",
          "type": "array",
          "items": {
            "type": "string"
          }
        },
        "use_certificate": {
          "title": "Use Certificate",
          "description": "Do you want to use certificate and private key to authenticate using oauth",
          "default": false,
          "type": "boolean"
        },
        "client_secret": {
          "title": "Client Secret",
          "description": "client secret of the application if use_certificate = false",
          "type": "string",
          "writeOnly": true,
          "format": "password"
        },
        "encoded_oauth_public_key": {
          "title": "Encoded Oauth Public Key",
          "description": "base64 encoded certificate content if use_certificate = true",
          "type": "string"
        },
        "encoded_oauth_private_key": {
          "title": "Encoded Oauth Private Key",
          "description": "base64 encoded private key content if use_certificate = true",
          "type": "string"
        }
      },
      "required": [
        "provider",
        "authority_url",
        "client_id",
        "scopes"
      ],
      "additionalProperties": false
    },
    "TagOption": {
      "title": "TagOption",
      "description": "An enumeration.",
      "enum": [
        "with_lineage",
        "without_lineage",
        "skip"
      ],
      "type": "string"
    }
  }
}
```

</TabItem>
</Tabs>

### Code Coordinates

- Class Name: `datahub.ingestion.source.snowflake.snowflake_v2.SnowflakeV2Source`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/snowflake/snowflake_v2.py)

<h2>Questions</h2>

If you've got any questions on configuring ingestion for Snowflake, feel free to ping us on [our Slack](https://slack.datahubproject.io).
