---
sidebar_position: 68
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
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Enabled by default. Supported for types - Database, Schema. |
| [Classification](../../../../metadata-ingestion/docs/dev_guides/classification.md) | ✅ | Optionally enabled via `classification.enabled`. |
| Column-level Lineage | ✅ | Enabled by default, can be disabled via configuration `include_column_lineage`. |
| [Data Profiling](../../../../metadata-ingestion/docs/dev_guides/sql_profiles.md) | ✅ | Optionally enabled via configuration `profiling.enabled`. |
| Dataset Usage | ✅ | Enabled by default, can be disabled via configuration `include_usage_stats`. |
| Descriptions | ✅ | Enabled by default. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| [Domains](../../../domains.md) | ✅ | Supported via the `domain` config field. |
| Extract Tags | ✅ | Optionally enabled via `extract_tags`. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Schema Metadata | ✅ | Enabled by default. |
| Table-Level Lineage | ✅ | Enabled by default, can be disabled via configuration `include_table_lineage`. |
| Test Connection | ✅ | Enabled by default. |


### Prerequisites

In order to execute this source, your Snowflake user will need to have specific privileges granted to it for reading metadata
from your warehouse.

Snowflake system admin can follow this guide to create a DataHub-specific role, assign it the required privileges, and assign it to a new DataHub user by executing the following Snowflake commands from a user with the `ACCOUNTADMIN` role or `MANAGE GRANTS` privilege.

```sql
create or replace role datahub_role;

// Grant access to a warehouse to run queries to view metadata
grant operate, usage on warehouse "<your-warehouse>" to role datahub_role;

// Grant access to view database and schema in which your tables/views/dynamic tables exist
grant usage on DATABASE "<your-database>" to role datahub_role;
grant usage on all schemas in database "<your-database>" to role datahub_role;
grant usage on future schemas in database "<your-database>" to role datahub_role;
grant select on all streams in database "<your-database>" to role datahub_role;
grant select on future streams in database "<your-database>" to role datahub_role;

// If you are NOT using Snowflake Profiling or Classification feature: Grant references privileges to your tables and views
grant references on all tables in database "<your-database>" to role datahub_role;
grant references on future tables in database "<your-database>" to role datahub_role;
grant references on all external tables in database "<your-database>" to role datahub_role;
grant references on future external tables in database "<your-database>" to role datahub_role;
grant references on all views in database "<your-database>" to role datahub_role;
grant references on future views in database "<your-database>" to role datahub_role;

// Grant monitor privileges for dynamic tables (Enterprise Edition feature)
grant monitor on all dynamic tables in database "<your-database>" to role datahub_role;
grant monitor on future dynamic tables in database "<your-database>" to role datahub_role;

// If you ARE using Snowflake Profiling or Classification feature: Grant select privileges to your tables
grant select on all tables in database "<your-database>" to role datahub_role;
grant select on future tables in database "<your-database>" to role datahub_role;
grant select on all external tables in database "<your-database>" to role datahub_role;
grant select on future external tables in database "<your-database>" to role datahub_role;
grant select on all dynamic tables in database "<your-database>" to role datahub_role;
grant select on future dynamic tables in database "<your-database>" to role datahub_role;

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
- `usage` on `database` and `schema` are required because without it tables, views, and streams inside them are not accessible. If an admin does the required grants on `table` but misses the grants on `schema` or the `database` in which the table/view/stream exists then we will not be able to get metadata for the table/view/stream.
- If metadata is required only on some schemas then you can grant the usage privileges only on a particular schema like

```sql
grant usage on schema "<your-database>"."<your-schema>" to role datahub_role;
```

- `select` on `streams` is required in order for stream definitions to be available. This does not allow selecting of the data (not required) unless the underlying dataset has select access as well.

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

#### Key Pair Authentication

To set up Key Pair authentication, follow the three steps in [this guide](https://docs.snowflake.com/en/user-guide/key-pair-auth#configuring-key-pair-authentication)

- Generate the private key
- Generate the public key
- Assign the public key to datahub user to be configured in recipe.

Pass in the following values in recipe config instead of password, ensuring the private key maintains proper PEM format with line breaks at the beginning, end, and approximately every 64 characters within the key:

```yml
authentication_type: KEY_PAIR_AUTHENTICATOR
private_key: <Private key in a form of '-----BEGIN PRIVATE KEY-----\nprivate-key\n-----END PRIVATE KEY-----'>

# Optional - if using encrypted private key
private_key_password: <Password for your private key>
```

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

### Snowflake Shares

If you are using [Snowflake Shares](https://docs.snowflake.com/en/user-guide/data-sharing-provider) to share data across different snowflake accounts, and you have set up DataHub recipes for ingesting metadata from all these accounts, you may end up having multiple similar dataset entities corresponding to virtual versions of same table in different snowflake accounts. DataHub Snowflake connector can automatically link such tables together through Siblings and Lineage relationship if user provides information necessary to establish the relationship using configuration `shares` in recipe.

#### Example

- Snowflake account `account1` (ingested as platform_instance `instance1`) owns a database `db1`. A share `X` is created in `account1` that includes database `db1` along with schemas and tables inside it.
- Now, `X` is shared with snowflake account `account2` (ingested as platform_instance `instance2`). A database `db1_from_X` is created from inbound share `X` in `account2`. In this case, all tables and views included in share `X` will also be present in `instance2`.`db1_from_X`.
- This can be represented in `shares` configuration section as
  ```yaml
  shares:
    X: # name of the share
      database: db1
      platform_instance: instance1
      consumers: # list of all databases created from share X
        - database: db1_from_X
          platform_instance: instance2
  ```
- If share `X` is shared with more snowflake accounts and database is created from share `X` in those account then additional entries need to be added in `consumers` list for share `X`, one per snowflake account. The same `shares` config can then be copied across recipes of all accounts.

### Caveats

- Some of the features are only available in the Snowflake Enterprise Edition. This includes dynamic tables, advanced lineage features, and tags. This doc has notes mentioning where this applies.
- Dynamic tables require the `monitor` privilege for metadata extraction. Without this privilege, dynamic tables will not be visible to DataHub.
- The underlying Snowflake views that we use to get metadata have a [latency of 45 minutes to 3 hours](https://docs.snowflake.com/en/sql-reference/account-usage.html#differences-between-account-usage-and-information-schema). So we would not be able to get very recent metadata in some cases like queries you ran within that time period etc. This is applicable particularly for lineage, usage and tags (without lineage) extraction.
- If there is any [incident going on for Snowflake](https://status.snowflake.com/) we will not be able to get the metadata until that incident is resolved.

### CLI based Ingestion

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: snowflake
  config:
    # This option is recommended to be used to ingest all lineage on the first run.
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
# See https://docs.datahub.com/docs/metadata-ingestion/sink_docs/datahub for customization options

```

### Config Details
<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">account_id</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Snowflake account identifier. e.g. xy12345,  xy12345.us-east-2.aws, xy12345.us-central1.gcp, xy12345.central-us.azure, xy12345.us-west-2.privatelink. Refer [Account Identifiers](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#format-2-legacy-account-locator-in-a-region) for more details.  |
| <div className="path-line"><span className="path-main">apply_view_usage_to_tables</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to apply view's usage to its base tables. If set to True, usage is applied to base tables only. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">authentication_type</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The type of authenticator to use when connecting to Snowflake. Supports "DEFAULT_AUTHENTICATOR", "OAUTH_AUTHENTICATOR", "EXTERNAL_BROWSER_AUTHENTICATOR" and "KEY_PAIR_AUTHENTICATOR". <div className="default-line default-line-with-docs">Default: <span className="default-value">DEFAULT&#95;AUTHENTICATOR</span></div> |
| <div className="path-line"><span className="path-main">bucket_duration</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "DAY", "HOUR"  |
| <div className="path-line"><span className="path-main">connect_args</span></div> <div className="type-name-line"><span className="type-name">One of object, null</span></div> | Connect args to pass to Snowflake SqlAlchemy driver <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to convert dataset urns to lowercase. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">email_domain</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Email domain of your organization so users can be displayed on UI appropriately. This is used only if we cannot infer email ID. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">enable_stateful_lineage_ingestion</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable stateful lineage ingestion. This will store lineage window timestamps after successful lineage ingestion. and will not run lineage ingestion for same timestamps in subsequent run.  <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">enable_stateful_profiling</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable stateful profiling. This will store profiling timestamps per dataset after successful profiling. and will not run profiling again in subsequent run if table has not been updated.  <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">enable_stateful_usage_ingestion</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable stateful lineage ingestion. This will store usage window timestamps after successful usage ingestion. and will not run usage ingestion for same timestamps in subsequent run.  <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">end_time</span></div> <div className="type-name-line"><span className="type-name">string(date-time)</span></div> | Latest date of lineage/usage to consider. Default: Current time in UTC  |
| <div className="path-line"><span className="path-main">extract_tags</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "with_lineage", "without_lineage", "skip"  |
| <div className="path-line"><span className="path-main">extract_tags_as_structured_properties</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled along with `extract_tags`, extracts snowflake's key-value tags as DataHub structured properties instead of DataHub tags. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">fetch_views_from_information_schema</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, uses information_schema.views to fetch view definitions instead of SHOW VIEWS command. This alternative method can be more reliable for databases with large numbers of views (> 10K views), as the SHOW VIEWS approach has proven unreliable and can lead to missing views in such scenarios. However, this method requires OWNERSHIP privileges on views to retrieve their definitions. For views without ownership permissions (where VIEW_DEFINITION is null/empty), the system will automatically fall back to using batched SHOW VIEWS queries to populate the missing definitions. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">format_sql_queries</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to format sql queries <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">ignore_start_time_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> |  <div className="default-line ">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_assertion_results</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to ingest assertion run results for assertions created using Datahub assertions CLI in snowflake <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_column_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Populates table->table and view->table column lineage. Requires appropriate grants given to the role and the Snowflake Enterprise Edition or above. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_external_url</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to populate Snowsight url for Snowflake Objects <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_foreign_keys</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, populates the snowflake foreign keys. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_operational_stats</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to display operational stats. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_primary_keys</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, populates the snowflake primary keys. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_procedures</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, procedures will be ingested as pipelines/tasks. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_queries</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, generate query entities associated with lineage edges. Only applicable if `use_queries_v2` is enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_query_usage_statistics</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, generate query popularity statistics. Only applicable if `use_queries_v2` is enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_read_operational_stats</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to report read operational stats. Experimental. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_streams</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, streams will be ingested as separate entities from tables/views. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_table_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, populates the snowflake table-to-table and s3-to-snowflake table lineage. Requires appropriate grants given to the role and Snowflake Enterprise Edition or above. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_table_location_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If the source supports it, include table lineage to the underlying storage location. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_tables</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether tables should be ingested. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_technical_schema</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, populates the snowflake technical schema and descriptions. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_top_n_queries</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to ingest the top_n_queries. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_usage_stats</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, populates the snowflake usage statistics. Requires appropriate grants given to the role. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_view_column_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Populates column-level lineage for  view->view and table->view lineage using DataHub's sql parser. Requires `include_view_lineage` to be enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_view_definitions</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, populates the ingested views' definitions. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_view_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Populates view->view and table->view lineage using DataHub's sql parser. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_views</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether views should be ingested. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">incremental_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, emits lineage as incremental to existing lineage already in DataHub. When disabled, re-states lineage on each run. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">incremental_properties</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When enabled, emits dataset properties as incremental to existing dataset properties in DataHub. When disabled, re-states dataset properties on each run. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">known_snowflake_edition</span></div> <div className="type-name-line"><span className="type-name">One of Enum, null</span></div> | Explicitly specify the Snowflake edition (STANDARD or ENTERPRISE). If unset, the edition will be inferred automatically using 'SHOW TAGS'. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">lazy_schema_resolver</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, uses lazy schema resolver to resolve schemas for tables and views. This is useful if you have a large number of schemas and want to avoid bulk fetching the schema for each table/view. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">match_fully_qualified_names</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether `schema_pattern` is matched against fully qualified schema name `<catalog>.<schema>`. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">options</span></div> <div className="type-name-line"><span className="type-name">object</span></div> | Any options specified here will be passed to [SQLAlchemy.create_engine](https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine) as kwargs.  |
| <div className="path-line"><span className="path-main">password</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Snowflake password. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">private_key</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Private key in a form of '-----BEGIN PRIVATE KEY-----\nprivate-key\n-----END PRIVATE KEY-----\n' if using key pair authentication. Encrypted version of private key will be in a form of '-----BEGIN ENCRYPTED PRIVATE KEY-----\nencrypted-private-key\n-----END ENCRYPTED PRIVATE KEY-----\n' See: https://docs.snowflake.com/en/user-guide/key-pair-auth.html <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">private_key_password</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Password for your private key. Required if using key pair authentication with encrypted private key. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">private_key_path</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The path to the private key if using key pair authentication. Ignored if `private_key` is set. See: https://docs.snowflake.com/en/user-guide/key-pair-auth.html <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">push_down_database_pattern_access_history</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, pushes down database pattern filtering to the access_history table for improved performance. This filters on the accessed objects in access_history. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">query_dedup_strategy</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "STANDARD", "NONE"  |
| <div className="path-line"><span className="path-main">role</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Snowflake role. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">snowflake_domain</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Snowflake domain. Use 'snowflakecomputing.com' for most regions or 'snowflakecomputing.cn' for China (cn-northwest-1) region. <div className="default-line default-line-with-docs">Default: <span className="default-value">snowflakecomputing.com</span></div> |
| <div className="path-line"><span className="path-main">start_time</span></div> <div className="type-name-line"><span className="type-name">string(date-time)</span></div> | Earliest date of lineage/usage to consider. Default: Last full day in UTC (or hour, depending on `bucket_duration`). You can also specify relative time with respect to end_time such as '-7 days' Or '-7d'. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">token</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | OAuth token from external identity provider. Not recommended for most use cases because it will not be able to refresh once expired. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">top_n_queries</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of top queries to save to each table. <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div> |
| <div className="path-line"><span className="path-main">upstream_lineage_in_report</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> |  <div className="default-line ">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">use_file_backed_cache</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to use a file backed cache for the view definitions. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">use_queries_v2</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, uses the new queries extractor to extract queries from snowflake. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">username</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Snowflake username. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">validate_upstreams_against_patterns</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to validate upstream snowflake tables against allow-deny patterns <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">warehouse</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Snowflake warehouse. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">additional_database_names_allowlist</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Additional database names (no pattern matching) to be included in the access_history filter. Only applies if push_down_database_pattern_access_history=True. These databases will be included in the filter being pushed down regardless of database_pattern settings.This may be required in the case of _eg_ temporary tables being created in a different database than the ones in the database_name patterns. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">additional_database_names_allowlist.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">database_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">database_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">domain</span></div> <div className="type-name-line"><span className="type-name">map(str,AllowDenyPattern)</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">allow</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to include in ingestion <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;.&#42;&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">domain.`key`.allow.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">domain.`key`.</span><span className="path-main">deny</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of regex patterns to exclude from ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">domain.`key`.deny.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">oauth_config</span></div> <div className="type-name-line"><span className="type-name">One of OAuthConfiguration, null</span></div> | oauth configuration - https://docs.snowflake.com/en/user-guide/python-connector-example.html#connecting-with-oauth <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">oauth_config.</span><span className="path-main">authority_url</span>&nbsp;<abbr title="Required if oauth_config is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Authority url of your identity provider  |
| <div className="path-line"><span className="path-prefix">oauth_config.</span><span className="path-main">client_id</span>&nbsp;<abbr title="Required if oauth_config is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | client id of your registered application  |
| <div className="path-line"><span className="path-prefix">oauth_config.</span><span className="path-main">provider</span>&nbsp;<abbr title="Required if oauth_config is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "microsoft", "okta"  |
| <div className="path-line"><span className="path-prefix">oauth_config.</span><span className="path-main">scopes</span>&nbsp;<abbr title="Required if oauth_config is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">array</span></div> | scopes required to connect to snowflake  |
| <div className="path-line"><span className="path-prefix">oauth_config.scopes.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">oauth_config.</span><span className="path-main">client_secret</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | client secret of the application if use_certificate = false <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">oauth_config.</span><span className="path-main">encoded_oauth_private_key</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | base64 encoded private key content if use_certificate = true <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">oauth_config.</span><span className="path-main">encoded_oauth_public_key</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | base64 encoded certificate content if use_certificate = true <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">oauth_config.</span><span className="path-main">use_certificate</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Do you want to use certificate and private key to authenticate using oauth <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">procedure_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">procedure_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">profile_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">profile_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">pushdown_allow_usernames</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of snowflake usernames (SQL LIKE patterns, e.g., 'ANALYST_%', '%_USER', 'MAIN_ACCOUNT') which WILL be considered for lineage/usage/queries extraction. This is primarily useful for improving performance by filtering in only specific users. Only applicable if `use_queries_v2` is enabled. If not specified, all users not in deny list are included. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">pushdown_allow_usernames.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">pushdown_deny_usernames</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of snowflake usernames (SQL LIKE patterns, e.g., 'SERVICE_%', '%_PROD', 'TEST_USER') which will NOT be considered for lineage/usage/queries extraction. This is primarily useful for improving performance by filtering out users with extremely high query volumes. Only applicable if `use_queries_v2` is enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">pushdown_deny_usernames.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">schema_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">schema_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">shares</span></div> <div className="type-name-line"><span className="type-name">One of SnowflakeShareConfig, null</span></div> | Required if current account owns or consumes snowflake share.If specified, connector creates lineage and siblings relationship between current account's database tables and consumer/producer account's database tables. Map of share name -> details of share. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">shares.`key`.</span><span className="path-main">database</span>&nbsp;<abbr title="Required if shares is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Database from which share is created.  |
| <div className="path-line"><span className="path-prefix">shares.`key`.</span><span className="path-main">consumers</span>&nbsp;<abbr title="Required if shares is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of databases created in consumer accounts.  |
| <div className="path-line"><span className="path-prefix">shares.`key`.consumers.</span><span className="path-main">DatabaseId</span></div> <div className="type-name-line"><span className="type-name">DatabaseId</span></div> |   |
| <div className="path-line"><span className="path-prefix">shares.`key`.consumers.DatabaseId.</span><span className="path-main">database</span>&nbsp;<abbr title="Required if DatabaseId is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">shares.`key`.consumers.DatabaseId.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> |  <div className="default-line ">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">shares.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Platform instance for snowflake account in which share is created. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">stream_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">stream_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">structured_property_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">structured_property_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">table_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">table_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">tag_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">tag_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">temporary_tables_pattern</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | [Advanced] Regex patterns for temporary tables to filter in lineage ingestion. Specify regex to match the entire table name in database.schema.table format. Defaults are to set in such a way to ignore the temporary staging tables created by known ETL tools. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;.&#42;\\.FIVETRAN&#95;.&#42;&#95;STAGING\\..&#42;&#x27;, &#x27;.&#42;&#95;&#95;DBT&#95;TMP$&#x27;, ...</span></div> |
| <div className="path-line"><span className="path-prefix">temporary_tables_pattern.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">user_email_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">user_email_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
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
    "BucketDuration": {
      "enum": [
        "DAY",
        "HOUR"
      ],
      "title": "BucketDuration",
      "type": "string"
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
    "DatabaseId": {
      "properties": {
        "database": {
          "title": "Database",
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
          "title": "Platform Instance"
        }
      },
      "required": [
        "database"
      ],
      "title": "DatabaseId",
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
    "OAuthConfiguration": {
      "additionalProperties": false,
      "properties": {
        "provider": {
          "$ref": "#/$defs/OAuthIdentityProvider",
          "description": "Identity provider for oauth.Supported providers are microsoft and okta."
        },
        "authority_url": {
          "description": "Authority url of your identity provider",
          "title": "Authority Url",
          "type": "string"
        },
        "client_id": {
          "description": "client id of your registered application",
          "title": "Client Id",
          "type": "string"
        },
        "scopes": {
          "description": "scopes required to connect to snowflake",
          "items": {
            "type": "string"
          },
          "title": "Scopes",
          "type": "array"
        },
        "use_certificate": {
          "default": false,
          "description": "Do you want to use certificate and private key to authenticate using oauth",
          "title": "Use Certificate",
          "type": "boolean"
        },
        "client_secret": {
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
          "description": "client secret of the application if use_certificate = false",
          "title": "Client Secret"
        },
        "encoded_oauth_public_key": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "base64 encoded certificate content if use_certificate = true",
          "title": "Encoded Oauth Public Key"
        },
        "encoded_oauth_private_key": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "base64 encoded private key content if use_certificate = true",
          "title": "Encoded Oauth Private Key"
        }
      },
      "required": [
        "provider",
        "authority_url",
        "client_id",
        "scopes"
      ],
      "title": "OAuthConfiguration",
      "type": "object"
    },
    "OAuthIdentityProvider": {
      "enum": [
        "microsoft",
        "okta"
      ],
      "title": "OAuthIdentityProvider",
      "type": "string"
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
    "QueryDedupStrategyType": {
      "enum": [
        "STANDARD",
        "NONE"
      ],
      "title": "QueryDedupStrategyType",
      "type": "string"
    },
    "SnowflakeEdition": {
      "enum": [
        "Standard",
        "Enterprise or above"
      ],
      "title": "SnowflakeEdition",
      "type": "string"
    },
    "SnowflakeShareConfig": {
      "additionalProperties": false,
      "properties": {
        "database": {
          "description": "Database from which share is created.",
          "title": "Database",
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
          "description": "Platform instance for snowflake account in which share is created.",
          "title": "Platform Instance"
        },
        "consumers": {
          "description": "List of databases created in consumer accounts.",
          "items": {
            "$ref": "#/$defs/DatabaseId"
          },
          "title": "Consumers",
          "type": "array",
          "uniqueItems": true
        }
      },
      "required": [
        "database",
        "consumers"
      ],
      "title": "SnowflakeShareConfig",
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
    },
    "TagOption": {
      "enum": [
        "with_lineage",
        "without_lineage",
        "skip"
      ],
      "title": "TagOption",
      "type": "string"
    }
  },
  "additionalProperties": false,
  "properties": {
    "incremental_properties": {
      "default": false,
      "description": "When enabled, emits dataset properties as incremental to existing dataset properties in DataHub. When disabled, re-states dataset properties on each run.",
      "title": "Incremental Properties",
      "type": "boolean"
    },
    "schema_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for schemas to filter in ingestion. Will match against the full `database.schema` name if `match_fully_qualified_names` is enabled."
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
    "enable_stateful_profiling": {
      "default": true,
      "description": "Enable stateful profiling. This will store profiling timestamps per dataset after successful profiling. and will not run profiling again in subsequent run if table has not been updated. ",
      "title": "Enable Stateful Profiling",
      "type": "boolean"
    },
    "incremental_lineage": {
      "default": false,
      "description": "When enabled, emits lineage as incremental to existing lineage already in DataHub. When disabled, re-states lineage on each run.",
      "title": "Incremental Lineage",
      "type": "boolean"
    },
    "convert_urns_to_lowercase": {
      "default": true,
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
      "description": "Any options specified here will be passed to [SQLAlchemy.create_engine](https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine) as kwargs.",
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
    "bucket_duration": {
      "$ref": "#/$defs/BucketDuration",
      "default": "DAY",
      "description": "Size of the time window to aggregate usage stats."
    },
    "end_time": {
      "description": "Latest date of lineage/usage to consider. Default: Current time in UTC",
      "format": "date-time",
      "title": "End Time",
      "type": "string"
    },
    "start_time": {
      "default": null,
      "description": "Earliest date of lineage/usage to consider. Default: Last full day in UTC (or hour, depending on `bucket_duration`). You can also specify relative time with respect to end_time such as '-7 days' Or '-7d'.",
      "format": "date-time",
      "title": "Start Time",
      "type": "string"
    },
    "enable_stateful_usage_ingestion": {
      "default": true,
      "description": "Enable stateful lineage ingestion. This will store usage window timestamps after successful usage ingestion. and will not run usage ingestion for same timestamps in subsequent run. ",
      "title": "Enable Stateful Usage Ingestion",
      "type": "boolean"
    },
    "enable_stateful_lineage_ingestion": {
      "default": true,
      "description": "Enable stateful lineage ingestion. This will store lineage window timestamps after successful lineage ingestion. and will not run lineage ingestion for same timestamps in subsequent run. ",
      "title": "Enable Stateful Lineage Ingestion",
      "type": "boolean"
    },
    "top_n_queries": {
      "default": 10,
      "description": "Number of top queries to save to each table.",
      "exclusiveMinimum": 0,
      "title": "Top N Queries",
      "type": "integer"
    },
    "user_email_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "regex patterns for user emails to filter in usage."
    },
    "include_operational_stats": {
      "default": true,
      "description": "Whether to display operational stats.",
      "title": "Include Operational Stats",
      "type": "boolean"
    },
    "include_read_operational_stats": {
      "default": false,
      "description": "Whether to report read operational stats. Experimental.",
      "title": "Include Read Operational Stats",
      "type": "boolean"
    },
    "format_sql_queries": {
      "default": false,
      "description": "Whether to format sql queries",
      "title": "Format Sql Queries",
      "type": "boolean"
    },
    "include_top_n_queries": {
      "default": true,
      "description": "Whether to ingest the top_n_queries.",
      "title": "Include Top N Queries",
      "type": "boolean"
    },
    "apply_view_usage_to_tables": {
      "default": false,
      "description": "Whether to apply view's usage to its base tables. If set to True, usage is applied to base tables only.",
      "title": "Apply View Usage To Tables",
      "type": "boolean"
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
      "description": "Snowflake username.",
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
      "description": "Snowflake password.",
      "title": "Password"
    },
    "private_key": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Private key in a form of '-----BEGIN PRIVATE KEY-----\\nprivate-key\\n-----END PRIVATE KEY-----\\n' if using key pair authentication. Encrypted version of private key will be in a form of '-----BEGIN ENCRYPTED PRIVATE KEY-----\\nencrypted-private-key\\n-----END ENCRYPTED PRIVATE KEY-----\\n' See: https://docs.snowflake.com/en/user-guide/key-pair-auth.html",
      "title": "Private Key"
    },
    "private_key_path": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "The path to the private key if using key pair authentication. Ignored if `private_key` is set. See: https://docs.snowflake.com/en/user-guide/key-pair-auth.html",
      "title": "Private Key Path"
    },
    "private_key_password": {
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
      "description": "Password for your private key. Required if using key pair authentication with encrypted private key.",
      "title": "Private Key Password"
    },
    "oauth_config": {
      "anyOf": [
        {
          "$ref": "#/$defs/OAuthConfiguration"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "oauth configuration - https://docs.snowflake.com/en/user-guide/python-connector-example.html#connecting-with-oauth"
    },
    "authentication_type": {
      "default": "DEFAULT_AUTHENTICATOR",
      "description": "The type of authenticator to use when connecting to Snowflake. Supports \"DEFAULT_AUTHENTICATOR\", \"OAUTH_AUTHENTICATOR\", \"EXTERNAL_BROWSER_AUTHENTICATOR\" and \"KEY_PAIR_AUTHENTICATOR\".",
      "title": "Authentication Type",
      "type": "string"
    },
    "account_id": {
      "description": "Snowflake account identifier. e.g. xy12345,  xy12345.us-east-2.aws, xy12345.us-central1.gcp, xy12345.central-us.azure, xy12345.us-west-2.privatelink. Refer [Account Identifiers](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#format-2-legacy-account-locator-in-a-region) for more details.",
      "title": "Account Id",
      "type": "string"
    },
    "warehouse": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Snowflake warehouse.",
      "title": "Warehouse"
    },
    "role": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Snowflake role.",
      "title": "Role"
    },
    "connect_args": {
      "anyOf": [
        {
          "additionalProperties": true,
          "type": "object"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Connect args to pass to Snowflake SqlAlchemy driver",
      "title": "Connect Args"
    },
    "token": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "OAuth token from external identity provider. Not recommended for most use cases because it will not be able to refresh once expired.",
      "title": "Token"
    },
    "snowflake_domain": {
      "default": "snowflakecomputing.com",
      "description": "Snowflake domain. Use 'snowflakecomputing.com' for most regions or 'snowflakecomputing.cn' for China (cn-northwest-1) region.",
      "title": "Snowflake Domain",
      "type": "string"
    },
    "database_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
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
      "description": "Regex patterns for databases to filter in ingestion."
    },
    "stream_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for streams to filter in ingestion. Specify regex to match the entire view name in database.schema.view format. e.g. to match all views starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'"
    },
    "procedure_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for procedures to filter in ingestion. Specify regex to match the entire procedure name in database.schema.procedure format. e.g. to match all procedures starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'"
    },
    "match_fully_qualified_names": {
      "default": false,
      "description": "Whether `schema_pattern` is matched against fully qualified schema name `<catalog>.<schema>`.",
      "title": "Match Fully Qualified Names",
      "type": "boolean"
    },
    "email_domain": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Email domain of your organization so users can be displayed on UI appropriately. This is used only if we cannot infer email ID.",
      "title": "Email Domain"
    },
    "include_table_lineage": {
      "default": true,
      "description": "If enabled, populates the snowflake table-to-table and s3-to-snowflake table lineage. Requires appropriate grants given to the role and Snowflake Enterprise Edition or above.",
      "title": "Include Table Lineage",
      "type": "boolean"
    },
    "ignore_start_time_lineage": {
      "default": false,
      "title": "Ignore Start Time Lineage",
      "type": "boolean"
    },
    "upstream_lineage_in_report": {
      "default": false,
      "title": "Upstream Lineage In Report",
      "type": "boolean"
    },
    "include_usage_stats": {
      "default": true,
      "description": "If enabled, populates the snowflake usage statistics. Requires appropriate grants given to the role.",
      "title": "Include Usage Stats",
      "type": "boolean"
    },
    "include_view_definitions": {
      "default": true,
      "description": "If enabled, populates the ingested views' definitions.",
      "title": "Include View Definitions",
      "type": "boolean"
    },
    "fetch_views_from_information_schema": {
      "default": false,
      "description": "If enabled, uses information_schema.views to fetch view definitions instead of SHOW VIEWS command. This alternative method can be more reliable for databases with large numbers of views (> 10K views), as the SHOW VIEWS approach has proven unreliable and can lead to missing views in such scenarios. However, this method requires OWNERSHIP privileges on views to retrieve their definitions. For views without ownership permissions (where VIEW_DEFINITION is null/empty), the system will automatically fall back to using batched SHOW VIEWS queries to populate the missing definitions.",
      "title": "Fetch Views From Information Schema",
      "type": "boolean"
    },
    "include_technical_schema": {
      "default": true,
      "description": "If enabled, populates the snowflake technical schema and descriptions.",
      "title": "Include Technical Schema",
      "type": "boolean"
    },
    "include_primary_keys": {
      "default": true,
      "description": "If enabled, populates the snowflake primary keys.",
      "title": "Include Primary Keys",
      "type": "boolean"
    },
    "include_foreign_keys": {
      "default": true,
      "description": "If enabled, populates the snowflake foreign keys.",
      "title": "Include Foreign Keys",
      "type": "boolean"
    },
    "include_column_lineage": {
      "default": true,
      "description": "Populates table->table and view->table column lineage. Requires appropriate grants given to the role and the Snowflake Enterprise Edition or above.",
      "title": "Include Column Lineage",
      "type": "boolean"
    },
    "use_queries_v2": {
      "default": true,
      "description": "If enabled, uses the new queries extractor to extract queries from snowflake.",
      "title": "Use Queries V2",
      "type": "boolean"
    },
    "include_queries": {
      "default": true,
      "description": "If enabled, generate query entities associated with lineage edges. Only applicable if `use_queries_v2` is enabled.",
      "title": "Include Queries",
      "type": "boolean"
    },
    "include_query_usage_statistics": {
      "default": true,
      "description": "If enabled, generate query popularity statistics. Only applicable if `use_queries_v2` is enabled.",
      "title": "Include Query Usage Statistics",
      "type": "boolean"
    },
    "lazy_schema_resolver": {
      "default": true,
      "description": "If enabled, uses lazy schema resolver to resolve schemas for tables and views. This is useful if you have a large number of schemas and want to avoid bulk fetching the schema for each table/view.",
      "title": "Lazy Schema Resolver",
      "type": "boolean"
    },
    "query_dedup_strategy": {
      "$ref": "#/$defs/QueryDedupStrategyType",
      "default": "STANDARD",
      "description": "Experimental: Choose the strategy for query deduplication (default value is appropriate for most use-cases; make sure you understand performance implications before changing it). Allowed values are: STANDARD, NONE"
    },
    "extract_tags": {
      "$ref": "#/$defs/TagOption",
      "default": "skip",
      "description": "Optional. Allowed values are `without_lineage`, `with_lineage`, and `skip` (default). `without_lineage` only extracts tags that have been applied directly to the given entity. `with_lineage` extracts both directly applied and propagated tags, but will be significantly slower. See the [Snowflake documentation](https://docs.snowflake.com/en/user-guide/object-tagging.html#tag-lineage) for information about tag lineage/propagation. "
    },
    "extract_tags_as_structured_properties": {
      "default": false,
      "description": "If enabled along with `extract_tags`, extracts snowflake's key-value tags as DataHub structured properties instead of DataHub tags.",
      "title": "Extract Tags As Structured Properties",
      "type": "boolean"
    },
    "include_external_url": {
      "default": true,
      "description": "Whether to populate Snowsight url for Snowflake Objects",
      "title": "Include External Url",
      "type": "boolean"
    },
    "validate_upstreams_against_patterns": {
      "default": true,
      "description": "Whether to validate upstream snowflake tables against allow-deny patterns",
      "title": "Validate Upstreams Against Patterns",
      "type": "boolean"
    },
    "tag_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "List of regex patterns for tags to include in ingestion. Only used if `extract_tags` is enabled."
    },
    "include_streams": {
      "default": true,
      "description": "If enabled, streams will be ingested as separate entities from tables/views.",
      "title": "Include Streams",
      "type": "boolean"
    },
    "include_procedures": {
      "default": true,
      "description": "If enabled, procedures will be ingested as pipelines/tasks.",
      "title": "Include Procedures",
      "type": "boolean"
    },
    "structured_property_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "List of regex patterns for structured properties to include in ingestion. Applied to tags with form `<database>.<schema>.<tag_name>`. Only used if `extract_tags` and `extract_tags_as_structured_properties` are enabled."
    },
    "temporary_tables_pattern": {
      "default": [
        ".*\\.FIVETRAN_.*_STAGING\\..*",
        ".*__DBT_TMP$",
        ".*\\.SEGMENT_[a-f0-9]{8}[-_][a-f0-9]{4}[-_][a-f0-9]{4}[-_][a-f0-9]{4}[-_][a-f0-9]{12}",
        ".*\\.STAGING_.*_[a-f0-9]{8}[-_][a-f0-9]{4}[-_][a-f0-9]{4}[-_][a-f0-9]{4}[-_][a-f0-9]{12}",
        ".*\\.(GE_TMP_|GE_TEMP_|GX_TEMP_)[0-9A-F]{8}",
        ".*\\.SNOWPARK_TEMP_TABLE_.+"
      ],
      "description": "[Advanced] Regex patterns for temporary tables to filter in lineage ingestion. Specify regex to match the entire table name in database.schema.table format. Defaults are to set in such a way to ignore the temporary staging tables created by known ETL tools.",
      "items": {
        "type": "string"
      },
      "title": "Temporary Tables Pattern",
      "type": "array"
    },
    "shares": {
      "anyOf": [
        {
          "additionalProperties": {
            "$ref": "#/$defs/SnowflakeShareConfig"
          },
          "type": "object"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Required if current account owns or consumes snowflake share.If specified, connector creates lineage and siblings relationship between current account's database tables and consumer/producer account's database tables. Map of share name -> details of share.",
      "title": "Shares"
    },
    "known_snowflake_edition": {
      "anyOf": [
        {
          "$ref": "#/$defs/SnowflakeEdition"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Explicitly specify the Snowflake edition (STANDARD or ENTERPRISE). If unset, the edition will be inferred automatically using 'SHOW TAGS'."
    },
    "include_assertion_results": {
      "default": false,
      "description": "Whether to ingest assertion run results for assertions created using Datahub assertions CLI in snowflake",
      "title": "Include Assertion Results",
      "type": "boolean"
    },
    "pushdown_deny_usernames": {
      "default": [],
      "description": "List of snowflake usernames (SQL LIKE patterns, e.g., 'SERVICE_%', '%_PROD', 'TEST_USER') which will NOT be considered for lineage/usage/queries extraction. This is primarily useful for improving performance by filtering out users with extremely high query volumes. Only applicable if `use_queries_v2` is enabled.",
      "items": {
        "type": "string"
      },
      "title": "Pushdown Deny Usernames",
      "type": "array"
    },
    "pushdown_allow_usernames": {
      "default": [],
      "description": "List of snowflake usernames (SQL LIKE patterns, e.g., 'ANALYST_%', '%_USER', 'MAIN_ACCOUNT') which WILL be considered for lineage/usage/queries extraction. This is primarily useful for improving performance by filtering in only specific users. Only applicable if `use_queries_v2` is enabled. If not specified, all users not in deny list are included.",
      "items": {
        "type": "string"
      },
      "title": "Pushdown Allow Usernames",
      "type": "array"
    },
    "push_down_database_pattern_access_history": {
      "default": false,
      "description": "If enabled, pushes down database pattern filtering to the access_history table for improved performance. This filters on the accessed objects in access_history.",
      "title": "Push Down Database Pattern Access History",
      "type": "boolean"
    },
    "additional_database_names_allowlist": {
      "default": [],
      "description": "Additional database names (no pattern matching) to be included in the access_history filter. Only applies if push_down_database_pattern_access_history=True. These databases will be included in the filter being pushed down regardless of database_pattern settings.This may be required in the case of _eg_ temporary tables being created in a different database than the ones in the database_name patterns.",
      "items": {
        "type": "string"
      },
      "title": "Additional Database Names Allowlist",
      "type": "array"
    }
  },
  "required": [
    "account_id"
  ],
  "title": "SnowflakeV2Config",
  "type": "object"
}
```


</TabItem>
</Tabs>


### Code Coordinates
- Class Name: `datahub.ingestion.source.snowflake.snowflake_v2.SnowflakeV2Source`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/snowflake/snowflake_v2.py)


<h2>Questions</h2>

If you've got any questions on configuring ingestion for Snowflake, feel free to ping us on [our Slack](https://datahub.com/slack).
