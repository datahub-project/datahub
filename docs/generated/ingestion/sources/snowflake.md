


# Snowflake

## Overview

Snowflake is a data platform used to store and query analytical or operational data. Learn more in the [official Snowflake documentation](https://www.snowflake.com/).

The DataHub integration for Snowflake covers core metadata entities such as datasets/tables/views, schema fields, and containers. It also captures table- and column-level lineage, usage statistics, data profiling, tags, and stateful deletion detection.

:::info Snowflake Ingestion through the UI

The following video shows you how to ingest Snowflake metadata through the UI.

[▶ Watch video](https://www.loom.com/share/15d0401caa1c4aa483afef1d351760db)

Read on if you are interested in ingesting Snowflake metadata using the **datahub** cli, or want to learn about all the configuration parameters that are supported by the connectors.
:::

## Concept Mapping

| Snowflake Concept               | DataHub Entity (Subtype)                                     | Notes                                                                                                                               |
| ------------------------------- | ------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------- |
| Account                         | Platform Instance                                            | Top-level scope; all URNs include the configured platform instance.                                                                 |
| Database                        | Container (DATABASE)                                         | Top-level namespace. Ingested with description, tags, and Snowsight URL.                                                            |
| Schema                          | Container (SCHEMA)                                           | Nested under its Database container.                                                                                                |
| Table                           | Dataset (TABLE)                                              | Includes regular, Iceberg, and hybrid tables. Schema, PKs/FKs, tags, and descriptions are extracted.                                |
| Dynamic Table                   | Dataset (DYNAMIC TABLE)                                      | Includes target lag, SQL definition, and lineage to source tables.                                                                  |
| View                            | Dataset (VIEW)                                               | Standard, materialized, and secure views. View definition is captured.                                                              |
| Semantic View                   | Dataset (SEMANTIC VIEW)                                      | Columns classified as DIMENSION, FACT, or METRIC. Column-level lineage to physical tables is extracted.                             |
| Stream                          | Dataset (SNOWFLAKE STREAM)                                   | Change-data-capture stream. Adds `METADATA$ACTION`, `METADATA$ISUPDATE`, `METADATA$ROW_ID` columns and lineage to the source table. |
| External Table                  | Dataset (TABLE)                                              | Lineage to the backing cloud storage location is emitted when available.                                                            |
| Internal Stage                  | Container (SNOWFLAKE STAGE) + Dataset (SNOWFLAKE STAGE DATA) | Emits both a Container (organizational) and a Dataset (for the resident data).                                                      |
| External Stage                  | Container (SNOWFLAKE STAGE)                                  | Container only; the backing cloud storage asset (S3/GCS/Azure) is referenced via lineage.                                           |
| Task                            | DataFlow (SNOWFLAKE TASK GROUP) + DataJob (SNOWFLAKE TASK)   | One DataFlow per schema; each task is a DataJob. Predecessor relationships appear as DataJob inputs.                                |
| Pipe                            | DataFlow (SNOWFLAKE PIPE GROUP) + DataJob (SNOWFLAKE PIPE)   | One DataFlow per schema; each pipe is a DataJob linking a stage to a target table via lineage.                                      |
| Streamlit App                   | Dashboard (STREAMLIT)                                        | App name, owner, and Snowsight URL captured as custom properties.                                                                   |
| Column / field                  | SchemaField                                                  | Column type, nullability, descriptions, and tags are extracted where available.                                                     |
| Role                            | CorpGroup                                                    | Ownership roles are mapped to `urn:li:corpGroup:{role_name}`.                                                                       |
| Tag                             | Tag or Structured Property                                   | Controlled by `extract_tags` config. Tags support database/schema/table/column inheritance.                                         |
| Table- and column-level lineage | Lineage edges                                                | Extracted from view definitions, dynamic table definitions, and SQL query history.                                                  |
| Query operations and usage      | DatasetUsageStatistics, Operation                            | Per-dataset query counts, user access patterns, and DML operation metrics.                                                          |


## Module `snowflake`
![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Enabled by default. Supported for types - Database, Schema. |
| Column-level Lineage | ✅ | Enabled by default, can be disabled via configuration `include_column_lineage`. |
| [Data Profiling](../../../../metadata-ingestion/docs/dev_guides/sql_profiles.md) | ✅ | Optionally enabled via configuration `profiling.enabled`. |
| Dataset Usage | ✅ | Enabled by default, can be disabled via configuration `include_usage_stats`. |
| Descriptions | ✅ | Enabled by default. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| [Domains](../../../domains.md) | ✅ | Supported via the `domain` config field. |
| Extract Tags | ✅ | Optionally enabled via `extract_tags`. |
| [Operation Capture](../../../api/tutorials/operations.md) | ✅ | Enabled by default, can be disabled via configuration `include_operational_stats`. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Schema Metadata | ✅ | Enabled by default. |
| Table-Level Lineage | ✅ | Enabled by default, can be disabled via configuration `include_table_lineage`. |
| Test Connection | ✅ | Enabled by default. |

### Overview

The `snowflake` module ingests metadata from Snowflake into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

Requires specific privileges to read metadata from your Snowflake warehouse.

Execute the following commands as `ACCOUNTADMIN` or a user with `MANAGE GRANTS` privilege to create a DataHub-specific role:

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
-- Note: Semantic views are covered by the above view grants

-- Grant monitor privileges for dynamic tables
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

// Optional - required for INTERNAL marketplace ingestion (private data sharing)
// This grants access to:
// - SHOW AVAILABLE LISTINGS (IS_ORGANIZATION = TRUE) for internal marketplace listings
// - SNOWFLAKE.ACCOUNT_USAGE.DATABASES for identifying imported databases
// - SNOWFLAKE.DATA_SHARING_USAGE.LISTING_ACCESS_HISTORY for usage statistics
// - DESCRIBE AVAILABLE LISTING for enriched metadata (if fetch_internal_marketplace_listing_details=true)
grant imported privileges on database snowflake to role datahub_role;

// For marketplace provider mode (marketplace_mode: provider or both):
// SHOW SHARES lists shares the role owns or is inheriting from a parent role.
// DESC SHARE requires OWNERSHIP of the share — Snowflake grants no finer-grained privilege.
// Marketplace-created shares are typically owned by SYSADMIN. To allow DESC SHARE,
// grant SYSADMIN to the DataHub role (use SECURITYADMIN to grant roles):
use role securityadmin;
grant role sysadmin to role datahub_role;

// Alternatively, set `role: SYSADMIN` directly in your recipe.

// Optional - required if extracting Streamlit Apps
grant usage on all streamlits in database "<your-database>" to role datahub_role;
grant usage on future streamlits in database "<your-database>" to role datahub_role;

// Optional - required if extracting Stages, Tasks, or Pipes
grant usage on all stages in database "<your-database>" to role datahub_role;
grant usage on future stages in database "<your-database>" to role datahub_role;
grant monitor on all tasks in database "<your-database>" to role datahub_role;
grant monitor on future tasks in database "<your-database>" to role datahub_role;
grant monitor on all pipes in database "<your-database>" to role datahub_role;
grant monitor on future pipes in database "<your-database>" to role datahub_role;
```

The details of each granted privilege can be viewed in the [Snowflake docs](https://docs.snowflake.com/en/user-guide/security-access-control-privileges.html). A summary of each privilege and why it is required for this connector:

- `operate` is required only to start the warehouse.
  If the warehouse is already running during ingestion or has auto-resume enabled,
  this permission is not required.
- `usage` is required to run queries using the warehouse
- `usage` on `database` and `schema` are required because without them, tables, views, and streams inside them are not accessible. If an admin does the required grants on `table` but misses the grants on `schema` or the `database` in which the table/view/stream exists, then we will not be able to get metadata for the table/view/stream.
- If metadata is required only on some schemas, then you can grant the usage privileges only on a particular schema like:

```sql
grant usage on schema "<your-database>"."<your-schema>" to role datahub_role;
```

- `select` on `streams` is required for stream definitions to be available. This does not allow selecting the data (not required) unless the underlying dataset has select access as well.
- `usage` on `streamlit` is required to show streamlits in a database. See the schema-level `usage` example above.
- `usage` on `stages` is required to list stages via `SHOW STAGES`. Only needed if `include_stages: true` or `include_pipes: true`.
- `monitor` on `tasks` is required to list tasks via `SHOW TASKS`. Only needed if `include_tasks: true`.
- `monitor` on `pipes` is required to list pipes via `SHOW PIPES`. Only needed if `include_pipes: true`.

This represents the bare minimum privileges required to extract databases, schemas, views, and tables from Snowflake.

If you plan to enable extraction of table lineage via the `include_table_lineage` config flag, extraction of usage statistics via the `include_usage_stats` config, or extraction of tags (without lineage) via the `extract_tags` config, you'll also need to grant access to the [Account Usage](https://docs.snowflake.com/en/sql-reference/account-usage.html) system tables from which the DataHub source extracts information. This can be done by granting access to the `snowflake` database.

```sql
grant imported privileges on database snowflake to role datahub_role;
```

Note that `imported privileges` grants access to all schemas and views in the shared `SNOWFLAKE` database, primarily:

- `SNOWFLAKE.ACCOUNT_USAGE.*` (all views: `QUERY_HISTORY`, `ACCESS_HISTORY`, `USERS`, etc.)
- `SNOWFLAKE.ORGANIZATION_USAGE.*` (requires separate enablement by Snowflake support at the organization level)

The `SNOWFLAKE` database is a shared database owned by Snowflake. Unlike regular databases where you can grant granular `SELECT` privileges on individual tables, shared databases require granting `IMPORTED PRIVILEGES` which provides all-or-nothing access to all objects in the database.

#### Which ACCOUNT_USAGE Tables Does DataHub Access?

When you grant `IMPORTED PRIVILEGES`, DataHub will specifically access the following `ACCOUNT_USAGE` tables:

| Table            | Purpose                                                      | Required For                                                      |
| ---------------- | ------------------------------------------------------------ | ----------------------------------------------------------------- |
| `QUERY_HISTORY`  | Query logs for lineage, usage stats, and semantic view usage | `include_table_lineage`, `include_usage_stats`, `include_queries` |
| `ACCESS_HISTORY` | Table/view lineage and access patterns                       | `include_table_lineage`, `include_usage_stats`                    |
| `USERS`          | User email mapping for corp user entities                    | `include_usage_stats` (for user attribution)                      |
| `TAG_REFERENCES` | Tag metadata extraction                                      | `extract_tags`                                                    |
| `VIEWS`          | View metadata (DDL, ownership, etc.) for all views           | Always (when views exist)                                         |
| `COPY_HISTORY`   | Lineage from `COPY INTO` operations (all stages/sources)     | `include_table_lineage`                                           |

If you cannot grant `IMPORTED PRIVILEGES` due to security policies, the related features (lineage, usage, tags) will not work, and you'll see permission errors in the ingestion logs.

#### Authentication

Authentication is most simply done via a Snowflake user and password.

Alternatively, other authentication methods are supported via the `authentication_type` config option.

##### Key Pair Authentication

To set up Key Pair authentication, follow the three steps in [this guide](https://docs.snowflake.com/en/user-guide/key-pair-auth#configuring-key-pair-authentication):

- Generate the private key
- Generate the public key
- Assign the public key to the DataHub user to be configured in the recipe.

Pass in the following values in the recipe config instead of a password, ensuring the private key maintains proper PEM format with line breaks at the beginning, end, and approximately every 64 characters within the key:

```yml
authentication_type: KEY_PAIR_AUTHENTICATOR
private_key: <Private key in a form of '-----BEGIN PRIVATE KEY-----\nprivate-key\n-----END PRIVATE KEY-----'>

# Optional - if using encrypted private key
private_key_password: <Password for your private key>
```

##### Okta OAuth

To set up Okta OAuth authentication, roughly follow the four steps in [this guide](https://docs.snowflake.com/en/user-guide/oauth-okta).

Pass in the following values, as described in the article, for your recipe's `oauth_config`:

- `provider`: okta
- `client_id`: `<OAUTH_CLIENT_ID>`
- `client_secret`: `<OAUTH_CLIENT_SECRET>`
- `authority_url`: `<OKTA_OAUTH_TOKEN_ENDPOINT>`
- `scopes`: The list of your _Okta_ scopes, i.e. with the `session:role:` prefix

DataHub only supports two OAuth grant types: `client_credentials` and `password`.
The steps slightly differ based on which you decide to use.

##### Client Credentials Grant Type (Simpler)

- When creating an Okta App Integration, choose type `API Services`
  - Ensure client authentication method is `Client secret`
  - Note your `Client ID`
- Create a Snowflake user to correspond to your newly created Okta client credentials
  - _Ensure the user's `Login Name` matches your Okta application's `Client ID`_
  - Ensure the user has been granted your DataHub role

##### Password Grant Type

- When creating an Okta App Integration, choose type `OIDC` -> `Native Application`
  - Add Grant Type `Resource Owner Password`
  - Ensure client authentication method is `Client secret`
- Create an Okta user to sign into, noting the `Username` and `Password`
- Create a Snowflake user to correspond to your newly created Okta client credentials
  - _Ensure the user's `Login Name` matches your Okta user's `Username` (likely an email)_
  - Ensure the user has been granted your DataHub role
- When running ingestion, provide the required `oauth_config` fields,
  including `client_id` and `client_secret`, plus your Okta user's `Username` and `Password`
  - Note: the `username` and `password` config options are not nested under `oauth_config`


### Install the Plugin
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

    # (Optional) Ingest INTERNAL marketplace (private data sharing) listings as Data Products
    # NOTE: This is for INTERNAL marketplace only, not the public Snowflake Data Marketplace
    # marketplace:
    #   enabled: true
    #   marketplace_mode: "consumer"  # Options: "consumer" (default), "provider", "both"
    #   internal_marketplace_listing_pattern:
    #     allow: [".*"]
    #   internal_marketplace_owner_patterns:
    #     "^Customer.*": ["data-team"]
    #     "^Finance.*": ["finance-team"]
    #   fetch_internal_marketplace_listing_details: false
    #   marketplace_properties_as_structured_properties: false
    #   # Time window for marketplace usage statistics (optional)
    #   # start_time: "-7 days"  # Default: -1 day
    #   # end_time: "now"
    #   # bucket_duration: "DAY"  # Options: "DAY", "HOUR"
    
    # (Optional) Map data products to domains using purchased database names
    # domain:
    #   "urn:li:domain:finance":
    #     allow: ["^FINANCE_.*", "^FIN_.*"]
    #   "Marketing":
    #     allow: ["^MARKETING_.*", "^MKT_.*"]
    
    # (REQUIRED for marketplace) Link imported databases to their source shares/listings
    # Without this, Data Products will be created but won't have any associated datasets
    # Run: SHOW SHARES; and SELECT DATABASE_NAME FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASES WHERE TYPE='IMPORTED DATABASE';
    # shares:
    #   <SHARE_NAME>:
    #     database: "<SOURCE_DATABASE>"  # Database in the share
    #     platform_instance: null
    #     consumers:
    #       - database: "<IMPORTED_DATABASE>"  # Your purchased/imported database
    #         platform_instance: null
    #         # (Optional) Explicit marketplace listing mapping for precise linking
    #         # listing_global_name: "ACME.DATA.LISTING"  # From: SHOW AVAILABLE LISTINGS

# Default sink is datahub-rest and doesn't need to be configured
# See https://docs.datahub.com/docs/metadata-ingestion/sink_docs/datahub for customization options

```

### Config Details

                
#### Options


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
| <div className="path-line"><span className="path-main">enable_stateful_lineage_ingestion</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable stateful lineage ingestion. This will store lineage window timestamps after successful lineage ingestion. and will not run lineage ingestion for same timestamps in subsequent run. NOTE: This only works with use_queries_v2=False (legacy extraction path). For queries v2, use enable_stateful_time_window instead. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">enable_stateful_profiling</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable stateful profiling. This will store profiling timestamps per dataset after successful profiling. and will not run profiling again in subsequent run if table has not been updated.  <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">enable_stateful_time_window</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable stateful time window tracking. This will store the time window after successful extraction and adjust the time window in subsequent runs to avoid reprocessing. NOTE: This is ONLY applicable when using queries v2 (use_queries_v2=True). This replaces enable_stateful_lineage_ingestion and enable_stateful_usage_ingestion for the queries v2 extraction path, since queries v2 extracts lineage, usage, operations, and queries together from a single audit log and uses a unified time window. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">enable_stateful_usage_ingestion</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable stateful lineage ingestion. This will store usage window timestamps after successful usage ingestion. and will not run usage ingestion for same timestamps in subsequent run. NOTE: This only works with use_queries_v2=False (legacy extraction path). For queries v2, use enable_stateful_time_window instead. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">end_time</span></div> <div className="type-name-line"><span className="type-name">string(date-time)</span></div> | Latest date of lineage/usage to consider. Default: Current time in UTC  |
| <div className="path-line"><span className="path-main">exclude_dynamic_tables</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, dynamic tables will be excluded from ingestion. Use this to speed up ingestion if you don't need dynamic tables in DataHub. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">extract_tags</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "with_lineage", "without_lineage", "skip"  |
| <div className="path-line"><span className="path-main">extract_tags_as_structured_properties</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled along with `extract_tags`, extracts snowflake's key-value tags as DataHub structured properties instead of DataHub tags. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">fetch_views_from_information_schema</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, uses information_schema.views to fetch view definitions instead of SHOW VIEWS command. This alternative method can be more reliable for databases with large numbers of views (> 10K views), as the SHOW VIEWS approach has proven unreliable and can lead to missing views in such scenarios. However, this method requires OWNERSHIP privileges on views to retrieve their definitions. For views without ownership permissions (where VIEW_DEFINITION is null/empty), the system will automatically fall back to using batched SHOW VIEWS queries to populate the missing definitions. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">format_sql_queries</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to format sql queries <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">ignore_start_time_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> |  <div className="default-line ">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_assertion_results</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to ingest assertion run results for assertions [created using DataHub assertions CLI](https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/docs/assertions/snowflake/snowflake_dmfs) in Snowflake. Also required for external DMF ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_column_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Populates table->table and view->table column lineage. Requires appropriate grants given to the role and the Snowflake Enterprise Edition or above. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_external_url</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to populate Snowsight url for Snowflake Objects <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_externally_managed_dmfs</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Ingest user-created Snowflake DMFs (not created via DataHub) as external assertions. Requires `include_assertion_results: true`. When enabled, all DMFs (not just datahub__* prefixed) will be ingested with their execution results. IMPORTANT: External DMFs must return 1 for SUCCESS and 0 for FAILURE. DataHub interprets VALUE=1 as passed, VALUE=0 as failed. See [Snowflake DMF Assertions](https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/docs/assertions/snowflake/snowflake_dmfs) for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_foreign_keys</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, populates the snowflake foreign keys. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_operational_stats</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to display operational stats. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_pipes</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, Snowflake Snowpipe objects will be ingested as DataJobs with COPY INTO lineage. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_primary_keys</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, populates the snowflake primary keys. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_procedures</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, procedures will be ingested as pipelines/tasks. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_queries</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, generate query entities associated with lineage edges. Only applicable if `use_queries_v2` is enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_query_usage_statistics</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, generate query popularity statistics. Only applicable if `use_queries_v2` is enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_read_operational_stats</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to report read operational stats. Experimental. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_stages</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, Snowflake Stages will be ingested as containers with associated metadata. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_streamlits</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, Streamlit apps will be ingested as dashboards. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_streams</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, streams will be ingested as separate entities from tables/views. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_table_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, populates the snowflake table-to-table and s3-to-snowflake table lineage. Requires appropriate grants given to the role and Snowflake Enterprise Edition or above. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_table_location_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If the source supports it, include table lineage to the underlying storage location. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_tables</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether tables should be ingested. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_tasks</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, Snowflake Tasks will be ingested as DataJobs with DAG dependencies and SQL lineage. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
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
| <div className="path-line"><span className="path-main">private_key</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Private key in a form of '-----BEGIN PRIVATE KEY-----\nprivate-key\n-----END PRIVATE KEY-----\n' if using key pair authentication. Encrypted version of private key will be in a form of '-----BEGIN ENCRYPTED PRIVATE KEY-----\nencrypted-private-key\n-----END ENCRYPTED PRIVATE KEY-----\n' See: https://docs.snowflake.com/en/user-guide/key-pair-auth.html <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">private_key_password</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Password for your private key. Required if using key pair authentication with encrypted private key. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">private_key_path</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The path to the private key if using key pair authentication. Ignored if `private_key` is set. See: https://docs.snowflake.com/en/user-guide/key-pair-auth.html <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">push_down_database_pattern_access_history</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, pushes down database pattern filtering to the access_history table for improved performance. This filters on the accessed objects in access_history. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">push_down_metadata_patterns</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, pushes down database_pattern, schema_pattern, table_pattern, and view_pattern filtering to Snowflake information_schema metadata queries using the RLIKE operator for improved performance. This applies only to metadata extraction queries (information_schema.databases, schemata, tables, views) — NOT to lineage/usage queries (for those, see push_down_database_pattern_access_history). NOTE: view_pattern pushdown only works when fetch_views_from_information_schema is also enabled. With the default SHOW VIEWS, view_pattern filtering falls back to Python re.match(). IMPORTANT: Snowflake RLIKE requires FULL STRING match, unlike Python re.match() which matches prefixes. For prefix matching use 'PATTERN.*', for suffix use '.*PATTERN$', for contains use '.*PATTERN.*'. If the composed filter would exceed Snowflake's per-query size limit, that filter is automatically skipped and applied client-side instead (slower). See the [Metadata Pattern Pushdown](#metadata-pattern-pushdown) section for detailed usage and examples, and the [Snowflake RLIKE documentation](https://docs.snowflake.com/en/sql-reference/functions/rlike) for regex syntax details. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">query_dedup_strategy</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "STANDARD", "NONE"  |
| <div className="path-line"><span className="path-main">role</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Snowflake role. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">snowflake_domain</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Snowflake domain. Use 'snowflakecomputing.com' for most regions or 'snowflakecomputing.cn' for China (cn-northwest-1) region. <div className="default-line default-line-with-docs">Default: <span className="default-value">snowflakecomputing.com</span></div> |
| <div className="path-line"><span className="path-main">snowsight_base_url</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Override for the Snowsight base URL used when generating external URLs for Snowflake assets. Set this when Snowsight is only reachable via private link (for example `https://app.<region>.privatelink.snowflakecomputing.com/` or `https://app-<org>-<account>.privatelink.snowflakecomputing.com/`). If unset, defaults to the public `app.snowflake.com` URL. The value can be obtained by running `SELECT SYSTEM$GET_PRIVATELINK_CONFIG()` in Snowflake as ACCOUNTADMIN. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">start_time</span></div> <div className="type-name-line"><span className="type-name">string(date-time)</span></div> | Earliest date of lineage/usage to consider. Default: Last full day in UTC (or hour, depending on `bucket_duration`). You can also specify relative time with respect to end_time such as '-7 days' Or '-7d'. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">structured_properties_write_mode</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "upsert", "patch"  |
| <div className="path-line"><span className="path-main">token</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | OAuth token from external identity provider. Not recommended for most use cases because it will not be able to refresh once expired. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
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
| <div className="path-line"><span className="path-main">marketplace</span></div> <div className="type-name-line"><span className="type-name">SnowflakeMarketplaceConfig</span></div> | Configuration for Snowflake Internal Marketplace (Private Data Sharing). <br />  <br /> IMPORTANT: This is for the INTERNAL Snowflake Marketplace where organizations privately share <br /> data within their account using Data Exchange. This is NOT for the public Snowflake Marketplace <br /> (Snowflake Data Marketplace) where external providers publicly list datasets. <br />  <br /> Use this when you want to track: <br /> - Internal marketplace listings (from SHOW AVAILABLE LISTINGS IS_ORGANIZATION = TRUE) <br /> - Databases purchased/imported from internal listings (IMPORTED DATABASE type - consumer mode) <br /> - Databases you're sharing via OUTBOUND shares (provider mode) <br /> - Usage of internal marketplace data products <br />  <br /> The usage time window and bucket duration come from the parent connector's <br /> ``start_time`` / ``end_time`` / ``bucket_duration`` (and from the same <br /> ``RedundantUsageRunSkipHandler`` as the main usage extractor when stateful <br /> usage ingestion is enabled), so marketplace usage follows the connector's <br /> overall schedule.  |
| <div className="path-line"><span className="path-prefix">marketplace.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to ingest Snowflake INTERNAL marketplace (private data exchange) listings as Data Products. When enabled, also ingests databases and usage statistics based on the marketplace_mode setting. NOTE: This is for INTERNAL marketplace only (IS_ORGANIZATION = TRUE), not the public Snowflake Data Marketplace. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">marketplace.</span><span className="path-main">fetch_internal_marketplace_listing_details</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, fetches additional details for each INTERNAL marketplace listing via DESCRIBE AVAILABLE LISTING. WARNING: This executes one additional query per listing and may impact performance for many listings. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">marketplace.</span><span className="path-main">listing_to_share_overrides</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div> |   |
| <div className="path-line"><span className="path-prefix">marketplace.</span><span className="path-main">marketplace_mode</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "consumer", "provider", "both"  |
| <div className="path-line"><span className="path-prefix">marketplace.</span><span className="path-main">marketplace_properties_as_structured_properties</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, ingests INTERNAL marketplace custom properties (provider, category, listing_created_on, etc.) as DataHub structured properties instead of simple custom properties. This makes marketplace metadata searchable and filterable in the DataHub UI. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">marketplace.</span><span className="path-main">organization_to_domain</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div> |   |
| <div className="path-line"><span className="path-prefix">marketplace.</span><span className="path-main">internal_marketplace_listing_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">marketplace.internal_marketplace_listing_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">marketplace.</span><span className="path-main">internal_marketplace_owner_patterns</span></div> <div className="type-name-line"><span className="type-name">map(str,array)</span></div> |   |
| <div className="path-line"><span className="path-prefix">marketplace.internal_marketplace_owner_patterns.`key`.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">marketplace.</span><span className="path-main">listing_to_schemas_overrides</span></div> <div className="type-name-line"><span className="type-name">map(str,array)</span></div> |   |
| <div className="path-line"><span className="path-prefix">marketplace.listing_to_schemas_overrides.`key`.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">oauth_config</span></div> <div className="type-name-line"><span className="type-name">One of OAuthConfiguration, null</span></div> | oauth configuration - https://docs.snowflake.com/en/user-guide/python-connector-example.html#connecting-with-oauth <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">oauth_config.</span><span className="path-main">authority_url</span>&nbsp;<abbr title="Required if oauth_config is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Authority url of your identity provider  |
| <div className="path-line"><span className="path-prefix">oauth_config.</span><span className="path-main">client_id</span>&nbsp;<abbr title="Required if oauth_config is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | client id of your registered application  |
| <div className="path-line"><span className="path-prefix">oauth_config.</span><span className="path-main">provider</span>&nbsp;<abbr title="Required if oauth_config is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "microsoft", "okta"  |
| <div className="path-line"><span className="path-prefix">oauth_config.</span><span className="path-main">scopes</span>&nbsp;<abbr title="Required if oauth_config is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">array</span></div> | scopes required to connect to snowflake  |
| <div className="path-line"><span className="path-prefix">oauth_config.scopes.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">oauth_config.</span><span className="path-main">client_secret</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | client secret of the application if use_certificate = false <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">oauth_config.</span><span className="path-main">encoded_oauth_private_key</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | base64 encoded private key content if use_certificate = true <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">oauth_config.</span><span className="path-main">encoded_oauth_public_key</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | base64 encoded certificate content if use_certificate = true <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">oauth_config.</span><span className="path-main">use_certificate</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Do you want to use certificate and private key to authenticate using oauth <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">pipe_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">pipe_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
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
| <div className="path-line"><span className="path-main">semantic_view_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">semantic_view_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">semantic_views</span></div> <div className="type-name-line"><span className="type-name">SemanticViewsConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">semantic_views.</span><span className="path-main">column_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, column-level lineage will be generated for semantic views, mapping dimensions, facts, and metrics to their source columns in base tables. Only applicable when enabled is True. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">semantic_views.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, semantic views will be ingested as datasets. Note: Semantic views require Snowflake Enterprise Edition or above, as they are part of the Cortex Analyst feature set. Set this to True only if you have Enterprise Edition or above. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">semantic_views.</span><span className="path-main">include_queries</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, generate query entities for queries against semantic views. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">semantic_views.</span><span className="path-main">include_usage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | If enabled, usage statistics will be extracted for semantic views. This scans QUERY_HISTORY which can be slow on accounts with high query volume. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">semantic_views.</span><span className="path-main">max_queries_per_view</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of query entities to emit per semantic view. Only applicable when include_queries is True. <div className="default-line default-line-with-docs">Default: <span className="default-value">100</span></div> |
| <div className="path-line"><span className="path-main">shares</span></div> <div className="type-name-line"><span className="type-name">One of SnowflakeShareConfig, null</span></div> | Required if current account owns or consumes snowflake share.If specified, connector creates lineage and siblings relationship between current account's database tables and consumer/producer account's database tables. Map of share name -> details of share. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">shares.`key`.</span><span className="path-main">database</span>&nbsp;<abbr title="Required if shares is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Database from which share is created.  |
| <div className="path-line"><span className="path-prefix">shares.`key`.</span><span className="path-main">consumers</span>&nbsp;<abbr title="Required if shares is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">array</span></div> | List of databases created in consumer accounts.  |
| <div className="path-line"><span className="path-prefix">shares.`key`.consumers.</span><span className="path-main">DatabaseId</span></div> <div className="type-name-line"><span className="type-name">DatabaseId</span></div> |   |
| <div className="path-line"><span className="path-prefix">shares.`key`.consumers.DatabaseId.</span><span className="path-main">database</span>&nbsp;<abbr title="Required if DatabaseId is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-prefix">shares.`key`.consumers.DatabaseId.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> |  <div className="default-line ">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">shares.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Platform instance for snowflake account in which share is created. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">stage_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">stage_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">stream_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">stream_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">streamlit_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">streamlit_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">structured_property_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">structured_property_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">table_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">table_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">table_types</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Set of Snowflake TABLE_TYPE values to include in ingestion. Currently Supported values: 'BASE TABLE', 'EXTERNAL TABLE'. Remove 'EXTERNAL TABLE' to exclude external tables from ingestion. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;BASE TABLE&#x27;, &#x27;EXTERNAL TABLE&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">table_types.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">tag_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">tag_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">task_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">task_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">temporary_tables_pattern</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | [Advanced] Regex patterns for temporary tables to filter in lineage ingestion. Specify regex to match the entire table name in database.schema.table format. Defaults are to set in such a way to ignore the temporary staging tables created by known ETL tools. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;.&#42;\\.FIVETRAN&#95;.&#42;&#95;STAGING\\..&#42;&#x27;, &#x27;.&#42;&#95;&#95;DBT&#95;TMP$&#x27;, ...</span></div> |
| <div className="path-line"><span className="path-prefix">temporary_tables_pattern.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">user_email_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">user_email_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">view_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">view_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">classification</span></div> <div className="type-name-line"><span className="type-name">ClassificationConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether classification should be used to auto-detect glossary terms <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">info_type_to_term</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div> |   |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">max_workers</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of worker processes to use for classification. Set to 1 to disable. <div className="default-line default-line-with-docs">Default: <span className="default-value">4</span></div> |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">sample_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of sample values used for classification. <div className="default-line default-line-with-docs">Default: <span className="default-value">100</span></div> |
| <div className="path-line"><span className="path-prefix">classification.</span><span className="path-main">classifiers</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Classifiers to use to auto-detect glossary terms. If more than one classifier, infotype predictions from the classifier defined later in sequence take precedance. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#123;&#x27;type&#x27;: &#x27;datahub&#x27;, &#x27;config&#x27;: None&#125;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">classification.classifiers.</span><span className="path-main">DynamicTypedClassifierConfig</span></div> <div className="type-name-line"><span className="type-name">DynamicTypedClassifierConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">classification.classifiers.DynamicTypedClassifierConfig.</span><span className="path-main">type</span>&nbsp;<abbr title="Required if DynamicTypedClassifierConfig is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | The type of the classifier to use. The built-in `datahub` classifier has been removed; register a custom classifier and reference its type here.  |
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
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">max_workers</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of worker threads to use for profiling. Set to 1 to disable. <div className="default-line default-line-with-docs">Default: <span className="default-value">20</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">method</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "ge", "sqlalchemy" <div className="default-line default-line-with-docs">Default: <span className="default-value">sqlalchemy</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">nested_field_max_depth</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum recursion depth when flattening nested JSON structures during profiling. Lower values prevent recursion errors but may truncate deeply nested data. Applies to connectors that process dynamic JSON content (e.g., Kafka, MongoDB, Elasticsearch). <div className="default-line default-line-with-docs">Default: <span className="default-value">10</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">offset</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Offset in documents to profile. By default, uses no offset. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">partition_datetime</span></div> <div className="type-name-line"><span className="type-name">One of string(date-time), null</span></div> | If specified, profile only the partition which matches this datetime. If not specified, profile the latest partition. Only Bigquery supports this. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">partition_profiling_enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile partitioned tables. Only BigQuery and Aws Athena supports this. If enabled, latest partition data is used for profiling. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_external_tables</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile external tables. Only Snowflake and Redshift supports this. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_if_updated_since_days</span></div> <div className="type-name-line"><span className="type-name">One of number, null</span></div> | Profile table only if it has been updated since these many number of days. If set to `null`, no constraint of last modified time for tables to profile. Supported in `Snowflake`, `BigQuery`, and `Dremio`. Note: for Dremio this compares against DataHub's last-profiled timestamp (Dremio exposes no table modification time), so it controls profile frequency rather than reacting to upstream change. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_nested_fields</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile complex types like structs, arrays and maps.  <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_level_only</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to perform profiling at table-level only, or include column-level profiling as well. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_row_count_estimate_only</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Use an approximate query for row count. This will be much faster but slightly less accurate. Only supported for Postgres and MySQL.  <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_row_limit</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Profile tables only if their row count is less than specified count. If set to `null`, no limit on the row count of tables to profile. Supported only in `Snowflake`, `BigQuery`. Supported for `Oracle` based on gathered stats. <div className="default-line default-line-with-docs">Default: <span className="default-value">5000000</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">profile_table_size_limit</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Profile tables only if their size is less than specified GBs. If set to `null`, no limit on the size of tables to profile. Supported in `Snowflake`, `BigQuery`, `Databricks`, `Oracle`, and `Teradata`. `Oracle` uses calculated size from gathered stats. `Teradata` uses DBC space accounting. <div className="default-line default-line-with-docs">Default: <span className="default-value">5</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">query_combiner_enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | *This feature is still experimental and can be disabled if it causes issues.* Reduces the total number of queries issued and speeds up profiling by dynamically combining SQL queries where possible. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">report_dropped_profiles</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to report datasets or dataset columns which were not profiled. Set to `True` for debugging purposes. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">sample_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of rows to be sampled from table for column level profiling.Applicable only if `use_sampling` is set to True. <div className="default-line default-line-with-docs">Default: <span className="default-value">10000</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">turn_off_expensive_profiling_metrics</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to turn off expensive profiling or not. This turns off profiling for quantiles, distinct_value_frequencies, histogram & sample_values. This also limits maximum number of fields being profiled to 10. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">use_sampling</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to profile column level stats on sample of table. Only BigQuery and Snowflake support this. If enabled, profiling is done on rows sampled from table. Sampling is not done for smaller tables.  <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">operation_config</span></div> <div className="type-name-line"><span className="type-name">OperationConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">lower_freq_profile_enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to do profiling at lower freq or not. This does not do any scheduling just adds additional checks to when not to run profiling. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">profile_date_of_month</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Number between 1 to 31 for date of month (both inclusive). If not specified, defaults to Nothing and this field does not take affect. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.operation_config.</span><span className="path-main">profile_day_of_week</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Number between 0 to 6 for day of week (both inclusive). 0 is Monday and 6 is Sunday. If not specified, defaults to Nothing and this field does not take affect. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.</span><span className="path-main">tags_to_ignore_sampling</span></div> <div className="type-name-line"><span className="type-name">One of array, null</span></div> | Fixed list of tags to ignore sampling. Each entry may be a full tag URN (e.g. `urn:li:tag:my_tag`) or just the tag name (e.g. `my_tag`). If not specified, tables will be sampled based on `use_sampling`. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">profiling.tags_to_ignore_sampling.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> |  <div className="default-line ">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">fail_safe_threshold</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'. <div className="default-line default-line-with-docs">Default: <span className="default-value">75.0</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |

</div>




#### Schema


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
          "default": 4,
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
          "description": "The type of the classifier to use. The built-in `datahub` classifier has been removed; register a custom classifier and reference its type here.",
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
        "method": {
          "default": "sqlalchemy",
          "description": "Profiling method to use. `sqlalchemy` (default) runs profiling queries directly against your source's existing SQLAlchemy connection. `ge` selects the legacy Great Expectations profiler, which is deprecated and requires `pip install 'acryl-datahub[profiling-ge]'`.",
          "enum": [
            "ge",
            "sqlalchemy"
          ],
          "title": "Method",
          "type": "string"
        },
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
          "default": 20,
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
          "description": "Profile table only if it has been updated since these many number of days. If set to `null`, no constraint of last modified time for tables to profile. Supported in `Snowflake`, `BigQuery`, and `Dremio`. Note: for Dremio this compares against DataHub's last-profiled timestamp (Dremio exposes no table modification time), so it controls profile frequency rather than reacting to upstream change.",
          "schema_extra": {
            "supported_sources": [
              "snowflake",
              "bigquery",
              "dremio"
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
          "description": "Profile tables only if their size is less than specified GBs. If set to `null`, no limit on the size of tables to profile. Supported in `Snowflake`, `BigQuery`, `Databricks`, `Oracle`, and `Teradata`. `Oracle` uses calculated size from gathered stats. `Teradata` uses DBC space accounting.",
          "schema_extra": {
            "supported_sources": [
              "snowflake",
              "bigquery",
              "unity-catalog",
              "oracle",
              "teradata"
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
          "description": "Fixed list of tags to ignore sampling. Each entry may be a full tag URN (e.g. `urn:li:tag:my_tag`) or just the tag name (e.g. `my_tag`). If not specified, tables will be sampled based on `use_sampling`.",
          "title": "Tags To Ignore Sampling"
        },
        "profile_nested_fields": {
          "default": false,
          "description": "Whether to profile complex types like structs, arrays and maps. ",
          "title": "Profile Nested Fields",
          "type": "boolean"
        },
        "nested_field_max_depth": {
          "default": 10,
          "description": "Maximum recursion depth when flattening nested JSON structures during profiling. Lower values prevent recursion errors but may truncate deeply nested data. Applies to connectors that process dynamic JSON content (e.g., Kafka, MongoDB, Elasticsearch).",
          "exclusiveMinimum": 0,
          "title": "Nested Field Max Depth",
          "type": "integer"
        }
      },
      "title": "GEProfilingConfig",
      "type": "object"
    },
    "MarketplaceMode": {
      "enum": [
        "consumer",
        "provider",
        "both"
      ],
      "title": "MarketplaceMode",
      "type": "string"
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
              "format": "password",
              "type": "string",
              "writeOnly": true
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
    "SemanticViewsConfig": {
      "additionalProperties": false,
      "properties": {
        "enabled": {
          "default": false,
          "description": "If enabled, semantic views will be ingested as datasets. Note: Semantic views require Snowflake Enterprise Edition or above, as they are part of the Cortex Analyst feature set. Set this to True only if you have Enterprise Edition or above.",
          "title": "Enabled",
          "type": "boolean"
        },
        "column_lineage": {
          "default": false,
          "description": "If enabled, column-level lineage will be generated for semantic views, mapping dimensions, facts, and metrics to their source columns in base tables. Only applicable when enabled is True.",
          "title": "Column Lineage",
          "type": "boolean"
        },
        "include_usage": {
          "default": false,
          "description": "If enabled, usage statistics will be extracted for semantic views. This scans QUERY_HISTORY which can be slow on accounts with high query volume.",
          "title": "Include Usage",
          "type": "boolean"
        },
        "include_queries": {
          "default": false,
          "description": "If enabled, generate query entities for queries against semantic views.",
          "title": "Include Queries",
          "type": "boolean"
        },
        "max_queries_per_view": {
          "default": 100,
          "description": "Maximum number of query entities to emit per semantic view. Only applicable when include_queries is True.",
          "maximum": 10000,
          "minimum": 1,
          "title": "Max Queries Per View",
          "type": "integer"
        }
      },
      "title": "SemanticViewsConfig",
      "type": "object"
    },
    "SnowflakeEdition": {
      "enum": [
        "Standard",
        "Enterprise or above"
      ],
      "title": "SnowflakeEdition",
      "type": "string"
    },
    "SnowflakeMarketplaceConfig": {
      "additionalProperties": false,
      "description": "Configuration for Snowflake Internal Marketplace (Private Data Sharing).\n\nIMPORTANT: This is for the INTERNAL Snowflake Marketplace where organizations privately share\ndata within their account using Data Exchange. This is NOT for the public Snowflake Marketplace\n(Snowflake Data Marketplace) where external providers publicly list datasets.\n\nUse this when you want to track:\n- Internal marketplace listings (from SHOW AVAILABLE LISTINGS IS_ORGANIZATION = TRUE)\n- Databases purchased/imported from internal listings (IMPORTED DATABASE type - consumer mode)\n- Databases you're sharing via OUTBOUND shares (provider mode)\n- Usage of internal marketplace data products\n\nThe usage time window and bucket duration come from the parent connector's\n``start_time`` / ``end_time`` / ``bucket_duration`` (and from the same\n``RedundantUsageRunSkipHandler`` as the main usage extractor when stateful\nusage ingestion is enabled), so marketplace usage follows the connector's\noverall schedule.",
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether to ingest Snowflake INTERNAL marketplace (private data exchange) listings as Data Products. When enabled, also ingests databases and usage statistics based on the marketplace_mode setting. NOTE: This is for INTERNAL marketplace only (IS_ORGANIZATION = TRUE), not the public Snowflake Data Marketplace.",
          "title": "Enabled",
          "type": "boolean"
        },
        "marketplace_mode": {
          "$ref": "#/$defs/MarketplaceMode",
          "default": "consumer",
          "description": "Mode for marketplace ingestion: 'consumer' (default) - Track purchased/imported databases (IMPORTED DATABASE type), 'provider' - Track databases you're sharing via OUTBOUND shares and marketplace listings, 'both' - Track both consumer and provider perspectives. Consumer mode requires shares config to link imported databases to listings. Provider mode works with OUTBOUND shares without requiring imported databases. IMPORTANT: For 'provider' or 'both' modes, you MUST grant 'imported privileges on database snowflake' to the USER (not just the role), as share access is granted at the user level in Snowflake."
        },
        "listing_to_share_overrides": {
          "additionalProperties": {
            "type": "string"
          },
          "default": {},
          "description": "Map of `listing_global_name` -> share name (top-level key in `shares`) to explicitly link a marketplace listing to a share. Useful when `SHOW SHARES` doesn't return `listing_global_name` or when automatic name-based matching fails.",
          "title": "Listing To Share Overrides",
          "type": "object"
        },
        "listing_to_schemas_overrides": {
          "additionalProperties": {
            "items": {
              "type": "string"
            },
            "type": "array"
          },
          "default": {},
          "description": "Map of `listing_global_name` -> list of schema names to enumerate when falling back to database-level asset discovery. Used in provider mode when `DESC SHARE` is not permitted (Snowflake requires share ownership for that command). Without this override the fallback enumerates all schemas in the source database, which may include schemas not exposed by the share. Example: `{GZSTZGQTPEW: [TPCH]}`.",
          "title": "Listing To Schemas Overrides",
          "type": "object"
        },
        "internal_marketplace_listing_pattern": {
          "$ref": "#/$defs/AllowDenyPattern",
          "default": {
            "allow": [
              ".*"
            ],
            "deny": [],
            "ignoreCase": true
          },
          "description": "Regex patterns for INTERNAL marketplace listings to include in ingestion"
        },
        "internal_marketplace_owner_patterns": {
          "additionalProperties": {
            "items": {
              "type": "string"
            },
            "type": "array"
          },
          "default": {},
          "description": "Map regex patterns (matched against INTERNAL listing title or provider) to owner identifiers. Owners can be usernames, group names, or full URNs. Example: {'^Finance.*': ['finance-team'], '^.*Analytics.*': ['analytics-lead', 'urn:li:corpGroup:data']}",
          "title": "Internal Marketplace Owner Patterns",
          "type": "object"
        },
        "fetch_internal_marketplace_listing_details": {
          "default": false,
          "description": "If enabled, fetches additional details for each INTERNAL marketplace listing via DESCRIBE AVAILABLE LISTING. WARNING: This executes one additional query per listing and may impact performance for many listings.",
          "title": "Fetch Internal Marketplace Listing Details",
          "type": "boolean"
        },
        "marketplace_properties_as_structured_properties": {
          "default": false,
          "description": "If enabled, ingests INTERNAL marketplace custom properties (provider, category, listing_created_on, etc.) as DataHub structured properties instead of simple custom properties. This makes marketplace metadata searchable and filterable in the DataHub UI.",
          "title": "Marketplace Properties As Structured Properties",
          "type": "boolean"
        },
        "organization_to_domain": {
          "additionalProperties": {
            "type": "string"
          },
          "default": {},
          "description": "Map of Snowflake ``ORGANIZATION_PROFILE_NAME`` to an existing DataHub domain (URN, GUID, or name resolvable via ``DomainRegistry``). Unmapped organizations get no domain; marketplace never auto-creates domain entities.",
          "title": "Organization To Domain",
          "type": "object"
        }
      },
      "title": "SnowflakeMarketplaceConfig",
      "type": "object"
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
    "StructuredPropertyWriteMode": {
      "description": "How `add_structured_properties_to_entity_wu` writes the aspect.\n\n`UPSERT` replaces the whole `structuredProperties` aspect each run (recipe is\nsource of truth). `PATCH` adds each property individually so user/UI/other-pipeline\nedits survive \u2014 at the cost of removals from the recipe no longer propagating;\nclean those up via the UI or API.",
      "enum": [
        "upsert",
        "patch"
      ],
      "title": "StructuredPropertyWriteMode",
      "type": "string"
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
        "max_workers": 4,
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
        "method": "sqlalchemy",
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
        "max_workers": 20,
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
        "profile_nested_fields": false,
        "nested_field_max_depth": 10
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
    "enable_stateful_time_window": {
      "default": false,
      "description": "Enable stateful time window tracking. This will store the time window after successful extraction and adjust the time window in subsequent runs to avoid reprocessing. NOTE: This is ONLY applicable when using queries v2 (use_queries_v2=True). This replaces enable_stateful_lineage_ingestion and enable_stateful_usage_ingestion for the queries v2 extraction path, since queries v2 extracts lineage, usage, operations, and queries together from a single audit log and uses a unified time window.",
      "title": "Enable Stateful Time Window",
      "type": "boolean"
    },
    "enable_stateful_usage_ingestion": {
      "default": true,
      "description": "Enable stateful lineage ingestion. This will store usage window timestamps after successful usage ingestion. and will not run usage ingestion for same timestamps in subsequent run. NOTE: This only works with use_queries_v2=False (legacy extraction path). For queries v2, use enable_stateful_time_window instead.",
      "title": "Enable Stateful Usage Ingestion",
      "type": "boolean"
    },
    "enable_stateful_lineage_ingestion": {
      "default": true,
      "description": "Enable stateful lineage ingestion. This will store lineage window timestamps after successful lineage ingestion. and will not run lineage ingestion for same timestamps in subsequent run. NOTE: This only works with use_queries_v2=False (legacy extraction path). For queries v2, use enable_stateful_time_window instead.",
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
          "format": "password",
          "type": "string",
          "writeOnly": true
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
          "format": "password",
          "type": "string",
          "writeOnly": true
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
    "streamlit_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for Streamlit app to filter in ingestion. Specify regex to match the entire Streamlit app name in database.schema.streamlit format. e.g. to match all Streamlit apps starting with dashboard in Analytics database and public schema, use the regex 'Analytics.public.dashboard.*'"
    },
    "semantic_view_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for semantic views to filter in ingestion. Specify regex to match the entire semantic view name in database.schema.semantic_view format. e.g. to match all semantic views starting with sales in Analytics database and public schema, use the regex 'Analytics.public.sales.*'"
    },
    "stage_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for stages to filter in ingestion. Specify regex to match the entire stage name in database.schema.stage format."
    },
    "task_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for tasks to filter in ingestion. Specify regex to match the entire task name in database.schema.task format."
    },
    "pipe_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns for pipes to filter in ingestion. Specify regex to match the entire pipe name in database.schema.pipe format."
    },
    "match_fully_qualified_names": {
      "default": false,
      "description": "Whether `schema_pattern` is matched against fully qualified schema name `<catalog>.<schema>`.",
      "title": "Match Fully Qualified Names",
      "type": "boolean"
    },
    "push_down_metadata_patterns": {
      "default": false,
      "description": "If enabled, pushes down database_pattern, schema_pattern, table_pattern, and view_pattern filtering to Snowflake information_schema metadata queries using the RLIKE operator for improved performance. This applies only to metadata extraction queries (information_schema.databases, schemata, tables, views) \u2014 NOT to lineage/usage queries (for those, see push_down_database_pattern_access_history). NOTE: view_pattern pushdown only works when fetch_views_from_information_schema is also enabled. With the default SHOW VIEWS, view_pattern filtering falls back to Python re.match(). IMPORTANT: Snowflake RLIKE requires FULL STRING match, unlike Python re.match() which matches prefixes. For prefix matching use 'PATTERN.*', for suffix use '.*PATTERN$', for contains use '.*PATTERN.*'. If the composed filter would exceed Snowflake's per-query size limit, that filter is automatically skipped and applied client-side instead (slower). See the [Metadata Pattern Pushdown](#metadata-pattern-pushdown) section for detailed usage and examples, and the [Snowflake RLIKE documentation](https://docs.snowflake.com/en/sql-reference/functions/rlike) for regex syntax details.",
      "title": "Push Down Metadata Patterns",
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
    "structured_properties_write_mode": {
      "$ref": "#/$defs/StructuredPropertyWriteMode",
      "default": "upsert",
      "description": "How to write structured properties extracted from Snowflake tags. `upsert` (default) replaces the aspect each run \u2014 recipe is source of truth. `patch` adds each property individually so user/UI edits survive, but properties removed from the recipe no longer propagate to DataHub (clean those up via the UI or API)."
    },
    "include_external_url": {
      "default": true,
      "description": "Whether to populate Snowsight url for Snowflake Objects",
      "title": "Include External Url",
      "type": "boolean"
    },
    "snowsight_base_url": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Override for the Snowsight base URL used when generating external URLs for Snowflake assets. Set this when Snowsight is only reachable via private link (for example `https://app.<region>.privatelink.snowflakecomputing.com/` or `https://app-<org>-<account>.privatelink.snowflakecomputing.com/`). If unset, defaults to the public `app.snowflake.com` URL. The value can be obtained by running `SELECT SYSTEM$GET_PRIVATELINK_CONFIG()` in Snowflake as ACCOUNTADMIN.",
      "title": "Snowsight Base Url"
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
    "table_types": {
      "default": [
        "BASE TABLE",
        "EXTERNAL TABLE"
      ],
      "description": "Set of Snowflake TABLE_TYPE values to include in ingestion. Currently Supported values: 'BASE TABLE', 'EXTERNAL TABLE'. Remove 'EXTERNAL TABLE' to exclude external tables from ingestion.",
      "items": {
        "type": "string"
      },
      "title": "Table Types",
      "type": "array",
      "uniqueItems": true
    },
    "exclude_dynamic_tables": {
      "default": false,
      "description": "If enabled, dynamic tables will be excluded from ingestion. Use this to speed up ingestion if you don't need dynamic tables in DataHub.",
      "title": "Exclude Dynamic Tables",
      "type": "boolean"
    },
    "include_procedures": {
      "default": true,
      "description": "If enabled, procedures will be ingested as pipelines/tasks.",
      "title": "Include Procedures",
      "type": "boolean"
    },
    "include_streamlits": {
      "default": false,
      "description": "If enabled, Streamlit apps will be ingested as dashboards.",
      "title": "Include Streamlits",
      "type": "boolean"
    },
    "semantic_views": {
      "$ref": "#/$defs/SemanticViewsConfig",
      "description": "Configuration for semantic views ingestion."
    },
    "include_stages": {
      "default": false,
      "description": "If enabled, Snowflake Stages will be ingested as containers with associated metadata.",
      "title": "Include Stages",
      "type": "boolean"
    },
    "include_tasks": {
      "default": false,
      "description": "If enabled, Snowflake Tasks will be ingested as DataJobs with DAG dependencies and SQL lineage.",
      "title": "Include Tasks",
      "type": "boolean"
    },
    "include_pipes": {
      "default": false,
      "description": "If enabled, Snowflake Snowpipe objects will be ingested as DataJobs with COPY INTO lineage.",
      "title": "Include Pipes",
      "type": "boolean"
    },
    "marketplace": {
      "$ref": "#/$defs/SnowflakeMarketplaceConfig",
      "description": "Configuration for Snowflake Internal Marketplace (private data exchange) ingestion."
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
      "description": "Whether to ingest assertion run results for assertions [created using DataHub assertions CLI](https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/docs/assertions/snowflake/snowflake_dmfs) in Snowflake. Also required for external DMF ingestion.",
      "title": "Include Assertion Results",
      "type": "boolean"
    },
    "include_externally_managed_dmfs": {
      "default": false,
      "description": "Ingest user-created Snowflake DMFs (not created via DataHub) as external assertions. Requires `include_assertion_results: true`. When enabled, all DMFs (not just datahub__* prefixed) will be ingested with their execution results. IMPORTANT: External DMFs must return 1 for SUCCESS and 0 for FAILURE. DataHub interprets VALUE=1 as passed, VALUE=0 as failed. See [Snowflake DMF Assertions](https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/docs/assertions/snowflake/snowflake_dmfs) for details.",
      "title": "Include Externally Managed Dmfs",
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





### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Snowflake Shares

If you are using [Snowflake Shares](https://docs.snowflake.com/en/user-guide/data-sharing-provider) to share data across different Snowflake accounts, and you have set up DataHub recipes for ingesting metadata from all these accounts, you may end up having multiple similar dataset entities corresponding to virtual versions of the same table in different Snowflake accounts. The DataHub Snowflake connector can automatically link such tables together through Siblings and Lineage relationships if the user provides information necessary to establish the relationship using the `shares` configuration in the recipe.

**Note:** The `shares` configuration is also strongly recommended when using the `marketplace.enabled` feature to ingest Snowflake internal marketplace listings as Data Products. Without it, ingestion still produces the Data Products, but `_find_listing_for_purchase` cannot map purchased databases back to their listings, so the Data Products end up with no associated datasets (assets) and a `structured_reporter.warning` is emitted. See the [Internal Marketplace](#internal-marketplace) section below for details.

##### Example

- Snowflake account `account1` (ingested as platform_instance `instance1`) owns a database `db1`. A share `X` is created in `account1` that includes database `db1` along with schemas and tables inside it.
- Now, `X` is shared with Snowflake account `account2` (ingested as platform_instance `instance2`). A database `db1_from_X` is created from inbound share `X` in `account2`. In this case, all tables and views included in share `X` will also be present in `instance2.db1_from_X`.
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
- If share `X` is shared with more Snowflake accounts and a database is created from share `X` in those accounts, then additional entries need to be added to the `consumers` list for share `X`, one per Snowflake account. The same `shares` config can then be copied across recipes for all accounts.

#### Internal Marketplace

If you are using the Snowflake internal marketplace (private data sharing within your organization via Data Exchange) and want to ingest marketplace listings as DataHub Data Products, you can operate in two modes:

##### Mode 1: Consumer Mode (Default) - Track Purchased Listings

For organizations that **purchase/install** internal marketplace listings:

1. **Grant the required privileges** (already covered in the Prerequisites section above):

   ```sql
   -- Basic marketplace access
   grant imported privileges on database snowflake to role datahub_role;

   -- For SHOW SHARES to discover INBOUND shares
   use role accountadmin;
   grant import share on account to role datahub_role;
   ```

2. **Enable marketplace ingestion** in your recipe:

   ```yaml
   marketplace:
     enabled: true
     marketplace_mode: "consumer" # Default
     # Optional: Configure time window for usage statistics
     start_time: "-7 days" # Default: -1 day
     end_time: "now"
     bucket_duration: "DAY" # Options: "DAY", "HOUR"
   ```

   The `marketplace` configuration inherits from `BaseTimeWindowConfig`, allowing you to control the time window for extracting marketplace usage statistics. This follows the same pattern as other DataHub connectors.

3. **Configure the `shares` mapping** (strongly recommended for linking Data Products to their purchased databases):

   Snowflake does not expose a direct link between imported databases and the marketplace listings they came from. Without `shares`, ingestion still succeeds but each affected Data Product is emitted without assets and a `structured_reporter.warning` is logged.

   ```yaml
   shares:
     <SHARE_NAME>: # From: SHOW SHARES
       database: "<SOURCE_DATABASE>" # Source database in the share
       platform_instance: null
       consumers:
         - database: "<IMPORTED_DATABASE>" # Your purchased/imported database
           platform_instance: null
           # Optional but recommended: Explicit listing mapping
           listing_global_name: "PROVIDER.REGION.LISTING_NAME" # From: SHOW AVAILABLE LISTINGS
   ```

   **Explicit Listing Mapping (Recommended)**: Add `listing_global_name` to explicitly link your purchased database to its marketplace listing. This ensures accurate Data Product associations, especially when you have multiple listings with similar names. Without it, the connector will attempt to match listings by finding the source database name within the listing's global name or title (case-insensitive substring match).

   **To discover the correct values**, run these SQL commands in Snowflake:

   ```sql
   -- Find marketplace listings
   SHOW AVAILABLE LISTINGS IS_ORGANIZATION = TRUE;

   -- Find imported databases
   SELECT DATABASE_NAME, TYPE FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASES
   WHERE TYPE = 'IMPORTED DATABASE' AND DELETED IS NULL;

   -- Find shares and their mappings
   SHOW SHARES;
   ```

   **Without the `shares` configuration:**

   - Data Products will be created from marketplace listings
   - Owners and custom properties will be populated
   - Data Products will NOT have any associated datasets (assets)
   - Warning messages will be logged

   **With the `shares` configuration:**

   - Data Products will be created with all metadata
   - Purchased databases will be linked as Data Product assets
   - Tables from imported databases will show as part of the Data Product

##### Mode 2: Provider Mode - Track Published Listings

For organizations that **publish/share** internal marketplace listings:

1. **Grant the required privileges** (already covered in the Prerequisites section above):

   ```sql
   -- Basic marketplace access
   grant imported privileges on database snowflake to role datahub_role;

   -- For SHOW SHARES to discover OUTBOUND shares (provider mode)
   -- The role can ONLY see shares it owns or inherits
   use role securityadmin;
   grant role sysadmin to role datahub_role;

   -- OR use SYSADMIN/ACCOUNTADMIN directly in your recipe:
   -- role: SYSADMIN
   ```

   **Important Note**: In Snowflake, `SHOW SHARES` returns OUTBOUND shares based on ownership. A role can see:

   - Shares owned by the current role (e.g., if `datahub_role` created the share)
   - Shares owned by roles it inherits from (e.g., `datahub_role` inherits from `SYSADMIN`)
   - But NOT shares owned by other roles (e.g., shares owned by `ACCOUNTADMIN` that `datahub_role` doesn't inherit from)

   **Solutions:**

   - If your shares are owned by `ACCOUNTADMIN` or `SYSADMIN`: Grant `SYSADMIN` role to `datahub_role` (recommended)
   - If `datahub_role` creates the shares: No additional grant needed
   - Alternative: Use `SYSADMIN` or `ACCOUNTADMIN` directly in your ingestion recipe

   Without proper role hierarchy, provider mode cannot discover outbound shares owned by other roles, and tables won't be added as assets to Data Products.

2. **Enable marketplace ingestion in provider mode** in your recipe:

   ```yaml
   marketplace:
     enabled: true
     marketplace_mode: "provider"
     # Assign owners to your Data Products
     internal_marketplace_owner_patterns:
       "^Your Listing.*": ["data-team"]
     # Optional: restrict the database fallback to specific schemas per listing.
     # Required when DESC SHARE is not permitted (see troubleshooting below).
     listing_to_schemas_overrides:
       YOUR_LISTING_GLOBAL_NAME: [YOUR_SCHEMA]

   # Include your source databases being shared
   database_pattern:
     allow:
       - "YOUR_SOURCE_DATABASE"
   ```

3. **No `shares` configuration needed!**

   Provider mode automatically:

   - Discovers your OUTBOUND shares with marketplace listings
   - Links them to your source databases
   - Creates Data Products with your source databases as assets

**What you'll get in provider mode:**

- Data Products for your published marketplace listings
- Source databases automatically linked as assets
- Owner assignment from config patterns
- Works without any imported databases

**Example use case:**
You publish a "Customer 360" listing from your `CUSTOMER_DATA` database. Provider mode will:

1. Find the listing via `SHOW AVAILABLE LISTINGS`
2. Find the OUTBOUND share via `SHOW SHARES`
3. Link the `CUSTOMER_DATA` database to the Data Product
4. No manual configuration needed!

##### Mode 3: Both - Track Both Perspectives

Set `marketplace_mode: "both"` to track both purchased listings (consumer) AND published listings (provider) in the same ingestion.

For more details, see the marketplace configuration guide in the connector documentation.

##### Mapping Marketplace Organizations to Existing Domains

Marketplace ingestion does not create domains. Map each Snowflake organization to an existing DataHub domain (URN, GUID, or name) via `marketplace.organization_to_domain`:

```yaml
marketplace:
  enabled: true
  organization_to_domain:
    "ACME Corp": "urn:li:domain:finance"
    "Weather Co": "data-products" # resolved via DomainRegistry
```

Unmapped organizations produce Data Products with no domain. The `domains` aspect is written as `CREATE` only, so UI-assigned domains survive subsequent runs.

**Caveats:**

- Editing `organization_to_domain` for an already-ingested organization has no effect — soft-delete the Data Product's `domains` aspect to re-map.
- Provider renames leave the old `Marketplace:Provider:<old>` tag on the purchased database container; remove it manually.

##### Troubleshooting Marketplace Ingestion

**Common Permission Errors:**

If marketplace ingestion fails or produces incomplete data, check for these common permission issues:

1. **"Failed to get marketplace listings - insufficient permissions"**

   - **Cause**: Role lacks access to run `SHOW AVAILABLE LISTINGS`
   - **Solution**: Grant imported privileges:
     ```sql
     grant imported privileges on database snowflake to role datahub_role;
     ```

2. **"Failed to describe provider share - insufficient permissions"**

   - **Cause**: `DESC SHARE` requires ownership of the share. Snowflake Marketplace creates shares
     under `ACCOUNTADMIN`, and Snowflake does not allow transferring share ownership.
   - **Automatic fallback**: The connector falls back to enumerating tables from the share's source
     database (visible in `SHOW SHARES`). By default this includes all schemas in that database;
     use `listing_to_schemas_overrides` to restrict to the specific schemas the share exposes:
     ```yaml
     marketplace:
       listing_to_schemas_overrides:
         YOUR_LISTING_GLOBAL_NAME: [YOUR_SCHEMA]
     ```
   - **For precise `DESC SHARE` output**: Set `role: ACCOUNTADMIN` in your recipe (broader
     permissions, not recommended for production).

3. **"Failed to query imported database tables - insufficient permissions"**

   - **Cause**: Role lacks access to query `INFORMATION_SCHEMA` in imported databases
   - **Solution**: Grant usage and references on the imported database:
     ```sql
     grant usage on database "<imported-database>" to role datahub_role;
     grant usage on all schemas in database "<imported-database>" to role datahub_role;
     grant references on all tables in database "<imported-database>" to role datahub_role;
     ```

4. **Data Products created but no assets appear**

   - **Cause**: Missing `shares` configuration (consumer mode)
   - **Solution**: Add explicit `listing_global_name` mapping in your `shares` config (see consumer mode documentation above)

5. **Incomplete listing metadata**
   - **Cause**: `fetch_internal_marketplace_listing_details: true` requires additional time per listing
   - **Note**: This is expected behavior. The connector runs `DESCRIBE AVAILABLE LISTING` for each listing to fetch enriched metadata (descriptions, owners, documentation links). For large catalogs (100+ listings), consider setting this to `false` to improve performance.

**Debugging Tips:**

- Check the DataHub logs for structured warnings about marketplace ingestion failures
- All marketplace errors are logged with clear titles and context (e.g., "Optional listing enrichment failed")
- Verify your role has `imported privileges` on the `snowflake` database
- Test your SQL grants manually using the DataHub role before running ingestion

#### Lineage and Usage

DataHub supports two strategies for extracting lineage and usage information from Snowflake:

##### New Strategy (Default - `use_queries_v2: true`)

The default and recommended approach uses an optimized query extraction method that:

- **Better Performance**: Fetches query logs in a single optimized query instead of multiple separate queries
- **Enhanced Features**:
  - Query entities generation (`include_queries`)
  - Query popularity statistics (`include_query_usage_statistics`)
  - User filtering with patterns (`pushdown_deny_usernames`, `pushdown_allow_usernames`)
  - Database pattern pushdown for performance (`push_down_database_pattern_access_history`)
  - Query deduplication strategies (`query_dedup_strategy`)

##### Legacy Strategy (`use_queries_v2: false`)

The older approach that will be deprecated in future versions:

- Uses separate extractors for lineage and usage
- Less performant due to multiple query executions
- Limited feature support compared to the new strategy

Both strategies access the same Snowflake system tables (`account_usage.query_history`, `account_usage.access_history`), but the new strategy provides significant performance improvements and additional functionality.

##### Snowflake Streams as Upstream Lineage Sources

DataHub extracts lineage when a query reads from a Snowflake Stream. Coverage details:

- **Multi-target `INSERT ALL` from a Stream** — emits one lineage entry per downstream table, including column-level lineage. Requires `use_queries_v2: true` (the default).
- **Single-target queries reading from a Stream** — fall back to SQL parsing of the query text rather than direct extraction from the audit log.
- **Audit log placeholder names** — Snowflake occasionally emits placeholder object names (`$SYS_VIEW_<id>` or other `$`-prefixed names) for stream-driven queries. When this happens for a given row, that row falls back to SQL parsing so DataHub never builds lineage from unusable URNs.

The Stream entity itself is also extracted as a top-level dataset; the lineage above is in addition to that.

#### Metadata Pattern Pushdown

When ingesting metadata from large Snowflake environments, you can improve performance by pushing down pattern filters directly to Snowflake SQL queries using the `push_down_metadata_patterns` configuration option.

> **Note:** This option applies only to **metadata extraction** queries (`information_schema.databases`, `schemata`, `tables`, `views`). For pushing down filters on **lineage/usage** queries (`account_usage.access_history`), use `push_down_database_pattern_access_history` instead. These two options are independent and target completely separate query paths.

##### Configuration

```yaml
source:
  type: snowflake
  config:
    # Enable pattern pushdown for improved performance
    push_down_metadata_patterns: true

    # Your existing patterns - MUST follow Snowflake RLIKE syntax
    database_pattern:
      allow:
        - "PROD_.*" # Matches databases starting with PROD_
        - "ANALYTICS.*" # Matches databases starting with ANALYTICS
      deny:
        - ".*_TEMP$" # Excludes databases ending with _TEMP

    table_pattern:
      allow:
        - ".*" # Allow all tables
      deny:
        - ".*_BACKUP$" # Exclude tables ending with _BACKUP
```

##### View Pattern Limitation

By default, Snowflake views are fetched using `SHOW VIEWS`, which does **not** support SQL-level filtering. When `push_down_metadata_patterns: true`, the `view_pattern` is pushed down **only** if `fetch_views_from_information_schema: true` is also set. Otherwise, view filtering falls back to Python's `re.match()`, even with pushdown enabled.

This means if you write patterns in Snowflake RLIKE syntax (e.g., `PROD.*` for prefix matching), they will still work correctly with Python filtering since `PROD.*` is valid in both. However, patterns that rely on RLIKE's full-string matching semantics (e.g., exact match `PROD_DB` without `.*`) will behave differently — Python `re.match()` treats it as a prefix match.

**Recommendation**: If you enable `push_down_metadata_patterns`, also enable `fetch_views_from_information_schema: true` to ensure consistent behavior for view patterns.

##### Important: Snowflake RLIKE Syntax Differences

When `push_down_metadata_patterns: true`, patterns are executed in Snowflake using the `RLIKE` operator instead of Python's `re.match()`. These have **different matching behaviors**:

| Behavior             | Python `re.match()`                       | Snowflake `RLIKE`                   | Fix for Snowflake     |
| -------------------- | ----------------------------------------- | ----------------------------------- | --------------------- |
| **Prefix match**     | `'PROD'` matches `'PROD_DB'`              | `'PROD'` does NOT match `'PROD_DB'` | Use `PROD.*`          |
| **Suffix match**     | `'.*_BACKUP'` matches `'TABLE_BACKUP_V2'` | Does NOT match                      | Use `.*_BACKUP$`      |
| **Start anchor**     | `'^PROD'` matches `'PROD_DB'`             | Does NOT match                      | Use `PROD.*`          |
| **Alternation**      | `'PROD\|DEV'` matches `'PROD_DB'`         | Does NOT match                      | Use `(PROD\|DEV).*`   |
| **Case sensitivity** | Case-insensitive by default               | Case-sensitive by default           | Handled automatically |

**Key difference**: Python `re.match()` anchors at the START only (prefix matching), while Snowflake `RLIKE` requires a FULL STRING match.

##### Pattern Conversion Examples

| Intent                       | Without Pushdown (Python) | With Pushdown (Snowflake RLIKE) |
| ---------------------------- | ------------------------- | ------------------------------- |
| Starts with `PROD`           | `PROD`                    | `PROD.*`                        |
| Ends with `_BACKUP`          | `.*_BACKUP`               | `.*_BACKUP$`                    |
| Contains `TEMP`              | `.*TEMP.*`                | `.*TEMP.*` (same)               |
| Exact match                  | `PROD_DB`                 | `PROD_DB` (same)                |
| Match `PROD` or `DEV` prefix | `PROD\|DEV`               | `(PROD\|DEV).*`                 |

##### Testing Your Patterns

Before enabling pushdown in production, test your patterns in Snowflake:

```sql
-- Test prefix matching
SELECT 'PROD_DB' RLIKE 'PROD';      -- FALSE (needs .*)
SELECT 'PROD_DB' RLIKE 'PROD.*';    -- TRUE

-- Test suffix matching
SELECT 'TABLE_BACKUP_V2' RLIKE '.*_BACKUP';    -- FALSE (needs $)
SELECT 'TABLE_BACKUP' RLIKE '.*_BACKUP$';      -- TRUE

-- Test FQN with escaped dots
SELECT 'PROD_DB.PUBLIC.TABLE' RLIKE 'PROD_DB\\.PUBLIC\\..*';  -- TRUE
```

#### Semantic Views

DataHub supports ingestion of Snowflake Semantic Views, which are business-defined views that define metrics, dimensions, and relationships for consistent data modeling and AI-powered analytics.

##### Configuration

Semantic view ingestion is disabled by default (requires Snowflake Enterprise Edition or above). You can enable it using the following configuration options:

```yaml
# Enable semantic view ingestion (requires Enterprise Edition)
semantic_views:
  enabled: true # Default: false
  column_lineage: true # Default: false - enable column-level lineage

# Filter semantic views using regex patterns
semantic_view_pattern:
  allow:
    - "ANALYTICS_DB.PUBLIC.*"
    - "SALES_DB.*"
  deny:
    - ".*_INTERNAL"
```

##### Features

- **Metadata Extraction**: Extracts semantic view definitions (YAML), columns, comments, and timestamps
- **Lineage Support**: Semantic views participate in lineage extraction like regular views
- **Tags Support**: Tags applied to semantic views are extracted if `extract_tags` is enabled
- **External URLs**: Direct links to Snowflake Snowsight UI for semantic views

##### Requirements

- Semantic views require appropriate Snowflake edition and privileges
- Requires `REFERENCES` or `SELECT` privileges on semantic views (they are treated as views in Snowflake's permission model)
- The semantic view definition (SQL DDL) is extracted when available through the `GET_DDL` function

#### Stages, Tasks, and Pipes

DataHub supports ingestion of Snowflake Stages, Tasks, and Snowpipe objects. All three features are disabled by default and can be enabled independently.

##### Stages (`include_stages: true`)

Stages are ingested as containers nested under their parent schema. Internal stages additionally emit a placeholder dataset representing the staged data, which is used for pipe lineage resolution. External stages (S3, GCS, Azure) resolve their URLs to the corresponding cloud platform dataset URN.

```yaml
include_stages: true
stage_pattern:
  allow:
    - "MY_DB.MY_SCHEMA.*"
```

##### Tasks (`include_tasks: true`)

Tasks are ingested as DataJob entities grouped under a per-schema DataFlow. Predecessor dependencies between tasks are captured as `inputDatajobs` on the DataJobInputOutput aspect, preserving the DAG structure.

```yaml
include_tasks: true
task_pattern:
  allow:
    - "MY_DB.MY_SCHEMA.*"
```

##### Pipes (`include_pipes: true`)

Snowpipe objects are ingested as DataJob entities with lineage derived from parsing the `COPY INTO` statement. The pipe's source stage resolves to an upstream dataset (internal placeholder or external cloud URN) and the target table resolves to a downstream dataset. Enabling pipes automatically scans stages for lineage resolution, even if `include_stages` is false.

The parser handles the following `COPY INTO` patterns:

- Column-list targets — `COPY INTO t(a, b) FROM @stage` resolves the target table correctly
- Subquery sources — `COPY INTO t FROM (SELECT ... FROM @s1 UNION ALL SELECT ... FROM @s2)` captures all referenced stages as upstream datasets
- Non-`COPY` pipe bodies are silently skipped; stage refs that cannot be normalized to a three-part FQN emit a warning in the ingestion report

```yaml
include_pipes: true
pipe_pattern:
  allow:
    - "MY_DB.MY_SCHEMA.*"
```

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

- Some features require specific Snowflake editions or additional privileges. This includes dynamic tables, semantic views, advanced lineage features, and tags.
- Dynamic tables require the `monitor` privilege for full metadata extraction. Without this privilege, dynamic table entities will still appear in DataHub but their DDL is not accessible, so lineage will not be extracted.
- Semantic views require `REFERENCES` or `SELECT` privileges for metadata extraction. Without these privileges, semantic views will not be visible to DataHub.
- The underlying Snowflake views that we use to get metadata have a [latency of 45 minutes to 3 hours](https://docs.snowflake.com/en/sql-reference/account-usage.html#differences-between-account-usage-and-information-schema). So we would not be able to get very recent metadata in some cases like queries you ran within that time period etc. This is applicable particularly for lineage, usage and tags (without lineage) extraction.
- If there is any [ongoing Snowflake incident](https://status.snowflake.com/), we will not be able to get the metadata until that incident is resolved.
- Lineage extraction, when got directly from Snowflake access history, has some limitations, as documented [here](https://docs.snowflake.com/en/sql-reference/account-usage/access_history#usage-notes), [here](https://docs.snowflake.com/en/sql-reference/account-usage/access_history#usage-notes-column-lineage), and [here](https://docs.snowflake.com/en/sql-reference/account-usage/access_history#usage-notes-object-modified-by-ddl-column).

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.

#### Snowsight Links Point to the Wrong URL (Private Link)

By default, DataHub generates Snowsight links of the form `https://app.snowflake.com/<region>/<account>/...`. If Snowsight is only reachable through private link in your environment, these links will not work for your users.

To fix this, set `snowsight_base_url` in the recipe to your private-link Snowsight URL. Retrieve the value from Snowflake as `ACCOUNTADMIN`:

```sql
SELECT SYSTEM$GET_PRIVATELINK_CONFIG();
-- Use either snowsight-privatelink-url or regionless-snowsight-privatelink-url.
```

```yaml
source:
  type: snowflake
  config:
    snowsight_base_url: "https://app.us-east-1.privatelink.snowflakecomputing.com/"
```


### Code Coordinates
- Class Name: `datahub.ingestion.source.snowflake.snowflake_v2.SnowflakeV2Source`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/snowflake/snowflake_v2.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Snowflake, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
