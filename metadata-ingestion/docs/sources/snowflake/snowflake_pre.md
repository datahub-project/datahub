### Prerequisites

In order to execute this source, your Snowflake user will need to have specific privileges granted to it for reading metadata
from your warehouse.

A Snowflake system admin can follow this guide to create a DataHub-specific role, assign it the required privileges, and assign it to a new DataHub user by executing the following Snowflake commands from a user with the `ACCOUNTADMIN` role or `MANAGE GRANTS` privilege.

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

// Optional - required if extracting Streamlit Apps
grant usage on all streamlits in database "<your-database>" to role datahub_role;
grant usage on future streamlits in database "<your-database>" to role datahub_role;
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
- `usage` on `streamlit` is required to show streamlits in a database.

```sql
grant usage on schema "<your-database>"."<your-schema>" to role datahub_role;
```

This represents the bare minimum privileges required to extract databases, schemas, views, and tables from Snowflake.

If you plan to enable extraction of table lineage via the `include_table_lineage` config flag, extraction of usage statistics via the `include_usage_stats` config, or extraction of tags (without lineage) via the `extract_tags` config, you'll also need to grant access to the [Account Usage](https://docs.snowflake.com/en/sql-reference/account-usage.html) system tables from which the DataHub source extracts information. This can be done by granting access to the `snowflake` database.

```sql
grant imported privileges on database snowflake to role datahub_role;
```

### Authentication

Authentication is most simply done via a Snowflake user and password.

Alternatively, other authentication methods are supported via the `authentication_type` config option.

#### Key Pair Authentication

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

#### Okta OAuth

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

### Snowflake Shares

If you are using [Snowflake Shares](https://docs.snowflake.com/en/user-guide/data-sharing-provider) to share data across different Snowflake accounts, and you have set up DataHub recipes for ingesting metadata from all these accounts, you may end up having multiple similar dataset entities corresponding to virtual versions of the same table in different Snowflake accounts. The DataHub Snowflake connector can automatically link such tables together through Siblings and Lineage relationships if the user provides information necessary to establish the relationship using the `shares` configuration in the recipe.

#### Example

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

### Lineage and Usage

DataHub supports two strategies for extracting lineage and usage information from Snowflake:

#### New Strategy (Default - `use_queries_v2: true`)

The default and recommended approach uses an optimized query extraction method that:

- **Better Performance**: Fetches query logs in a single optimized query instead of multiple separate queries
- **Enhanced Features**:
  - Query entities generation (`include_queries`)
  - Query popularity statistics (`include_query_usage_statistics`)
  - User filtering with patterns (`pushdown_deny_usernames`, `pushdown_allow_usernames`)
  - Database pattern pushdown for performance (`push_down_database_pattern_access_history`)
  - Query deduplication strategies (`query_dedup_strategy`)

#### Legacy Strategy (`use_queries_v2: false`)

The older approach that will be deprecated in future versions:

- Uses separate extractors for lineage and usage
- Less performant due to multiple query executions
- Limited feature support compared to the new strategy

Both strategies access the same Snowflake system tables (`account_usage.query_history`, `account_usage.access_history`), but the new strategy provides significant performance improvements and additional functionality.

### Semantic Views

DataHub supports ingestion of Snowflake Semantic Views, which are business-defined views that define metrics, dimensions, and relationships for consistent data modeling and AI-powered analytics.

#### Configuration

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

#### Features

- **Metadata Extraction**: Extracts semantic view definitions (YAML), columns, comments, and timestamps
- **Lineage Support**: Semantic views participate in lineage extraction like regular views
- **Tags Support**: Tags applied to semantic views are extracted if `extract_tags` is enabled
- **External URLs**: Direct links to Snowflake Snowsight UI for semantic views

#### Requirements

- Semantic views require appropriate Snowflake edition and privileges
- Requires `REFERENCES` or `SELECT` privileges on semantic views (they are treated as views in Snowflake's permission model)
- The semantic view definition (SQL DDL) is extracted when available through the `GET_DDL` function

### Caveats

- Some features require specific Snowflake editions or additional privileges. This includes dynamic tables, semantic views, advanced lineage features, and tags.
- Dynamic tables require the `monitor` privilege for metadata extraction. Without this privilege, dynamic tables will not be visible to DataHub.
- Semantic views require `REFERENCES` or `SELECT` privileges for metadata extraction. Without these privileges, semantic views will not be visible to DataHub.
- The underlying Snowflake views that we use to get metadata have a [latency of 45 minutes to 3 hours](https://docs.snowflake.com/en/sql-reference/account-usage.html#differences-between-account-usage-and-information-schema). So we would not be able to get very recent metadata in some cases like queries you ran within that time period etc. This is applicable particularly for lineage, usage and tags (without lineage) extraction.
- If there is any [ongoing Snowflake incident](https://status.snowflake.com/), we will not be able to get the metadata until that incident is resolved.
- Lineage extraction, when got directly from Snowflake access history, has some limitations, as documented [here](https://docs.snowflake.com/en/sql-reference/account-usage/access_history#usage-notes), [here](https://docs.snowflake.com/en/sql-reference/account-usage/access_history#usage-notes-column-lineage), and [here](https://docs.snowflake.com/en/sql-reference/account-usage/access_history#usage-notes-object-modified-by-ddl-column).
