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
