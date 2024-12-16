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
- `scopes`: The list of your *Okta* scopes, i.e. with the `session:role:` prefix

Datahub only supports two OAuth grant types: `client_credentials` and `password`.
The steps slightly differ based on which you decide to use.

##### Client Credentials Grant Type (Simpler)
- When creating an Okta App Integration, choose type `API Services`
  + Ensure client authentication method is `Client secret`
  + Note your `Client ID`
- Create a Snowflake user to correspond to your newly created Okta client credentials
  + *Ensure the user's `Login Name` matches your Okta application's `Client ID`*
  + Ensure the user has been granted your datahub role

##### Password Grant Type
- When creating an Okta App Integration, choose type `OIDC` -> `Native Application`
  + Add Grant Type `Resource Owner Password`
  + Ensure client authentication method is `Client secret`
- Create an Okta user to sign into, noting the `Username` and `Password`
- Create a Snowflake user to correspond to your newly created Okta client credentials
  + *Ensure the user's `Login Name` matches your Okta user's `Username` (likely an email)*
  + Ensure the user has been granted your datahub role
- When running ingestion, provide the required `oauth_config` fields,
  including `client_id` and `client_secret`, plus your Okta user's `Username` and `Password`
  * Note: the `username` and `password` config options are not nested under `oauth_config`

### Snowflake Shares
If you are using [Snowflake Shares](https://docs.snowflake.com/en/user-guide/data-sharing-provider) to share data across different snowflake accounts, and you have set up DataHub recipes for ingesting metadata from all these accounts, you may end up having multiple similar dataset entities corresponding to virtual versions of same table in different snowflake accounts. DataHub Snowflake connector can automatically link such tables together through Siblings and Lineage relationship if user provides information necessary to establish the relationship using configuration `shares` in recipe. 

#### Example
- Snowflake account `account1` (ingested as platform_instance `instance1`) owns a database `db1`. A share `X` is created in `account1` that includes database `db1` along with schemas and tables inside it. 
- Now, `X` is shared with snowflake account `account2` (ingested as platform_instance `instance2`). A database `db1_from_X` is created from inbound share `X` in `account2`. In this case, all tables and views included in share `X` will also be present in `instance2`.`db1_from_X`.
- This can be represented in `shares` configuration section as
  ```yaml
  shares:
    X: # name of the share
      database_name: db1
      platform_instance: instance1
      consumers: # list of all databases created from share X
        - database_name: db1_from_X
          platform_instance: instance2 
          
  ```
- If share `X` is shared with more snowflake accounts and database is created from share `X` in those account then additional entries need to be added in `consumers` list for share `X`, one per snowflake account. The same `shares` config can then be copied across recipes of all accounts.
### Caveats

- Some of the features are only available in the Snowflake Enterprise Edition. This doc has notes mentioning where this applies.
- The underlying Snowflake views that we use to get metadata have a [latency of 45 minutes to 3 hours](https://docs.snowflake.com/en/sql-reference/account-usage.html#differences-between-account-usage-and-information-schema). So we would not be able to get very recent metadata in some cases like queries you ran within that time period etc. This is applicable particularly for lineage, usage and tags (without lineage) extraction.
- If there is any [incident going on for Snowflake](https://status.snowflake.com/) we will not be able to get the metadata until that incident is resolved.
