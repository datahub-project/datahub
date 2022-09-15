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
