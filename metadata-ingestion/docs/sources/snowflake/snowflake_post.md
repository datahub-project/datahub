### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

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

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
