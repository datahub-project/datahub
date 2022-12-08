---
title: Setup
---
# Snowflake Ingestion Guide: Setup & Prerequisites

In order to configure ingestion from Snowflake, you'll first have to ensure you have a Snowflake user with the `ACCOUNTADMIN` role or `MANAGE GRANTS` privilege and you know where to [execute queries](https://docs.snowflake.com/en/user-guide/ui-worksheet.html#) in Snowflake.

## Snowflake Prerequisites

1. Create a DataHub-specific role by executing following queries. Replace `<your-warehouse>` with an existing warehouse that you wish to use for DataHub ingestion.

   ```sql
   create or replace role datahub_role;
   -- Grant access to a warehouse to run queries to view metadata
   grant operate, usage on warehouse "<your-warehouse>" to role datahub_role;
   ```

   Make note of this role and warehouse. You'll need this in next step.

2. Create a DataHub-specific user by executing following queries. Replace `<your-password>` with a strong password. Replace `<your-warehouse>` with same warehouse used above.

   ```sql
   create user datahub_user display_name = 'DataHub' password='<your-password>' default_role = datahub_role default_warehouse = '<your-warehouse>';
   -- Grant access to the DataHub role created above
   grant role datahub_role to user datahub_user;
   ```

   Make note of the user and its password. You'll need this in next step.

3. Assign privileges to read metadata about your assets by executing following queries. Replace `<your-database>` with an existing database. Repeat for all databases from your snowflake instance that you wish to integrate with DataHub. This is a bit tedious but very important step.

   ```sql
   set db_var = '"<your-database>"';
   -- Grant access to view database and schema in which your tables/views exist
   grant usage on DATABASE identifier($db_var) to role datahub_role;
   grant usage on all schemas in database identifier($db_var) to role datahub_role;
   grant usage on future schemas in database identifier($db_var) to role datahub_role;

   --  Grant access to view tables and views
   grant references on all tables in database identifier($db_var) to role datahub_role;
   grant references on future tables in database identifier($db_var) to role datahub_role;
   grant references on all external tables in database identifier($db_var) to role datahub_role;
   grant references on future external tables in database identifier($db_var) to role datahub_role;
   grant references on all views in database identifier($db_var) to role datahub_role;
   grant references on future views in database identifier($db_var) to role datahub_role;

   ```

   If you have imported databases in your snowflake instance that you wish to integrate with DataHub, you'll need to use below query for them.

   ```sql
   grant IMPORTED PRIVILEGES on database "<your-database>" to role datahub_role;  
   ```

4. Assign privileges to run profiling on your assets. Replace `<your-database>` with an existing database name. Repeat for all databases from your snowflake instance that you wish to integrate with DataHub.

   ```sql
   set db_var = '"<your-database>"';
   grant select on all tables in database identifier($db_var) to role datahub_role;
   grant select on future tables in database identifier($db_var) to role datahub_role;
   grant select on all external tables in database identifier($db_var) to role datahub_role;
   grant select on future external tables in database identifier($db_var) to role datahub_role;
   grant select on all views in database identifier($db_var) to role datahub_role;
   grant select on future views in database identifier($db_var) to role datahub_role;
   ```

5. Assign privileges to extract lineage and usage statistics from Snowflake by executing below query.

   ```sql
   grant imported privileges on database snowflake to role datahub_role;
   ```

## Next Steps

Once you've done all of the above in Snowflake, it's time to [move on](Configuration.md) to configuring the actual ingestion source within the DataHub UI

*Need more help? Join the conversation in [Slack](http://slack.datahubproject.io)!*
