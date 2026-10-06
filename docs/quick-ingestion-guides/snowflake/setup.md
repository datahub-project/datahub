---
title: Snowflake Setup
---

# Snowflake Ingestion Guide: Setup & Prerequisites

In order to configure ingestion from Snowflake, you'll first have to ensure you have a Snowflake user with the `ACCOUNTADMIN` role or `MANAGE GRANTS` privilege.

:::note Already ingesting with password auth?
Snowflake is deprecating username + password authentication (`DEFAULT_AUTHENTICATOR`) as part of its Strong Authentication rollout. If you have an existing recipe using password auth, follow the [migration guide](migrate-to-key-pair-auth.md) to switch to key-pair auth before your account's enforcement date.
:::

## Snowflake Prerequisites

1. Create a DataHub-specific role by executing the following queries in Snowflake. Replace `<your-warehouse>` with an existing warehouse that you wish to use for DataHub ingestion.

   ```sql
   create or replace role datahub_role;
   -- Grant access to a warehouse to run queries to view metadata
   grant operate, usage on warehouse "<your-warehouse>" to role datahub_role;
   ```

   Make note of this role and warehouse. You'll need this in the next step.

2. Create a DataHub-specific user with **key-pair authentication**. Key-pair auth is the recommended target for scheduled, headless ingestion — Snowflake is deprecating username + password auth as part of its Strong Authentication rollout (see the [migration guide](migrate-to-key-pair-auth.md) for background).

   First, generate an RSA key-pair on your machine:

   ```bash
   # Unencrypted 2048-bit RSA private key (PEM) — simplest for headless ingestion
   openssl genrsa 2048 | openssl pkcs8 -topk8 -nocrypt -out snowflake_key.p8
   # Public key to assign to the Snowflake user
   openssl rsa -in snowflake_key.p8 -pubout -out snowflake_key.pub
   ```

   :::caution Keep the private key safe
   `snowflake_key.p8` is the private key DataHub will use to connect. Store it as a DataHub secret (see the next page) — do not commit it to source control. If your security policy requires an encrypted private key, add a passphrase and use it as `private_key_password` later; see the [migration guide](migrate-to-key-pair-auth.md#1a-generate-the-key-pair) for the encrypted-key commands.
   :::

   Then create the DataHub user as a service account and assign the public key. Strip the `-----BEGIN/END-----` lines and newlines from `snowflake_key.pub` when pasting it into `RSA_PUBLIC_KEY`. Replace `<your-warehouse>` with the same warehouse used above.

   ```sql
   create user datahub_user display_name = 'DataHub' default_role = datahub_role type='SERVICE' default_warehouse = '<your-warehouse>';
   -- Assign the public key to the DataHub user
   alter user datahub_user set RSA_PUBLIC_KEY = 'MIIBIjANBgkqhkiG9w0B...';
   -- Grant access to the DataHub role created above
   grant role datahub_role to user datahub_user;
   ```

   Make note of the user and the path to the private key (`snowflake_key.p8`). You'll need both in the next step.

3. Assign privileges to read metadata about your assets by executing the following queries. Replace `<your-database>` with an existing database. Repeat for all databases from your Snowflake instance that you wish to integrate with DataHub.

   ```sql
   set db_var = '"<your-database>"';
   -- Grant access to view database and schema in which your tables/views exist
   grant usage on DATABASE identifier($db_var) to role datahub_role;
   grant usage on all schemas in database identifier($db_var) to role datahub_role;
   grant usage on future schemas in database identifier($db_var) to role datahub_role;

   -- Grant Select access enable Data Profiling
   grant select on all tables in database identifier($db_var) to role datahub_role;
   grant select on future tables in database identifier($db_var) to role datahub_role;
   grant select on all external tables in database identifier($db_var) to role datahub_role;
   grant select on future external tables in database identifier($db_var) to role datahub_role;
   grant select on all views in database identifier($db_var) to role datahub_role;
   grant select on future views in database identifier($db_var) to role datahub_role;
   grant select on all dynamic tables in database identifier($db_var) to role datahub_role;
   grant select on future dynamic tables in database identifier($db_var) to role datahub_role;

   --  Grant access to view tables and views
   grant references on all tables in database identifier($db_var) to role datahub_role;
   grant references on future tables in database identifier($db_var) to role datahub_role;
   grant references on all external tables in database identifier($db_var) to role datahub_role;
   grant references on future external tables in database identifier($db_var) to role datahub_role;
   grant references on all views in database identifier($db_var) to role datahub_role;
   grant references on future views in database identifier($db_var) to role datahub_role;
   --  Grant access to dynamic tables
   grant monitor on all dynamic tables in database identifier($db_var) to role datahub_role;
   grant monitor on future dynamic tables in database identifier($db_var) to role datahub_role;

   -- Assign privileges to extract lineage and usage statistics from Snowflake by executing the below query.
   grant imported privileges on database snowflake to role datahub_role;

   -- Optional: If you want to ingest Snowflake internal marketplace listings as Data Products
   -- Grant IMPORT SHARE for consumer mode (requires ACCOUNTADMIN to grant account-level privileges)
   use role accountadmin;
   grant import share on account to role datahub_role;  -- For INBOUND shares

   -- For provider mode (OUTBOUND shares), grant SYSADMIN role (use SECURITYADMIN to grant roles)
   -- Note: Only needed if shares are owned by ACCOUNTADMIN/SYSADMIN
   -- If datahub_role creates/owns the shares, no additional grant needed
   use role securityadmin;
   grant role sysadmin to role datahub_role;  -- Allows seeing shares owned by SYSADMIN/ACCOUNTADMIN

   -- Alternatively, use role: SYSADMIN directly in your recipe

   ```

   If you have imported databases in your Snowflake instance that you wish to integrate with DataHub, you'll need to use the below query for them.

   ```sql
   grant IMPORTED PRIVILEGES on database "<your-database>" to role datahub_role;
   ```

## Next Steps

Once you've done all of the above in Snowflake, it's time to [move on](configuration.md) to configuring the actual ingestion source within DataHub.
