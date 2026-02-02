# Snowflake

Snowflake context connector allows creating [Snowflake Intelligence](https://www.snowflake.com/en/developers/guides/getting-started-with-snowflake-intelligence) agents with UDFs that can access Datahub Context. This guide and agent create tool will create an end to end experience for setting up a Snowflake Intelligence agent that can be used in Snowflake Intelligence.

## Setup

### 1) Generate or Execute SQL for creating Snowflake Agent

#### Generate SQL for Snowflake

```bash
datahub agent create snowflake \
  --sf-account YOUR_ACCOUNT \
  --sf-user YOUR_USER \
  --sf-role YOUR_ROLE \
  --sf-warehouse YOUR_WAREHOUSE \
  --sf-database YOUR_DATABASE \
  --sf-schema YOUR_SCHEMA \
  --datahub-url https://your-datahub.acryl.io \
  --datahub-token YOUR_TOKEN \
  --enable-mutations
```

In this version you will need to execute 5 SQL files in your snowflake UI as a notebook. This is recommended for advanced workflows or if you want to make changes to the configuration before publishing.

```sql
-- 1. Set up configuration and secrets
@00_configuration.sql;

-- 2. Create network rules
@01_network_rules.sql;

-- 3. Create DataHub UDFs
@02_datahub_udfs.sql;

-- 4. Create stored procedure
@03_stored_procedure.sql;

-- 5. Create Cortex Agent
@04_cortex_agent.sql;
```

#### Execute SQL for Snowflake directly

Add `--execute` to automatically execute the SQL as your Snowflake user.

Authentication:

- Use `--sf-password` to automatically run the generated scripts with your password or PAT.
- SSO support using an external browser with `--sf-authenticator externalBrowser`.

```bash
datahub agent create snowflake \
  --sf-account YOUR_ACCOUNT \
  --sf-user YOUR_USER \
  --sf-password YOUR_PASSWORD \
  --sf-role YOUR_ROLE \
  --sf-warehouse YOUR_WAREHOUSE \
  --sf-database YOUR_DATABASE \
  --sf-schema YOUR_SCHEMA \
  --datahub-url https://your-datahub.acryl.io \
  --datahub-token YOUR_TOKEN \
  --enable-mutations \
  --execute
```

This will automatically execute the commands in your Snowflake environment and output the results. This workflow is recommended for a hands off default experience.

### 2) Configure Snowflake agent in the UI

Additional configuration canb e done via the snowflake UI if necessary such as prompt tweaking or other settings.

### 3) Use Snowflake Intelligence to access Datahub

Open [Snowflake Intelligence](https://ai.snowflake.com/) and select the Datahub Agent.

## Updating UDFs

Re-run the SQL generators and apply the new UDFs and cortex agent upates.

-- 1. Create updated DataHub UDFs
@02_datahub_udfs.sql;

-- 2. Create updated Cortex Agent
@04_cortex_agent.sql;

## Agent Context

For more info on the tools exposed and Datahub Context, see the [DataHub Agent Context Documentation](./agent-context.md).
