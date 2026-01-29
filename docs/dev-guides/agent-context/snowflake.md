# Snowflake

Snowflake context connector allows creating [Snowflake Cortex Intelligence](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-intelligence) agents with UDFs that can access Datahub Context.

## Setup Steps

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

In this version you will need to execute 5 SQL files in your snowflake UI as a notebook. This is recommended for advanced workflows

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

Add `--execute` and `--sf-password` to automatically run the generated scripts:

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

This version will automatically execute the commands in your Snowflake environment.

### 2) Configure Snowflake agent in the UI

Additional configuration canb e done via the snowflake UI if necessary such as prompt tweaking or other settings.

### 3) Use Snowflake Cortex to access Datahub

## Agent Context

For more info on the tools exposed and Datahub Context, see the [DataHub Agent Context Documentation](./agent-context.md).
