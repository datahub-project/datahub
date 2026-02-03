# Snowflake

Snowflake only sees tables and columns natively, and requires special configuration of a semantic view to enable text to sql. Datahub's Snowflake context connector allows creating [Snowflake Intelligence](https://www.snowflake.com/en/developers/guides/getting-started-with-snowflake-intelligence) agents with UDFs that can access Datahub Context to power text to sql generation. The connector creates an integration with Datahub that can search Datahub for documents and assets, as well as read Snowflake data tables and generate queries to help answer questions.

This guide and agent create tool will create an end to end experience for setting up a Snowflake Intelligence agent that can be used in Snowflake Intelligence.

## Setup

### Install the add on to acryl-datahub pacakge

`pip install datahub-agent-context[snowflake]`

### Permissions

#### Datahub

In order to interact with Datahub, you will need the following from your Datahub account

- The URL of your DataHub Cloud instance e.g. `https://<tenant>.acryl.io`
- A [personal access token](../../authentication/personal-access-tokens.md)

#### Snowflake

In order to execute the SQL in Snowflake, you will need a user with the `SNOWFLAKE_ADMIN` role to configure the rules and UDFs. This is necessary to set up the secrets and networking options. Once the initial setup is completed, the `SNOWFLAKE_INTELLIGENCE_ADMIN` role is required to do eits and further configuration.

### Generate or Execute SQL for creating Snowflake Agent

In order to use DataHub tools in Snowflake intelligence, we'll create a new agent that has access to UDFs that can use the context inside of DataHub to answer using business questions. We can either run the SQL to register the agent and tools ourselves, or let the DataHub Agents CLI execute it for us.

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

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/ai/agent-context/snowflake/snowflake-execute-generator.png"/>
</p>

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

In this version you will need to execute 5 SQL files in your snowflake UI as a notebook. This is recommended for advanced workflows or if you want to review or make changes to the configuration before publishing. These configuration files can be pushed into version control if desired to maintain history.

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

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/ai/agent-context/snowflake/snowflake-sql.png"/>
</p>

### Configure Snowflake agent in the UI

Once your agent is created, you can further customize it's prompt settings, models, tools, & more inside the Snowflake user interface. It is recommended to make sure your prompt and model is tweaked for your specific use case and requirements.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/ai/agent-context/snowflake/snowflake-cortex-agent.png"/>
</p>

### Using your Datahub Agent

Open [Snowflake Intelligence](https://ai.snowflake.com/) and select the Datahub Agent.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/ai/agent-context/snowflake/snowflake-intellgence.png"/>
</p>

## Updating UDFs

Periodically we expect to add new tools and update existing tools. To update the tools for your agent, simply run the following SQL snippets for your dataHub Agent to update the tool definitions and SDK.

-- 1. Create updated DataHub UDFs
@02_datahub_udfs.sql;

-- 2. Create updated Cortex Agent
@04_cortex_agent.sql;

## Agent Context

For more info on the tools exposed and Datahub Context, see the [DataHub Agent Context Documentation](./agent-context.md).
