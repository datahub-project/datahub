# Snowflake Cortex Agents

Give [Cortex Agents](https://www.snowflake.com/en/developers/guides/getting-started-with-snowflake-intelligence) access to your enterprise data context in DataHub — business definitions, ownership, lineage, and quality signals — so they can generate better SQL and answer data questions accurately.

The integration works through UDFs created by the DataHub CLI. Once set up, your Cortex Agent calls DataHub tools alongside your Snowflake tables.

## Prerequisites

- `pip install datahub-agent-context[snowflake]`
- A DataHub Cloud instance URL and [personal access token](../../authentication/personal-access-tokens.md)
- A Snowflake user with the `ACCOUNTADMIN` role (for initial setup)

## Setup

You can either let the CLI execute the SQL directly, or generate the SQL files and run them yourself.

**Option A: Execute directly**

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

Use `--sf-authenticator externalbrowser` for SSO instead of `--sf-password`.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/ai/agent-context/snowflake/snowflake-execute-generator.png"/>
</p>

**Option B: Generate SQL**

Drop `--execute` and `--sf-password` to generate SQL files instead. Then run them in order in a Snowflake worksheet:

```sql
@00_configuration.sql;
@01_network_rules.sql;
@02_datahub_udfs.sql;
@03_stored_procedure.sql;
@04_cortex_agent.sql;
```

## Configure and Use

Customize the agent's prompt, model, and tools in the Snowflake UI, then open [Snowflake Intelligence](https://ai.snowflake.com/) and select the DataHub Agent.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/ai/agent-context/snowflake/snowflake-cortex-agent.png"/>
</p>

## Updating UDFs

When new tools are released, re-run the UDF and agent SQL:

```sql
@02_datahub_udfs.sql;
@04_cortex_agent.sql;
```

## Troubleshooting

- **Permission denied?** Initial setup requires `ACCOUNTADMIN`. After that, `SNOWFLAKE_INTELLIGENCE_ADMIN` is sufficient.
- **UDFs not found?** Run `SHOW USER FUNCTIONS LIKE 'datahub%';` to verify.
- **Agent not using DataHub tools?** Update the agent system prompt to explicitly mention DataHub tools.
- **Connection errors?** Verify the DataHub URL is reachable and the token hasn't expired (should start with `eyJ`).
- **Empty results?** Check token permissions and that entities exist in the DataHub UI.

**Links:** [Cortex Agents Docs](https://www.snowflake.com/en/developers/guides/getting-started-with-snowflake-intelligence) · [Agent Context Kit](./agent-context.md) · [MCP Server Guide](../../features/feature-guides/mcp.md)
