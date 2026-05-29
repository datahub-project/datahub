# Snowflake Cortex Agents

Give [Snowflake Cortex Agents](https://www.snowflake.com/en/developers/guides/getting-started-with-snowflake-intelligence) access to your enterprise data context in DataHub — business definitions, ownership, lineage, and quality signals — so they can generate better SQL and answer data questions accurately.

Snowflake connects to the [DataHub MCP Server](../../features/feature-guides/mcp.md) as an **External MCP Server**, using OAuth2 with [Dynamic Client Registration (DCR)](https://datatracker.ietf.org/doc/html/rfc7591). Each user signs in with their own DataHub credentials (including SSO), and tokens are scoped per-user and refreshed automatically.

## Prerequisites

- DataHub Cloud v1.0.2+ (required for OAuth2 + DCR on the MCP endpoint)
- A Snowflake account with Cortex Agents / Snowflake Intelligence enabled
- A Snowflake user with the `ACCOUNTADMIN` role (for initial setup of the API integration and MCP server object)

## Setup

### 1. Create the API integration

Run as `ACCOUNTADMIN`:

```sql
CREATE API INTEGRATION datahub_mcp_api_integration
  API_PROVIDER = external_mcp
  API_ALLOWED_PREFIXES = ('https://mcp.datahub.com')
  API_USER_AUTHENTICATION = (
    TYPE = OAUTH_DYNAMIC_CLIENT,
    OAUTH_RESOURCE_URL = 'https://mcp.datahub.com/mcp'
  )
  ENABLED = TRUE;
```

If you prefer to point directly at your tenant URL, replace both `https://mcp.datahub.com` values with `https://<tenant>.acryl.io` and `https://<tenant>.acryl.io/integrations/ai/mcp` respectively.

### 2. Create the External MCP Server

```sql
CREATE EXTERNAL MCP SERVER datahub_mcp_server
  WITH DISPLAY_NAME = 'DataHub'
  URL = 'https://mcp.datahub.com/mcp'
  API_INTEGRATION = datahub_mcp_api_integration;
```

### 3. Add the connector to your agent

1. In Snowsight, navigate to **AI & ML → Agents** and open (or create) your agent.
2. Choose **MCP Connectors** and add the DataHub connector.
3. Customize the agent's prompt, model, and tools as needed.

### 4. Connect and sign in

Open [Snowflake Intelligence](https://ai.snowflake.com/) and select your agent. Click **Connect** next to the DataHub connector — Snowflake walks each user through the DataHub OAuth flow once, then reuses the credential on subsequent calls.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/ai/agent-context/snowflake/snowflake-cortex-agent.png"/>
</p>

## Troubleshooting

- **Permission denied creating the integration?** `CREATE API INTEGRATION` and `CREATE EXTERNAL MCP SERVER` both require `ACCOUNTADMIN`.
- **Agent not using DataHub tools?** Update the agent system prompt to explicitly mention DataHub tools (search, lineage, schema lookup, etc.).
- **OAuth flow fails?** Confirm your DataHub Cloud instance is on v1.0.2+ and that the `OAUTH_RESOURCE_URL` exactly matches the MCP endpoint (`https://mcp.datahub.com/mcp` or `https://<tenant>.acryl.io/integrations/ai/mcp`).
- **Empty results?** The signed-in user's DataHub permissions apply — check that they can see the entities in the DataHub UI.

**Links:** [Cortex Agents Docs](https://www.snowflake.com/en/developers/guides/getting-started-with-snowflake-intelligence) · [Agent Context Kit](./agent-context.md) · [MCP Server Guide](../../features/feature-guides/mcp.md)

---

## Deprecated: UDF-Based Setup

:::warning Deprecated
The UDF-based integration below is deprecated in favor of the External MCP Server flow above. It is preserved here for users on DataHub Cloud versions prior to v1.0.2 or for existing UDF deployments. New integrations should use the External MCP Server.
:::

The legacy integration works through UDFs created by the DataHub CLI. Once set up, your Cortex Agent calls DataHub tools alongside your Snowflake tables.

### Prerequisites

- `pip install datahub-agent-context[snowflake]`
- A DataHub Cloud instance URL and [personal access token](../../authentication/personal-access-tokens.md)
- A Snowflake user with the `ACCOUNTADMIN` role (for initial setup)

### Setup

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

### Configure and Use

Customize the agent's prompt, model, and tools in the Snowflake UI, then open [Snowflake Intelligence](https://ai.snowflake.com/) and select the DataHub Agent.

### Updating UDFs

When new tools are released, re-run the UDF and agent SQL:

```sql
@02_datahub_udfs.sql;
@04_cortex_agent.sql;
```

### Troubleshooting (UDF setup)

- **Permission denied?** Initial setup requires `ACCOUNTADMIN`. After that, `SNOWFLAKE_INTELLIGENCE_ADMIN` is sufficient.
- **UDFs not found?** Run `SHOW USER FUNCTIONS LIKE 'datahub%';` to verify.
- **Agent not using DataHub tools?** Update the agent system prompt to explicitly mention DataHub tools.
- **Connection errors?** Verify the DataHub URL is reachable and the token hasn't expired (should start with `eyJ`).
- **Empty results?** Check token permissions and that entities exist in the DataHub UI.
