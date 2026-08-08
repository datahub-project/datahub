# Snowflake Cortex Code

Give [Cortex Code](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code-cli) access to your enterprise data context in DataHub — find trustworthy data, resolve business definitions, trace lineage, and generate better SQL alongside your Snowflake data.

## Prerequisites

- [Cortex Code CLI](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code-cli) installed and connected to your Snowflake account
- A DataHub Cloud instance and [personal access token](../../authentication/personal-access-tokens.md)

## Setup

Register DataHub as an [MCP server](../../features/feature-guides/mcp.md):

```bash
cortex mcp add datahub https://<tenant>.acryl.io/integrations/ai/mcp \
  --transport=http \
  --header "Authorization: Bearer <token>"
```

Replace `<tenant>` with your DataHub Cloud tenant name and `<token>` with your access token.

## Try It Out

Start Cortex Code (`cortex`) and ask questions — it calls DataHub tools automatically:

```
"Find all tables with customer data in the marketing domain"
"What's the business definition of MRR from our data glossary?"
"Which tables does the finance team own?"
"Write a query for top 10 customers by revenue"
```

## Troubleshooting

- **DataHub tools not used?** Run `cortex mcp list` to verify the server is registered. Check that the URL and token are correct.
- **Connection errors?** Confirm the MCP endpoint is reachable: `https://<tenant>.acryl.io/integrations/ai/mcp`. Token should start with `eyJ`.
- **Empty results?** Verify the token has correct permissions and that entities exist in the DataHub UI.

**Links:** [Cortex Code Docs](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code-cli) · [Agent Context Kit](./agent-context.md) · [MCP Server Guide](../../features/feature-guides/mcp.md)
