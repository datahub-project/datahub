# Databricks Genie Code

Give [Genie Code](https://docs.databricks.com/en/genie/genie-code.html) access to your enterprise data context in DataHub — find trustworthy data, understand lineage, look up ownership, and generate better SQL queries, all without leaving your notebook.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/ai/agent-context/databricks/genie-code-datahub-query.png" alt="Genie Code querying DataHub catalog"/>
</p>

## Prerequisites

- A Databricks workspace with the **Managed MCP Servers** preview enabled
- A DataHub instance with the [MCP server](../../features/feature-guides/mcp.md) enabled
- A DataHub access token (personal access token or service account token)
- `CREATE CONNECTION` privilege on your Unity Catalog metastore

## Step 1: Register DataHub as an External MCP Server

This creates a Unity Catalog HTTP connection to your DataHub [MCP server](../../features/feature-guides/mcp.md). You only need to do this once — the same connection is used by [Databricks Agent Bricks](./databricks-agent-bricks.md).

1. In your Databricks workspace, go to **Catalog > Connections > Create connection**.

2. Configure the connection:

   - **Name**: `datahub` (or your preferred name)
   - **Connection type**: HTTP
   - **Auth type**: Bearer Token
   - **Bearer token**: Your DataHub access token
   - **Host**: Your DataHub MCP server URL:
     - DataHub Cloud: `https://<tenant>.acryl.io`
     - Self-hosted: `http://<gms-host>:8080`
   - **Base path**: `/integrations/ai/mcp` (DataHub Cloud) or `/mcp` (self-hosted)

3. Click **Create connection**.

4. Grant access to users who need it: Go to **Catalog > Connections > datahub > Permissions** and grant `USE CONNECTION` to the appropriate users or groups.

Once created, the connection shows up under **Agents > MCP Servers** in your workspace.

## Step 2: Add DataHub to Genie Code

1. Open **Genie Code** and click the **Settings** gear icon.

2. Under **MCP Servers**, click **Add Server**.

3. Select **External MCP servers** and choose the `datahub` connection you created above.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/ai/agent-context/databricks/genie-code-add-mcp-servers.png" alt="Add MCP Servers dialog showing External MCP: datahub"/>
</p>

4. Click **Save**.

## Try It Out

Switch to **Agent mode** in Genie Code and try:

```
Find all datasets related to "customer churn" and show me their owners
```

```
Search DataHub for datasets in the Marketing domain, then query the customer_segments
table to show me the top 10 segments by size
```

> **Tip**: MCP servers require Genie Code **Agent mode**. There's a limit of 20 tools across all connected MCP servers.

## Troubleshooting

- **DataHub not showing up?** Check the connection under **Catalog > Connections**, verify the MCP URL and base path, and confirm the bearer token is still valid.
- **`401 Unauthorized`?** Regenerate the DataHub access token and update the UC connection.
- **No tools discovered?** Make sure DataHub's [MCP server](../../features/feature-guides/mcp.md) is enabled. Genie Code caps at 20 tools across all MCP servers. Base path is `/integrations/ai/mcp` (Cloud) or `/mcp` (self-hosted).

**Links:** [MCP on Databricks](https://docs.databricks.com/en/generative-ai/agent-framework/mcp.html) · [DataHub MCP Server](../../features/feature-guides/mcp.md) · [Agent Context Kit](./agent-context.md)
