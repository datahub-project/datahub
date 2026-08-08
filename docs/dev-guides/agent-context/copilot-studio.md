# Microsoft Copilot Studio

Build [Copilot Studio](https://copilotstudio.microsoft.com/) agents that can find trustworthy data, trace lineage, look up ownership, and answer data questions grounded in your enterprise context from DataHub.

## Prerequisites

- A [Microsoft Copilot Studio](https://copilotstudio.microsoft.com/) account
- A DataHub Cloud instance (v0.3.12+) or self-hosted DataHub with the [MCP server](../../features/feature-guides/mcp.md) running
- A DataHub [personal access token](../../authentication/personal-access-tokens.md)

## Setup

### 1. Create or Open an Agent

In [Copilot Studio](https://copilotstudio.microsoft.com/), click **+ Create a blank agent** or open an existing one.

<p align="center">
  <img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/ai/agent-context/copilot-studio/copilot_studio_agents_page.png"/>
</p>

### 2. Add an MCP Tool

From your agent's overview, scroll to **Tools** and click **+ Add tool**. Select **Model Context Protocol** under "Create new".

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/ai/agent-context/copilot-studio/copilot_studio_add_tool.png"/>
</p>

### 3. Configure the MCP Connection

| Field              | Value                                                 |
| ------------------ | ----------------------------------------------------- |
| **Server name**    | `DataHub MCP Server`                                  |
| **Server URL**     | `https://<tenant>.acryl.io/integrations/ai/mcp`       |
| **Authentication** | API key · Header · `Authorization` · `Bearer <token>` |

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/ai/agent-context/copilot-studio/copilot_studio_mcp_config.png"/>
</p>

:::note Self-Hosted DataHub
For self-hosted instances, expose the [MCP server](../../features/feature-guides/mcp.md#self-hosted-mcp-server-usage) via a publicly accessible URL and use that as the Server URL.
:::

### 4. Enable Tools

Click **Next** — Copilot Studio discovers DataHub's tools automatically. Click **Add and configure**, then toggle on the tools you want.

<p align="center">
  <img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/ai/agent-context/copilot-studio/copilot_studio_tools_list.png"/>
</p>

### 5. Test

Click **Test** in the top-right corner and try:

- _"What datasets does the analytics team own?"_
- _"Show me the lineage for the revenue dashboard"_

<p align="center">
  <img width="50%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/ai/agent-context/copilot-studio/copilot_studio_test_agent.png"/>
</p>

## Tips

- Use the **Instructions** field to guide behavior — e.g., _"Always search DataHub before answering data questions."_
- **Publish** your agent to Teams, your website, or other channels when ready.

## Troubleshooting

- **Can't connect?** Verify the DataHub URL, check the token hasn't expired, and confirm auth is set to **API key** (not OAuth). Include the `Bearer ` prefix.
- **Tools not appearing?** Click refresh on the Tools page. Verify the [MCP server](../../features/feature-guides/mcp.md) is running and the token has the right permissions.
- **Empty results?** Check that your DataHub instance has ingested metadata. Try broader search terms.

**Links:** [Copilot Studio Docs](https://learn.microsoft.com/en-us/microsoft-copilot-studio/) · [Agent Context Kit](./agent-context.md) · [MCP Server Guide](../../features/feature-guides/mcp.md)
