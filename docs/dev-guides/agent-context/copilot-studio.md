# Microsoft Copilot Studio Integration

> **📚 Navigation**: [← Back to Agent Context Kit](./agent-context.md) | [← Google Vertex AI Integration](./google-vertex-ai.md)

## What Problem Does This Solve?

[Microsoft Copilot Studio](https://www.microsoft.com/en-us/microsoft-copilot/microsoft-copilot-studio) lets you build custom AI agents that work across Microsoft 365, Teams, and other channels. Out of the box, these agents don't know anything about your organization's data assets — they can't search your data catalog, trace lineage, or answer questions about dataset ownership and quality.

By connecting DataHub's MCP server to Copilot Studio, your agents gain direct access to your metadata catalog. Users can ask questions like _"What are the most queried datasets?"_ or _"Who owns the revenue table?"_ and get grounded answers drawn from real metadata.

## Prerequisites

- A [Microsoft Copilot Studio](https://copilotstudio.microsoft.com/) account
- A DataHub Cloud instance (v0.3.12+) or a self-hosted DataHub instance with the [MCP server](../../features/feature-guides/mcp.md) running
- A DataHub [personal access token](../../authentication/personal-access-tokens.md)

## Setup Guide

### Step 1: Create or Open an Agent

1. Navigate to [Copilot Studio](https://copilotstudio.microsoft.com/) and sign in.
2. Create a new agent by clicking **+ Create a blank agent**, or open an existing one.

<p align="center">
  <img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/ai/agent-context/copilot-studio/copilot_studio_agents_page.png"/>
</p>

### Step 2: Add an MCP Tool

1. From your agent's overview page, scroll down to the **Tools** section and click **+ Add tool**.

<p align="center">
  <img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/ai/agent-context/copilot-studio/copilot_studio_agent_overview.png"/>
</p>

2. In the "Add tool" dialog, select **Model Context Protocol** under "Create new".

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/ai/agent-context/copilot-studio/copilot_studio_add_tool.png"/>
</p>

### Step 3: Configure the MCP Server Connection

Fill in the MCP server details:

| Field                  | Value                                                          |
| ---------------------- | -------------------------------------------------------------- |
| **Server name**        | `DataHub MCP Server`                                           |
| **Server description** | `DataHub MCP server to connect to datasets, documents, & more` |
| **Server URL**         | `https://<tenant>.acryl.io/integrations/ai/mcp`                |
| **Authentication**     | `API key`                                                      |
| **Type**               | `Header`                                                       |
| **Header name**        | `Authorization`                                                |
| **Header value**       | `Bearer <your-datahub-token>`                                  |

Replace `<tenant>` with your DataHub Cloud tenant name and `<your-datahub-token>` with your [personal access token](../../authentication/personal-access-tokens.md).

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/ai/agent-context/copilot-studio/copilot_studio_mcp_config.png"/>
</p>

:::note Self-Hosted DataHub
If you're using a self-hosted DataHub instance, you'll need to run the [self-hosted MCP server](../../features/feature-guides/mcp.md#self-hosted-mcp-server-usage) and expose it via a publicly accessible URL. Use that URL as the **Server URL** above.
:::

### Step 4: Add and Configure Tools

1. After entering the connection details, click **Next**. Copilot Studio will discover the available tools from the DataHub MCP server.
2. Review the connection and click **Add and configure** to add the MCP server to your agent.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/ai/agent-context/copilot-studio/copilot_studio_add_and_configure.png"/>
</p>

3. You should now see all available DataHub tools listed. Toggle on the tools you want your agent to have access to.

<p align="center">
  <img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/ai/agent-context/copilot-studio/copilot_studio_tools_list.png"/>
</p>

### Step 5: Test Your Agent

Click the **Test** button in the top-right corner to open the test panel. Try asking your agent a question like:

- _"Find the most important data at my organization"_
- _"What datasets does the analytics team own?"_
- _"Show me the lineage for the revenue dashboard"_

<p align="center">
  <img width="50%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/ai/agent-context/copilot-studio/copilot_studio_test_agent.png"/>
</p>

## Tips

- **Add instructions**: Use the **Instructions** field on the agent overview page to guide how the agent uses DataHub tools — e.g., _"Always search DataHub before answering data questions"_ or _"Include dataset owners in your responses."_
- **Add knowledge**: You can attach additional documents or data sources under the **Knowledge** section to complement DataHub metadata.
- **Publish your agent**: Once you're happy with the agent, click **Publish** to make it available in Teams, your website, or other channels.

## Troubleshooting

### Connection Errors

If Copilot Studio can't connect to the MCP server:

- Verify your DataHub Cloud instance URL is correct (`https://<tenant>.acryl.io/integrations/ai/mcp`)
- Ensure the personal access token is valid and hasn't expired
- Confirm that authentication is set to **API key** (not OAuth 2.0)
- Make sure the header value includes the `Bearer ` prefix (with the trailing space)

### Tools Not Appearing

If tools don't appear after adding the MCP server:

- Click the refresh button on the Tools page to re-discover tools
- Verify the MCP server is running and healthy
- Check that your token has the required permissions

### Empty Results

If the agent returns no results for queries:

- Verify that your DataHub instance has ingested metadata
- Try broader search terms
- Check the [MCP server troubleshooting guide](../../features/feature-guides/mcp.md#troubleshooting) for more details
