# Google Vertex AI Integration

> **📚 Navigation**: [← Back to Agent Context Kit](./agent-context.md) | [← Google ADK Integration](./google-adk.md) | [Copilot Studio Integration →](./copilot-studio.md)

## What Problem Does This Solve?

Google Vertex AI Agent Builder provides a visual, low-code environment for building AI agents — but agents often struggle with data questions because they:

- Don't have access to your organization's data catalog
- Hallucinate table names, schemas, and relationships
- Can't discover data ownership or documentation
- Have no context about data quality or lineage

**The Vertex AI integration** lets you connect the DataHub MCP server to your Vertex AI agent visually, enabling your agents to answer data questions accurately using real metadata from your organization.

## Overview

[Agent Designer](https://docs.cloud.google.com/agent-builder/agent-designer) is a low-code visual designer built into the Google Cloud Console as part of Vertex AI Agent Builder. It lets you design and test agents in the browser, then export the generated code for further development.

> **Note**: Agent Designer is currently a **preview feature**.

## Connecting DataHub via the Agent Designer UI

### Prerequisites

- A Google Cloud project with Vertex AI Agent Builder enabled
- A running DataHub instance with the [MCP server](../../features/feature-guides/mcp.md) enabled
- Your DataHub MCP server URL (e.g., `https://<tenant>.acryl.io/integrations/ai/mcp`)

### Steps

1. Open the **Agent Designer** in the [Google Cloud Console](https://console.cloud.google.com/gen-app-builder/agents).
2. Click **"Create agent"** to open the visual canvas.
3. Configure the agent:
   - Set a **name** and **description** for your agent.
   - Write **instructions** to guide the agent's behavior (e.g., "You are a data catalog assistant. Use DataHub tools to find datasets, schemas, and lineage.").
   - Select your preferred **model** (e.g., Gemini 2.5 Flash).
4. Click **"Add tools"** and select **"MCP Server"**.
5. Enter a display name (e.g., `DataHub`) and your DataHub MCP endpoint URL.
6. Click **"Save"** — Agent Designer will discover all available tools from the MCP server automatically.
7. Use the **Preview tab** to test your agent by chatting with it.

### MCP Authentication Limitation

> **Important**: The Agent Designer UI only supports MCP servers that do **not** require authentication. If your DataHub MCP server requires a bearer token (as DataHub Cloud does), the UI cannot pass authentication headers.

To work around this, use the **"Get code"** button in Agent Designer to export the generated agent code, then add the `Authorization` header manually:

```python
from google.adk.tools.mcp_tool import McpToolset
from google.adk.tools.mcp_tool.mcp_session_manager import StreamableHTTPConnectionParams

MCP_URL = "https://<tenant>.acryl.io/integrations/ai/mcp"
YOUR_TOKEN = "<your-datahub-token>"

toolset = McpToolset(
    connection_params=StreamableHTTPConnectionParams(url=MCP_URL),
    # Add the Authorization header here — the UI cannot do this for you
    headers={"Authorization": f"Bearer {YOUR_TOKEN}"},
)
```

See the [Google ADK Integration](./google-adk.md#connecting-via-datahub-mcp-server) page for a complete working example with authentication.

## Exporting to Code

Once you're happy with your agent design, click **"Get code"** to view the full source code representation of your agent. You can copy this code into your editor and continue development using:

- The **Google ADK** — see the [Google ADK Integration](./google-adk.md) guide
- Frameworks such as LangChain or LangGraph

This allows you to start with the visual designer for rapid prototyping and then transition to code for production-grade agents that require authentication, custom logic, or CI/CD deployment.

## Getting Help

- **Agent Designer Docs**: [Google Cloud Agent Designer](https://docs.cloud.google.com/agent-builder/agent-designer)
- **Google ADK Docs**: [Google ADK Documentation](https://google.github.io/adk-docs/)
- **DataHub Agent Context**: [Agent Context Kit Guide](./agent-context.md)
- **GitHub Issues**: [Report issues](https://github.com/datahub-project/datahub/issues)
- **Community Slack**: [Join DataHub Slack](https://datahub.com/slack/)
