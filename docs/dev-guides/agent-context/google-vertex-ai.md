# Google Vertex AI

Give [Agent Designer](https://docs.cloud.google.com/agent-builder/agent-designer) agents access to your enterprise data context in DataHub — prototype data agents visually using the low-code builder in Vertex AI Agent Builder.

> **Note**: Agent Designer is currently a **preview feature**.

## Prerequisites

- A Google Cloud project with Vertex AI Agent Builder enabled
- A DataHub instance with the [MCP server](../../features/feature-guides/mcp.md) enabled

## Setup

1. Open [Agent Designer](https://console.cloud.google.com/gen-app-builder/agents) and click **Create agent**.
2. Set a **name**, **instructions** (e.g., "You are a data catalog assistant. Use DataHub tools to find datasets, schemas, and lineage."), and pick a **model** (e.g., Gemini 2.5 Flash).
3. Click **Add tools** → **MCP Server**.
4. Enter a display name (e.g., `DataHub`) and your MCP endpoint URL.
5. Click **Save** — Agent Designer discovers the tools automatically.
6. Use the **Preview tab** to test.

### MCP Authentication Limitation

The Agent Designer UI only supports MCP servers that do **not** require authentication. If your DataHub instance requires a bearer token, use the **Get code** button to export the agent, then add the `Authorization` header manually:

```python
from google.adk.tools.mcp_tool import McpToolset
from google.adk.tools.mcp_tool.mcp_session_manager import StreamableHTTPConnectionParams

toolset = McpToolset(
    connection_params=StreamableHTTPConnectionParams(
        url="https://<tenant>.acryl.io/integrations/ai/mcp"
    ),
    headers={"Authorization": f"Bearer {YOUR_TOKEN}"},
)
```

See the [Google ADK Integration](./google-adk.md#connecting-via-mcp-server) for a complete working example.

## Exporting to Code

Click **Get code** to export your agent as Python, then continue development with the [Google ADK](./google-adk.md) or [LangChain](./langchain.md). This lets you prototype visually and transition to code for production.

**Links:** [Agent Designer Docs](https://docs.cloud.google.com/agent-builder/agent-designer) · [Google ADK Integration](./google-adk.md) · [Agent Context Kit](./agent-context.md)
