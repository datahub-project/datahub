# Google ADK

Build autonomous data agents with [Google ADK](https://google.github.io/adk-docs/) that are grounded in your enterprise data context from DataHub — ownership, lineage, documentation, quality signals, and more.

The integration works two ways:

- **Python tools** — embed DataHub tools directly in your ADK agent
- **[MCP server](../../features/feature-guides/mcp.md)** — connect via ADK's built-in `McpToolset`

## Prerequisites

- Python 3.10+
- Google ADK (`pip install google-adk`)
- A DataHub instance and [access token](../../authentication/personal-access-tokens.md)
- A Google API key (Gemini Developer API) **or** Google Cloud credentials (Vertex AI)

## Installation

```bash
pip install datahub-agent-context[google-adk]
```

## Quick Start

```python
from datahub.sdk.main_client import DataHubClient
from datahub_agent_context.google_adk_tools import build_google_adk_tools

client = DataHubClient.from_env()

# Read-only tools by default; set include_mutations=True for write operations
tools = build_google_adk_tools(client, include_mutations=False)
```

Wire the tools into an ADK `Agent`:

```python
from google.adk.agents import Agent

agent = Agent(
    model="gemini-2.5-flash",
    name="datahub_agent",
    description="A data discovery assistant with access to DataHub.",
    instruction="Use the available tools to search for datasets, get entity details, and trace lineage. Always include URNs in your answers.",
    tools=tools,
)
```

Full working examples: [basic_agent.py](https://github.com/datahub-project/datahub/blob/master/datahub-agent-context/examples/google_adk/basic_agent.py) · [simple_search.py](https://github.com/datahub-project/datahub/blob/master/datahub-agent-context/examples/google_adk/simple_search.py)

## Connecting via MCP Server

Instead of embedding tools, you can connect ADK to the DataHub [MCP server](../../features/feature-guides/mcp.md) using `McpToolset`:

```python
from google.adk.tools.mcp_tool import McpToolset
from google.adk.tools.mcp_tool.mcp_session_manager import StreamableHTTPConnectionParams

toolset = McpToolset(
    connection_params=StreamableHTTPConnectionParams(
        url="https://<tenant>.acryl.io/integrations/ai/mcp"
    ),
    headers={"Authorization": f"Bearer {YOUR_TOKEN}"},
)
# Initialize eagerly so the AsyncExitStack is owned in this task
await toolset.get_tools()

agent = Agent(
    model="gemini-2.5-flash",
    name="datahub_agent",
    instruction="You help users find datasets in DataHub.",
    tools=[toolset],
)
```

Call `await toolset.close()` when done.

### Using Vertex AI Instead of Gemini Developer API

Don't set `GOOGLE_API_KEY` — ADK falls back to Application Default Credentials automatically. Make sure you've run `gcloud auth application-default login`.

## Troubleshooting

- **Tool execution errors?** Verify your DataHub connection (`client.config`) and token permissions.
- **Agent not using tools?** Strengthen the `instruction` prompt, or try a model with better tool-calling (Gemini 2.0+).
- **`AsyncExitStack` / task errors with `McpToolset`?** Call `await toolset.get_tools()` in the same async task that owns the toolset, and `await toolset.close()` in a `finally` block.
- **Import errors?** Run `pip install datahub-agent-context[google-adk] google-adk`.

**Links:** [Google ADK Docs](https://google.github.io/adk-docs/) · [Agent Context Kit](./agent-context.md) · [MCP Server Guide](../../features/feature-guides/mcp.md)
