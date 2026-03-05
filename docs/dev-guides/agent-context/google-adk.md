# Google ADK Integration

> **📚 Navigation**: [← Back to Agent Context Kit](./agent-context.md) | [LangChain Integration →](./langchain.md)

## What Problem Does This Solve?

Google ADK (Agent Development Kit) is Google's framework for building AI agents with Gemini models, but agents often struggle with data questions because they:

- Don't have access to your organization's data catalog
- Hallucinate table names, schemas, and relationships
- Can't discover data ownership or documentation
- Have no context about data quality or lineage

**The Google ADK integration** provides pre-built tools that give your Gemini-powered agents direct access to DataHub metadata, enabling them to answer data questions accurately using real metadata from your organization.

### What You Can Build

- **Data Discovery Chatbot**: "Show me all datasets about customers in the marketing domain"
- **Schema Explorer**: "What columns exist in the user_events table and what do they mean?"
- **Lineage Tracer**: "What dashboards use data from this raw table?"
- **Documentation Assistant**: "Find the business definition of 'monthly recurring revenue'"
- **Compliance Helper**: "List all tables with PII and their owners"

## Overview

The Google ADK optional add-on lets you connect DataHub's context tools directly to a Google ADK `Agent`. You can also connect via the DataHub MCP server using ADK's built-in `McpToolset` — useful if you prefer to run a standalone MCP server rather than embedding tools in your Python process.

## Installation

```bash
pip install datahub-agent-context[google-adk]
```

### Prerequisites

- Python 3.10 or higher
- Google ADK (`pip install google-adk`)
- DataHub instance with access token
- Google API key (Gemini Developer API) **or** Google Cloud credentials (Vertex AI)

## Quick Start

### Basic Setup

Build AI agents with pre-built Google ADK tools:

```python
from datahub.sdk.main_client import DataHubClient
from datahub_agent_context.google_adk_tools import build_google_adk_tools

# Initialize DataHub client from environment (recommended)
client = DataHubClient.from_env()
# Or specify server and token explicitly:
# client = DataHubClient(server="http://localhost:8080", token="YOUR_TOKEN")

# Build all tools (read-only by default)
tools = build_google_adk_tools(client, include_mutations=False)

# Or include mutation tools for tagging, descriptions, etc.
tools = build_google_adk_tools(client, include_mutations=True)
```

**Note**: `include_mutations=False` provides read-only tools (search, get entities, lineage). Set to `True` to enable tools that modify metadata (add tags, update descriptions, etc.).

## Complete Working Example

Here's a full example of a DataHub-powered Google ADK agent:

```python
import asyncio
import os

from google.adk.agents import Agent
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types

from datahub.sdk.main_client import DataHubClient
from datahub_agent_context.google_adk_tools import build_google_adk_tools

# Initialize DataHub connection
datahub_gms_url = os.getenv("DATAHUB_GMS_URL")
if datahub_gms_url is None:
    client = DataHubClient.from_env()
else:
    client = DataHubClient(
        server=datahub_gms_url, token=os.getenv("DATAHUB_GMS_TOKEN")
    )

# Build DataHub tools (read-only)
tools = build_google_adk_tools(client, include_mutations=False)

# Create agent
agent = Agent(
    model="gemini-2.5-flash",
    name="datahub_agent",
    description="A data discovery assistant with access to DataHub metadata.",
    instruction="""You are a helpful data catalog assistant with access to DataHub metadata.

Use the available tools to:
- Search for datasets, dashboards, and other data assets
- Get detailed entity information including schemas and descriptions
- Trace data lineage to understand data flow
- Find documentation and business glossary terms

Always provide URNs when referencing entities so users can find them in DataHub.
Be concise but thorough in your explanations.""",
    tools=tools,
)


async def ask_datahub(question: str) -> str:
    """Ask a question about your data catalog."""
    session_service = InMemorySessionService()
    session = await session_service.create_session(
        app_name="datahub_agent",
        user_id="user",
    )
    runner = Runner(
        agent=agent,
        app_name="datahub_agent",
        session_service=session_service,
    )

    response_text = ""
    async for event in runner.run_async(
        user_id="user",
        session_id=session.id,
        new_message=types.Content(role="user", parts=[types.Part(text=question)]),
    ):
        if event.is_final_response() and event.content and event.content.parts:
            response_text = event.content.parts[0].text
    return response_text


# Example queries
if __name__ == "__main__":
    # Find datasets
    print(asyncio.run(ask_datahub("Find all datasets about customers")))

    # Get schema information
    print(asyncio.run(ask_datahub("What columns are in the user_events dataset?")))

    # Trace lineage
    print(asyncio.run(ask_datahub("Show me the upstream sources for the revenue dashboard")))

    # Find documentation
    print(asyncio.run(ask_datahub("What's the business definition of 'churn rate'?")))
```

### Example Output

```
Query: Find all datasets about customers

Agent: I found 3 datasets related to customers:

1. **customer_profiles** (urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.analytics.customer_profiles,PROD))
   - Description: Core customer profile data including demographics and preferences
   - Platform: Snowflake
   - Domain: Marketing

2. **customer_transactions** (urn:li:dataset:(urn:li:dataPlatform:postgres,transactions.customer_orders,PROD))
   - Description: Historical customer purchase transactions
   - Platform: PostgreSQL
   - Domain: Finance

3. **customer_360** (urn:li:dataset:(urn:li:dataPlatform:bigquery,analytics.customer_360,PROD))
   - Description: Unified customer view combining profile, transactions, and interactions
   - Platform: BigQuery
   - Domain: Analytics

You can view these in DataHub at: https://your-datahub.acryl.io
```

## Advanced Usage

### Interactive Agent

For a conversational experience with multi-turn support, create a fresh session per turn to keep memory usage bounded:

```python
async def interactive_agent(agent: Agent) -> None:
    session_service = InMemorySessionService()
    runner = Runner(
        agent=agent,
        app_name="datahub_agent",
        session_service=session_service,
    )

    print("Interactive Mode - Type 'quit' to exit")
    while True:
        user_input = input("\nYou: ").strip()
        if not user_input or user_input.lower() in ["quit", "exit", "q"]:
            break

        # New session per turn keeps memory usage bounded
        session = await session_service.create_session(
            app_name="datahub_agent",
            user_id="user",
        )
        response = ""
        async for event in runner.run_async(
            user_id="user",
            session_id=session.id,
            new_message=types.Content(role="user", parts=[types.Part(text=user_input)]),
        ):
            if event.is_final_response() and event.content and event.content.parts:
                response = event.content.parts[0].text
        print(f"\nAgent: {response}")
```

### Connecting via DataHub MCP Server

If you're running the [DataHub MCP Server](../../features/feature-guides/mcp.md), you can connect Google ADK directly to it using `McpToolset` instead of the Python tools builder:

```python
import asyncio

from google.adk.agents import Agent
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.adk.tools.mcp_tool import McpToolset
from google.adk.tools.mcp_tool.mcp_session_manager import StreamableHTTPConnectionParams
from google.genai import types

# Replace with your MCP server URL.
# Use https://<tenant>.acryl.io/integrations/ai/mcp for Datahub cloud
MCP_URL = "http://localhost:8080/mcp?token=YOUR_TOKEN"


async def main() -> None:
    session_service = InMemorySessionService()
    session = await session_service.create_session(
        app_name="datahub_mcp_agent",
        user_id="user",
    )

    toolset = McpToolset(
        connection_params=StreamableHTTPConnectionParams(url=MCP_URL),
    )
    # Eagerly initialize the MCP session so the AsyncExitStack is owned here —
    # otherwise ADK creates it in a spawned task and close() raises an error.
    await toolset.get_tools()

    agent = Agent(
        model="gemini-2.5-flash",
        name="datahub_agent",
        instruction="You help users find datasets in DataHub. Provide clear, concise answers.",
        tools=[toolset],
    )

    runner = Runner(
        agent=agent,
        app_name="datahub_mcp_agent",
        session_service=session_service,
    )

    try:
        async for event in runner.run_async(
            user_id="user",
            session_id=session.id,
            new_message=types.Content(
                role="user",
                parts=[types.Part(text="Find datasets about users")],
            ),
        ):
            if event.is_final_response() and event.content and event.content.parts:
                print(f"Agent: {event.content.parts[0].text}")
    finally:
        await toolset.close()


if __name__ == "__main__":
    asyncio.run(main())
```

### Using Vertex AI Instead of Gemini Developer API

To use Vertex AI (Application Default Credentials) instead of an API key:

```python
import os

# Do not set GOOGLE_API_KEY — ADK falls back to ADC automatically
# Ensure your environment is authenticated: gcloud auth application-default login

agent = Agent(
    model="gemini-2.5-flash",  # or a Vertex AI model ID
    name="datahub_agent",
    instruction="...",
    tools=tools,
)
```

## More Examples

Complete examples are available in the datahub-project repo:

- [Basic Agent](https://github.com/datahub-project/datahub/blob/master/datahub-agent-context/examples/google_adk/basic_agent.py)
- [Simple Search](https://github.com/datahub-project/datahub/blob/master/datahub-agent-context/examples/google_adk/simple_search.py)

## Troubleshooting

### Tool Execution Errors

**Problem**: Agent tries to use tools but gets errors

**Solutions**:

- Verify DataHub connection: Test `client.config` is correctly set
- Check tool availability: Print `[tool.name for tool in tools]` to see available tools
- Validate token permissions: Ensure token has read access (and write access if using mutations)
- Set `GOOGLE_GENAI_USE_VERTEXAI=0` if mixing Gemini Developer API and Vertex AI configs

### Agent Not Using Tools

**Problem**: Agent responds without calling DataHub tools

**Solutions**:

- Improve the `instruction` prompt: explicitly instruct the agent to use tools
- Check model compatibility: Gemini 2.0+ models have the best tool-calling support
- Reduce temperature: ADK uses `temperature=0` by default for tool-heavy agents

### AsyncExitStack / Task Errors

**Problem**: `Attempted to exit cancel scope in a different task` when using `McpToolset`

**Solution**: Call `await toolset.get_tools()` in the same async task that owns the toolset before passing it to the `Agent`, and call `await toolset.close()` in a `finally` block.

### Performance Issues

**Problem**: Agent responses are slow

**Solutions**:

- Limit search results: Use `num_results` filter in tool calls
- Use `gemini-2.0-flash` or `gemini-2.5-flash` for faster responses
- Cache results: Implement caching for frequently accessed metadata

### Import Errors

**Problem**: `ModuleNotFoundError` for Google ADK components

**Solutions**:

```bash
# Install all required dependencies
pip install datahub-agent-context[google-adk]
pip install google-adk

# Or install from scratch
pip install datahub-agent-context google-adk google-genai
```

### Getting Help

- **Google ADK Docs**: [Google ADK Documentation](https://google.github.io/adk-docs/)
- **DataHub Agent Context**: [Agent Context Kit Guide](./agent-context.md)
- **GitHub Issues**: [Report issues](https://github.com/datahub-project/datahub/issues)
- **Community Slack**: [Join DataHub Slack](https://datahub.com/slack/)
