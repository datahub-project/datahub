# LangChain Integration

> **📚 Navigation**: [← Back to Agent Context Kit](./agent-context.md) | [Snowflake Integration →](./snowflake.md)

## What Problem Does This Solve?

LangChain is a popular framework for building AI agent applications, but agents often struggle with data questions because they:

- Don't have access to your organization's data catalog
- Hallucinate table names, schemas, and relationships
- Can't discover data ownership or documentation
- Have no context about data quality or lineage

**The LangChain integration** provides pre-built LangChain tools that give your agents direct access to DataHub metadata, enabling them to answer data questions accurately using real metadata from your organization.

### What You Can Build

- **Data Discovery Chatbot**: "Show me all datasets about customers in the marketing domain"
- **Schema Explorer**: "What columns exist in the user_events table and what do they mean?"
- **Lineage Tracer**: "What dashboards use data from this raw table?"
- **Documentation Assistant**: "Find the business definition of 'monthly recurring revenue'"
- **Compliance Helper**: "List all tables with PII and their owners"

## Overview

The LangChain optional add-on lets you connect DataHub's context tools directly to your LangChain Agent.

## Installation

```bash
pip install datahub-agent-context[langchain]
```

### Prerequisites

- Python 3.10 or higher
- LangChain (`pip install langchain langchain-openai`)
- DataHub instance with access token
- OpenAI API key (or other LLM provider)

## Quick Start

### Basic Setup

Build AI agents with pre-built LangChain tools:

```python
from datahub.sdk.main_client import DataHubClient
from datahub_agent_context.langchain_tools import build_langchain_tools

# Initialize DataHub client from environment (recommended)
client = DataHubClient.from_env()
# Or specify server and token explicitly:
# client = DataHubClient(server="http://localhost:8080", token="YOUR_TOKEN")

# Build all tools (read-only by default)
tools = build_langchain_tools(client, include_mutations=False)

# Or include mutation tools for tagging, descriptions, etc.
tools = build_langchain_tools(client, include_mutations=True)
```

**Note**: `include_mutations=False` provides read-only tools (search, get entities, lineage). Set to `True` to enable tools that modify metadata (add tags, update descriptions, etc.).

## Complete Working Example

Here's a full example of a DataHub-powered LangChain agent:

```python
from datahub.sdk.main_client import DataHubClient
from datahub_agent_context.langchain_tools import build_langchain_tools
from langchain_openai import ChatOpenAI
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain.prompts import ChatPromptTemplate

# Initialize DataHub client
client = DataHubClient.from_env()
# Or: client = DataHubClient(server="http://localhost:8080", token="YOUR_TOKEN")

# Build DataHub tools (read-only)
tools = build_langchain_tools(client, include_mutations=False)

# Initialize LLM
llm = ChatOpenAI(
    model="gpt-4",
    temperature=0,
    openai_api_key="YOUR_OPENAI_KEY"
)

# Create agent prompt
prompt = ChatPromptTemplate.from_messages([
    ("system", """You are a helpful data catalog assistant with access to DataHub metadata.

    Use the available tools to:
    - Search for datasets, dashboards, and other data assets
    - Get detailed entity information including schemas and descriptions
    - Trace data lineage to understand data flow
    - Find documentation and business glossary terms

    Always provide URNs when referencing entities so users can find them in DataHub.
    Be concise but thorough in your explanations."""),
    ("human", "{input}"),
    ("placeholder", "{agent_scratchpad}"),
])

# Create agent
agent = create_tool_calling_agent(llm, tools, prompt)
agent_executor = AgentExecutor(
    agent=agent,
    tools=tools,
    verbose=True,
    handle_parsing_errors=True
)

# Use the agent
def ask_datahub(question: str):
    """Ask a question about your data catalog."""
    result = agent_executor.invoke({"input": question})
    return result["output"]

# Example queries
if __name__ == "__main__":
    # Find datasets
    print(ask_datahub("Find all datasets about customers"))

    # Get schema information
    print(ask_datahub("What columns are in the user_events dataset?"))

    # Trace lineage
    print(ask_datahub("Show me the upstream sources for the revenue dashboard"))

    # Find documentation
    print(ask_datahub("What's the business definition of 'churn rate'?"))
```

### Example Output

```
User: Find all datasets about customers

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

### Using with Memory

Add conversation memory to your agent:

```python
from langchain.memory import ConversationBufferMemory

memory = ConversationBufferMemory(
    memory_key="chat_history",
    return_messages=True
)

agent_executor = AgentExecutor(
    agent=agent,
    tools=tools,
    memory=memory,
    verbose=True
)
```

### Filtering Tools

Include only specific tool categories:

```python
from datahub_agent_context.langchain_tools import (
    build_search_tools,
    build_entity_tools,
    build_lineage_tools
)

# Only search and entity tools
search_tools = build_search_tools(client)
entity_tools = build_entity_tools(client)
tools = search_tools + entity_tools
```

### Custom Tool Configuration

```python
# Configure specific tools
tools = build_langchain_tools(
    client,
    include_mutations=False,
    max_search_results=20,  # Increase search result limit
    max_lineage_depth=5      # Deeper lineage traversal
)
```

## More Examples

Complete examples are available in the datahub-project repo:

- [Basic Agent](https://github.com/datahub-project/datahub/blob/master/datahub-agent-context/examples/langchain/basic_agent.py)
- [Simple Search](https://github.com/datahub-project/datahub/blob/master/datahub-agent-context/examples/langchain/simple_search.py)

## Troubleshooting

### Tool Execution Errors

**Problem**: Agent tries to use tools but gets errors

**Solutions**:

- Verify DataHub connection: Test `client.config` is correctly set
- Check tool availability: Print `[tool.name for tool in tools]` to see available tools
- Enable verbose mode: Set `verbose=True` in AgentExecutor to see tool calls
- Validate token permissions: Ensure token has read access (and write access if using mutations)

### Agent Not Using Tools

**Problem**: Agent responds without calling DataHub tools

**Solutions**:

- Improve system prompt: Explicitly instruct agent to use tools
- Use better examples: Add few-shot examples of tool usage to prompt
- Check model compatibility: Ensure LLM supports tool/function calling (GPT-4, GPT-3.5-turbo, Claude 3+)
- Reduce temperature: Set `temperature=0` for more deterministic tool usage

### Performance Issues

**Problem**: Agent responses are slow

**Solutions**:

- Limit search results: Reduce `max_search_results` in tool configuration
- Use streaming: Enable streaming responses with `stream=True`
- Cache results: Implement caching for frequently accessed metadata
- Use faster models: Consider gpt-3.5-turbo for simpler queries

### Import Errors

**Problem**: `ModuleNotFoundError` for LangChain components

**Solutions**:

```bash
# Install all required dependencies
pip install datahub-agent-context[langchain]
pip install langchain langchain-openai

# Or install from scratch
pip install datahub-agent-context langchain langchain-openai langchain-community
```

### Getting Help

- **LangChain Docs**: [LangChain Agent Documentation](https://python.langchain.com/docs/modules/agents/)
- **DataHub Agent Context**: [Agent Context Kit Guide](./agent-context.md)
- **GitHub Issues**: [Report issues](https://github.com/datahub-project/datahub/issues)
- **Community Slack**: [Join DataHub Slack](https://datahub.com/slack/)
