# Langchain

The Langchain optional add-on lets you connect datahub's context tools directly to your Langchain Agent

### Installation

```
pip install datahub-agent-context[langchain]
```

### Example

Build AI agents with pre-built LangChain tools:

```python

from datahub.sdk.main_client import DataHubClient
from datahub_agent_context.langchain_tools import build_langchain_tools
from langchain.agents import create_agent

# Initialize DataHub client
client = DataHubClient.from_env()

# Build all tools (read-only by default)
tools = build_langchain_tools(client, include_mutations=False)

# Or include mutation tools for tagging, descriptions, etc.
tools = build_langchain_tools(client, include_mutations=True)

# Create agent
agent = create_agent(model, tools=tools, system_prompt="...")

```

Examples are available in the datahub-project repo [Example Code](https://github.com/datahub-project/datahub/blob/master/datahub-agent-context/examples/langchain/basic_agent.py)
