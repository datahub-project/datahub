# LangChain

Build autonomous data agents with [LangChain](https://python.langchain.com/) that are grounded in your enterprise data context from DataHub — ownership, lineage, documentation, quality signals, and more.

## Prerequisites

- Python 3.10+
- A DataHub instance and [access token](../../authentication/personal-access-tokens.md)
- An OpenAI API key (or another LLM provider with tool-calling support)

## Installation

```bash
pip install "datahub-agent-context[langchain]" langchain-openai
```

The `[langchain]` extra includes `langchain` and `langchain-core`. `langchain-openai` is a separate install for the OpenAI provider shown below.

## Quick Start

```python
from datahub.sdk.main_client import DataHubClient
from datahub_agent_context.langchain_tools import build_langchain_tools

client = DataHubClient.from_env()

# Read-only tools by default; set include_mutations=True for write operations
tools = build_langchain_tools(client, include_mutations=False)
```

Wire the tools into a LangChain agent:

```python
from langchain_openai import ChatOpenAI
from langchain.agents import create_agent

llm = ChatOpenAI(model="gpt-4o", temperature=0)  # any tool-calling model works here

agent = create_agent(
    llm,
    tools=tools,
    system_prompt="You are a data catalog assistant. Use the available tools to search for datasets, get entity details, and trace lineage. Always include URNs in your answers.",
)

response = agent.invoke({"messages": [{"role": "user", "content": "Find all datasets about customers"}]})
print(response["messages"][-1].content)
```

**Prefer Bedrock?** Swap only the model lines — `tools`, `create_agent`, and `invoke` stay identical:

```python
import boto3
from langchain_aws import ChatBedrock  # pip install langchain-aws boto3

bedrock_runtime = boto3.client(service_name="bedrock-runtime", region_name="us-west-2")
llm = ChatBedrock(
    client=bedrock_runtime,
    model_id="us.anthropic.claude-haiku-4-5-20251001-v1:0",
    model_kwargs={"max_tokens": 2048, "temperature": 0},
)
```

Full working example: [simple_search.py](https://github.com/datahub-project/datahub/blob/master/datahub-agent-context/examples/langchain/simple_search.py)

## Troubleshooting

- **Tool execution errors?** Verify your DataHub connection with `client.test_connection()` and check token permissions.
- **Agent not using tools?** Strengthen the system prompt, or try `temperature=0` for more deterministic tool use.
- **Import errors?** Run `pip install "datahub-agent-context[langchain]" langchain-openai`.

**Links:** [LangChain Agent Docs](https://docs.langchain.com/oss/python/langchain/agents) · [Agent Context Kit](./agent-context.md) · [MCP Server Guide](../../features/feature-guides/mcp.md)
