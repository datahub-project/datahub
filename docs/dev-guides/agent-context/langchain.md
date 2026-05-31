# LangChain

Build autonomous data agents with [LangChain](https://python.langchain.com/) that are grounded in your enterprise data context from DataHub — ownership, lineage, documentation, quality signals, and more.

## Prerequisites

- Python 3.10+
- LangChain (`pip install langchain langchain-openai`)
- A DataHub instance and [access token](../../authentication/personal-access-tokens.md)
- An OpenAI API key (or another LLM provider with tool-calling support)

## Installation

```bash
pip install datahub-agent-context[langchain]
```

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
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain.prompts import ChatPromptTemplate

llm = ChatOpenAI(model="gpt-4", temperature=0)

prompt = ChatPromptTemplate.from_messages([
    ("system", "You are a data catalog assistant. Use the available tools to search for datasets, get entity details, and trace lineage. Always include URNs in your answers."),
    ("human", "{input}"),
    ("placeholder", "{agent_scratchpad}"),
])

agent = create_tool_calling_agent(llm, tools, prompt)
executor = AgentExecutor(agent=agent, tools=tools, verbose=True)

result = executor.invoke({"input": "Find all datasets about customers"})
```

Full working examples: [basic_agent.py](https://github.com/datahub-project/datahub/blob/master/datahub-agent-context/examples/langchain/basic_agent.py) · [simple_search.py](https://github.com/datahub-project/datahub/blob/master/datahub-agent-context/examples/langchain/simple_search.py)

## Troubleshooting

- **Tool execution errors?** Verify your DataHub connection (`client.config`) and token permissions. Set `verbose=True` on the `AgentExecutor` to see tool calls.
- **Agent not using tools?** Strengthen the system prompt, or try `temperature=0` for more deterministic tool use.
- **Import errors?** Run `pip install datahub-agent-context[langchain] langchain langchain-openai`.

**Links:** [LangChain Agent Docs](https://python.langchain.com/docs/modules/agents/) · [Agent Context Kit](./agent-context.md) · [MCP Server Guide](../../features/feature-guides/mcp.md)
