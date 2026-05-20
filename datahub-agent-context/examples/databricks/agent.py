#!/usr/bin/env python3
"""
Text-to-SQL agent with Genie Space + DataHub catalog access.

This agent connects to two MCP servers via the Databricks proxy:
  - Managed MCP (Genie Space): natural language to SQL on Databricks tables
  - External MCP (DataHub): catalog search, lineage, ownership, documentation

Deploy as a Databricks App using the ResponsesAgent interface.

Prerequisites:
    pip install databricks-mcp openai mlflow>=2.20.2
    A Unity Catalog HTTP connection named "datahub" pointing to your
    DataHub MCP server (see docs for setup instructions).
    A Genie Space configured in your Databricks workspace.

Environment variables:
    OPENAI_API_KEY: OpenAI API key (or use Databricks Foundation Models)
    DATAHUB_CONNECTION_NAME: UC connection name (default: datahub)
    GENIE_SPACE_ID: Genie Space ID for SQL queries
"""

import json
import os

import mlflow
from databricks.sdk import WorkspaceClient
from databricks_mcp import DatabricksMCPClient
from mlflow.entities import SpanType
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
)
from openai import OpenAI

SYSTEM_PROMPT = """\
You are a data analyst assistant with access to two tool sources:

1. **Genie Space** — Run SQL queries against Databricks tables. Use this when
   users want to query data, generate reports, or analyze specific datasets.

2. **DataHub** — Search the data catalog, explore schemas, trace data lineage,
   find data owners, and look up documentation. Use this when users want to
   understand what data exists, where it comes from, or who is responsible.

When answering questions:
- Search DataHub first to find the right datasets and understand context.
- Use the Genie Space to run SQL queries against those datasets.
- Combine catalog context with query results for complete answers.
- Always provide DataHub URNs so users can find assets in the catalog.
"""


def _collect_mcp_tools(workspace_client: WorkspaceClient):
    """Discover tools from both Genie Space and DataHub MCP servers."""
    host = workspace_client.config.host
    datahub_connection = os.getenv("DATAHUB_CONNECTION_NAME", "datahub")
    genie_space_id = os.getenv("GENIE_SPACE_ID")

    server_urls = [
        f"{host}/api/2.0/mcp/genie/{genie_space_id}",
        f"{host}/api/2.0/mcp/external/{datahub_connection}",
    ]

    all_tools = []
    clients = {}
    for url in server_urls:
        client = DatabricksMCPClient(
            server_url=url, workspace_client=workspace_client
        )
        for tool in client.list_tools():
            all_tools.append(tool)
            clients[tool.name] = client

    return all_tools, clients


class DataHubGeniAgent(ResponsesAgent):
    """Text-to-SQL agent with Genie Space + DataHub catalog access."""

    def __init__(self):
        self.openai = OpenAI()
        self.model = "gpt-4o"
        ws = WorkspaceClient()
        self.tools, self.clients = _collect_mcp_tools(ws)

    @mlflow.trace(span_type=SpanType.AGENT)
    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        messages = [{"role": "system", "content": SYSTEM_PROMPT}]
        for item in request.input:
            messages.append({"role": item["role"], "content": item["content"]})

        tools_spec = [
            {
                "type": "function",
                "function": {
                    "name": t.name,
                    "description": t.description or "",
                    "parameters": t.inputSchema,
                },
            }
            for t in self.tools
        ]

        while True:
            response = self.openai.chat.completions.create(
                model=self.model,
                messages=messages,
                tools=tools_spec if tools_spec else None,
            )
            choice = response.choices[0]

            if choice.finish_reason == "tool_calls":
                messages.append(choice.message)
                for tc in choice.message.tool_calls:
                    mcp_client = self.clients[tc.function.name]
                    args = json.loads(tc.function.arguments)
                    result = mcp_client.call_tool(
                        tc.function.name, args
                    )
                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": tc.id,
                            "content": str(result),
                        }
                    )
            else:
                return ResponsesAgentResponse.from_chat_completion(response)


agent = DataHubGeniAgent()
mlflow.models.set_model(agent)
