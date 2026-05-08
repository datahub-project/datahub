# Databricks Agent Bricks

Build a deployed agent on Databricks that combines SQL execution (via a [Genie Space](https://docs.databricks.com/en/genie/set-up-genie-space.html)) with DataHub's catalog context in a single conversation. The agent discovers tools from both [MCP](../../features/feature-guides/mcp.md) endpoints and the LLM decides which to call per request — search DataHub to find the right table, then query the Genie Space for the actual data.

## Prerequisites

- Everything in [Databricks Genie Code — Prerequisites & Step 1](./databricks-genie-code.md#prerequisites) (the UC connection setup is shared)
- A [Genie Space](https://docs.databricks.com/en/genie/set-up-genie-space.html) configured with the tables you want the agent to query
- Python 3.10+
- An OpenAI API key (or a [Databricks Foundation Model](https://docs.databricks.com/en/machine-learning/model-serving/score-foundation-models.html) endpoint)
- The [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/install.html) (for local testing)

## Create a Genie Space

You need a [Genie Space](https://docs.databricks.com/en/genie/set-up-genie-space.html) with the tables you want the agent to query. If you don't have one yet, create one in the Databricks UI under **Genie**. Grab the **Space ID** from the URL: `https://<workspace>.databricks.com/genie/rooms/<space-id>`.

## Build the Agent

Full example: [agent.py](https://github.com/datahub-project/datahub/blob/master/datahub-agent-context/examples/databricks/agent.py)

The core idea — connect to both MCP endpoints and build a unified tool registry:

```python
def _collect_mcp_tools(workspace_client: WorkspaceClient):
    host = workspace_client.config.host
    server_urls = [
        f"{host}/api/2.0/mcp/genie/{os.getenv('GENIE_SPACE_ID')}",
        f"{host}/api/2.0/mcp/external/{os.getenv('DATAHUB_CONNECTION_NAME', 'datahub')}",
    ]
    all_tools, clients = [], {}
    for url in server_urls:
        client = DatabricksMCPClient(server_url=url, workspace_client=workspace_client)
        for tool in client.list_tools():
            all_tools.append(tool)
            clients[tool.name] = client
    return all_tools, clients
```

## Run It

```bash
export GENIE_SPACE_ID="your-genie-space-id"          # from the Genie Space URL (see above)
export DATAHUB_CONNECTION_NAME="datahub"              # name of your UC connection
export OPENAI_API_KEY="sk-..."                        # or use Databricks Foundation Models

databricks auth login --host https://your-workspace.cloud.databricks.com
uv run start-app
# Chat at http://localhost:8000
```

For deployment, see Databricks' [Apps deployment guide](https://docs.databricks.com/en/dev-tools/databricks-apps/index.html). You can also swap OpenAI for a [Databricks Foundation Model](https://docs.databricks.com/en/machine-learning/model-serving/score-foundation-models.html) by pointing the OpenAI client at your workspace's serving endpoint.

## Example

> **User:** What are our top customer segments? Who owns that data?
>
> **Agent:** _(searches DataHub, then queries the Genie Space)_
>
> The `customer_segments` table is owned by the Marketing Analytics team. Top segments: High-Value Loyalists (45k), At-Risk Champions (32k), New Engaged (28k).

For a full list of available DataHub tools, see the [Agent Context Kit](./agent-context.md#available-tools).

## Troubleshooting

- **DataHub not showing up?** Check the connection under **Catalog > Connections**, verify the MCP URL and base path, and confirm the bearer token is still valid.
- **`401 Unauthorized`?** Regenerate the DataHub access token and update the UC connection.
- **No tools discovered?** Make sure DataHub's [MCP server](../../features/feature-guides/mcp.md) is enabled. Base path is `/integrations/ai/mcp` (Cloud) or `/mcp` (self-hosted).

**Links:** [MCP on Databricks](https://docs.databricks.com/en/generative-ai/agent-framework/mcp.html) · [DataHub MCP Server](../../features/feature-guides/mcp.md) · [Agent Context Kit](./agent-context.md)
