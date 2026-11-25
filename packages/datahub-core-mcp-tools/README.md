# datahub-core-mcp-tools

DataHub's [Model Context Protocol](https://modelcontextprotocol.io) tools. The `datahub-ai-service`
service serves them over HTTP at `/mcp`, and DataHub Cloud builds the same source.

A host creates a FastMCP instance, owns transport and authentication, and registers the tools
on it:

```python
from fastmcp import FastMCP

from datahub_core_mcp_tools.graphql_helpers import with_datahub_client
from datahub_core_mcp_tools.mcp_server import register_tools

mcp = FastMCP("datahub")
register_tools(mcp, is_oss=True)
```

Tools read their DataHub client from the request context. The host sets it for each request,
built from the caller's own credential, with `with_datahub_client(client)`.

## Run locally over stdio

`datahub-mcp-stdio` serves the same tools to one local user over stdin/stdout, with no service to
install. Point an MCP client (an IDE, Claude Desktop, an agent) at it:

```json
{
  "mcpServers": {
    "datahub": {
      "command": "uvx",
      "args": ["--from", "datahub-core-mcp-tools", "datahub-mcp-stdio"],
      "env": {
        "DATAHUB_GMS_URL": "http://localhost:8080",
        "DATAHUB_GMS_TOKEN": "<personal access token>"
      }
    }
  }
}
```

Without `DATAHUB_GMS_URL` it uses the `~/.datahubenv` that `datahub init` writes. No token is
needed when DataHub's authentication is off. The service's switches choose the tools
(`TOOLS_IS_MUTATION_ENABLED=false` hides every write tool, plus the per-tool flags), and tools
the connected DataHub is too old for are hidden. It sends anonymous usage telemetry like the
DataHub CLI; `DATAHUB_TELEMETRY_ENABLED=false` turns it off.

To embed the tools in your own process (an agent, a test), build the server around a client:

```python
from datahub.sdk.main_client import DataHubClient

from datahub_core_mcp_tools.stdio import build_stdio_server

mcp = build_stdio_server(DataHubClient.from_env(), groups=["read_metadata"])
```

## Development

```bash
../../gradlew :packages:datahub-core-mcp-tools:lintFix
../../gradlew :packages:datahub-core-mcp-tools:lint
../../gradlew :packages:datahub-core-mcp-tools:testQuick
```
