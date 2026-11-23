# datahub-core-mcp-tools

DataHub's [Model Context Protocol](https://modelcontextprotocol.io) tools. The `datahub-ai-service`
service serves them over HTTP at `/mcp`, and DataHub Cloud builds the same source.

The package has no server of its own. A host creates a FastMCP instance, owns transport and
authentication, and registers the tools on it:

```python
from fastmcp import FastMCP

from datahub_core_mcp_tools.graphql_helpers import with_datahub_client
from datahub_core_mcp_tools.mcp_server import register_tools

mcp = FastMCP("datahub")
register_tools(mcp, is_oss=True)
```

Tools read their DataHub client from the request context. The host sets it for each request,
built from the caller's own credential, with `with_datahub_client(client)`.

## Development

```bash
../../gradlew :packages:datahub-core-mcp-tools:lintFix
../../gradlew :packages:datahub-core-mcp-tools:lint
../../gradlew :packages:datahub-core-mcp-tools:testQuick
```
