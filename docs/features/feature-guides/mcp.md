# DataHub MCP Server

The DataHub MCP Server implements the [Model Context Protocol (MCP)](https://modelcontextprotocol.io/introduction), giving AI agents direct access to your DataHub metadata. Search for data assets, traverse lineage, inspect schemas, and generate SQL — all through natural language in tools like Cursor, Windsurf, Claude Desktop, and OpenAI.

Want to learn more about the motivation, architecture, and advanced use cases? Check out our [deep dive blog post](https://datahub.com/blog/datahub-mcp-server-block-ai-agents-use-case/).

## Deployment Options

- [Managed MCP Server](#managed-mcp-server-usage) - Available on DataHub Cloud v0.3.12+
- [Self-Hosted MCP Server](#self-hosted-mcp-server-usage) - Available for DataHub Core

## Capabilities

**Search for Data** <br />
Find the right data by asking questions in plain English. Supports wildcard matching (`revenue_*`), field searches (`tag:PII`), and boolean logic (`(sales OR revenue) AND quarterly`).

**Dive Deeper** <br />
Get usage stats, ownership, documentation, tags, glossary terms, and quality signals for any table, column, dashboard, & more — so agents can separate signal from noise.

**Lineage & Impact Analysis** <br />
Trace data flow at table and column level, upstream or downstream, across multiple hops. Understand the origins of your data, and plan for upcoming changes.

**Query Analysis & Authoring** <br />
Surface real SQL queries that reference a dataset — see join patterns, common filters, and aggregation behavior — then generate new queries grounded in actual usage.

**Works Where You Work** <br />
Seamlessly integrates with Cursor, Windsurf, Claude Desktop, OpenAI, and any other MCP-compatible client.

## Tools

The DataHub MCP Server provides the following tools:

`search`

Search DataHub using structured keyword search (/q syntax) with boolean logic, filters, pagination, and optional sorting by usage metrics.

`get_lineage`

Retrieve upstream or downstream lineage for any entity (datasets, columns, dashboards, etc.) with filtering, query-within-lineage, pagination, and hop control.

`get_dataset_queries`

Fetch real SQL queries referencing a dataset or column—manual or system-generated—to understand usage patterns, joins, filters, and aggregation behavior.

`get_entities`

Fetch detailed metadata for one or more entities by URN; supports batch retrieval for efficient inspection of search results.

`list_schema_fields`

List schema fields for a dataset with keyword filtering and pagination, useful when search results truncate fields or when exploring large schemas.

`get_lineage_paths_between`

Retrieve the exact lineage paths between two assets or columns, including intermediate transformations and SQL query information.

### Mutation Tools

:::info
Mutation tools are available in [mcp-server-datahub](https://github.com/acryldata/mcp-server-datahub) v0.5.0+. They are enabled via the `TOOLS_IS_MUTATION_ENABLED=true` environment variable.
:::

`add_tags` / `remove_tags`

Add or remove tags from entities or schema fields (columns). Supports bulk operations on multiple entities.

`add_terms` / `remove_terms`

Add or remove glossary terms from entities or schema fields. Useful for applying business definitions and data classification.

`add_owners` / `remove_owners`

Add or remove ownership assignments from entities. Supports different ownership types (technical owner, data owner, etc.).

`set_domains` / `remove_domains`

Assign or remove domain membership for entities. Each entity can belong to one domain.

`update_description`

Update, append to, or remove descriptions for entities or schema fields. Supports markdown formatting.

`add_structured_properties` / `remove_structured_properties`

Manage structured properties (typed metadata fields) on entities. Supports string, number, URN, date, and rich text value types.

### User Tools

:::info
User tools are available in [mcp-server-datahub](https://github.com/acryldata/mcp-server-datahub) v0.5.0+. They are enabled via the `TOOLS_IS_USER_ENABLED=true` environment variable.
:::

`get_me`

Retrieve information about the currently authenticated user, including profile details and group memberships.

### Document Tools

:::info
Document tools are available in [mcp-server-datahub](https://github.com/acryldata/mcp-server-datahub) v0.5.0+. Document tools are automatically hidden if no documents exist in the catalog.
:::

`search_documents`

Search for documents using keyword search with filters for platforms, domains, tags, glossary terms, and owners.

`grep_documents`

Search within document content using regex patterns. Useful for finding specific information across multiple documents.

`save_document`

Save standalone documents (insights, decisions, FAQs, notes) to DataHub's knowledge base. Documents are organized under a configurable parent folder.

## Managed MCP Server Usage

For DataHub Cloud v0.3.12+, you can connect directly to the hosted MCP server endpoint — no local installation required.

:::info
The managed MCP server endpoint is only available with DataHub Cloud v0.3.12+. For DataHub Core and older versions of DataHub Cloud, [self-host the MCP server](#self-hosted-mcp-server-usage) instead.
:::

:::note Streamable HTTP Only
DataHub's managed MCP server uses the [streamable HTTP transport](https://modelcontextprotocol.io/docs/concepts/transports). Some older MCP clients (e.g. chatgpt.com) may only support the deprecated SSE transport — for those, use [mcp-remote](https://github.com/geelen/mcp-remote) to bridge the gap.
:::

### Prerequisites

- The URL of your DataHub Cloud instance, e.g. `https://<tenant>.acryl.io`
- A [personal access token](../../authentication/personal-access-tokens.md)

### Connecting & Authenticating

Your managed MCP server URL is:

```
https://<tenant>.acryl.io/integrations/ai/mcp/
```

There are two ways to authenticate:

1. **Authorization header** — pass your token as a Bearer token in the `Authorization` header:

   ```
   Authorization: Bearer <token>
   ```

2. **Token in URL** — append your token as a query parameter:

   ```
   https://<tenant>.acryl.io/integrations/ai/mcp/?token=<token>
   ```

   This is a convenient alternative when your MCP client doesn't support custom headers.

<details>
  <summary>On-Premises DataHub Cloud</summary>

For on-premises DataHub Cloud, replace `<tenant>.acryl.io` with your DataHub FQDN, e.g. `https://datahub.example.com/integrations/ai/mcp/?token=<token>`.

</details>

### Configure

<details>
  <summary>Claude Desktop</summary>

1. Open your `claude_desktop_config.json` file. You can find it by navigating to Claude Desktop -> Settings -> Developer -> Edit Config.
1. Update the file to include the following content. Be sure to replace `<tenant>` and `<token>` with your own values.

```json
{
  "mcpServers": {
    "datahub-cloud": {
      "command": "npx",
      "args": [
        "-y",
        "mcp-remote",
        "https://<tenant>.acryl.io/integrations/ai/mcp/?token=<token>"
      ]
    }
  }
}
```

</details>

<details>
  <summary>Cursor</summary>

1. Make sure you're using Cursor v1.1 or newer.
2. Navigate to Cursor -> Settings -> Cursor Settings -> MCP -> add a new MCP server.
3. Enter the following into the file:

```json
{
  "mcpServers": {
    "datahub-cloud": {
      "url": "https://<tenant>.acryl.io/integrations/ai/mcp/?token=<token>"
    }
  }
}
```

4. Once you've saved the file, confirm that the MCP settings page shows a green dot and the DataHub tools listed.

</details>

<details>
  <summary>Other</summary>

Most AI tools support remote MCP servers. Provide the hosted MCP server URL:

```
https://<tenant>.acryl.io/integrations/ai/mcp/?token=<token>
```

Make sure authentication mode is _not_ set to "OAuth" (if applicable).

For clients that don't yet support remote MCP servers, use `mcp-remote`:

- Command: `npx`
- Args: `-y mcp-remote https://<tenant>.acryl.io/integrations/ai/mcp/?token=<token>`

</details>

## Self-Hosted MCP Server Usage

Run the [open-source MCP server](https://github.com/acryldata/mcp-server-datahub) locally. This works with any DataHub instance — both DataHub Core and DataHub Cloud.

### Prerequisites

1. Install [`uv`](https://github.com/astral-sh/uv):

   ```bash
   # macOS and Linux
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. The URL of your DataHub instance's GMS endpoint, e.g. `http://localhost:8080` or `https://<tenant>.acryl.io`
3. A [personal access token](../../authentication/personal-access-tokens.md)

### Connecting & Authenticating

The self-hosted server authenticates via environment variables:

- `DATAHUB_GMS_URL` — your DataHub GMS endpoint
- `DATAHUB_GMS_TOKEN` — your personal access token

These are passed to the `mcp-server-datahub` process at startup (see configuration examples below).

### Configure

<details>
  <summary>Claude Desktop</summary>

1. Run `which uvx` to find the full path to the `uvx` command.

1. Open your `claude_desktop_config.json` file. You can find it by navigating to Claude Desktop -> Settings -> Developer -> Edit Config.

1. Update the file to include the following content. Be sure to replace the placeholder values.

```js
{
  "mcpServers": {
    "datahub": {
      "command": "<full-path-to-uvx>",  // e.g. /Users/hsheth/.local/bin/uvx
      "args": ["mcp-server-datahub@latest"],
      "env": {
        "DATAHUB_GMS_URL": "<your-datahub-url>",
        "DATAHUB_GMS_TOKEN": "<your-datahub-token>"
      }
    }
  }
}
```

</details>

<details>
  <summary>Cursor</summary>

1. Navigate to Cursor -> Settings -> Cursor Settings -> MCP -> add a new MCP server.
2. Enter the following into the file:

```json
{
  "mcpServers": {
    "datahub": {
      "command": "uvx",
      "args": ["mcp-server-datahub@latest"],
      "env": {
        "DATAHUB_GMS_URL": "<your-datahub-url>",
        "DATAHUB_GMS_TOKEN": "<your-datahub-token>"
      }
    }
  }
}
```

3. Once you've saved the file, confirm that the MCP settings page shows a green dot and the DataHub tools listed.

</details>

<details>
  <summary>Other</summary>

For other AI tools, provide the following configuration:

- Command: `uvx`
- Args: `mcp-server-datahub@latest`
- Env:
  - `DATAHUB_GMS_URL`: `<your-datahub-url>`
  - `DATAHUB_GMS_TOKEN`: `<your-datahub-token>`

</details>

### Troubleshooting

#### `spawn uvx ENOENT`

The full stack trace might look like this:

```
2025-04-08T19:58:16.593Z [datahub] [error] spawn uvx ENOENT {"stack":"Error: spawn uvx ENOENT\n    at ChildProcess._handle.onexit (node:internal/child_process:285:19)\n    at onErrorNT (node:internal/child_process:483:16)\n    at process.processTicksAndRejections (node:internal/process/task_queues:82:21)"}
```

Solution: Replace the `uvx` bit of the command with the output of `which uvx`.
