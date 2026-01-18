# DataHub MCP Server

The DataHub MCP Server implements the [Model Context Protocol (MCP)](https://modelcontextprotocol.io/introduction), which standardizes how applications provide context to LLMs and AI agents. This enables AI agents to query DataHub metadata and use it to find relevant assets, traverse lineage, and more.

Want to learn more about the motivation, architecture, and advanced use cases? Check out our [deep dive blog post](https://datahub.com/blog/datahub-mcp-server-block-ai-agents-use-case/).

There's two ways to use the MCP server, which vary in setup required but offer the same capabilities.

- [Managed MCP Server](#managed-mcp-server-usage) - Available on DataHub Cloud v0.3.12+
- [Self-Hosted MCP Server](#self-hosted-mcp-server-usage) - Available for DataHub Core

## Capabilities

**Search for Data** <br />
Find the right data to use for your projects & analysis by asking questions in plain English - skip the tribal knowledge.

**Dive Deeper** <br />
Separate the signal from noise with rich context about your data, including usage, ownership, documentation, tags, and quality.

**Lineage & Impact Analysis** <br />
Understand the impact of upcoming changes to tables, reports, and dashboards using DataHub’s end-to-end lineage graph.

**Query Analysis & Authoring** <br />
Understand how your mission-critical data is typically queried, or build custom queries for your tables.

**Works Where You Work** <br />
Seamlessly integrates with AI-native tools like Cursor, Windsurf, Claude Desktop, and OpenAI to supercharge your workflows.

## Why DataHub MCP Server?

With DataHub MCP Server, you can instantly give AI agents visibility into of your entire data ecosystem. Find and understand data stored in your databases, data lake, data warehouse, BI visualization tools, and AI/ML Feature stores. Explore data lineage, understand usage & use cases, identify the data experts, and generate SQL - all through natural language.

### **Structured Search with Context Filtering**

Go beyond keyword matching with powerful query & filtering syntax:

- Wildcard matching: `/q revenue_*` finds `revenue_kpis`, `revenue_daily`, `revenue_forecast`
- Field searches: `/q tag:PII` finds all PII-tagged data
- Boolean logic: `/q (sales OR revenue) AND quarterly` for complex queries

### **SQL Intelligence & Query Generation**

Access popular SQL queries, and generate new ones with accuracy:

- See how analysts query tables (perfect for SQL generation)
- Understand join patterns and common filters
- Learn from production query patterns

### **Table & Column-Level Lineage**

Trace data flow at both the table and column level:

- Track how `user_id` becomes `customer_key` downstream
- Understand transformation logic
- Upstream and downstream exploration (1-3+ hops)
- Handle enterprise-scale lineage graphs

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

## Managed MCP Server Usage

For folks on DataHub Cloud v0.3.12+, you can use our hosted MCP server endpoint.

:::info

The managed MCP server endpoint is only available with DataHub Cloud v0.3.12+. For DataHub Core and older versions of DataHub Cloud, you'll need to [self-host the MCP server](#self-hosted-mcp-server-usage).

:::

:::note Streamable HTTP Only

There are two [transports types](https://modelcontextprotocol.io/docs/concepts/transports) for remote MCP servers: streamable HTTP and server-sent events (SSE). SSE has been deprecated in favor of streamable HTTP, so DataHub only supports the newer streamable HTTP transport. Some older MCP clients (e.g. chatgpt.com) may still only support SSE. For those cases, you'll need to use something like [mcp-remote](https://github.com/geelen/mcp-remote) to bridge the gap.

:::

### Prerequisites

To connect to the MCP server, you'll need the following:

- **Node.js 18+** - Required for AI tools that use the `npx` command (for example Claude Desktop)

  ```bash
  # Check if Node.js is installed
  node --version

  # If not installed, download from https://nodejs.org/
  # Or use a package manager:

  # macOS (using Homebrew)
  brew install node

  # Linux (using apt)
  sudo apt update && sudo apt install nodejs npm

  # Or use nvm (Node Version Manager) for easier version management
  curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash
  nvm install --lts
  ```

- The URL of your DataHub Cloud instance e.g. `https://<tenant>.acryl.io`
- A [personal access token](../../authentication/personal-access-tokens.md)

Your hosted MCP server URL is `https://<tenant>.acryl.io/integrations/ai/mcp/?token=<token>`.

<details>
  <summary>On-Premises DataHub Cloud</summary>

For on-premises DataHub Cloud, your hosted MCP server URL is `https://<datahub-fqdn>/integrations/ai/mcp/?token=<token>`.

For example, it might look something like `https://datahub.example.com/integrations/ai/mcp/?token=eyJh...`.

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
2. Navigate to Cursor -> Settings -> Cursor Settings -> MCP -> add a new MCP server
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

3. Once you've saved the file, confirm that the MCP settings page shows a green dot and a couple tools associated with the server.

</details>

<details>
  <summary>Other</summary>

Most AI tools support remote MCP servers. For those, you'll typically need to:

- Provide the hosted MCP server URL: `https://<tenant>.acryl.io/integrations/ai/mcp/?token=<token>`
- Ensure that the authentication mode is _not_ set to "OAuth" (if applicable)

For AI tools that don't yet support remote MCP servers, you can use the `mcp-remote` tool to connect to the MCP server.

- Command: `npx`
- Args: `-y mcp-remote https://<tenant>.acryl.io/integrations/ai/mcp/?token=<token>`

</details>

## Self-Hosted MCP Server Usage

You can run the [open-source MCP server](https://github.com/acryldata/mcp-server-datahub) locally.

### Prerequisites

1. Install [`uv`](https://github.com/astral-sh/uv)

   ```bash
   # On macOS and Linux.
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. For authentication, you'll need the following:
   - The URL of your DataHub instance's GMS endpoint; e.g. `http://<localhost>:8080` or `https://<tenant>.acryl.io`
   - A [personal access token](../../authentication/personal-access-tokens.md)

### Configure

<details>
  <summary>Claude Desktop</summary>

1. Run `which uvx` to find the full path to the `uvx` command.

1. Open your `claude_desktop_config.json` file. You can find it by navigating to Claude Desktop -> Settings -> Developer -> Edit Config.

1. Update the file to include the following content. Be sure to replace `<tenant>` and `<token>` with your own values.

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

1. Navigate to Cursor -> Settings -> Cursor Settings -> MCP -> add a new MCP server
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

3. Once you've saved the file, confirm that the MCP settings page shows a green dot and a couple tools associated with the server.

</details>

<details>
  <summary>Other</summary>

For other AI tools, you'll typically need to provide the following configuration:

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
