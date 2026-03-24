# Cursor Integration

> **📚 Navigation**: [← Back to Agent Context Kit](./agent-context.md) | [Claude Code Integration →](./claude-code.md) | [Copilot Studio Integration →](./copilot-studio.md)

## What Problem Does This Solve?

[Cursor](https://www.cursor.com/) is an AI-powered code editor. Out of the box, its AI features have no visibility into your organization's data catalog — it can't look up table schemas, trace lineage, or check data ownership while you code.

By connecting DataHub's MCP server to Cursor, you get data context right in the editor. Ask questions like _"What columns does the orders table have?"_ or _"Who owns this dataset?"_ and get answers grounded in real metadata — without leaving your IDE.

## Prerequisites

- [Cursor](https://www.cursor.com/) v1.1 or newer
- A DataHub Cloud instance (v0.3.12+) or a self-hosted DataHub instance with the [MCP server](../../features/feature-guides/mcp.md) running
- A DataHub [personal access token](../../authentication/personal-access-tokens.md)

## Setup Guide

### DataHub Cloud

1. Navigate to **Cursor → Settings → Cursor Settings → MCP** and add a new MCP server.
2. Enter the following configuration, replacing `<tenant>` and `<token>` with your values:

```json
{
  "mcpServers": {
    "datahub-cloud": {
      "url": "https://<tenant>.acryl.io/integrations/ai/mcp/",
      "headers": {
        "Authorization": "Bearer <token>"
      }
    }
  }
}
```

:::tip Keep Your Token Secret
Prefer the `Authorization` header approach shown above rather than putting the token directly in the URL as a query parameter. If your MCP config is checked into version control, consider referencing an environment variable instead:

```json
{
  "mcpServers": {
    "datahub-cloud": {
      "url": "https://<tenant>.acryl.io/integrations/ai/mcp/",
      "headers": {
        "Authorization": "Bearer ${DATAHUB_TOKEN}"
      }
    }
  }
}
```

:::

3. Save the file. Confirm that the MCP settings page shows a **green dot** and the DataHub tools are listed.

### Self-Hosted DataHub

1. Install [`uv`](https://github.com/astral-sh/uv) if you haven't already:

   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. Navigate to **Cursor → Settings → Cursor Settings → MCP** and add a new MCP server.
3. Enter the following configuration, replacing the placeholder values:

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

4. Save the file. Confirm that the MCP settings page shows a **green dot** and the DataHub tools are listed.

## Try It Out

Open Cursor's AI chat (Agent mode) and try asking:

- _"What datasets contain PII?"_
- _"Show me the schema for the orders table"_
- _"Who owns the revenue dashboard?"_
- _"Trace lineage upstream from this dataset"_

## Troubleshooting

### Green Dot Not Appearing

- Ensure you're on Cursor v1.1 or newer.
- Verify the JSON is valid (no trailing commas, correct quoting).
- For self-hosted: run `which uvx` and use the full path in the `command` field if `uvx` is not on Cursor's PATH.

### Authentication Errors

- Confirm your personal access token is valid and hasn't expired.
- Make sure the `Authorization` header includes the `Bearer ` prefix (with trailing space).
- For self-hosted: verify `DATAHUB_GMS_URL` and `DATAHUB_GMS_TOKEN` are correct.

### No Results

- Verify that your DataHub instance has ingested metadata.
- Try broader search terms.
- See the [MCP server troubleshooting guide](../../features/feature-guides/mcp.md#troubleshooting) for more details.
