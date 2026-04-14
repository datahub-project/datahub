# Cursor

Give [Cursor](https://www.cursor.com/) access to your enterprise data context in DataHub — find trustworthy data, trace lineage, look up ownership, and reference documentation without leaving the editor.

## Prerequisites

- Cursor v1.1+
- A DataHub instance ([Cloud](../../features/feature-guides/mcp.md#managed-mcp-server-usage) v0.3.12+ or [self-hosted](../../features/feature-guides/mcp.md#self-hosted-mcp-server-usage)) and a [personal access token](../../authentication/personal-access-tokens.md)

## DataHub Cloud

Navigate to **Cursor → Settings → Cursor Settings → MCP**, add a new server, and paste:

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

Replace `<tenant>` with your DataHub Cloud tenant name and `<token>` with your personal access token.

:::tip Keep Your Token Secret
If your MCP config is checked into version control, reference an environment variable instead of a literal token:

```json
"Authorization": "Bearer ${DATAHUB_TOKEN}"
```

:::

## Self-Hosted DataHub

Install [`uv`](https://github.com/astral-sh/uv) (`curl -LsSf https://astral.sh/uv/install.sh | sh`), then add a new MCP server in Cursor settings with:

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

## Verify

After saving, the MCP settings page should show a **green dot** and list the DataHub tools. If the dot doesn't appear, check that:

- The JSON is valid (no trailing commas).
- For self-hosted: `uvx` is on Cursor's PATH — run `which uvx` and use the full path in `command` if needed.

For general troubleshooting, see the [MCP server guide](../../features/feature-guides/mcp.md#troubleshooting).
