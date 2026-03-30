# Claude Integration

> **📚 Navigation**: [← Back to Agent Context Kit](./agent-context.md) | [Cursor Integration →](./cursor.md) | [Copilot Studio Integration →](./copilot-studio.md)

Connect [Claude Code](https://docs.anthropic.com/en/docs/agents-and-tools/claude-code/overview) or [Claude Desktop](https://claude.ai/download) to DataHub so Claude can search your data catalog, inspect schemas, and trace lineage while you work.

## Prerequisites

- Claude Code or Claude Desktop installed
- A DataHub instance ([Cloud](../../features/feature-guides/mcp.md#managed-mcp-server-usage) v0.3.12+ or [self-hosted](../../features/feature-guides/mcp.md#self-hosted-mcp-server-usage)) and a [personal access token](../../authentication/personal-access-tokens.md)

## Claude Code

### DataHub Cloud

Claude Code natively supports streamable HTTP — no proxy needed.

```bash
claude mcp add --transport http \
  --header "Authorization: Bearer <token>" \
  datahub-cloud \
  "https://<tenant>.acryl.io/integrations/ai/mcp/"
```

:::tip Keep Your Token Secret
To avoid putting the token in your shell history, set it as an environment variable first:

```bash
export DATAHUB_TOKEN="<your-token>"
claude mcp add --transport http \
  --header "Authorization: Bearer $DATAHUB_TOKEN" \
  datahub-cloud \
  "https://<tenant>.acryl.io/integrations/ai/mcp/"
```

:::

### Self-Hosted DataHub

Install [`uv`](https://github.com/astral-sh/uv) (`curl -LsSf https://astral.sh/uv/install.sh | sh`), then:

```bash
claude mcp add datahub \
  -e DATAHUB_GMS_URL="<your-datahub-url>" \
  -e DATAHUB_GMS_TOKEN="<your-datahub-token>" \
  -- uvx mcp-server-datahub@latest
```

### Verify

Run `claude mcp list` to confirm the DataHub server appears.

## Claude Desktop

### DataHub Cloud

Claude Desktop's config file doesn't reliably connect to remote MCP servers directly. Use [`mcp-remote`](https://github.com/geelen/mcp-remote) as a local bridge.

Open **Claude Desktop → Settings → Developer → Edit Config** and update `claude_desktop_config.json`:

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

### Self-Hosted DataHub

1. Run `which uvx` to find the full path to `uvx`.
2. Open **Claude Desktop → Settings → Developer → Edit Config** and update `claude_desktop_config.json`:

```js
{
  "mcpServers": {
    "datahub": {
      "command": "<full-path-to-uvx>",  // e.g. /Users/you/.local/bin/uvx
      "args": ["mcp-server-datahub@latest"],
      "env": {
        "DATAHUB_GMS_URL": "<your-datahub-url>",
        "DATAHUB_GMS_TOKEN": "<your-datahub-token>"
      }
    }
  }
}
```

### Verify

Restart Claude Desktop. The DataHub tools should appear in the tools menu (hammer icon).

## Troubleshooting

For general troubleshooting (authentication errors, empty results, `uvx` not found), see the [MCP server guide](../../features/feature-guides/mcp.md#troubleshooting).
