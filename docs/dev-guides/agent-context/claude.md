# Claude

Give [Claude Code](https://docs.anthropic.com/en/docs/agents-and-tools/claude-code/overview) or [Claude Desktop](https://claude.ai/download) access to your enterprise data context in DataHub — find trustworthy data, trace lineage, look up ownership, and reference documentation while you work.

## Prerequisites

- Claude Code or Claude Desktop installed
- A DataHub instance: [Cloud](../../features/feature-guides/mcp.md#managed-mcp-server-usage) (OAuth on v1.0.2+, PAT on v0.3.12+) or [self-hosted](../../features/feature-guides/mcp.md#self-hosted-mcp-server-usage)

## Claude Code

### DataHub Cloud — OAuth (Recommended, v1.0.2+)

On DataHub Cloud v1.0.2+, Claude Code can use OAuth2 with Dynamic Client Registration — no token to mint or paste.

```bash
claude mcp add --transport http datahub https://mcp.datahub.com/mcp
```

The first DataHub call returns `401 Unauthorized`, which Claude Code flags as needing authentication. Run `/mcp` inside Claude Code, select the DataHub server, and choose **Authenticate** — a browser opens for the DataHub OAuth flow. Enter your DataHub domain (e.g. `<tenant>` for `https://<tenant>.acryl.io`) and sign in. Tokens are stored and refreshed automatically.

Prefer your tenant URL directly? Swap the URL for `https://<tenant>.acryl.io/integrations/ai/mcp`.

### DataHub Cloud — Personal Access Token (Legacy)

For service accounts or DataHub Cloud versions prior to v1.0.2, use a [personal access token](../../authentication/personal-access-tokens.md):

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

### DataHub Cloud — OAuth Connector (Recommended, v1.0.2+)

Custom remote MCP connectors are available on **Free, Pro, Max, Team, and Enterprise** plans (Free is limited to one custom connector; Team/Enterprise restricts adding to Owners).

1. In Claude Desktop, open **Settings → Connectors** (Team/Enterprise: **Organization settings → Connectors**).
2. Click **Add custom connector**.
3. Name: `DataHub`. Remote MCP server URL: `https://mcp.datahub.com/mcp`. Leave **Advanced settings** empty — DataHub registers the client via DCR automatically.
4. Click **Add**, then **Connect**. A browser window opens for the DataHub OAuth flow.
5. Enter your DataHub domain (e.g. `<tenant>`), sign in, and approve. The DataHub tools appear in Claude's tool menu (hammer icon).

Prefer your tenant URL directly? Use `https://<tenant>.acryl.io/integrations/ai/mcp` as the connector URL instead.

:::note
Remote MCP connectors are configured via the Claude Desktop UI, not `claude_desktop_config.json` — that file is for local stdio servers only.
:::

### DataHub Cloud — `mcp-remote` Bridge (Legacy)

For older Claude Desktop versions without native remote MCP support, use [`mcp-remote`](https://github.com/geelen/mcp-remote) as a local bridge.

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
