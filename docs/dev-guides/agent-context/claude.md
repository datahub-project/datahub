# Claude

Give Claude ([web, desktop](https://claude.ai/download), Cowork, and [Claude Code](https://docs.anthropic.com/en/docs/agents-and-tools/claude-code/overview)) access to your enterprise data context in DataHub — find trustworthy data, trace lineage, look up ownership, and reference documentation while you work.

## Prerequisites

- A Claude account, or Claude Code installed
- A DataHub instance: [Cloud](../../features/feature-guides/mcp.md#managed-mcp-server-usage) (OAuth on v1.0.2+, PAT on v0.3.12+) or [self-hosted](../../features/feature-guides/mcp.md#self-hosted-mcp-server-usage)

:::tip Quickest start: the DataHub Cloud plugin
On DataHub Cloud, install the [DataHub Cloud plugin](./plugins.md#claude). It includes the MCP server and skills, and you sign in with your DataHub account. Use the steps below if you need a scoped MCP server, an access token, or DataHub Core.
:::

## Claude Code

### DataHub Cloud — OAuth (Recommended, v1.0.2+)

On DataHub Cloud v1.0.2+, Claude Code can use OAuth2 with Dynamic Client Registration — no token to mint or paste.

```bash
claude mcp add --transport http datahub https://mcp.datahub.com/mcp
```

The first DataHub call returns `401 Unauthorized`, which Claude Code flags as needing authentication. Run `/mcp` inside Claude Code, select the DataHub server, and choose **Authenticate** — a browser opens for the DataHub OAuth flow. Enter your DataHub domain (e.g. `<tenant>` for `https://<tenant>.acryl.io`) and sign in. Tokens are stored and refreshed automatically.

Prefer your tenant URL directly? Swap the URL for `https://<tenant>.acryl.io/mcp`.

### DataHub Cloud — Personal Access Token (Legacy)

For service accounts or DataHub Cloud versions prior to v1.0.2, use a [personal access token](../../authentication/personal-access-tokens.md):

```bash
claude mcp add --transport http \
  --header "Authorization: Bearer <token>" \
  datahub-cloud \
  "https://<tenant>.acryl.io/mcp"
```

:::tip Keep Your Token Secret
To avoid putting the token in your shell history, set it as an environment variable first:

```bash
export DATAHUB_TOKEN="<your-token>"
claude mcp add --transport http \
  --header "Authorization: Bearer $DATAHUB_TOKEN" \
  datahub-cloud \
  "https://<tenant>.acryl.io/mcp"
```

:::

### DataHub Core

DataHub Core v1.9.0+ serves MCP at `/mcp` on the same address as the UI. Replace `<your-datahub-url>` with that address (for example `http://localhost:9002` on a quickstart):

```bash
claude mcp add --transport http datahub <your-datahub-url>/mcp \
  --header "Authorization: Bearer <your-datahub-token>"
```

Leave out the `--header` line when Metadata Service Authentication is off, as on a quickstart. To run the tools locally over stdio instead, see [Run Locally over stdio](../../features/feature-guides/mcp.md#run-locally-over-stdio).

### Verify

Run `claude mcp list` to confirm the DataHub server appears.

## Claude Desktop

### DataHub Cloud — OAuth Connector (Recommended, v1.0.2+)

Custom remote MCP connectors are available on **Free, Pro, Max, Team, and Enterprise** plans (Free is limited to one custom connector; Team/Enterprise restricts adding to Owners).

1. Open **Customize → Connectors**. On Team/Enterprise, an owner adds it under **Organization settings → Connectors**.
2. Click **Add custom connector**.
3. Name: `DataHub`. Remote MCP server URL: `https://mcp.datahub.com/mcp`. Leave **Advanced settings** empty — DataHub registers the client via DCR automatically.
4. Click **Add**, then **Connect**. A browser window opens for the DataHub OAuth flow.
5. Enter your DataHub domain (e.g. `<tenant>`), sign in, and approve. The DataHub tools appear in Claude's tool menu (hammer icon).

Prefer your tenant URL directly? Use `https://<tenant>.acryl.io/mcp` as the connector URL instead.

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
        "https://<tenant>.acryl.io/mcp",
        "--header",
        "Authorization: Bearer <token>"
      ]
    }
  }
}
```

### DataHub Core

Claude Desktop's custom connectors are called from Anthropic's cloud and can't reach a private DataHub, so connect through the [`mcp-remote`](https://github.com/geelen/mcp-remote) bridge, which runs on your machine (it needs Node.js). Open **Claude Desktop → Settings → Developer → Edit Config** and update `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "datahub": {
      "command": "npx",
      "args": [
        "-y",
        "mcp-remote",
        "<your-datahub-url>/mcp",
        "--header",
        "Authorization:${AUTH_HEADER}"
      ],
      "env": {
        "AUTH_HEADER": "Bearer <your-datahub-token>"
      }
    }
  }
}
```

Without a token (Metadata Service Authentication off), keep only `"args": ["-y", "mcp-remote", "<your-datahub-url>/mcp"]`. See [Connect a Client](../../features/feature-guides/mcp.md#connect-a-client) for details.

### Verify

Restart Claude Desktop. The DataHub tools should appear in the tools menu (hammer icon).

## Troubleshooting

For general troubleshooting (authentication errors, empty results, missing tools), see the [MCP server guide](../../features/feature-guides/mcp.md#troubleshooting).
