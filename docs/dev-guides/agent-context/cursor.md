# Cursor

Give [Cursor](https://www.cursor.com/) access to your enterprise data context in DataHub — find trustworthy data, trace lineage, look up ownership, and reference documentation without leaving the editor.

## Prerequisites

- Cursor v1.1+
- A DataHub instance: [Cloud](../../features/feature-guides/mcp.md#managed-mcp-server-usage) (OAuth on v1.0.2+, PAT on v0.3.12+) or [self-hosted](../../features/feature-guides/mcp.md#self-hosted-mcp-server-usage)

## DataHub Cloud — OAuth (Recommended, v1.0.2+)

On DataHub Cloud v1.0.2+, Cursor can connect via OAuth2 with Dynamic Client Registration — no token to mint or paste.

Navigate to **Cursor → Settings → Cursor Settings → Tools & MCP → New MCP Server** (or edit `~/.cursor/mcp.json` globally / `.cursor/mcp.json` per project) and paste:

```json
{
  "mcpServers": {
    "datahub": {
      "url": "https://mcp.datahub.com/mcp"
    }
  }
}
```

Save. Cursor opens a browser tab for the DataHub OAuth flow (callback `cursor://anysphere.cursor-mcp/oauth/callback`) — enter your DataHub domain (e.g. `<tenant>` for `https://<tenant>.acryl.io`) and sign in. Tokens are stored and refreshed automatically.

Prefer your tenant URL directly? Replace the URL with `https://<tenant>.acryl.io/mcp`.

## DataHub Cloud — Personal Access Token (Legacy)

For service accounts or DataHub Cloud versions prior to v1.0.2, use a [personal access token](../../authentication/personal-access-tokens.md). Navigate to **Cursor → Settings → Cursor Settings → MCP**, add a new server, and paste:

```json
{
  "mcpServers": {
    "datahub-cloud": {
      "url": "https://<tenant>.acryl.io/mcp",
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

## DataHub Core

DataHub Core v1.9.0+ serves MCP at `/mcp` on the same address as the UI. Add a new MCP server in Cursor settings (or `~/.cursor/mcp.json`), replacing `<your-datahub-url>` with that address (for example `http://localhost:9002` on a quickstart):

```json
{
  "mcpServers": {
    "datahub": {
      "url": "<your-datahub-url>/mcp",
      "headers": {
        "Authorization": "Bearer <your-datahub-token>"
      }
    }
  }
}
```

Leave out `headers` when Metadata Service Authentication is off, as on a quickstart. To run the tools locally over stdio instead, see [Run Locally over stdio](../../features/feature-guides/mcp.md#run-locally-over-stdio).

## Verify

After saving, the MCP settings page should show a **green dot** and list the DataHub tools. If the dot doesn't appear, check that:

- The JSON is valid (no trailing commas).
- For self-hosted: Cursor can reach `<your-datahub-url>/mcp`, and the token is valid when Metadata Service Authentication is on.

For general troubleshooting, see the [MCP server guide](../../features/feature-guides/mcp.md#troubleshooting).
