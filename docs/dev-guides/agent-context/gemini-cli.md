# Google Gemini CLI

Give [Gemini CLI](https://github.com/google-gemini/gemini-cli) access to your enterprise data context in DataHub — find trustworthy data, trace lineage, and look up ownership right from the terminal.

## Prerequisites

- [Gemini CLI](https://github.com/google-gemini/gemini-cli) installed
- A DataHub instance ([Cloud](../../features/feature-guides/mcp.md#managed-mcp-server-usage) v0.3.12+ or [self-hosted](../../features/feature-guides/mcp.md#self-hosted-mcp-server-usage)) and a [personal access token](../../authentication/personal-access-tokens.md) (not needed for DataHub Core with Metadata Service Authentication off)

## DataHub Cloud

Gemini CLI natively supports streamable HTTP with custom headers.

```bash
gemini mcp add --transport http \
  --header "Authorization: Bearer <token>" \
  datahub-cloud \
  "https://<tenant>.acryl.io/mcp"
```

Replace `<tenant>` with your DataHub Cloud tenant name and `<token>` with your personal access token.

Or add it directly to your `settings.json` (`~/.gemini/settings.json` for user-level, or `.gemini/settings.json` for project-level):

```json
{
  "mcpServers": {
    "datahub-cloud": {
      "httpUrl": "https://<tenant>.acryl.io/mcp",
      "headers": {
        "Authorization": "Bearer <token>"
      }
    }
  }
}
```

## DataHub Core

DataHub Core v1.9.0+ serves MCP at `/mcp` on the same address as the UI. Replace `<your-datahub-url>` with that address (for example `http://localhost:9002` on a quickstart):

```bash
gemini mcp add --transport http \
  --header "Authorization: Bearer <your-datahub-token>" \
  datahub \
  "<your-datahub-url>/mcp"
```

Leave out the `--header` line when Metadata Service Authentication is off, as on a quickstart. To run the tools locally over stdio instead, see [Run Locally over stdio](../../features/feature-guides/mcp.md#run-locally-over-stdio).

## Verify

Run `gemini mcp list` or use the `/mcp` command inside a session to confirm the DataHub server is connected.

For general troubleshooting, see the [MCP server guide](../../features/feature-guides/mcp.md#troubleshooting).
