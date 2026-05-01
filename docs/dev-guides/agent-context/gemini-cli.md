# Google Gemini CLI

Give [Gemini CLI](https://github.com/google-gemini/gemini-cli) access to your enterprise data context in DataHub — find trustworthy data, trace lineage, and look up ownership right from the terminal.

## Prerequisites

- [Gemini CLI](https://github.com/google-gemini/gemini-cli) installed
- A DataHub instance ([Cloud](../../features/feature-guides/mcp.md#managed-mcp-server-usage) v0.3.12+ or [self-hosted](../../features/feature-guides/mcp.md#self-hosted-mcp-server-usage)) and a [personal access token](../../authentication/personal-access-tokens.md)

## DataHub Cloud

Gemini CLI natively supports streamable HTTP with custom headers.

```bash
gemini mcp add --transport http \
  --header "Authorization: Bearer <token>" \
  datahub-cloud \
  "https://<tenant>.acryl.io/integrations/ai/mcp/"
```

Replace `<tenant>` with your DataHub Cloud tenant name and `<token>` with your personal access token.

Or add it directly to your `settings.json` (`~/.gemini/settings.json` for user-level, or `.gemini/settings.json` for project-level):

```json
{
  "mcpServers": {
    "datahub-cloud": {
      "httpUrl": "https://<tenant>.acryl.io/integrations/ai/mcp/",
      "headers": {
        "Authorization": "Bearer <token>"
      }
    }
  }
}
```

## Self-Hosted DataHub

Install [`uv`](https://github.com/astral-sh/uv) (`curl -LsSf https://astral.sh/uv/install.sh | sh`), then:

```bash
gemini mcp add \
  -e DATAHUB_GMS_URL="<your-datahub-url>" \
  -e DATAHUB_GMS_TOKEN="<your-datahub-token>" \
  datahub \
  uvx mcp-server-datahub@latest
```

## Verify

Run `gemini mcp list` or use the `/mcp` command inside a session to confirm the DataHub server is connected.

For general troubleshooting, see the [MCP server guide](../../features/feature-guides/mcp.md#troubleshooting).
