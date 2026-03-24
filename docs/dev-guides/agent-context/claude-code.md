# Claude Code Integration

> **📚 Navigation**: [← Back to Agent Context Kit](./agent-context.md) | [Cursor Integration →](./cursor.md) | [Copilot Studio Integration →](./copilot-studio.md)

## What Problem Does This Solve?

[Claude Code](https://docs.anthropic.com/en/docs/agents-and-tools/claude-code/overview) is Anthropic's agentic coding tool that works directly in your terminal. By default, it has no awareness of your organization's data catalog — it can't look up schemas, trace lineage, or discover data ownership.

By connecting DataHub's MCP server to Claude Code, you bring your full metadata catalog into the coding workflow. Ask _"What columns does the users table have?"_ or _"Show me lineage for this dashboard"_ and get answers grounded in real metadata — right from the terminal.

## Prerequisites

- [Claude Code](https://docs.anthropic.com/en/docs/agents-and-tools/claude-code/overview) installed
- A DataHub Cloud instance (v0.3.12+) or a self-hosted DataHub instance with the [MCP server](../../features/feature-guides/mcp.md) running
- A DataHub [personal access token](../../authentication/personal-access-tokens.md)

## Setup Guide

### DataHub Cloud

Claude Code natively supports streamable HTTP, so no proxy or additional dependencies are needed.

Run the following command, replacing `<tenant>` and `<token>` with your values:

```bash
claude mcp add --transport http \
  --header "Authorization: Bearer <token>" \
  datahub-cloud \
  "https://<tenant>.acryl.io/integrations/ai/mcp/"
```

:::tip Keep Your Token Secret
The command above passes the token via the `Authorization` header, which keeps it out of the URL. If you'd like to avoid putting the token in your shell history, set it as an environment variable first:

```bash
export DATAHUB_TOKEN="<your-token>"
claude mcp add --transport http \
  --header "Authorization: Bearer $DATAHUB_TOKEN" \
  datahub-cloud \
  "https://<tenant>.acryl.io/integrations/ai/mcp/"
```

:::

### Self-Hosted DataHub

1. Install [`uv`](https://github.com/astral-sh/uv) if you haven't already:

   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. Run the following command, replacing the placeholder values:

```bash
claude mcp add datahub \
  -e DATAHUB_GMS_URL="<your-datahub-url>" \
  -e DATAHUB_GMS_TOKEN="<your-datahub-token>" \
  -- uvx mcp-server-datahub@latest
```

### Verify the Connection

Run `claude mcp list` to confirm the DataHub server appears and is reachable.

## Try It Out

Start a Claude Code session and try asking:

- _"What datasets contain PII?"_
- _"Show me the schema for the orders table"_
- _"Who owns the revenue dashboard?"_
- _"Trace lineage upstream from this dataset"_

## Troubleshooting

### Connection Errors

- Run `claude mcp list` to check the server status.
- For DataHub Cloud: verify your tenant URL is correct (`https://<tenant>.acryl.io`).
- Confirm your personal access token is valid and hasn't expired.

### Self-Hosted: `uvx` Not Found

- Run `which uvx` to find the full path and use that in the command.
- Ensure `uv` is installed: `curl -LsSf https://astral.sh/uv/install.sh | sh`

### No Results

- Verify that your DataHub instance has ingested metadata.
- Try broader search terms.
- See the [MCP server troubleshooting guide](../../features/feature-guides/mcp.md#troubleshooting) for more details.
