# Snowflake Cortex Code Integration

> **📚 Navigation**: [← Back to Agent Context Kit](./agent-context.md) | [← Snowflake Intelligence Integration](./snowflake.md) | [Google ADK Integration →](./google-adk.md)

## What Problem Does This Solve?

Snowflake Cortex Code is an AI-powered coding assistant that lives in your terminal. Out of the box, it only sees what your Snowflake account exposes — raw table and column names with no business context. This means Cortex Code:

- Doesn't understand business definitions or hierarchies captured in glossary terms or domains
- Can't tell how a column or metric is calculated from upstream inputs, or what depends on it downstream
- Cannot surface which datasets are deprecated, certified, or owned by a specific team

**DataHub's MCP server** bridges this gap by giving Cortex Code direct access to your DataHub metadata catalog over the [Model Context Protocol (MCP)](https://modelcontextprotocol.io). Cortex Code calls DataHub MCP tools during reasoning, grounding its SQL generation and data discovery in real metadata.

### What You Can Do

With the integration enabled, Cortex Code can:

- **Search by meaning, not just names**: "Find all revenue tables owned by the finance team"
- **Resolve business terminology**: "What's the business definition of 'churn'?"
- **Surface quality signals**: "Show me certified customer datasets"
- **Access documentation**: Search across data docs, descriptions, and glossary entries
- **Identify ownership**: Discover who owns and maintains datasets

### Example Queries DataHub Enables

```
"List every table tagged PII = TRUE in ANALYTICS_DB"
"What datasets are used by the finance dashboard?"
"Find the business definition for MRR from our data glossary"
"Which tables does the analytics team own?"
```

## Overview

DataHub exposes a built-in MCP server at `/integrations/ai/mcp` on your DataHub Cloud instance. Cortex Code connects to it via the `cortex mcp add` command — no extra packages or UDFs required.

## Prerequisites

**Cortex Code CLI:**

- Cortex Code CLI installed (see [Installation](#installation) below)
- Supported platform: macOS, Linux, WSL, or Windows (preview)
- Snowflake user account with the `SNOWFLAKE.CORTEX_USER` database role

**DataHub:**

- A DataHub Cloud instance URL (e.g. `https://<tenant>.acryl.io`)
- A [personal access token](../../authentication/personal-access-tokens.md)

## Installation

### Install Cortex Code CLI

**Linux / macOS / WSL:**

```bash
curl -LsS https://ai.snowflake.com/static/cc-scripts/install.sh | sh
```

The `cortex` executable installs to `~/.local/bin`. PATH modifications are handled automatically.

**Windows Native (Preview):**

```powershell
irm https://ai.snowflake.com/static/cc-scripts/install.ps1 | iex
```

The executable installs to `%LOCALAPPDATA%\cortex`.

### Connect to Snowflake

Run `cortex` to launch the setup wizard. You can either select an existing connection from `~/.snowflake/connections.toml` or create a new connection by entering your Snowflake account credentials.

## Setup: Connect DataHub MCP Server

Register DataHub as an MCP server in Cortex Code with a single command:

```bash
cortex mcp add datahub https://<your-instance>.acryl.io/integrations/ai/mcp \
  --transport=http \
  --header "Authorization: Bearer <token>"
```

Replace `<your-instance>` with your DataHub Cloud tenant name and `<token>` with your personal access token.

That's it. Cortex Code will now call DataHub MCP tools during reasoning sessions.

## Using Cortex Code with DataHub

Start Cortex Code:

```bash
cortex
```

Cortex Code automatically uses the registered DataHub MCP tools when answering questions. You can ask natural-language questions and it will query DataHub to ground its answers in your real metadata:

```
# Data discovery
"Find all tables with customer data in the marketing domain"
"What datasets are tagged as PII?"
"Which tables does the finance team own?"

# Business context
"What's the business definition of MRR from our data glossary?"
"Show me certified revenue datasets"

# SQL generation with context
"Write a query for top 10 customers by revenue"
"List every table tagged PII = TRUE in ANALYTICS_DB"
```

## Agent Context

For more info on the tools exposed by the DataHub MCP server, see the [DataHub Agent Context Documentation](./agent-context.md).

## Troubleshooting

### Problem: DataHub MCP Tools Not Used

**Symptoms**: Cortex Code generates SQL without consulting DataHub metadata

**Solutions**:

- Verify the MCP server is registered: `cortex mcp list`
- Check the DataHub URL and token are correct
- Ensure your DataHub token hasn't expired
- Re-run `cortex mcp add` with updated credentials

### Problem: MCP Connection Errors

**Symptoms**: Errors connecting to DataHub MCP endpoint

**Solutions**:

- Verify the DataHub instance URL is reachable from your machine
- Check the token format — it should start with `eyJ`
- Confirm the MCP endpoint path: `https://<your-instance>.acryl.io/integrations/ai/mcp`
- Try accessing the URL in your browser to check for auth errors

### Problem: Empty or No Results from DataHub

**Symptoms**: DataHub tools return no results for queries

**Solutions**:

- Verify the token has correct permissions in DataHub
- Check that entities exist and are searchable in the DataHub UI
- Try the same search directly in the DataHub UI
- Verify the DataHub search index is up to date

### Getting Help

- **DataHub Documentation**: [Agent Context Kit](./agent-context.md)
- **Snowflake Cortex Code Docs**: [Cortex Code CLI](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code-cli)
- **Community Support**: [DataHub Slack](https://datahub.com/slack/)
- **GitHub Issues**: [Report bugs](https://github.com/datahub-project/datahub/issues)
- **DataHub Cloud Support**: Email support@acryl.io
