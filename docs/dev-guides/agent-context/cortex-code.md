# Snowflake Cortex Code Integration

> **📚 Navigation**: [← Back to Agent Context Kit](./agent-context.md)

## What Problem Does This Solve?

Snowflake Cortex Code is an AI-powered coding assistant that lives in your terminal, but out of the box it only knows what your Snowflake account exposes — raw table and column names with no business context. Without metadata enrichment, Cortex Code:

- ❌ Can't distinguish `customer_revenue` from `customer_revenue_archive`
- ❌ Doesn't understand business glossary terms like "churn" or "LTV"
- ❌ Can't surface which datasets are certified vs deprecated
- ❌ Has no visibility into data ownership, documentation, or lineage
- ❌ Hallucinates table names when answering questions about your data

**DataHub's MCP server** solves this by giving Cortex Code direct access to your DataHub metadata catalog over the [Model Context Protocol (MCP)](https://modelcontextprotocol.io). Cortex Code calls DataHub MCP tools during reasoning, grounding its SQL generation and data discovery in your real metadata.

### What You Can Do

- ✅ **Semantic Search**: "Find all revenue tables owned by the finance team"
- ✅ **Business Context**: "What's the business definition of 'churn'?"
- ✅ **Quality Signals**: "Show me certified customer datasets"
- ✅ **Documentation Access**: Search across data docs and descriptions
- ✅ **Ownership Info**: Discover who owns and maintains datasets

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

### Switching Models

Use the `/model` command during a session to switch models:

| Model               | Notes                                                               |
| ------------------- | ------------------------------------------------------------------- |
| `auto`              | Recommended — automatically selects highest-quality available model |
| `claude-opus-4-6`   | Highest capability                                                  |
| `claude-sonnet-4-6` | Balanced speed and capability                                       |
| `claude-sonnet-4-5` |                                                                     |
| `openai-gpt-5.2`    |                                                                     |

### Cross-Region Inference

If a model is unavailable in your Snowflake region, an `ACCOUNTADMIN` can enable cross-region inference:

```sql
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'AWS_US';
```

| Setting      | Use Case                        |
| ------------ | ------------------------------- |
| `AWS_US`     | Best for Claude Opus 4.x models |
| `AWS_EU`     | Claude access from EU regions   |
| `AWS_APJ`    | Claude access from APJ regions  |
| `ANY_REGION` | Global routing                  |
| `AZURE_US`   | OpenAI GPT 5.2 access           |

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
