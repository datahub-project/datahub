# Agent Context Kit

The DataHub Agent Context Kit is a set of guides, SDKs, and an [MCP server](../../features/feature-guides/mcp.md) that help you build AI agents with access to the capabilities and context in your DataHub instance — business definitions, context documents, data ownership, lineage, quality signals, sample queries, and more.

## What Can You Build?

### Data Analytics Agent (Text-to-SQL)

An agent that answers business questions by finding the right data, then querying it.

1. **Find & understand trustworthy data** — Search DataHub for relevant datasets, read descriptions, check glossary terms, review sample queries, and look at usage stats to pick the right table.
2. **Execute SQL** — Generate and run SQL against your data warehouse (Snowflake, BigQuery, Databricks, etc.).

_"What was our revenue by region last quarter?" → finds the revenue table in DataHub, confirms it's the certified source, generates the SQL, runs it._

### Data Quality Agent

An agent that provisions data quality checks and reports on the health of your data estate.

1. **Find important tables** — Identify high-usage or business-critical datasets using usage stats and ownership from DataHub.
2. **Add assertions** — Provision data quality assertions in DataHub (and in external tools).
3. **Generate health reports** — Daily or on-demand, broken down by domain or owning team, using assertion results and incident data.

_"Set up freshness checks on all tables owned by the Finance team, and send me a weekly health summary."_

### Data Steward / Governance Agent

An agent that applies descriptions and compliance-related glossary terms to tables and columns, then reports on coverage.

1. **Find target tables** — Filter by platform, domain, or ownership to find datasets that need attention.
2. **Apply context** — Cross-reference schema information against critical glossary terms, then apply glossary terms, descriptions, and tags.
3. **Report on compliance** — Generate reports on where sensitive data lives, PII usage across the organization, or gaps in documentation.

_"Tag all columns containing email addresses with the PII glossary term across our Snowflake datasets, then show me a coverage report."_

## Where Do Your Agents Run?

### AI Coding Assistants

| Platform                | Guide                               |
| ----------------------- | ----------------------------------- |
| Cursor                  | [Guide](./cursor.md)                |
| Claude (Code & Desktop) | [Guide](./claude.md)                |
| Google Gemini CLI       | [Guide](./gemini-cli.md)            |
| Snowflake Cortex Code   | [Guide](./snowflake-cortex-code.md) |

### Agent Frameworks (SDK)

| Platform            | Guide                                         |
| ------------------- | --------------------------------------------- |
| LangChain           | [Guide](./langchain.md)                       |
| Google ADK          | [Guide](./google-adk.md)                      |
| Custom / Direct MCP | See [Getting Started](#getting-started) below |

### Managed Agent Platforms

| Platform                 | Guide                                 |
| ------------------------ | ------------------------------------- |
| Databricks Genie Code    | [Guide](./databricks-genie-code.md)   |
| Databricks Agent Bricks  | [Guide](./databricks-agent-bricks.md) |
| Snowflake Cortex Agents  | [Guide](./snowflake.md)               |
| Google Vertex AI         | [Guide](./google-vertex-ai.md)        |
| Microsoft Copilot Studio | [Guide](./copilot-studio.md)          |

## Getting Started

The fastest way to connect any agent to DataHub is through the [MCP server](../../features/feature-guides/mcp.md) — just point your agent at the endpoint:

- **DataHub Cloud**: `https://<tenant>.acryl.io/integrations/ai/mcp`
- **Self-hosted**: `http://<gms-host>:8080/mcp`

See the [MCP Server Guide](../../features/feature-guides/mcp.md) for authentication and setup.

For Python SDK usage (LangChain, Google ADK, etc.):

```bash
pip install datahub-agent-context
```

**Requirements:** Python 3.10+, a DataHub instance, and a [personal access token](../../authentication/personal-access-tokens.md).

## Available Tools

Agents discover these automatically via MCP. See the [MCP Server Guide](../../features/feature-guides/mcp.md) for details.

| Tool                                           | What it does                                                           |
| ---------------------------------------------- | ---------------------------------------------------------------------- |
| `search`                                       | Find datasets, dashboards, and other entities by keyword               |
| `get_entities`                                 | Get full details for a specific entity (schema, ownership, docs, tags) |
| `get_lineage`                                  | Trace upstream or downstream lineage                                   |
| `list_schema_fields`                           | List columns for a dataset                                             |
| `get_dataset_queries`                          | Get SQL queries associated with a dataset                              |
| `search_documents` / `grep_documents`          | Search knowledge base articles and docs                                |
| `add_tags` / `remove_tags`                     | Manage tags                                                            |
| `update_description`                           | Set or update descriptions                                             |
| `add_glossary_terms` / `remove_glossary_terms` | Link to glossary terms                                                 |
| `set_domains` / `add_owners` / `save_document` | Manage domains, ownership, and documents                               |

**Need help?** [DataHub Slack](https://datahub.com/slack/) · [GitHub Issues](https://github.com/datahub-project/datahub/issues) · DataHub Cloud: support@acryl.io
