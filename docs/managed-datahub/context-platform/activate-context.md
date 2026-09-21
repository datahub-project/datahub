---
title: Activate Context
description: "Deliver trusted, published context to the AI agents your team already uses via the DataHub MCP server and agent skills."
visible-if:
  showContextHub: true
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Activate Context

<FeatureAvailability saasOnly />

:::caution Public Beta
DataHub Context is currently in Public Beta. Features, UI, and configuration options are subject to change.
:::

Context Activation delivers trusted context to any agent your team is already using. Once a Data Expert has [published context documents](review-context-proposals.md), agents retrieve them through the DataHub MCP server and use them to ground their answers.

:::note
Only **published** context documents are visible to agents. Unpublished documents are hidden from both agents and search.
:::

## Connect Your Agent to DataHub MCP

Configure your MCP client to connect to the DataHub MCP server. See [Configure your client](https://docs.datahub.com/docs/features/feature-guides/mcp#configure-your-client) in the MCP Server guide for client-specific setup instructions.

## Agent Skills

Analytics agents (text-to-SQL) should be instructed to use the `datahub-sql-workflow` skill, which searches DataHub context for grounded truth when answering natural language business questions.

### Claude Code

Install the skill from the DataHub skills repository:

- [datahub-sql-workflow](https://github.com/datahub-project/datahub-skills/tree/main/skills/datahub-sql-workflow)

### Snowflake Cortex Agent and Databricks Genie

Skills for Snowflake Cortex Agent and Databricks Genie are available in Private Beta. Please contact DataHub support to get set up.

## Validating Activation

Before rolling out broadly, confirm that published context is actually changing agent behavior:

- Use **Test on Ask DataHub** from the context document review view to compare responses with and without the document.
- Run your [metadata eval questions](review-context-proposals.md#step-1-create-metadata-eval-questions) against the published set and confirm they pass.
- Start with a small set of high-confidence documents in one domain before publishing broadly.

### Related Features

- [MCP Server](https://docs.datahub.com/docs/features/feature-guides/mcp)
- [Ask DataHub](https://docs.datahub.com/docs/features/feature-guides/ask-datahub)
- [Context Documents](https://docs.datahub.com/docs/features/feature-guides/context/context-documents)

## Next Steps

If your generated context is missing query history or business meaning, review the reference [Metadata Ingestion Recipes](metadata-ingestion-recipes.md) to confirm your sources are emitting the query entities and usage statistics that context generation depends on.
