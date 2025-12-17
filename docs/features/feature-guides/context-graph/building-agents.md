---
title: Building Agents with Context Graph
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Building Agents with Context Graph

<FeatureAvailability/>

Context Graph is designed to make your organization’s unstructured knowledge usable by AI agents. By indexing context documents and linking them to assets, agents can combine **tribal knowledge** with **metadata graph** signals to answer richer questions and take better actions.

## Why Use Context Graph for Agents?

Traditional agents can see your data's _structure_ (schemas, lineage, owners). Context Graph lets them also understand your data's _story_—the policies, definitions, and tribal knowledge that turn raw metadata into actionable intelligence.

## What You Can Build

- **Data governance & compliance agents**: Ground decisions in compliance policies and handling guidelines linked to regulated datasets and dashboards.
- **Data quality agents**: Recommend checks, triage incidents, and follow runbooks that are linked directly to the tables they apply to.
- **Data analytics agents**: Answer metric and KPI questions by combining asset metadata with definitions, FAQs, and decision records stored as context documents.

**Example**: Imagine an agent that, when asked "Can I use this customer table for analytics?", checks the table's properties AND references your compliance policy document to give a definitive yes/no with proper citations.

## Tools You Can Use

### Python SDK

Use the Python SDK to create and manage documents (native and external) and to build agent workflows that attach context to assets.

- [Documents API tutorial](../../../api/tutorials/documents.md)

**Quick example**: Create and retrieve documents in agent workflows

```python
from datahub.sdk import DataHubClient, Document

client = DataHubClient.from_env()

# Create a context document for agent use
runbook = Document.create_document(
    id="quality-runbook",
    title="Customer Table Quality Checks",
    text="# Quality Guidelines\n\n- Check for PII compliance\n- Validate email formats...",
)
client.entities.upsert(runbook)

# Agent retrieves document by URN when needed
doc = client.entities.get(runbook.urn, Document)
# Use doc.text to inform agent's recommendations
```

::::info Documentation placeholder
_TODO_: Add a Python guide for search and retrieval of context documents once available.
::::

### MCP Server (for agent runtimes)

The MCP server enables AI tools to query DataHub for metadata and context.

- [DataHub MCP Server](../mcp.md)

::::info Documentation placeholder
_TODO_: Add links / examples for MCP tools that support: searching documents, fetching document contents, and saving context documents once finalized.
::::

:::tip Try Ask DataHub
Want to see Context Graph in action without building an agent? [Ask DataHub](../ask-datahub.md) uses Context Graph documents to answer questions about your data with citations to your organization's knowledge.
:::

## What's Next

We're actively expanding Context Graph capabilities:

- **Agent framework integrations**: Native support for LangChain, Google ADK, and CrewAI
- **Persistent agent memory**: Use Context Graph to store and manage long-lived agent notes, decisions, and learned context
- **Enhanced MCP tools**: Search, fetch, and save document operations

## Next Steps

- [DataHub Context Documents](datahub-context-documents.md)
- [External Context Documents](external-context-documents.md)
