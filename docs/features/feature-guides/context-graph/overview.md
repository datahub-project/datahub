---
title: Overview
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# About Context Graph

<FeatureAvailability/>

Your team's best knowledge lives in runbooks, Notion pages, and tribal wisdom. **Context Graph brings that knowledge into DataHub** so it's discoverable, governable, and—most importantly—usable by both people and AI. You can create documents directly in DataHub, ingest documents from external tools, and link documents to the data assets they relate to.

:::info Screenshot placeholder
_TODO_: Add a screenshot of the Context Graph / Documents directory experience (left navigation / directory tree).
:::

## What You Can Do

Context Graph gives you three core capabilities:

### Create Context Documents in DataHub

Create and manage documents inside the DataHub directory:

- **Organize hierarchically**: Create parent-child documents and move documents between parents.
- **Edit safely**: View document history and restore previous versions.
- **Govern & enrich**: Add Tags, Glossary Terms, Domains, and Structured Properties.
- **Control visibility**: Publish / unpublish documents to control whether they’re visible to other users and AI experiences.

### Connect External Context Documents

Ingest and index contents of documents authored in external tools like Notion so they show up in DataHub search results and can be navigated to from DataHub.

### Link Documents to Data Assets

The real power of Context Graph comes from connecting documents to the assets they describe. Link both DataHub and external documents to existing assets (Datasets, Dashboards, Charts, etc.) to:

- **Navigate from assets → related documents** (and vice versa)
- **Preserve "why" and "how" context** close to the metadata graph

## Use Cases

Context Graph helps you **find and govern documents alongside your data**, but its real superpower is making **tribal knowledge available to AI** so agents can answer a broader set of questions by combining:

- **Asset metadata** (schemas, lineage, ownership, domains, tags, usage, etc.)
- **Unstructured context** (runbooks, policies, FAQs, guidelines, decision history)

### Practical use cases

- **Metric definitions & approvals**: Keep an “Approved Metrics for Executive Reporting” FAQ discoverable from both metrics and the datasets powering them.
- **Data quality runbooks**: Document recommended checks and common failure modes, and link them to the tables they apply to.
- **Compliance & sensitive data policies**: Centralize handling guidance and connect it to regulated datasets and dashboards.
- **Access & operating guidelines**: Make “how we work with data” easy to find and easy for AI to reference.

### Examples

By combining unstructured context with your data assets, you can begin to answer broader questions like:

- **"Which metrics are approved for executive reporting?"** → FAQ
- **"What data quality checks should I add to my new table?"** → Quality Runbook
- **"How should I handle sensitive or regulated data?"** → Compliance Policies
- **"What's the process for requesting new data access?"** → Data Access Guidelines

:::tip Quick Win
Start small: Document 3-5 of your most-asked questions as FAQs, link them to the relevant datasets, and watch Ask DataHub start citing them in responses.
:::

## How People and AI Find Documents

Once you've added documents, they become instantly searchable. Both DataHub and external documents have their contents indexed so they can be found via:

- **DataHub Search** via the user interface & APIs
- **Ask DataHub** (DataHub Cloud’s AI assistant)
- **Programmatic access**
  - Python SDK (see below)
  - MCP server tools (see below)

:::note
Visibility controls apply: **only published documents** are intended to be broadly visible to other DataHub users and AI experiences.
:::

## Next Steps

Learn more about how Context Graph documents are created, connected, and used across DataHub:

- [DataHub Context Documents](datahub-context-documents.md): Create, organize, publish, and link documents authored in DataHub.
- [External Context Documents](external-context-documents.md): Ingest documents from third-party tools while keeping the source of truth external.
- [Building Agents with Context Graph](building-agents.md): Use context documents to ground agents in governance, quality, and analytics knowledge.

### Related Docs

- **Ask DataHub**: [Ask DataHub feature guide](../ask-datahub.md)
- **MCP server**: [DataHub MCP Server](../mcp.md)
- **Create documents via Python**: [Documents API tutorial](../../../api/tutorials/documents.md)
