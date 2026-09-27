---
title: FAQ
description: "Answers to common questions about building data agents on DataHub: semantic layers, agent platforms, evals, data access, and the Context add-on."
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# FAQ

<FeatureAvailability saasOnly />

## Is DataHub a semantic layer?

No. DataHub activates the semantic layer you already have, and fills in the context around it.

A semantic layer defines your most important, best-governed metrics. It should remain the source of truth for those metrics, and the first stop for your most frequent questions. Agents that answer questions across all of your data need more: which tables to use for everything else, how they join, and the rules that were never written down. DataHub fills in that long tail based on how people already use your data, and keeps it reviewed and tested.

## We already have a semantic layer. Do we still need DataHub?

Only if your agent needs to go beyond the metrics your semantic layer defines, which most do. Bring your semantic layer into DataHub ([step 2](./ingest-context.md)), and DataHub builds on it.

## Why not build this ourselves?

Many teams start with a folder of prompts and Markdown files. That works for a single team, then breaks down: the context isn't tied to the tables it describes, it goes stale as your data changes, it lives in several places, and only engineers can maintain it.

DataHub ties every piece of context to the assets it describes, keeps it current from your existing tools and query history, verifies it with review and evals, and lets the people who know the data maintain it without writing code.

## Do we need a particular agent?

No. DataHub complements your agent stack rather than replacing it. Claude, ChatGPT, Databricks Genie, Snowflake Cortex and CoWork, LangChain, CrewAI, Google ADK, and custom agents can all use DataHub over MCP. [Step 4](./activate-context.md) covers three options: use Ask DataHub directly, connect your agent to a DataHub agent, or connect your agent to DataHub's tools.

## We already have an evals platform. Why use DataHub evals?

DataHub evals answer a narrower question: can an agent find and use the right context in DataHub? That makes them the right tool for deciding whether a change to your context helps before it goes live. Keep your end-to-end agent evals in tools such as Langfuse or LangSmith as well. The two work well together, and you can report results from your own harness into DataHub.

## Can we use context we already have, such as documents in Git or a wiki?

Yes. Import it, or write it directly in DataHub. DataHub gives it one place to be reviewed, tested with evals, and published to your agents.

## Can agents search everything in natural language?

Context documents are searched by meaning, so an agent's question finds the right document even when the wording differs. Tables, metrics, and other assets are found through DataHub search, ranked by popularity, freshness, and the signals you set. Natural-language search for table and column descriptions is coming soon.

## Does DataHub read our data?

No. DataHub works from metadata: schemas, BI and semantic definitions, your documents, and the query logs from your warehouse. It never reads the data in your tables. When an agent runs SQL, the query runs in your warehouse, with the permissions you grant.

## Are query logs sent to a language model?

No. Query logs can contain literal values, such as a customer ID in a filter, so Context Generation doesn't send them to a language model. Instead, it groups queries deterministically by parsing their SQL, then generates documents that describe the general query pattern (the tables, joins, metrics, and filters involved) rather than any specific query.

## How is AI usage billed?

With AI Credits. AI features such as Context Generation, Ask DataHub, and custom agents consume AI Credits as they run. Your DataHub Cloud subscription includes a bundle of AI Credits, which roughly corresponds to the underlying model usage. Contact your DataHub account team for details about your plan.

## Who needs which permissions?

| To...                                                             | You need                                           | Granted by default to |
| ----------------------------------------------------------------- | -------------------------------------------------- | --------------------- |
| Connect data sources and import documents                         | **Manage Metadata Ingestion**                      | Admins                |
| Create Context Generation jobs, MCP servers, and AI plugins       | **Manage Platform Settings**                       | Admins                |
| Create and run evals                                              | **Manage Evals**                                   | Admins and Editors    |
| Create custom agents, generate evals, and approve generated evals | **Manage Agents**                                  | Admins                |
| Review Context Feedback                                           | **Manage Context Feedback**                        | Admins and Editors    |
| Approve changes to a context document                             | **Manage Documents**, or ownership of the document | Admins and owners     |

## Can we use this with DataHub Core?

Partly. DataHub Core, the open-source edition, includes context documents, metrics and semantic models, and the [MCP server](../../features/feature-guides/mcp.md), so you can connect your own agent to DataHub's tools, as in option c, without the SQL context tools that come with the add-on. Context Generation, evals, review workflows, custom agents, and scoped MCP servers require DataHub Cloud with the Context add-on.

## What's included in the Context add-on?

Context Generation, evals, review workflows for context changes, natural-language search for context documents, custom agents, and scoped MCP servers. Connecting your data, writing and importing documents, and the main [MCP server](../../features/feature-guides/mcp.md) are part of DataHub Cloud.
