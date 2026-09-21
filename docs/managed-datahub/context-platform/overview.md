---
title: Context Overview
description: "DataHub Context delivers trusted, human-validated context to AI agents, increasing their accuracy and reliability in production."
visible-if:
  showContextHub: true
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# About DataHub Context

<FeatureAvailability saasOnly />

:::caution Public Beta
DataHub Context is currently in Public Beta. Features, UI, and configuration options are subject to change.
:::

**DataHub Cloud is an enterprise context platform that provides trusted context to agents**, dramatically increasing their accuracy, reliability, and efficiency in production.

AI agents fail on real data not because they cannot write SQL, but because they do not know which table is authoritative, what a metric actually means, or how your business defines a term. The Context Platform closes that gap by capturing the semantic meaning already latent in your query logs, dbt projects, and BI dashboards, putting it in front of a human expert for validation, and then serving the validated result to any agent your team already uses.

## The Four Pillars

| Pillar                   | What it does                                                                                                       |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------ |
| **Context Ingestion**    | 100+ connectors unify fragmented context from everywhere it lives.                                                 |
| **Context Intelligence** | Extracts semantic meaning from your existing query logs, dbt projects, and BI dashboards.                          |
| **Context Hub**          | A dedicated workspace where Data Experts and Subject Matter Experts validate, refine, enrich, and publish context. |
| **Context Activation**   | Delivers trusted context to any agent your team is already using.                                                  |

## How the Workflow Fits Together

DataHub Context is a human-in-the-loop pipeline with three distinct roles:

1. **An Admin configures context generation** — selecting the domains to scope, defining metadata eval questions that assess quality, and controlling when generated context becomes visible to agents. See [Configure Context Generation](configure-context-generation.md).
2. **A Data Expert or SME validates generated context** — checks accuracy using evaluations, editing business questions and anchor patterns, and deciding what gets published. See [Review Context Proposals](review-context-proposals.md).
3. **Agents consume the published context** — via the DataHub MCP server and agent skills. See [Activate Context](activate-context.md).

By default, generated context is **not** visible to agents until a human has reviewed and published it.

## Prerequisites

### 1. Pin the CLI version in your ingestion recipes

Context generation requires CLI version **1.6.0.9** or later.

- **Ingestion via UI:** specify the version under **Advanced Settings**.
- **Ingestion via CLI:** check your current version with `datahub version` in the environment where the `default` executor runs, then upgrade:

```bash
pip install --upgrade 'acryl-datahub[datahub-rest]==1.6.0.9'
```

### 2. Grant the right privileges

An Admin must assign users to the [Editor role](https://docs.datahub.com/docs/authorization/roles), or grant the individual privileges below, so that expert reviewers and SMEs can review generated context documents.

### 3. Configure your data source connectors

Configure [data source connectors](https://docs.datahub.com/docs/metadata-ingestion) with **query ingestion enabled**. Supported sources in Private Beta: **Snowflake, Databricks, BigQuery, and Redshift**.

Verify your recipes against the reference [Metadata Ingestion Recipes](configure-context-generation.md#reference-metadata-ingestion-recipes).

### 4. Configure BI and transformation connectors

Configure **Looker** and **dbt** connectors for the datasets in any domain you plan to scope for context generation. These enrich generated context with business meaning that raw query logs alone do not carry.

### 5. Configure Domains (recommended)

Configure [Domains](https://docs.datahub.com/docs/domains) with owners (users or groups) and assets. Context generation is scoped by domain, and domain owners are who receive context proposals for review.

## Who Needs What Access

| Role               | Action Required                                                                                                                                 | Allowable Context Actions                                                                                                                                                        |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Admin**          | Admin adds users who need the allowed context actions. Default privileges: `MANAGE_DOCUMENTS`, `MANAGE_ALL_DOCUMENT_PROPOSALS`, `MANAGE_AGENTS` | Configure context generation; create and edit metadata eval questions; view and edit all unpublished context documents and context proposals                                     |
| **Editor**         | Admin adds Data Expert / SME users to the Editor role. Default privileges: `MANAGE_DOCUMENTS`, `MANAGE_DOCUMENT_PROPOSALS, MANAGE_EVALS`        | View, edit, comment on, manage publish state of, and approve/reject context document proposals; edit and delete context documents; view, create, edit, and delete metadata evals |
| **Reader**         | Admin adds users to view published context                                                                                                      | View published context documents only                                                                                                                                            |
| **Specific users** | Admin grants the `MANAGE_DOCUMENTS` privilege                                                                                                   | View and edit context proposals; view, edit, and delete context documents                                                                                                        |
| **Specific users** | Admin grants the `MANAGE_DOCUMENT_PROPOSALS` privilege                                                                                          | View, manage publish state of, edit, comment on, and approve/reject context proposals                                                                                            |

## Best Practices for Context Validation

- **Define metadata evals up front.** Automate validation of your business questions with explicit pass criteria before you start reviewing at scale.
- **Run evals first.** Before editing a document, run the eval questions to see what the system believes is correct and where it falls short.
- **Test with Ask DataHub.** Use the **Test on Ask DataHub** action to observe the real-world impact of publishing a document before you commit to it.
- **Use comments to communicate.** Leave notes for colleagues on documents you are unsure about rather than approving or rejecting immediately.
- **Publish incrementally.** Start with a small set of high-confidence documents to validate agent performance before publishing broadly.

## Key Concepts

**Context Document (Semantic Anchor)**
A structured document that captures business knowledge about a data asset — including what questions it answers, how it should be interpreted, and what SQL patterns are associated with it. Context documents are what AI agents use to understand your data.

**Document Library**
A place to view Context Documents that have been published.

**Context Proposal**
A batch of unpublished context documents generated by the system and submitted for SME review. Proposals appear in **Task Center** > **Requests**.

**Publish State**
Whether a context document is visible to AI agents and search. Published = visible; Unpublished = hidden. Only Admins and Editors with the `MANAGE_DOCUMENT_PROPOSALS` privilege can change publish state.

**Metadata Eval Questions**
Questions defined by Admins to assess the quality of generated context. Evals run automatically during generation and can also be triggered manually during review.

**Anchor Patterns**
SQL patterns and schema references used by the AI agent to connect business questions to the right data. Reviewers can edit these to improve accuracy.

**Domain**
A grouping of related data assets in DataHub. Context generation is scoped by domain — each domain you select will have context generated for its datasets.

## Getting Help

If you run into an issue with the Context Platform, post it in your shared Slack channel with the ticket 🎫 emoji. We respond within one business day.

## Next Steps

Now that you understand the basics of the DataHub Context Platform, you're ready to [Configure Context Generation](configure-context-generation.md).
