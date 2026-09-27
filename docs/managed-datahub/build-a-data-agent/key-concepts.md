---
title: Key Concepts
description: "The ideas behind a reliable data agent on DataHub: context documents, trust signals, domains, ranking, and human review."
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Key Concepts

<FeatureAvailability saasOnly />

This page explains the ideas the guide relies on. You can follow the steps without it, but it's a good place to start if the terms are new.

## Context documents

An agent can read table and column names on its own. What it can't infer is everything around them: what "active customer" means, which of three revenue tables Finance trusts, or who approves access to customer PII.

A **context document** captures that knowledge. It's a short, plain-language page that an agent finds by asking a question in its own words. The best ones read like a map: "To answer questions about trial conversion, start with these tables, join them like this, and exclude internal accounts."

If you've worked with agent skills, the idea is similar: a focused piece of know-how that an agent loads only when a question calls for it. The difference is authorship. Skills are written by whoever builds the agent. Context documents are written, generated, and reviewed by the people who know the data.

Context documents can cover:

- **Analytics:** metric definitions, which tables to use, how they join, and common filters
- **Governance and process:** how to request access, what counts as PII, and who approves what
- **Guardrails:** rules for specific tables, such as "always exclude test accounts" or "this is a daily snapshot, so never sum across days"

When you relate a document to a table, dashboard, metric, or glossary term, the document travels with it. Whenever your agent looks up that asset, it sees the document too.

Context documents come from two sources:

- **Your team.** People write them in DataHub or import them from Notion, Confluence, or GitHub ([step 2](./ingest-context.md)). These cover the questions everyone asks.
- **Your analytics exhaust.** DataHub reads the queries your analysts run, along with your BI and semantic models, and writes a document for each business question it sees answered repeatedly ([step 3](./generate-context.md)). These cover the long tail no one has time to write down.

## Trust signals

Context documents explain your data. The rest of DataHub tells your agent how far to trust it. Once your warehouse and tools are connected, DataHub knows:

- **Lineage:** where a table comes from and what depends on it, so the agent can tell a curated table from the raw data behind it
- **Ownership:** who is responsible for a table, so the agent can favor maintained data and tell users whom to ask
- **Usage:** how often a table is queried and by how many people, so the agent picks the table analysts rely on over an abandoned copy
- **Past queries:** SQL that has already run against a table, so the agent can reuse proven joins and filters
- **Freshness and status:** when a table last changed, and whether it's deprecated

Your agent uses these signals the way an experienced analyst would: to choose between similar tables, to check its work before running a query, and to cite its sources. They're what make an answer correct and consistent, rather than merely plausible.

## Global agents and domain agents

The most consequential decision is what kind of agent you're building.

|                          | **Global agent**                                      | **Domain agent**                                                       |
| ------------------------ | ----------------------------------------------------- | ---------------------------------------------------------------------- |
| **Who uses it**          | Analysts, data engineers, and data scientists         | A business team, such as Finance, Sales Ops, or Marketing              |
| **Typical question**     | "Where do we keep clickstream data, and who owns it?" | "What was net revenue retention for enterprise accounts last quarter?" |
| **What it can see**      | Everything the person asking can see                  | A curated set of trusted tables, metrics, and documents                |
| **What good looks like** | Points people to the right data quickly               | Returns the right number every time, or says it can't                  |
| **Who catches mistakes** | Usually the user, who can read the SQL                | Often no one, until the number reaches a report                        |
| **Evals**                | A small set of discovery questions                    | A thorough suite, owned by the business team                           |

We recommend rolling out domain agents one domain at a time, starting with a team that has real questions and an owner who cares about the answers. A global agent for your data team is easy to add alongside them.

## Domains

Domain agents work because everything they need is grouped together. In DataHub, that grouping is a [domain](../../domains.md). You can assign almost everything to one:

| What                                 | How it joins the domain                                               |
| ------------------------------------ | --------------------------------------------------------------------- |
| Tables, metrics, and semantic models | Add them from the domain's page                                       |
| Context documents                    | Set the domain yourself, or have Context Generation assign it         |
| Evals                                | Set the eval's domain                                                 |
| Owners                               | The domain's owners, and the owners of its tables, review its context |

You then point an agent at the domain, either through a custom agent in DataHub or a scoped MCP server. The agent searches less and answers more accurately. You can run many domain agents side by side, each with its own context, evals, and owners.

## How DataHub ranks context

When your agent searches DataHub, results are ranked so the most trustworthy context comes first:

- **Meaning.** Context documents are searched by meaning, so a question about "customer churn" finds a document titled "Retention analysis."
- **Popularity.** Tables, metrics, and documents people actually use rank above those nobody touches.
- **Freshness.** Recently updated, actively used context ranks above stale material.
- **Your signals.** Deprecated tables rank lower, and published documents rank above drafts.

<!-- TODO(certification): When certification ships, add a bullet here, e.g.
"**Certification.** Tables and docs your team has certified get a boost." Link to the certification feature guide. -->

DataHub handles the ranking. Your part is to make good context easy to distinguish: bring in documentation people maintain, deprecate look-alike tables, and publish only what you've verified.

## Human review

Agents act on what they read, so the people who know the data decide what goes in. Anyone can propose a change, and the right people approve it:

- **Generated documents** publish automatically by default and are verified by your evals. For sensitive domains, turn off auto-publish and name the reviewers who must approve them.
- **Edits proposed in chat or through MCP**, and edits from people who can't change a document directly, go to the document's owners for review.
- **Generated evals** go to your admins for approval.
- **Table and column descriptions** follow the same flow, reviewed by the table's owners.

Reviewers can run evals against a proposed change before approving it, so you learn whether it helps before your agent does. See [Reviewing Context Changes](../../features/feature-guides/context/context-review.md).
