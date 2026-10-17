

# Key Concepts

> **Availability:** DataHub Cloud only

This page explains the ideas the guide relies on. You can follow the steps without it, but it's a good place to start if the terms are new.

| Concept                                                                                         | Where it appears in the guide                                                                    |
| ----------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| [The Context Layer](#the-context-layer)                                                         | [Step 2: Ingest context](./ingest-context.md), [Step 3: Generate context](./generate-context.md) |
| [Validating Context Through Evals](#validating-context-through-evals)                           | [Step 1: Define evals](./define-evals.md)                                                        |
| [Validating Context Changes (Human in the Loop)](#validating-context-changes-human-in-the-loop) | [Step 3: Generate context](./generate-context.md)                                                |
| [Global vs. Domain-Scoped Data Agents](#global-vs-domain-scoped-data-agents)                    | [Step 4: Activate context](./activate-context.md)                                                |
| [Maintaining Context Through Feedback](#maintaining-context-through-feedback)                   | [Step 5: Context feedback & improvement](./improve-with-feedback.md)                             |

## The Context Layer

DataHub gives your agents a single context layer with two parts: the **technical context** that inventories your data, and the **business context** that explains what it means. **Domains** organize both into smaller units.

### Technical Context

Technical context is the foundation. When you connect your warehouse, BI tools, and pipelines, DataHub builds an inventory of your data landscape:

- **Assets:** tables, columns, dashboards, charts, pipelines, and the queries that run against them
- **Lineage:** where each asset comes from and what depends on it
- **Ownership:** who is responsible for each asset
- **Usage:** how often each asset is used, and by how many people
- **Past queries:** SQL that has already run against each table
- **Status:** when each asset last changed, and whether it's deprecated

An agent uses these as **trust signals**, the way an experienced analyst would. They help it tell a curated table from the raw data behind it, prefer the table people actually use over an abandoned copy, reuse proven joins and filters, and cite its sources.

DataHub also uses these signals to rank what your agent sees. When an agent searches, context that's widely used and recently updated ranks higher, deprecated assets rank lower, and published documents rank above drafts. The more clearly your technical context separates good data from bad, the more reliably your agent finds the right answer.

<!-- TODO(certification): When certification ships, add it here, e.g.
"Assets and documents your team has certified also rank higher." Link to the certification feature guide. -->

### Business Context

Business context is the semantic knowledge that explains what your data means and how to use it. It includes:

- **Context documents:** guides, recipes, runbooks, and examples an agent can follow to give the canonical answer to a business question. A good one reads like a map: "To calculate net revenue retention, start with these tables, join them like this, and exclude trial accounts." Context documents also capture governance and process ("how to request access to customer PII") and guardrails for specific tables ("amounts are in cents"). Relate a document to the tables it describes, and your agent sees it whenever it looks those tables up.
- **Descriptions:** plain-language explanations of tables and columns
- **Glossary terms:** shared definitions of business concepts, such as "active customer," linked to the columns that implement them
- **Semantic models and metrics:** precise metric definitions from tools like dbt, Snowflake, Looker, and Cube

Context documents come from two sources:

- **Your team** writes them, or imports them from tools like Notion, Confluence, and GitHub ([step 2](./ingest-context.md)).
- **[Context Generation](../../features/feature-guides/context/context-generation.md)** writes them for you, by capturing the patterns your team already uses to answer important business questions in its query history, BI tools, and semantic models ([step 3](./generate-context.md)). These cover the long tail no one has time to document by hand.

Context documents are searched by meaning, so an agent's question finds the right document even when its wording differs. If you've worked with agent skills, the idea is similar: focused know-how that an agent loads only when a question calls for it. The difference is that context documents are written, generated, and reviewed by the people who know the data.

### Domains

A [domain](../../domains.md) is an optional way to organize your data and context into smaller units, such as everything that belongs to Finance. You can assign almost everything to a domain:

| What                                 | How it joins the domain                                               |
| ------------------------------------ | --------------------------------------------------------------------- |
| Tables, metrics, and semantic models | Add them from the domain's page                                       |
| Context documents                    | Set the domain yourself, or have Context Generation assign it         |
| Evals                                | Set the eval's domain                                                 |
| Owners                               | The domain's owners, and the owners of its tables, review its context |

Domains let you manage context one area at a time, and they make [domain-scoped data agents](#global-vs-domain-scoped-data-agents) possible. There are three ways to expose a domain to an agent:

- **A custom agent** in DataHub, scoped to the domain
- **A scoped MCP server,** limited to the domain with a View
- **Instructions** that teach your agent how to navigate your domain hierarchy, such as which domain to search for which kinds of questions

The first two limit what the agent sees; the third guides where it looks.

## Validating Context Through Evals

An **eval** is a real business question paired with the answer an expert would give, usually as SQL. DataHub asks an agent the question, and an AI judge checks whether the agent's answer is equivalent to the expected one.

Together, your evals form a test suite for your context. They give you a baseline before you add any context, show whether each change helps, and catch regressions the day after they happen. That's why the guide starts with them: every later step is measured against your evals. See [Context Evals](../../features/feature-guides/context/context-evals.md).

## Validating Context Changes (Human in the Loop)

Agents act on what they read, so the people who know the data decide what goes in. DataHub lets anyone propose a change to context, whether it comes from a person, DataHub's AI, your own agents, or Context Generation. The owners of the context involved review it before agents see it.

Reviewers can run evals against a proposed change before approving it, so they learn whether it helps before your agent does. You choose where review is required: for example, generated documents can publish automatically and be verified by evals, or require approval for sensitive domains. See [Reviewing Context Changes](../../features/feature-guides/context/context-review.md).

## Global vs. Domain-Scoped Data Agents

The most consequential decision is what kind of agent you're building. The guide calls them **global agents** and **domain agents** for short.

|                      | **Global data agent**                                 | **Domain-scoped data agent**                                           |
| -------------------- | ----------------------------------------------------- | ---------------------------------------------------------------------- |
| **Who uses it**      | Analysts, data engineers, and data scientists         | A business team, such as Finance, Sales Ops, or Marketing              |
| **Typical question** | "Where do we keep clickstream data, and who owns it?" | "What was net revenue retention for enterprise accounts last quarter?" |
| **What it can see**  | All the data and context the person asking can see    | One [domain's](#domains) trusted tables, metrics, and documents        |

We recommend rolling out domain-scoped agents one domain at a time. A global agent for your data team is easy to add alongside them. You can run many domain-scoped agents side by side, each with its own context, evals, and owners. See [Step 4](./activate-context.md#what-to-set-up) for how to set up each kind.

## Maintaining Context Through Feedback

Your data and your questions change constantly, so context needs ongoing maintenance. Feedback is how you keep it current.

No eval suite anticipates every question. Once your agent is in real use, it reports the gaps it runs into: context that's missing, incorrect, or conflicting, and answers a user has corrected. These notes collect in one place, so your team can fix each gap once, and add an eval so it stays fixed. Scheduled Context Generation and [custom agent](../../features/feature-guides/agents.md) tasks can help with upkeep too.

Feedback closes the loop: real usage improves your context, and your evals confirm each improvement. See [Context Feedback](../../features/feature-guides/context/context-feedback.md).
