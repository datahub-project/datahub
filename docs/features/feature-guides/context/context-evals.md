---
title: Context Evals
description: "Test whether DataHub's context is good enough for AI agents to answer your real business questions, and catch regressions before your users do."
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Context Evals

<FeatureAvailability saasOnly />

:::caution Public Beta
Context Evals are part of the DataHub Cloud **Context** add-on and are in Public Beta. Screens and settings may change as we learn from you.
:::

"Is the agent getting better?" is a hard question to answer by feel. It answered the CFO's question correctly on Tuesday. On Thursday, after someone published a new document, it didn't. Nobody noticed until the board meeting.

**Context Evals** are questions with known-good answers that DataHub re-runs on a schedule. They tell you whether your context is good enough for an agent to get real questions right, and whether a change made things better or worse.

We recommend defining evals first, before you ingest or generate any context. Then every change you make can be measured against them. See [Build a Data Agent](../../../managed-datahub/build-a-data-agent/define-evals.md).

:::tip Already have an evals platform?
Keep it. DataHub evals focus on one thing: whether an agent can find and use the right context in DataHub. That makes them the right tool for judging a change to context before it goes live. They complement end-to-end agent evals, and you can report results from your own harness into DataHub.
:::

## How an eval works

An eval is a **question** plus the **conditions** a good answer must meet:

- **Expected answer**: the SQL (or prose) a correct answer should match. You can add more than one; matching any of them counts.
- **Must reference assets**: tables the answer should use
- **Must not reference assets**: trap tables it should avoid, like that deprecated `orders_old` everyone keeps finding
- **Additional guidelines**: anything else the judge should check, in plain English

When an eval runs, an agent answers the question using DataHub's context. Then two checks run:

1. An **AI judge** compares the answer to your expected answer and guidelines. For SQL, it checks whether the two queries are equivalent: same tables, joins, filters, grain, and aggregation. It doesn't care about formatting or aliases, and it doesn't execute the SQL.
2. A **simple rule check** confirms the answer used the tables it should and none it shouldn't.

Every condition has to pass for the eval to pass. Each result keeps the judge's reasoning, the agent's answer, the assets it cited, and the documents it retrieved, so you can see _why_ it failed rather than just _that_ it failed.

### Eval types

| Type               | Use it for                                                                                     |
| ------------------ | ---------------------------------------------------------------------------------------------- |
| **SQL Generation** | Analytics questions. You provide the SQL a correct answer should be equivalent to.             |
| **Basic**          | Catalog questions: finding the right table, owner, definition, or lineage. You write criteria. |

## Create an eval

You need the **Manage Evals** privilege. Admins and Editors have it by default.

1. Go to **Validation > Evals**.
2. Click **Create Question**.
3. Fill in the **Question**, **Type**, **Expected answer**, and any asset rules or guidelines.
4. Optionally set a **Domain** (for grouping and filtering) and a **Model** for the answering agent.
5. Click **Check for problems**. DataHub flags issues like a table that doesn't exist, contradictory asset rules, or a table no document covers yet. These are warnings, not blockers.
6. Click **Simulate eval run** to try it without recording a result, then save.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/context/context-eval-create.png"/>
</p>

_Screenshot: creating an eval._

### Writing evals that are worth having

- **Use real questions.** Pull them from Slack, your analytics request queue, or that one spreadsheet your analysts keep. Made-up questions test made-up problems.
- **Start with 20 to 50.** Enough to see a trend, few enough that a human can read every failure.
- **Mix easy and hard.** A few "should be obvious" questions catch the embarrassing regressions.
- **Add traps.** If there's a deprecated or look-alike table people confuse, add it under **Must not reference**.
- **Don't over-specify.** If three tables are all valid, don't require one. Add them as alternative expected answers instead. Most "the eval is wrong" moments come from criteria that were stricter than intended.
- **For domain agents, test refusals.** Add a question the agent _shouldn't_ answer and a guideline like "The response should say this is outside its scope."

### Generate evals from your context

Staring at an empty eval list? Ask an admin to click **Generate Evals**. DataHub drafts questions and reference SQL from the business questions in your published context documents, starting with the most-used patterns. You can generate up to 20 at a time and scope them to a domain.

<p align="center">
  <img width="60%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/context/context-evals-generate.png"/>
</p>

_Screenshot: the Generate Evals dialog._

With **Require review before publishing** on (the default), drafts go to your DataHub admins for review. Only generated evals go through review; evals people create with **Create Question** are added directly. They can edit each question and expected answer, then **Approve Question** or reject it. See [Reviewing Context Changes](./context-review.md#review-proposed-evals). Please do review them. An eval that encodes a wrong answer is worse than no eval.

## Run evals

- **On demand:** run a single eval, or **Run all DataHub evals**.
- **Daily:** turn on **Evals run daily** to re-run everything once a day. It's off by default, and turning it on requires the **Manage Platform Settings** privilege.
- **Against a proposed change:** when reviewing a change to context, add the relevant evals in the **Impact on Evals** section and run them. DataHub answers as if the change were already published and labels each result **Fixed**, **Broken**, or **Unchanged**. This is the fastest way to decide what to publish. See [Reviewing Context Changes](./context-review.md#test-the-impact-before-you-approve).

The Evals page summarizes **Passing**, **Failing**, **Flaky** (flipped between pass and fail in the last five runs), and overall pass rate. Open an eval to see its latest result, run history, expected vs. generated answer, and which documents the agent found. If an eval is failing, **Fix with Ask DataHub** opens a chat that investigates and proposes the smallest fix.

<p align="center">
  <img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/context/context-evals-list.png"/>
</p>

_Screenshot: the Evals page, with pass rate, flaky evals, and run history._

:::note Which agent answers?
By default, evals run against a read-only DataHub agent that answers from your context documents and catalog. Evals attached to a [custom agent](../agents.md) run against that agent instead. Its **Evals** tab shows pass rate and trend.
:::

<p align="center">
  <img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/context/context-eval-detail.png"/>
</p>

_Screenshot: an eval's details, comparing expected and generated SQL._

## Evals for external agents

Your production agent might not live in DataHub. Set the eval's **Eval Runner** to **External**. Your own harness asks the question, and reports the answer back to DataHub, which judges it, or you can supply your own verdict. Results show up alongside everything else, labeled by source (Claude Code, Cursor, LangSmith, and so on).

## Evals as code

Keep evals in version control and run them in CI with the `acryl-datahub-cloud evals` CLI:

```yaml
- name: Completed orders last month
  question: How many orders were completed last month?
  evalType: SQL
  referenceOutput: |
    SELECT COUNT(*) FROM my_db.sales.orders
    WHERE status = 'completed'
      AND order_date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
      AND order_date < DATE_TRUNC('month', CURRENT_DATE)
  conditions:
    - type: LLM_JUDGE
      llmJudge:
        guidelines: Must count completed orders for the previous calendar month.
    - type: ASSET_REFERENCE
      assetReference:
        mustReference:
          - urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.sales.orders,PROD)
```

See the [evals CLI reference](../../../cli-commands/evals.md) for commands and a CI example. The [`datahub-evals`](https://github.com/datahub-project/datahub-skills) skill lets coding agents manage evals for you. Eval tools can also be added to a [scoped MCP server](../scoped-mcp-servers.md).

## AI Credits

Eval runs consume AI Credits: an agent answers each question, and an AI judge grades the answer. Daily runs, on-demand runs, simulated runs, and runs against a proposal all count. To manage usage, keep your suite focused on the questions that matter, and run individual evals when you only need a quick check.

## Limits

- Up to 1,000 evals per run, five running at a time by default
- The daily schedule runs at midnight UTC

## Troubleshooting

**An eval fails, but the answer looks right.**
Open the result and read the judge's reasoning. Usually the criteria are stricter than intended, for example requiring one table when several are valid. Add each valid query with **Add another answer**, or loosen the guidelines.

**An eval passes one day and fails the next.**
It's marked **Flaky**. The usual causes are two documents that disagree, or a question that can be read more than one way. Resolve the conflict, or make the question more specific.

**I don't see Validation > Evals.**
Evals require the Context add-on and the **Manage Evals** privilege, which Admins and Editors have by default.

**Generate Evals is unavailable.**
Generating evals requires published context documents and an admin account. It also pauses while 20 or more generated evals are waiting for review.

**Evals aren't running every day.**
Check that **Evals run daily** is on. Changing it requires the **Manage Platform Settings** privilege.

**External evals show no results.**
Your harness must report each answer back to DataHub, using `acryl-datahub-cloud evals report` or the API. See the [evals CLI reference](../../../cli-commands/evals.md).

## Related

- [Build a Data Agent: Define evals](../../../managed-datahub/build-a-data-agent/define-evals.md), the step-by-step walkthrough
- [Context Generation](./context-generation.md)
- [Context Feedback](./context-feedback.md)
