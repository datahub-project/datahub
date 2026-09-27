---
title: Context Evals
description: "Context Evals test whether DataHub's context is good enough for AI agents to answer your real business questions, and catch regressions before your users do."
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Context Evals

<FeatureAvailability saasOnly />

:::caution Public Beta
Context Evals are part of the DataHub Cloud **Context** add-on and are in Public Beta. Details on this page may change as the feature evolves.
:::

**Context Evals** are real business questions paired with known-good answers. DataHub runs them on a schedule to measure whether an agent can answer your questions correctly using the context in DataHub, and whether a change to that context made things better or worse.

## Why it matters

Without evals, "is our agent getting better?" is a matter of opinion. With them, it's a number you can track. Evals let you:

- **Measure a baseline** before you add any context
- **Prove improvement** as you ingest and generate context
- **Catch regressions** the day after a change, rather than when a user notices
- **Decide with confidence** whether a proposed change to context should go live

We recommend defining evals before you ingest or generate any context, so every change can be measured against them.

## How it works

An eval is a question, the answer an expert would give (usually SQL), and optional rules, such as tables a correct answer must or must not use.

When an eval runs, an agent answers the question using your DataHub context, and an AI judge compares its answer with the expected one. For SQL, the judge checks whether the two queries are equivalent, rather than identical. Each result records why it passed or failed, so you can see what context was missing.

Evals can come from three places:

- **Your team** writes them from the questions people actually ask.
- **DataHub** can draft them from your published context documents. Generated evals are reviewed by an admin before they're added.
- **Your code** can define them in YAML and run them in CI with the [evals CLI](../../../cli-commands/evals.md).

By default, evals are answered by a built-in DataHub agent. Evals attached to a [custom agent](../agents.md) run against that agent instead, and your own agents can report their answers to DataHub for grading.

## Where to find it

Go to **Validation > Evals** to create, run, and track evals. Creating and running evals requires the **Manage Evals** privilege, which Admins and Editors have by default. Evals for a custom agent are also available on that agent's **Evals** tab.

<p align="center">
  <img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/context/context-evals-list.png"/>
</p>

_Screenshot: tracking eval results in Validation > Evals._

## How it fits

Evals are step 1 of [Build a Data Agent](../../../managed-datahub/build-a-data-agent/define-evals.md), and every later step is measured against them:

- **Ingesting and generating context** should raise your pass rate.
- **Reviewing a proposed change** can include running evals against it before it's published. See [Reviewing Context Changes](./context-review.md).
- **Feedback from real usage** becomes new evals, so gaps stay fixed. See [Context Feedback](./context-feedback.md).

:::tip Already have an evals platform?
Keep it. DataHub evals focus on one question: can an agent find and use the right context in DataHub? They complement end-to-end agent evals in tools such as Langfuse or LangSmith, and you can report results from your own harness into DataHub.
:::

## FAQ

**Do evals use AI Credits?**
Yes. Each eval run consumes AI Credits, because an agent answers the question and an AI judge grades it. Keep your suite focused on the questions that matter most.

**An eval fails, but the answer looks right.**
Read the judge's reasoning in the result. Usually the expected answer is stricter than intended, for example requiring one table when several are valid. Add each valid answer, or relax the guidelines.

**An eval passes one day and fails the next.**
The usual causes are two documents that disagree, or a question that can be read more than one way. Resolve the conflict, or make the question more specific.

**I don't see Validation > Evals.**
Evals require the Context add-on and the **Manage Evals** privilege.

## Related

- [Build a Data Agent: Define evals](../../../managed-datahub/build-a-data-agent/define-evals.md)
- [Evals CLI reference](../../../cli-commands/evals.md)
- [Context Generation](./context-generation.md)
- [Context Feedback](./context-feedback.md)
