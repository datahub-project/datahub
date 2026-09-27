---
title: Context Feedback
description: "Context Feedback collects the gaps AI agents encounter in your context, so you can find and fix them."
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Context Feedback

<FeatureAvailability saasOnly />

:::caution Public Beta
Context Feedback is part of the DataHub Cloud **Context** add-on and is in Public Beta. Details on this page may change as the feature evolves.
:::

Agents know when they're uncertain. They search for a definition and find none, or find two that disagree, or a user tells them an answer was wrong. **Context Feedback** captures those moments as notes, and collects them in one place, so you can fix each gap once.

## Why it matters

No eval suite anticipates every question. Once your agent is in real use, Context Feedback shows you where your context falls short, based on the questions people actually ask. It turns everyday usage into a steady list of improvements, and each fix can become a new eval so the gap stays closed.

## How it works

When an agent runs into a gap, it records a short note describing the problem, the question that triggered it, and the assets involved. Agents report four kinds of gaps:

| Type                    | What it means                                        |
| ----------------------- | ---------------------------------------------------- |
| **Missing context**     | The agent couldn't find what it needed.              |
| **Incorrect context**   | What it found was wrong or out of date.              |
| **Conflicting context** | Two sources disagree.                                |
| **User correction**     | A user said the answer was wrong, and explained why. |

Ask DataHub and custom agents report gaps automatically. Your own agents can report them through the [DataHub MCP server](../mcp.md), and the open-source [`datahub-sql-workflow`](https://github.com/datahub-project/datahub-skills/tree/main/skills/datahub-sql-workflow) skill instructs them to. Feedback describes gaps in context; it never includes data values or query results.

From each note, reviewers can dismiss it, resolve it, or ask DataHub's assistant to investigate and propose a fix.

## Where to find it

Go to **Validation > Feedback** to review what agents have reported. Reviewing feedback requires the **Manage Context Feedback** privilege, which Admins and Editors have by default. Admins can turn feedback collection on or off under **Settings > AI**.

<p align="center">
  <img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/context/context-feedback-list.png"/>
</p>

_Screenshot: reviewing feedback reported by agents in Validation > Feedback._

## How it fits

Context Feedback is central to step 5 of [Build a Data Agent](../../../managed-datahub/build-a-data-agent/improve-with-feedback.md), maintaining your context as your organization changes. Make reviewing feedback part of how each domain's owners work: fix what's real, and add an [eval](./context-evals.md) for each question that exposed a gap.

## FAQ

**No feedback is showing up.**
Check that feedback collection is turned on under **Settings > AI**. External agents report gaps only when they're instructed to, for example by the `datahub-sql-workflow` skill.

**Do thumbs up and down in Ask DataHub create feedback?**
No. Those ratings help DataHub improve the product. Context Feedback comes from agents noticing gaps in your context, often because a user corrected them.

## Related

- [Build a Data Agent: Maintain context](../../../managed-datahub/build-a-data-agent/improve-with-feedback.md)
- [Context Evals](./context-evals.md)
- [Reviewing Context Changes](./context-review.md)
