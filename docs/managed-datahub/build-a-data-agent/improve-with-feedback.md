---
title: "Step 5: Improve with Feedback"
description: "Use Context Feedback and evals to find and close the gaps your data agent encounters, then roll out to the next domain."
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Step 5: Improve with Feedback

<FeatureAvailability saasOnly />

:::info Context add-on
This step uses features from the DataHub Cloud **Context** add-on, currently in Public Beta.
:::

Once your agent is live, it will meet questions no one anticipated. That's expected. The goal isn't an agent that's perfect on day one, but one that measurably improves every week.

## Let your agent report what's missing

Whenever your agent runs into a gap in your context, it records a note in [Context Feedback](../../features/feature-guides/context/context-feedback.md). It reports four kinds of gaps:

| Type                | Meaning                                              |
| ------------------- | ---------------------------------------------------- |
| **Missing**         | It couldn't find what it needed.                     |
| **Incorrect**       | What it found was wrong or out of date.              |
| **Conflicting**     | Two sources disagree.                                |
| **User correction** | A user said the answer was wrong, and explained why. |

Ask DataHub and your custom agents report gaps automatically. Your own agents do too when they use the [`datahub-sql-workflow`](https://github.com/datahub-project/datahub-skills/tree/main/skills/datahub-sql-workflow) skill.

You'll find the notes under **Validation > Feedback**.

<p align="center">
  <img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/context/context-feedback-list.png"/>
</p>

_Screenshot: open feedback reported by agents._

## Review feedback weekly

Set aside 30 minutes a week with your data expert. For each open item:

1. **Confirm it's real.** If it isn't, click **Dismiss**.
2. **Fix the context.** Click **Fix**, and DataHub's assistant investigates and proposes the smallest change that would help, such as a new document, a clearer description, or deprecating a look-alike table. Nothing changes until you approve it.
3. **Add an eval** for the question that exposed the gap, so it stays fixed.
4. **Mark it resolved.**

The third step turns a one-time fix into a lasting one. Over time, your eval suite should grow mostly from real questions and real failures.

:::tip
Group feedback by asset. Several notes about the same table usually point to a single underlying problem.
:::

## Invite everyone to suggest fixes

The people using your agent often notice problems first, and they don't need edit rights to help:

- **In DataHub,** anyone viewing a document they can't edit can click **Propose** to suggest a change.
- **In chat,** people can ask Ask DataHub or their own agent to "propose an edit to the churn definition." This also works in Slack and Teams.

Suggestions go to the owners' **Tasks > Proposals** inbox, where they can test changes against your evals before approving. See [Reviewing Context Changes](../../features/feature-guides/context/context-review.md).

## Track your results

On the Evals page, watch three numbers:

- **Pass rate** should trend upward as you close gaps.
- **Failing** evals should each have an owner or a known cause.
- **Flaky** evals, which alternate between passing and failing, usually signal conflicting documents or an ambiguous question.

If the pass rate drops overnight, review what was published the day before.

## Expand to the next domain

Once the agent meets your target pass rate and the first domain's team uses its answers without double-checking them, take the next domain through the same path: define its evals, ingest and generate its context, and activate it for that team.

Each domain goes faster than the last, because the practices are already in place. Over time, you'll have a set of domain agents, each with its own context, evals, and owners, alongside a global agent that benefits from all of it.

## What success looks like after a month

- [ ] One domain agent in regular use, limited to its domain
- [ ] 50 or more evals, most drawn from real questions and failures
- [ ] Daily eval runs, with a pass rate you're comfortable sharing
- [ ] Generated context refreshing on a schedule, verified by evals
- [ ] A weekly feedback review on the calendar
- [ ] The next domain chosen, with an owner ready to begin

## Related

- [Context Evals](../../features/feature-guides/context/context-evals.md)
- [Context Feedback](../../features/feature-guides/context/context-feedback.md)
- [Reviewing Context Changes](../../features/feature-guides/context/context-review.md)
- [Back to the overview](./overview.md)
