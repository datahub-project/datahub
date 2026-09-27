---
title: Context Feedback
description: "See where AI agents ran out of context while answering questions, and fix the gaps."
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Context Feedback

<FeatureAvailability saasOnly />

:::caution Public Beta
Context Feedback is part of the DataHub Cloud **Context** add-on and is in Public Beta. Screens and settings may change as we learn from you.
:::

Your agents know when they're guessing. They search for a definition of "net revenue" and find three, or find nothing, or a user tells them the table they picked is wrong. Usually that knowledge disappears when the chat ends.

**Context Feedback** keeps it. Whenever an agent hits a gap in your context, it files a short note. The notes collect under **Validation > Feedback**, so you can fix each gap once instead of hearing about it forever.

## What agents report

Agents report four kinds of gaps:

| Type                    | When the agent reports it              | Example                                                                             |
| ----------------------- | -------------------------------------- | ----------------------------------------------------------------------------------- |
| **Missing context**     | It couldn't find what it needed        | "No definition of 'active customer' anywhere."                                      |
| **Incorrect context**   | What it found was wrong or out of date | "The docs point to `orders_v1`, which stopped updating in March."                   |
| **Conflicting context** | It found sources that disagree         | "Two documents define net revenue differently: one subtracts refunds, one doesn't." |
| **User correction**     | A user explicitly corrected its answer | "User said revenue reporting should use `fct_revenue`, not `stg_payments`."         |

User corrections are the most valuable of the four. Someone who knows the data just told you exactly what the agent got wrong.

## Where feedback comes from

Agents report gaps through a built-in MCP tool (named `note_metadata_observation`, if you're curious). You don't need to wire anything up:

- **Ask DataHub** and **agents built on DataHub** report gaps automatically, without interrupting the user.
- **External agents** connected to the [DataHub MCP server](../mcp.md), such as Claude Code, Cursor, or your own, can call the same tool. The [`datahub-sql-workflow`](https://github.com/datahub-project/datahub-skills/tree/main/skills/datahub-sql-workflow) skill tells agents to do this.

Each note records:

| Field              | What it is                                                         |
| ------------------ | ------------------------------------------------------------------ |
| **Type**           | Missing, incorrect, or conflicting context, or a user correction   |
| **Summary**        | A one-line description written for a human reviewer                |
| **User message**   | The question or correction that triggered it                       |
| **Related assets** | The tables, columns, documents, or terms involved                  |
| **Source**         | Which client reported it (DataHub, Claude Code, Cursor, and so on) |
| **Initiated by**   | The person whose conversation it came from                         |

Agents are told never to include data values or query results in feedback. It's about the map, not the territory.

## Review feedback

You need the **Manage Context Feedback** privilege. Admins and Editors have it by default.

Go to **Validation > Feedback**. You'll see open feedback, newest first. Filter by status or search by keyword.

<p align="center">
  <img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/context/context-feedback-list.png"/>
</p>

_Screenshot: the Feedback page, listing gaps reported by agents._

For each item you can:

- **Fix**: opens **Fix with Ask DataHub**, which looks at the related context and proposes the smallest change that would help. That might be a new or edited document, a better table or column description, or a new eval. It makes changes only after you agree.
- **Dismiss** it if it's noise.
- **Mark resolved** once it's fixed, or **Reopen** it if it isn't.

<p align="center">
  <img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/context/context-feedback-fix.png"/>
</p>

_Screenshot: Fix with Ask DataHub proposing a change for a piece of feedback._

### A triage habit that works

Set aside 30 minutes a week. Sort by related asset. The same table showing up five times is telling you something. For each real gap:

1. Fix the context: document it, correct the description, deprecate the imposter table.
2. Add an [eval](./context-evals.md) for the question that exposed it, so it stays fixed.
3. Mark it resolved.

Step 2 is the one people skip. Don't skip it.

## Settings

Admins can control feedback collection under **Settings > AI**:

- **Metadata Gap Reporting Tool** (on by default): lets AI assistants and MCP clients report gaps. Turn it off to stop collecting feedback entirely; agents will no longer see the tool.
- **MCP Observation Telemetry** (on by default): sends anonymous counts to DataHub to help us improve the product. It never includes summaries, user messages, or other free text.

:::note What about thumbs up and down?
The thumbs up and down buttons in Ask DataHub and Slack tell us how the product is doing. They don't create Context Feedback. Feedback comes from agents noticing gaps, often _because_ a user corrected them.
:::

## Troubleshooting

**No feedback is showing up.**
Check that **Metadata Gap Reporting Tool** is on under **Settings > AI**. Also check the status filter: the page shows only **Open** feedback by default. External agents report gaps only if they're instructed to, for example with the [`datahub-sql-workflow`](https://github.com/datahub-project/datahub-skills/tree/main/skills/datahub-sql-workflow) skill.

**I don't see Validation > Feedback.**
Feedback requires the Context add-on and the **Manage Context Feedback** privilege, which Admins and Editors have by default.

**The Fix button is missing.**
**Fix** opens Ask DataHub, so it appears only when Ask DataHub is enabled for your organization.

**A feedback item has no related assets.**
Agents can only link assets that exist in DataHub. If the gap is about a table that isn't ingested yet, connect its data source first.

## Related

- [Build a Data Agent: Improve with feedback](../../../managed-datahub/build-a-data-agent/improve-with-feedback.md), the step-by-step walkthrough
- [Context Evals](./context-evals.md)
- [Context Documents](./context-documents.md)
