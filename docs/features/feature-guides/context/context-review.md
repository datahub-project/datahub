---
title: Reviewing Context Changes
description: "Keep people in control of what your AI agents learn. See which context changes go through review, who reviews them, and how to test a change before it goes live."
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Reviewing Context Changes

<FeatureAvailability saasOnly />

:::caution Public Beta
Context review is part of the DataHub Cloud **Context** add-on and is in Public Beta. Screens and settings may change as we learn from you.
:::

Your agents will believe whatever context you give them. So the people who know the data should decide what goes in.

DataHub lets anyone suggest a change to context: people, DataHub's AI, your MCP-connected agents, or a generation job. Your context owners (the people who own the documents, tables, and domains involved) review it before agents see it. The flow is simple: **propose**, **review**, **publish**. Reviewers can test a change against your evals before approving it, so "looks right to me" becomes "passes the questions we care about."

## What goes through review

| When this happens...                                                           | ...this is proposed                                         | Reviewed by (by default)                                                                     |
| ------------------------------------------------------------------------------ | ----------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| A [Context Generation](./context-generation.md) job runs with auto-publish off | New documents, to be published                              | The reviewers you picked, or else the owners of the tables and domains involved, plus Admins |
| Someone asks Ask DataHub or an MCP-connected agent to change a document        | Edits to title or content, publish or unpublish, new drafts | Admins and the document's owners                                                             |
| Someone without edit rights edits a document in DataHub                        | Edits to title or content, publish or unpublish             | Admins and the document's owners                                                             |
| An admin clicks **Generate Evals** with review turned on                       | New [evals](./context-evals.md)                             | Admins                                                                                       |
| Someone suggests a table or column description, tag, or glossary term          | The description, tag, or term                               | Admins, Editors, and the table's owners                                                      |

"Owners" includes owners of parent documents, so a team that owns a folder of documents reviews changes to everything in it.

Today, Context Generation writes documents only. Table and column descriptions come from people and AI assistants, and go through the same review.

## Where reviews show up

Proposals assigned to you, your groups, or your role appear in **Tasks > Proposals**, under **Inbox**. Proposals you've made are under **My Requests**.

When a Context Generation run finishes, each assigned reviewer also gets one digest by Slack or email summarizing what's waiting.

You can also review a proposal that isn't assigned to you, as long as you have permission to approve it. Open the document and look for **Proposals** in its sidebar.

## Review a document proposal

Open a proposal to see each proposed document or change. For each one, you can:

- **Edit** the title and content before approving
- Choose whether it **will be published** or stays unpublished
- **Comment** for other reviewers. Agents never read comments.

When you're done, click **Apply Changes**, or **Reject** to close the proposal without changing anything.

<p align="center">
  <img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/context/context-proposal-review.png"/>
</p>

_Screenshot: reviewing a context proposal._

### Test the impact before you approve

This is the most useful part of review. In the **Impact on Evals** section:

1. Click **Add Question** and pick the evals this change could affect. Start with the evals for the same domain.
2. Click **Run Evals** (or **Run Selected**).
3. DataHub answers each question _as if the proposal were already published_ and shows **Pass** or **Fail**, plus how that compares with the last run: **Fixed**, **Broken**, or **Unchanged**.

Anything **Fixed** is a good sign. Anything **Broken** deserves a close look before you approve.

<p align="center">
  <img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/context/guides/proposal-impact-on-evals.png"/>
</p>

_Screenshot: Impact on Evals, with Fixed, Broken, and Unchanged results._

Want to poke at it yourself? **Try in Ask DataHub** opens a chat with the proposal applied, so you can ask your own questions.

## Review proposed evals

When an admin clicks **Generate Evals** with **Require review before publishing** turned on, DataHub drafts eval questions from your published context documents and sends them for review. Admins see a banner on the Evals page: "You have N evals to review!"

Only generated evals go through review. Evals that people create with **Create Question** are added to your eval set directly.

1. Click **Review Proposed Questions**.
2. For each question, check the question and the expected answer. You can edit both, and see which document it came from.
3. Click **Approve Question** to add it to your eval set, or **Reject** it.

<p align="center">
  <img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/context/context-eval-proposal-review.png"/>
</p>

_Screenshot: reviewing a proposed eval before approving it._

Please read these carefully. An eval with a wrong expected answer will quietly reward wrong context.

## Who can propose changes

By default, everyone can propose. Readers can suggest edits to existing documents, ask for a document to be published or unpublished, and suggest descriptions and tags. Editors and document owners can edit documents directly, so their changes don't need review.

- **In DataHub:** people who can't edit a document directly see **Propose** instead of saving. If they try to leave with unsaved edits, DataHub offers to **Propose Changes**.
- **In Ask DataHub and MCP clients:** ask the assistant to suggest a change ("propose an edit to the churn definition doc"). It becomes a proposal like any other. The same works from Slack and Teams.

## Tips

- **Assign reviewers who will actually review.** If proposals pile up, set explicit **Reviewers** on your Context Generation job instead of relying on automatic assignment.
- **Choose where review matters.** For generated documents, auto-publish plus evals is the faster default. Turn auto-publish off, and name reviewers, for domains where a person must sign off.
- **Reject freely.** A rejected proposal costs nothing. A wrong document costs trust.

## Related

- [Build a Data Agent: Generate context](../../../managed-datahub/build-a-data-agent/generate-context.md)
- [Context Generation](./context-generation.md)
- [Context Evals](./context-evals.md)
- [Context Documents](./context-documents.md)
