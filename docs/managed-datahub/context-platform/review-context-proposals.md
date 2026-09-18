---
title: Review Context Proposals
description: "Review, validate, and publish AI-generated context documents as a Data Expert or Subject Matter Expert."
visible-if:
  showContextHub: true
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Review Context Proposals

<FeatureAvailability saasOnly />

:::caution Public Beta
The Context Platform is currently in Public Beta. Features, UI, and configuration options are subject to change.
:::

**Role: Data Expert / SME (Data Engineer, Analytics Engineer)**

Data Experts act as human-in-the-loop reviewers for AI-generated context. You review proposed changes, validate accuracy, add commentary, and decide whether to publish context documents so AI agents can use them.

This guide walks you through:

1. Creating metadata evaluations
2. Accessing your proposals
3. Reviewing individual context documents
4. Making your review decision

### Prerequisites

In order to review context proposals, an Admin must have added you to the **Editor** role, or granted you the `MANAGE_DOCUMENTS, MANAGE_DOCUMENT_PROPOSALS, and MANAGE_EVALS` privileges. See [Who Needs What Access](overview.md#who-needs-what-access).

## **Step 1: Create Metadata Eval Questions**

Eval questions let you assess the quality of generated context before it reaches reviewers. Each question has a pass/fail criterion that runs automatically against generated documents.

#### **_Manage eval questions_**

- Navigate to Context > Evals
- Toggle to run all evals daily, Run All Evals once manually, or run individual evals once manually
- Click the menu icon to Edit or Delete the question.

:::note

Evaluations are executed on PUBLISHED context documents and UNPUBLISHED context documents in a proposal.

:::

#### **_Create an eval question_**

1. Click Create Question
2. Select the eval type

- SQL Generation: Agent must produce a SQL query (or the metric definition behind one)
  - Judged on **semantic equivalence** to a reference query, not exact string match
  - When creating: you write the exact SQL a correct answer should produce in a code editor
  - Example placeholder: `SELECT COUNT(*) FROM orders WHERE status = 'completed'`
- Generic: Agent must answer a catalog Q&A question — find assets, owners, docs, or lineage
  - No SQL to compare against, so judgment relies entirely on the **criteria text you write**
  - When creating: you write prose grading criteria (e.g. `The response should name the canonical source and explain why`)

3. Domain (optional): Select a domain for ownership of the evaluation
4. Question: Enter a golden question. Example: "How many orders were placed last month?"
5. Answer: Set the pass criteria — the condition the generated answer must meet to pass.
6. Must reference assets: Select assets the agent response recommends.
7. Must not reference assets: Select trap assets the agent response should not recommend.
8. Additional guidelines: Free form instructions for the response
9. Check for problems before you create the question.
10. Optionally run the question in Ask DataHub to test.
11. Create evaluations for each quality dimension you want to validate.

### Step 2: Access Your Proposals

Navigate to **Task Center** > **Requests** in the DataHub sidebar.

You will see a list of context proposals assigned for your review as a domain owner and expert reviewer. Each proposal corresponds to a set of context documents generated for a domain.

Click a proposal to open it and review its documents.

### Step 3: Review Individual Context Documents

Each proposal contains one or more context documents. For each document, you can take the following actions:

| Action                      | What It Does                                                                                                                                       |
| --------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Edit Business Questions** | Update the business questions associated with this context document. These define what analytics questions the context helps answer.               |
| **Edit Anchor Patterns**    | Modify the SQL patterns used to answer the business questions. Use this to correct or refine how the AI interprets your data.                      |
| **Add Comment**             | Leave a note on the document for other editors. Comments are internal and not visible to AI agents.                                                |
| **Publish / Unpublish**     | Toggle the document's visibility. **Publish** makes the document visible to AI agents and search; **Unpublish** hides it from both.                |
| **Run Evals**               | Execute the metadata eval questions configured by your Admin against this document. Results show whether the context passes your quality criteria. |
| **Test on Ask DataHub**     | Run the context document against Ask DataHub to see how it affects AI agent responses. Use this to validate changes before finalizing.             |
| **Apply Changes**           | Approve the context document changes and update its publish state. The proposal is marked as completed.                                            |

:::tip
Run evals **before** you start editing. Seeing where the generated context already passes and where it fails tells you which documents actually need your attention, and which are ready to publish as-is.
:::

### Step 4: Make Your Review Decision

After reviewing the documents in a proposal, you have three options at the proposal level:

#### Apply Changes

Select **Apply Changes** to approve the context document changes and set the publish state you configured during review. This finalizes the proposal and makes published documents available to AI agents.

#### Reject

Select **Reject** to decline the proposed changes. The context documents are not updated, and the proposal is marked as completed (rejected). Use this when the generated context does not meet your quality bar and is not worth editing.

#### Cancel

Select **Cancel** to exit the proposal view and return to the Task Center without taking any action. The proposal remains open for future review.

## Best Practices for Context Validation

- **Specify metadata evals** to automate validation of your business questions with explicit pass criteria.
- **Run evals first** — before editing, run the eval questions to understand what the system thinks is right and where it falls short.
- **Test with Ask DataHub** — use the **Test on Ask DataHub** action to see the real-world impact of publishing a document before committing.
- **Use comments to communicate** — leave notes for colleagues on documents you're unsure about rather than approving or rejecting immediately.
- **Publish incrementally** — start by publishing a small set of high-confidence documents to validate AI agent performance before publishing broadly.

## FAQ and Troubleshooting

**Will my edits survive the next context generation run?**

Yes. Context generation is currently a full refresh, but human-edited context is preserved on regeneration. Human-edited business metadata takes precedence over agent-generated metadata.

**What happens to a document I unpublish?**

It is hidden from AI agents and from search, but it is not deleted. You can publish it again later from **Context** > **Documents**.

**Why does an eval fail on a document that looks correct?**

Hover over the pass/fail result and click **View details** for the explanation. In many cases the pass criteria is more specific than intended — for example, it names a table that is one of several valid answers. Ask your Admin to refine the criteria.

**Where do I find context documents that are not part of a proposal?**

Admins can view all generated documents at **Context** > **Documents**. Published documents are also available in the Document Library.

### Related Features

- [Context Documents](https://docs.datahub.com/docs/features/feature-guides/context/context-documents)
- [Ask DataHub](https://docs.datahub.com/docs/features/feature-guides/ask-datahub)
- [Domains](https://docs.datahub.com/docs/domains)

## Next Steps

Now that you have published validated context, you're ready to [Activate Context](activate-context.md) for your agents.
