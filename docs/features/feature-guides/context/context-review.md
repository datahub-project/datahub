---
title: Reviewing Context Changes
description: "Keep the people who know your data in control of what AI agents learn. Changes to context can be proposed, tested against evals, and approved before they go live."
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Reviewing Context Changes

<FeatureAvailability saasOnly />

:::caution Public Beta
Context review is part of the DataHub Cloud **Context** add-on and is in Public Beta. Details on this page may change as the feature evolves.
:::

AI agents act on whatever context you give them, so the people who know the data should decide what goes in. DataHub lets anyone propose a change to context, and routes it to the right people for review before agents see it. The flow is simple: **propose**, **review**, **publish**.

## Why it matters

Context changes constantly, and it comes from many sources: your team, DataHub's AI, your own agents, and Context Generation. Review keeps quality high without making your data experts the bottleneck for every edit. And because reviewers can test a change against your evals before approving it, "looks right to me" becomes "passes the questions we care about."

## How it works

Changes can be proposed in several ways:

- **Generated documents** can require approval before publishing. By default they publish automatically and are verified by evals; for sensitive domains, you can require review and choose the reviewers.
- **Edits requested through chat or MCP,** such as asking Ask DataHub to update a definition, become proposals.
- **Edits from people without edit rights** become proposals, so anyone can suggest an improvement.
- **Generated evals** are reviewed before they're added to your eval suite. Evals your team writes are added directly.
- **Table and column descriptions** follow the same flow.

Proposals go to the owners of the context involved (such as the owners of a document, table, or domain) and to admins. Reviewers can edit a proposal, test its impact on your evals, and then approve or reject it.

## Where to find it

Proposals assigned to you appear under **Tasks > Proposals**. Anyone can propose a change; approving one requires ownership of the context involved, or admin privileges.

<p align="center">
  <img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/context/context-proposal-review.png"/>
</p>

_Screenshot: reviewing a proposed change to context._

## How it fits

Review is part of how [Build a Data Agent](../../../managed-datahub/build-a-data-agent/overview.md) keeps context trustworthy:

- In step 3, you decide whether generated documents publish automatically or require approval. See [Context Generation](./context-generation.md).
- In step 5, fixes from [Context Feedback](./context-feedback.md) and suggestions from your users flow through review.
- Throughout, [evals](./context-evals.md) let reviewers see whether a change helps before it goes live.

## FAQ

**Who can propose changes?**
Everyone, by default. People who can't edit a document directly can still propose changes to it, in DataHub or through an agent.

**Do changes from owners and editors need review?**
No. People with edit rights change context directly. Review applies to proposed changes, and to generated documents and evals when review is turned on.

## Related

- [Build a Data Agent](../../../managed-datahub/build-a-data-agent/overview.md)
- [Context Generation](./context-generation.md)
- [Context Evals](./context-evals.md)
- [Context Documents](./context-documents.md)
