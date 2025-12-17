---
title: DataHub Context Documents
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# DataHub Context Documents

<FeatureAvailability/>

Creating documents in DataHub is like having a wiki that lives alongside your data—but smarter. Everything you document is searchable, governable, and instantly usable by AI.

This guide walks through the core UI workflows for **context documents authored in DataHub** and how to connect them to the rest of your metadata graph.

## Create a New Document

Create a document from the **Documents directory** in the left navigation.

1. Click **+ New** (or equivalent) in the Documents directory.
2. Choose a **parent** (optional) to create a hierarchical document tree.
3. Enter a **title** and start writing content.
4. Add metadata like **Tags**, **Glossary Terms**, **Domain**, and **Structured Properties** as needed.

💡 **Why this matters**: Documents you create in DataHub are automatically indexed for search and can be cited by Ask DataHub.

::::info Screenshot placeholder
_TODO_: Add screenshot showing "Create new document" entry point + creation modal/editor.
::::

## Move a Document

Reorganize your document tree by moving documents between parents.

1. Open the document (or use the context menu in the directory tree).
2. Select **Move**.
3. Choose the new **parent** (or move to top-level).
4. Confirm.

::::info Screenshot placeholder
_TODO_: Add screenshot showing the Move flow (context menu + parent selector).
::::

## Publish / Unpublish a Document

Publishing controls who can discover a document through global experiences (including AI).

- **Publish** when the content is ready for broader discovery and reuse.
- **Unpublish** to remove it from broad visibility (while keeping it accessible for continued iteration, depending on your setup).

💡 **Why this matters**: Published documents become part of your organization's searchable knowledge base and can be referenced by Ask DataHub when answering questions.

1. Open the document.
2. Click **Publish** (or **Unpublish**) in the document header/actions menu.

::::info Screenshot placeholder
_TODO_: Add screenshot showing Publish/Unpublish action and the resulting published status indicator.
::::

## View History and Restore a Previous Version

Use history to understand what changed over time and to safely restore a prior version.

1. Open the document.
2. Open **History** (or equivalent) from the document actions menu.
3. Select a previous version.
4. Click **Restore**.

::::info Screenshot placeholder
_TODO_: Add screenshot showing Document history with a restore action.
::::

## Rename or Edit a Document

1. Open the document.
2. Use the title field (or **Rename**) to update the name.
3. Edit the content in the editor and save.

::::info Screenshot placeholder
_TODO_: Add screenshot showing rename + edit experience for a document.
::::

## Link Related Assets to a Document

Attach context to the assets it describes (Datasets, Dashboards, Charts, etc.).

1. Open the document profile.
2. Find the **Related Assets** section.
3. Click **Add** and search for the asset(s) to link.
4. Save.

::::info Screenshot placeholder
_TODO_: Add screenshot showing "Related Assets" section on a Document profile.
::::

## Tips

- **Use hierarchy intentionally**: Create top-level folders like "Runbooks", "Metric Definitions", "Onboarding", "Data Contracts", and nest content below.
- **Treat documents like governed metadata**: Apply domains, tags, terms, and structured properties to make documents easier to find and easier to trust.

:::info See it in action
Teams using Context Graph report 40% fewer "how do I..." questions in Slack after documenting their data access process and linking it to relevant datasets.
:::

## Next Steps

- [External Context Documents](external-context-documents.md)
- [Building Agents with Context Graph](building-agents.md)
