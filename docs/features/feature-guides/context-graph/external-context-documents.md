---
title: External Context Documents
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# External Context Documents

<FeatureAvailability/>

Already have great documentation in Notion? **Don't recreate it—connect it.** Context Graph lets you keep your source of truth where it is while making everything discoverable in DataHub and accessible to AI.

## Why External Context Documents?

Use external context documents when you want to:

- **Keep the source of truth in the third-party tool** (e.g. existing authoring workflows stay the same)
- **Make documents discoverable alongside data assets** in DataHub search
- **Enable AI agents to use tribal knowledge** embedded in docs when answering questions
- **Link documentation to the assets it describes** (datasets, dashboards, charts, etc.)

## External vs. DataHub Documents: Which to Use?

**Use External Documents when:**

- Your team already maintains docs in Notion/Confluence
- You want to preserve existing authoring workflows
- Documents are updated frequently by non-DataHub users

**Use DataHub Documents when:**

- You want version control and history built-in
- The content is DataHub-specific (policies, definitions)
- You need tight governance (domains, tags, properties)

## How External Documents Show Up in DataHub

When external documents are ingested into DataHub:

- Document contents are **indexed** in DataHub
- Documents appear in **DataHub search results** (with navigation back to the source system)
- Documents can be **linked to assets** so users can navigate assets → related documentation

:::info Screenshot placeholder
_TODO_: Add screenshot showing an external document in DataHub search results, and the document profile linking back to the external source.
:::

## Setting Up External Document Ingestion

To connect external documents to DataHub:

1. **Configure a connector** for your external source (Notion, Confluence, etc.)
2. **Run the ingestion** to index document contents into DataHub
3. **Link documents to assets** to preserve relationships between documentation and data
4. **Verify in search** that documents appear and link back to the source system

The specific setup steps depend on your external source—see the connector-specific guides below.

## Connect Your Notion Workspace

We're starting with Notion because it's where many teams already document their data practices. More connectors coming soon.

:::info Documentation placeholder
_TODO_: Add a link to the Notion connector documentation once available.

- Notion connector docs: **TBD**
  :::

## Link External Documents to Assets

Once your external documents are indexed in DataHub, you can link them to the assets they're about:

- From the **Document** profile, add **Related Assets**
- From an **Asset** profile, add **Related Documents** (if supported in your deployment)

This creates bidirectional navigation so users can discover relevant documentation while exploring data assets.

## Next Steps

- [DataHub Context Documents](datahub-context-documents.md)
- [Building Agents with Context Graph](building-agents.md)
