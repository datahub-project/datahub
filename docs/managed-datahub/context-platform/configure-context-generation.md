---
title: Configure Context Generation
description: "Configure which domains have context generated, define metadata eval questions, and control when generated context becomes visible to AI agents."
visible-if:
  showContextHub: true
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Configure Context Generation

<FeatureAvailability saasOnly />

:::caution Public Beta
The Context Platform is currently in Public Beta. Features, UI, and configuration options are subject to change.
:::

**Role: Admin (Platform Engineer)**

Admins configure which data domains should have context generated, specify metadata eval questions to assess quality, and control when generated context becomes visible to AI agents.

This guide walks you through:

1. Opening the Context Generation settings
2. Creating a context generation source
3. Creating metadata eval questions

### Prerequisites

In order to configure context generation, you must have the `MANAGE_DOCUMENTS`, `MANAGE_ALL_DOCUMENT_PROPOSALS`, and `MANAGE_AGENTS` privileges — granted by default with the Admin role.

You must also have completed the setup described in [Prerequisites](overview.md#prerequisites), including pinning CLI version 1.6.0.9 and configuring source connectors with query ingestion enabled.

### Step 1: Open the Context Generation Settings

From the DataHub sidebar, head to **Settings** > **Context**.

You will see a list of context generation jobs with their status, last run, and other configured settings. From this list you can:

- **Run** — manually trigger context generation using the Run icon
- **Stop** — stop a running context generation job using the Stop icon
- **Edit or Delete** — modify or remove a context generation source from the menu icon
- **Run History** — view the last run's schedule, duration, and status. Select a run for more detail, and work with the Context Curator agent to resolve run issues.

### Step 2: Create a Context Generation Source

Click **Create** to configure a context generation job. You can either complete the context generation form on the left, or work with the **Context Curator** agent to configure the job conversationally.

Context generation is scoped by **domain** or **container** to focus on data assets that have owners (SMEs) and to keep costs manageable.

1. **Name:** Give your context generation job a clear, descriptive name.
2. **Scope:** At least one domain or container selection is required in order to run context generation.
   Ensure each selected domain or container (database / schema) has datasets ingested from Private Beta supported sources: **Snowflake, Databricks, BigQuery**, and BI tools (**Looker, dbt**). See the reference [Metadata Ingestion Recipes](metadata-ingestion-recipes.md).
3. **Publishing settings:** By default, **Auto-publish documents** is disabled. DataHub-generated context is not visible until a Data Expert has validated and published the context documents.
4. **Save:** Choose **Save as Draft** to preserve the configuration only, or **Save & Run** to preserve the configuration and execute it immediately.

The system will process datasets in the selected domains, analyze query history and schema metadata, and produce context documents (also called **semantic anchors**). If it detects associated dbt models or downstream Looker charts or dashboards, it will also incorporate that business context to enrich the quality of the semantic anchors.

:::tip
By default auto-publish is disabled, so generated context documents are unpublished — not visible to agents or humans via search. When auto-publish is enabled, context documents are published **without** SME review.

We recommend creating metadata evals before enabling auto-publish, so that evals run against context documents daily. This gives your team full control over what context the AI can use.
:::

## What Happens Next

After generation completes, unpublished context documents are surfaced as **proposals** in **Task Center** > **Requests** for Data Experts and SMEs (domain owners). They validate and publish context from there — see [Review Context Proposals](review-context-proposals.md).

As an Admin, you can also view generated documents directly at **Context** > **Documents**.

### Related Features

- [Domains](https://docs.datahub.com/docs/domains)
- [Metadata Ingestion](https://docs.datahub.com/docs/metadata-ingestion)
- [Roles and Privileges](https://docs.datahub.com/docs/authorization/roles)

## Next Steps

Now that context generation is configured, you're ready to [Review Context Proposals](review-context-proposals.md).
