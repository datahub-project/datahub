---
description: "Import Notion pages into DataHub Context Documents for search and AI assistants."
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Import from Notion

<FeatureAvailability saasOnly />

## Overview

Bring Notion pages into DataHub as Context Documents for browsing, search, and AI assistants.

By default, imports are **Native** (editable in DataHub). Switch **Document import mode** to **External** for read-only documents linked to Notion. Sync-back to Notion is not supported.

## Prerequisites

- A Notion internal integration token, with target pages shared to that integration
- Permission to manage Data Sources / ingestion in DataHub

## Set Up Import

1. Open **Documents** → **Import** → **Notion**.
2. Configure credentials, which pages to include, and Document import mode (Native or External).
3. Save and run (or schedule) the source.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/context/context-document-import-notion.png"/>
</p>

_Screenshot: Notion import / data source configuration._

## View Run History

Open **Data Sources** (Ingestion) to view runs, schedules, and failures for the Notion source.

## Next steps

- Full connector reference: [Notion source](../../../generated/ingestion/sources/notion.md)
- Back to [Context Documents](./context-documents.md)
