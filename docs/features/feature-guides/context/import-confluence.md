---
description: "Import Confluence pages into DataHub Context Documents for search and AI assistants."
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Import from Confluence

<FeatureAvailability saasOnly />

## Overview

Bring Confluence spaces and pages into DataHub as Context Documents for browsing, search, and AI assistants.

By default, imports are **Native** (editable in DataHub). Switch **Document import mode** to **External** for read-only documents linked to Confluence. Sync-back to Confluence is not supported.

## Prerequisites

- Confluence Cloud or Data Center API access (site URL plus username/API token or personal access token)
- Permission to manage Data Sources / ingestion in DataHub

## Set Up Import

1. Open **Documents** → **Import** → **Confluence**.
2. Configure site URL, credentials, spaces or pages to include, and Document import mode (Native or External).
3. Save and run (or schedule) the source.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/context/context-document-import-confluence.png"/>
</p>

_Screenshot: Confluence import / data source configuration._

## View Run History

Open **Data Sources** (Ingestion) to view runs, schedules, and failures for the Confluence source.

## Next steps

- Full connector reference: [Confluence source](../../../generated/ingestion/sources/confluence.md)
- Back to [Context Documents](./context-documents.md)
