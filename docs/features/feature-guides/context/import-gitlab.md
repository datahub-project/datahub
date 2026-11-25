---
description: "Import GitLab project docs into DataHub Context Documents."
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Import from GitLab

<FeatureAvailability saasOnly />

## Overview

Import markdown and text files from a GitLab project into DataHub as Context Documents. Both GitLab.com and self-managed GitLab are supported, and folder hierarchy is preserved.

By default, imports are **Native** (editable in DataHub). Switch **Document import mode** to **External** for read-only documents that link back to GitLab. Edits made in DataHub are not written back to GitLab.

## Prerequisites

- A GitLab personal, project, or group access token with the `read_api` scope. Project and group tokens need at least the Reporter role.
- For self-managed GitLab, the instance URL, and network access from DataHub (or your remote executor) to it.
- Permission to manage Data Sources / ingestion in DataHub.

## Set Up Import

1. Open **Documents** → **Import** → **GitLab**.
2. Enter the GitLab token. Leave **GitLab URL** as `https://gitlab.com`, or set your self-managed instance URL.
3. Set the **project**, either as its full path (`group/subgroup/project`) or its URL, then the branch, an optional path prefix and file extensions, and the Document import mode (Native or External).
4. Save and run (or schedule) the source.

If the project URL is pasted into **Project**, the GitLab URL is taken from it, so self-managed instances work without setting it separately.

## View Run History

Open **Data Sources** (Ingestion) to view runs, schedules, and failures.

## Next steps

- Full connector reference: [GitLab Documents source](../../../generated/ingestion/sources/gitlab.md)
- Back to [Context Documents](./context-documents.md)
