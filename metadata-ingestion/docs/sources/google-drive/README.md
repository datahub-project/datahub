## Overview

Google Drive is a cloud file storage and collaboration platform by Google. Learn more in the [official Google Drive documentation](https://developers.google.com/drive).

The DataHub integration for Google Drive ingests Google Docs, Slides, and Sheets as DataHub Document entities with full-text markdown content. It also captures folder hierarchy, optional stateful deletion detection, and optional semantic embeddings for semantic search.

:::warning Not Supported with Remote Executor
This source is not supported with the Remote Executor in DataHub Cloud. It must be run using a self-hosted ingestion setup.
:::

## Concept Mapping

| Source Concept | DataHub Concept | Notes                                                                |
| -------------- | --------------- | -------------------------------------------------------------------- |
| Google Doc     | Document        | Full-text content exported as markdown.                              |
| Google Slides  | Document        | Exported as plain text (one block per slide). Disabled by default.   |
| Google Sheets  | Document        | Exported as CSV-formatted text. Disabled by default.                 |
| Drive Folder   | Document        | Materialised to preserve parent-child hierarchy. Enabled by default. |
| Drive owner    | CorpUser        | Emitted when ownership metadata is available.                        |
