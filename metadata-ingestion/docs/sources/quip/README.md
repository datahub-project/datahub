## Overview

Quip is a collaborative documents, spreadsheets, and chat platform from Salesforce. Learn more in the [official Quip documentation](https://quip.com/dev/automation/documentation/all).

The DataHub integration for Quip ingests Quip threads (documents, spreadsheets, slides, and chats) and their containing folders as knowledge Document entities, preserving the Quip folder hierarchy as parent-child document relationships. It optionally generates embeddings for semantic search and supports stateful deletion detection.

:::warning Not Supported with Remote Executor
This source is not supported with the Remote Executor in DataHub Cloud. It must be run using a self-hosted ingestion setup.
:::

## Concept Mapping

| Source Concept                                | DataHub Concept             | Notes                                                                |
| --------------------------------------------- | --------------------------- | -------------------------------------------------------------------- |
| Quip site                                     | Platform Instance           | Distinguishes multiple Quip sites; defaults to a hash of `base_url`. |
| Folder                                        | Document (subtype `Folder`) | Organizational container; modeled as a native Document.              |
| Thread (document, spreadsheet, slides, chat)  | Document                    | Primary ingested knowledge asset, with the thread type as subtype.   |
| Folder → thread / folder → folder containment | `parentDocument`            | Reproduces the Quip folder hierarchy in DataHub.                     |
