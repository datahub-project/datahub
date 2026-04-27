## Overview

SharePoint is Microsoft's collaboration and document management platform, available as part of Microsoft 365 and on-premises via SharePoint Server. Learn more in the [official SharePoint documentation](https://docs.microsoft.com/en-us/sharepoint/).

The DataHub integration for SharePoint covers two distinct use cases: ingesting structured files stored in SharePoint document libraries as Datasets (data lake mode), and ingesting SharePoint pages and documents as Document entities for semantic search and knowledge discovery (document mode). Both modes authenticate via the Microsoft Graph API using Azure service principal credentials.

## Concept Mapping

| Source Concept            | DataHub Concept | Notes                                                        |
| ------------------------- | --------------- | ------------------------------------------------------------ |
| SharePoint Site           | Container       | Top-level site container, e.g. `/sites/Engineering`          |
| Document Library          | Container       | Library within a site, e.g. `Documents`, `RawData`           |
| Folder                    | Container       | Nested folder hierarchy within a library                     |
| File (structured)         | Dataset         | CSV, Parquet, JSON, Avro, Excel, PDF files in data lake mode |
| SharePoint Page / DocFile | Document        | Site pages and Word/PDF/PowerPoint files in document mode    |
| Schema fields             | SchemaField     | Inferred from CSV, Parquet, JSON, Avro, and Excel files      |
