# Google Sheets Metadata Ingestion

## Overview

The Google Sheets connector extracts metadata from Google Sheets documents, including schema information, lineage relationships, usage statistics, and data profiles. It uses the Google Drive and Google Sheets APIs to extract this information.

### Concept Mapping

Here's a table for **Concept Mapping** between Google Sheets and DataHub to provide a clear overview of how entities and concepts in Google Sheets are mapped to corresponding entities in DataHub:

#### Default Mode (sheets_as_datasets = False)

| Source Concept          | DataHub Concept          | Notes                                         |
| ----------------------- | ------------------------ | --------------------------------------------- |
| Google Sheet Document   | `Dataset`                | Subtype: `Google Sheet`                       |
| Sheet in a Google Sheet | Field Path in Dataset    | e.g., "Sheet1.Column1"                        |
| Sheet Formula           | `Fine-Grained Lineage`   | Extracted from IMPORTRANGE and other formulas |
| Sheet Usage Statistics  | `DatasetUsageStatistics` | View counts, unique user access               |
| Sheet Data Profile      | `DatasetProfile`         | Statistical information about the data        |

#### Granular Mode (sheets_as_datasets = True)

| Source Concept         | DataHub Concept          | Notes                                          |
| ---------------------- | ------------------------ | ---------------------------------------------- |
| Google Sheet Document  | `Container`              | Logical grouping of datasets                   |
| Individual Sheet       | `Dataset`                | Each sheet is a separate dataset               |
| Column in a Sheet      | Field Path in Dataset    | e.g., "Column1"                                |
| Sheet Formula          | Fine-Grained Lineage     | More precise lineage between individual sheets |
| Sheet Usage Statistics | `DatasetUsageStatistics` | More granular usage stats at the sheet level   |
| Sheet Data Profile     | `DatasetProfile`         | Profile for each individual sheet              |
