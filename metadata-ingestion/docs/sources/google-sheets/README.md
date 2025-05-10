# Google Sheets Metadata Ingestion

## Overview

The Google Sheets connector extracts metadata from Google Sheets documents, including schema information, lineage relationships, usage statistics, and data profiles. It uses the Google Drive and Google Sheets APIs to extract this information.

## Concept Mapping

The connector supports two different metadata modeling approaches based on the `sheets_as_datasets` configuration option:

### Default Mode (sheets_as_datasets = False)

| Source Concept          | DataHub Concept                                               | Notes                                         |
| ----------------------- | ------------------------------------------------------------- | --------------------------------------------- |
| Google Sheet Document   | [Dataset](../../metamodel/entities/dataset.md)                | Subtype `Google Sheet`                        |
| Sheet in a Google Sheet | Field Path in Dataset                                         | e.g., "Sheet1.Column1"                        |
| Sheet Formula           | [Fine-Grained Lineage](../../metadata-model/lineage-model.md) | Extracted from IMPORTRANGE and other formulas |
| Sheet Usage Statistics  | [DatasetUsageStatistics](../../metamodel/entities/dataset.md) | View counts, unique user access               |
| Sheet Data Profile      | [DatasetProfile](../../metamodel/entities/dataset.md)         | Statistical information about the data        |

### Granular Mode (sheets_as_datasets = True)

| Source Concept         | DataHub Concept                                               | Notes                                          |
| ---------------------- | ------------------------------------------------------------- | ---------------------------------------------- |
| Google Sheet Document  | Container (similar to a Database or Schema)                   | Logical grouping of datasets                   |
| Individual Sheet       | [Dataset](../../metamodel/entities/dataset.md)                | Each sheet is a separate dataset               |
| Column in a Sheet      | Field Path in Dataset                                         | e.g., "Column1"                                |
| Sheet Formula          | [Fine-Grained Lineage](../../metadata-model/lineage-model.md) | More precise lineage between individual sheets |
| Sheet Usage Statistics | [DatasetUsageStatistics](../../metamodel/entities/dataset.md) | More granular usage stats at the sheet level   |
| Sheet Data Profile     | [DatasetProfile](../../metamodel/entities/dataset.md)         | Profile for each individual sheet              |

## Features

- **Sheet Metadata**: Extracts basic information about each Google Sheet including name, description, creation date, and modification date
- **Schema Metadata**: Extracts sheets, columns, and inferred data types
- **Lineage Extraction**: Detects relationships between sheets and with other data platforms (e.g., BigQuery)
- **Usage Statistics**: Collects usage metrics like view counts and unique users
- **Data Profiling**: Optional profiling of data to extract statistics like null counts, min/max values, distributions, etc.
- **Stateful Ingestion**: Support for tracking metadata changes over time and detecting stale entities

## Requirements

- A Google Cloud project with the Google Drive API, Google Sheets API, and optionally the Google Drive Activity API enabled
- A service account with appropriate permissions to access the Google Sheets of interest
- The service account credentials JSON file

## Permissions

The service account used for this connector needs the following permissions:

- Google Drive API: Read access to the sheets and folders you want to index
- Google Sheets API: Read access to sheet content
- Google Drive Activity API: Read access to activity data (for usage statistics)

## Setup Guide

1. Create a Google Cloud project or use an existing one
2. Enable the required APIs (Google Drive, Google Sheets, Google Drive Activity)
3. Create a service account with appropriate permissions
4. Generate and download the service account JSON credentials file
5. Share your Google Sheets with the service account email address
6. Configure the Google Sheets source in your DataHub ingestion recipe

See the [Setup](#setup) section in the connector documentation for detailed instructions.
