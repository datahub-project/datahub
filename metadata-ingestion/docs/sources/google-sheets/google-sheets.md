# Google Sheets

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

For common workflows and configurations, check out our [common workflows](../common_workflows.md) page.

## Overview

The Google Sheets connector extracts metadata from Google Sheets documents, including schema information, lineage relationships, usage statistics, and data profiles. It uses the Google Drive and Google Sheets APIs to extract this information.

## Prerequisites

1. **Create a Google Cloud Project**:

   - Go to the [Google Cloud Console](https://console.cloud.google.com/)
   - Create a new project or select an existing one

2. **Enable Required APIs**:

   - Go to [API Library](https://console.cloud.google.com/apis/library)
   - Enable the following APIs:
     - Google Drive API
     - Google Sheets API
     - Google Drive Activity API (optional, for usage statistics)

3. **Create Service Account Credentials**:

   - Go to [Credentials](https://console.cloud.google.com/apis/credentials)
   - Click "Create Credentials" and select "Service Account"
   - Fill in the required fields and create the service account
   - Generate a JSON key file for the service account
   - Download and securely store the JSON key file

4. **Grant Access to Your Google Sheets**:
   - Share your Google Sheets or Drive folders with the service account email address
   - The service account email will look like: `service-account-name@project-id.iam.gserviceaccount.com`
   - For proper lineage extraction, ensure the service account has access to all linked sheets

## Capabilities

This connector supports the following capabilities:

- Platform instance (enabled by default)
- Descriptions (enabled by default)
- Schema metadata (extracts schema from Google Sheets)
- Data profiling (can profile Google Sheets data)
- Usage statistics (can extract usage statistics from Google Sheets)
- Lineage (both coarse and fine-grained, extracted from formulas)
- Deletion detection (enabled by default when stateful ingestion is turned on)

## Concept Mapping

Here's a table for **Concept Mapping** between Google Sheets and DataHub to provide a clear overview of how entities and concepts in Google Sheets are mapped to corresponding entities in DataHub:

### Default Mode (sheets_as_datasets = False)

| Source Concept          | DataHub Concept          | Notes                                         |
| ----------------------- | ------------------------ | --------------------------------------------- |
| Google Sheet Document   | `Dataset`                | Subtype: `Google Sheet`                       |
| Sheet in a Google Sheet | Field Path in Dataset    | e.g., "Sheet1.Column1"                        |
| Sheet Formula           | Fine-Grained Lineage     | Extracted from IMPORTRANGE and other formulas |
| Sheet Usage Statistics  | `DatasetUsageStatistics` | View counts, unique user access               |
| Sheet Data Profile      | `DatasetProfile`         | Statistical information about the data        |

### Granular Mode (sheets_as_datasets = True)

| Source Concept         | DataHub Concept          | Notes                                          |
| ---------------------- | ------------------------ | ---------------------------------------------- |
| Google Sheet Document  | `Container`              | Logical grouping of datasets                   |
| Individual Sheet       | `Dataset`                | Each sheet is a separate dataset               |
| Column in a Sheet      | Field Path in Dataset    | e.g., "Column1"                                |
| Sheet Formula          | Fine-Grained Lineage     | More precise lineage between individual sheets |
| Sheet Usage Statistics | `DatasetUsageStatistics` | More granular usage stats at the sheet level   |
| Sheet Data Profile     | `DatasetProfile`         | Profile for each individual sheet              |

## Troubleshooting

- **Permission Issues**: Ensure the service account has at least "Viewer" access to all sheets you want to catalog
- **Rate Limiting**: The connector includes retry logic, but Google API rate limits may still apply
- **Lineage Extraction Failures**: Check that all referenced sheets are accessible to the service account
