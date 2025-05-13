### Setup

This source pulls metadata directly from Google Sheets and enables data profiling, lineage extraction, and usage statistics collection.

#### Prerequisites

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

#### Key Features

1. **Schema Extraction**:

   - Extracts sheet names and column information
   - Infers data types from column values

2. **Lineage Tracking**:

   - Detects connections between sheets using IMPORTRANGE formulas
   - Identifies cross-platform references (e.g., to BigQuery tables)
   - Supports column-level lineage

3. **Data Profiling**:

   - Row counts and column statistics
   - Value distributions and patterns
   - Null values and uniqueness metrics

4. **Usage Statistics**:
   - Tracks sheet view counts
   - Monitors unique user access

#### Metadata Extracted

- **Sheet Properties**: Name, description, creation time, last modified time
- **Sheet Structure**: Worksheets, columns, data types
- **Ownership Information**: Sheet owners
- **Lineage Data**: Upstream and downstream dependencies
- **Usage Metrics**: View counts, unique user counts
- **Data Profiles**: Statistical information about data in each sheet

#### Metadata Structure Options

The connector supports two different ways of modeling Google Sheets metadata:

1. **Default Mode (sheets_as_datasets = False)**:

   - Each Google Sheets document is represented as a dataset
   - Individual sheets within the document are treated as fields or components of that dataset
   - Lineage is tracked between Google Sheets documents

2. **Granular Mode (sheets_as_datasets = True)**:
   - Each Google Sheets document is represented as a container (similar to a database or schema)
   - Individual sheets within the document are treated as separate datasets
   - Lineage is tracked between individual sheets
   - Schema and profiling information is more granular
   - This mode is recommended when your individual sheets represent distinct logical datasets

#### Troubleshooting

- **Permission Issues**: Ensure the service account has at least "Viewer" access to all sheets you want to catalog
- **Rate Limiting**: The connector includes retry logic, but Google API rate limits may still apply
- **Lineage Extraction Failures**: Check that all referenced sheets are accessible to the service account
