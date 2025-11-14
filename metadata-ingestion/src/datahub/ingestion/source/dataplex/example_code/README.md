# Dataplex Explorer

A comprehensive Python tool that extracts all Google Cloud Dataplex objects (Lakes, Zones, Assets, Entities, Entry Groups, Entries, Data Scans, Data Quality Results, and Data Profiling Results) and writes them to a structured log file with summary statistics and DataHub URN mappings.

## Features

- Extracts complete Dataplex hierarchy from a GCP project
- Automatically detects platform types (BigQuery, GCS) from asset resource types
- Generates DataHub URNs for all objects
- Produces detailed log file with summary at the top
- Shows platform breakdown for entities
- Includes object counts and relationships
- Extracts Data Quality scans with rules and latest results
- Extracts Data Profiling scans with latest results
- **Captures upstream and downstream lineage information for all entities using the Data Lineage API**
- Provides data quality scores, rule evaluations, and profiling statistics

## Installation

### Prerequisites

- Python 3.7 or higher
- Google Cloud Project with Dataplex enabled
- Appropriate IAM permissions (see Authentication section)

### Install Dependencies

```bash
pip install google-cloud-dataplex google-cloud-datacatalog-lineage
```

## Authentication

This script uses Google Cloud Application Default Credentials (ADC). You need to authenticate before running the script.

### Option 1: Development - Using gcloud CLI (Recommended for Local Development)

1. Install the [Google Cloud SDK](https://cloud.google.com/sdk/docs/install)

2. Authenticate with your user account:

```bash
gcloud auth application-default login
```

3. Set your default project (optional but recommended):

```bash
gcloud config set project YOUR_PROJECT_ID
```

### Option 2: Production - Using Service Account Key File

1. Create a service account in your GCP project:

```bash
gcloud iam service-accounts create dataplex-extractor \
    --display-name="Dataplex Data Extractor"
```

2. Grant the necessary permissions:

```bash
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
    --member="serviceAccount:dataplex-extractor@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/dataplex.viewer"
```

3. Create and download a key file:

```bash
gcloud iam service-accounts keys create ~/dataplex-key.json \
    --iam-account=dataplex-extractor@YOUR_PROJECT_ID.iam.gserviceaccount.com
```

4. Set the environment variable:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="$HOME/dataplex-key.json"
```

### Option 3: Running on Google Cloud

If running on Compute Engine, GKE, Cloud Run, or other Google Cloud services, the script will automatically use the attached service account. Just ensure the service account has the `roles/dataplex.viewer` role.

### Required IAM Permissions

The authenticated user or service account needs the following permissions:

- `dataplex.lakes.list`
- `dataplex.lakes.get`
- `dataplex.zones.list`
- `dataplex.zones.get`
- `dataplex.assets.list`
- `dataplex.assets.get`
- `dataplex.entities.list`
- `dataplex.entities.get`
- `dataplex.entryGroups.list`
- `dataplex.entryGroups.get`
- `dataplex.entries.list`
- `dataplex.entries.get`
- `dataplex.datascans.list`
- `dataplex.datascans.get`
- `dataplex.datascanjobs.list`
- `dataplex.datascanjobs.get`
- `datalineage.locations.searchLinks`
- `datalineage.events.get`

The Dataplex permissions are included in the `roles/dataplex.viewer` predefined role.
The lineage permissions are included in the `roles/datalineage.viewer` predefined role.

## Configuration

### Environment Variables

The script uses environment variables for configuration:

| Variable                         | Description                         | Example           |
| -------------------------------- | ----------------------------------- | ----------------- |
| `DATAPLEX_PROJECT_ID`            | GCP Project ID to extract data from | `your-project-id` |
| `DATAPLEX_LOCATION`              | GCP region/location                 | `us-central1`     |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to service account key file    | (uses ADC)        |

### Setting Environment Variables

**Linux/macOS:**

```bash
export DATAPLEX_PROJECT_ID="your-project-id"
export DATAPLEX_LOCATION="us-central1"
```

**Windows (Command Prompt):**

```cmd
set DATAPLEX_PROJECT_ID=your-project-id
set DATAPLEX_LOCATION=us-central1
```

**Windows (PowerShell):**

```powershell
$env:DATAPLEX_PROJECT_ID="your-project-id"
$env:DATAPLEX_LOCATION="us-central1"
```

## Usage

### Basic Usage

Run with default settings (uses environment variables or defaults):

```bash
python dataplex_explorer.py
```

This will create a file named `dataplex_extraction.log` in the current directory.

### Custom Output File

Specify a custom output filename:

```bash
python dataplex_explorer.py my_extraction.log
```

### Complete Example with Environment Variables

```bash
# Set configuration
export DATAPLEX_PROJECT_ID="my-gcp-project"
export DATAPLEX_LOCATION="us-central1"
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"

# Run extraction
python dataplex_explorer.py dataplex_output.log
```

## Output

The script generates a log file with the following structure:

### Summary Section

- Extraction timestamp
- Project ID and location
- Object counts by type (Lakes, Zones, Assets, Entities, Entry Groups, Entries, Data Scans, Quality Results, Profile Results)
- Total object count
- Platform breakdown for entities (BigQuery, GCS, etc.)

### Detailed Sections

- **Lakes** - All Dataplex lakes with metadata and DataHub URNs as domains
- **Zones** - All zones with type, state, and DataHub URNs as subdomains
- **Assets** - All assets with resource types and DataHub URNs as data products
- **Entities** - All discovered entities/datasets with platform detection, lineage information (upstream/downstream), and DataHub URNs
- **Entry Groups** - All catalog entry groups with DataHub URNs as containers
- **Entries** - All catalog entries with DataHub URNs as containers
- **Data Scans** - All data quality and profiling scans with configurations
- **Data Quality Results** - Latest quality scan results with pass/fail status, scores, and rule evaluations
- **Data Profile Results** - Latest profiling results with row counts and field statistics

## DataHub URN Mapping

The script generates DataHub URNs for all Dataplex objects following DataHub's metadata model:

| Dataplex Object       | DataHub Entity         | URN Format                                                         | Notes                    |
| --------------------- | ---------------------- | ------------------------------------------------------------------ | ------------------------ |
| Lake                  | Domain                 | `urn:li:domain:<lake-id>`                                          | Top-level domain         |
| Zone                  | Domain (Subdomain)     | `urn:li:domain:<zone-id>`                                          | Subdomain under lake     |
| Asset                 | Data Product           | `urn:li:dataProduct:<asset-id>`                                    | Data product grouping    |
| Entity                | Dataset                | `urn:li:dataset:(urn:li:dataPlatform:<platform>,<entity-id>,PROD)` | Actual tables/files      |
| Entry Group           | Container              | `urn:li:container:<entry-group-id>`                                | Catalog grouping         |
| Entry                 | Container              | `urn:li:container:<entry-id>`                                      | Catalog entries          |
| **Data Quality Scan** | **Assertion**          | `urn:li:assertion:<scan-id>`                                       | ✅ Proper DataHub entity |
| Data Quality Result   | Assertion Run Event    | _(attached to assertion)_                                          | No separate URN needed   |
| Data Profile Result   | Dataset Profile Aspect | _(attached to dataset)_                                            | No separate URN needed   |

**Platform Detection for Entities:**

- BigQuery assets → `platform: bigquery`
- GCS storage buckets → `platform: gcs`
- Other/unknown → `platform: dataplex`

## Lineage Extraction

The script uses the **Google Cloud Data Lineage API** to extract upstream and downstream relationships for each entity:

### How It Works

For each entity discovered in Dataplex, the script:

1. Constructs a fully qualified name in the format `platform:identifier` (e.g., `bigquery:project.dataset.table`)
2. Queries the Data Lineage API for upstream sources (data that flows INTO this entity)
3. Queries the Data Lineage API for downstream targets (data that flows OUT OF this entity)
4. Adds `lineage_upstream` and `lineage_downstream` properties to the entity data

### Lineage Data Structure

Each entity includes two lineage properties:

- **`lineage_upstream`**: List of fully qualified names of source datasets
- **`lineage_downstream`**: List of fully qualified names of target datasets

These correspond to DataHub's lineage aspect and can be directly mapped to upstream/downstream relationships in DataHub's metadata model.

### Requirements

- Lineage data must already exist in the Data Lineage API (written by BigQuery, Dataflow, Composer, or custom processes)
- The `roles/datalineage.viewer` role is required for the service account or user
- The Data Lineage API must be enabled: `gcloud services enable datalineage.googleapis.com`

### Example Output

```json
{
  "id": "my_table",
  "platform": "bigquery",
  "data_path": "project.dataset.my_table",
  "lineage_upstream": [
    "bigquery:project.dataset.source_table1",
    "bigquery:project.dataset.source_table2"
  ],
  "lineage_downstream": ["bigquery:project.dataset.target_table"]
}
```

## Lineage Visibility in the UI

### Current Status

The lineage extraction **works perfectly via the API** - as you can see in the extraction log, all upstream and downstream relationships are captured. However, viewing lineage in the Dataplex Console UI has additional requirements:

### Why Lineage Doesn't Show in Dataplex UI

Lineage visualization in the Dataplex Console requires:

1. Tables must exist as **catalog entries** in Dataplex Universal Catalog
2. BigQuery tables are automatically synced to catalog entries, but only when certain conditions are met:
   - Dataplex Discovery must be enabled and run
   - Tables must be in a dataset that's attached to a Dataplex asset
   - Automatic sync can take time to propagate

### Where You CAN View the Lineage

Even without catalog entries, your lineage data is fully functional:

1. **Programmatic Access** (✅ Working Now):

   - Use the `dataplex_explorer.py` script to extract lineage
   - Query the Data Lineage API directly
   - Integrate with DataHub (your use case)

2. **BigQuery Console** (May work):

   - Go to BigQuery → Your Table → "Lineage" tab
   - BigQuery Console may show lineage even without Dataplex catalog entries

3. **Dataplex Console** (Requires catalog entries):
   - Currently not available without catalog entries
   - Would require enabling Dataplex Discovery or waiting for automatic sync

### Verification Script

Use `verify_catalog_entries.py` to check if catalog entries exist:

```bash
python verify_catalog_entries.py
```

This shows whether your BigQuery tables have been synced to the Dataplex catalog yet.

## Testing Lineage Extraction

A helper script `generate_sample_lineage.py` is provided to create sample lineage relationships for testing:

### Usage

```bash
# Set your environment variables
export DATAPLEX_PROJECT_ID="your-project-id"
export DATAPLEX_LOCATION="us-central1"

# Generate sample lineage
python generate_sample_lineage.py
```

### What It Does

The script creates lineage relationships between the adoption dataset entities:

```
humans ────────────┐
                   ↓
            human_profiles ─┐
                            ↓
                       adoptions
                            ↑
             pet_profiles ──┘
                   ↑
pets ─────────────┘
```

This creates:

- **Upstream lineage**: `adoptions` will show `human_profiles` and `pet_profiles` as sources
- **Downstream lineage**: `humans` will show `human_profiles` as a target
- **Multi-hop lineage**: You can trace from `humans` → `human_profiles` → `adoptions`

After running this script, run `dataplex_explorer.py` again to see the lineage data populated in the extraction log.

### Complete Workflow for Testing Lineage

To set up and test the complete lineage extraction pipeline:

```bash
# 1. Authenticate with Google Cloud
gcloud auth application-default login

# 2. Set environment variables
export DATAPLEX_PROJECT_ID="your-project-id"
export DATAPLEX_LOCATION="us-central1"

# 3. Generate sample lineage relationships
python generate_sample_lineage.py

# 4. Extract everything including lineage
python dataplex_explorer.py

# 5. Check the log file for lineage data
grep -A 3 "Lineage" dataplex_extraction.log

# 6. (Optional) Verify if catalog entries exist for UI viewing
python verify_catalog_entries.py
```

The lineage data is now available programmatically and ready to be ingested into DataHub!

## References

For issues related to:

- **Dataplex API**: See [Dataplex Documentation](https://cloud.google.com/dataplex/docs)
- **Data Lineage API**: See [Data Lineage Documentation](https://cloud.google.com/dataplex/docs/about-data-lineage)
- **Authentication**: See [Google Cloud Authentication Guide](https://cloud.google.com/docs/authentication)
- **This Script**: Open an issue or contact your administrator

## License

This script is provided as-is for internal use.
