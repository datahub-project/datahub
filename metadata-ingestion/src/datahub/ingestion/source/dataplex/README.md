# Google Dataplex Source

This DataHub connector extracts metadata from Google Dataplex and ingests it into DataHub.

## Overview

Google Dataplex is a data management service that enables organizations to centrally discover, manage, monitor, and govern their data across data lakes, data warehouses, and data marts. This connector extracts metadata from Dataplex and maps it to DataHub's metadata model.

## Entity Mapping

The connector maps Dataplex resources to DataHub entities as follows:

| Dataplex Resource | DataHub Entity Type | Description                                 |
| ----------------- | ------------------- | ------------------------------------------- |
| Project           | Container           | GCP Project containing Dataplex resources   |
| Lake              | Domain              | Business domain representing a data lake    |
| Zone              | Sub-domain          | Sub-domain under a Lake (RAW or CURATED)    |
| Asset             | Data Product        | Data asset (BigQuery dataset or GCS bucket) |
| Entity            | Dataset             | Discovered table or fileset                 |
| Entry Group       | Container           | Universal Catalog entry group (Phase 2)     |
| Entry             | Dataset/Container   | Universal Catalog entry (Phase 2)           |

## Setup

### Prerequisites

1. **GCP Project with Dataplex enabled**

   - Ensure Dataplex API is enabled in your GCP project

2. **Service Account with appropriate permissions**

   Required IAM roles:

   ```
   roles/dataplex.viewer
   roles/datacatalog.viewer (if using Universal Catalog)
   roles/datalineage.viewer (if extracting lineage)
   ```

   Or specific permissions:

   ```
   # Dataplex permissions
   dataplex.lakes.get
   dataplex.lakes.list
   dataplex.zones.get
   dataplex.zones.list
   dataplex.assets.get
   dataplex.assets.list
   dataplex.entities.get
   dataplex.entities.list

   # Optional: For lineage extraction
   datalineage.links.get
   datalineage.links.search

   # Optional: For BigQuery schema details
   bigquery.tables.get
   bigquery.tables.list
   ```

3. **Python Dependencies**

   Install required packages:

   ```bash
   pip install google-cloud-dataplex>=1.0.0
   pip install google-cloud-datacatalog-lineage>=0.3.0
   ```

## Configuration

### Basic Configuration

```yaml
source:
  type: dataplex
  config:
    # Required
    project_id: "my-gcp-project"

    # Optional: GCP credentials
    credential:
      project_id: "my-gcp-project"
      private_key_id: "d0121d0000882411234e11166c6aaa23ed5d74e0"
      private_key: "-----BEGIN PRIVATE KEY-----\nMIIyourkey\n-----END PRIVATE KEY-----\n"
      client_email: "test@suppproject-id-1234567.iam.gserviceaccount.com"
      client_id: "123456678890"

    # Optional: Location (default: us-central1)
    location: "us-central1"

    # Optional: Environment (default: PROD)
    env: "PROD"
```

### Advanced Configuration

```yaml
source:
  type: dataplex
  config:
    project_id: "my-gcp-project"
    location: "us-central1"

    # Filtering
    filter_config:
      lake_pattern:
        allow:
          - "retail-.*"
          - "finance-.*"
        deny:
          - ".*-test"

      zone_pattern:
        allow:
          - ".*"
        deny:
          - "deprecated-.*"

      asset_pattern:
        allow:
          - ".*"
        deny:
          - "temp-.*"

    # Feature flags
    extract_lakes: true
    extract_zones: true
    extract_assets: true
    extract_entities: true
    extract_entry_groups: false # Phase 2
    extract_entries: false # Phase 2
    extract_lineage: true

    # Relationship creation
    create_sibling_relationships: true

    # Tagging
    apply_zone_type_tags: true
    apply_label_tags: true

    # Performance
    max_workers: 10
```

## Authentication

The connector supports multiple authentication methods:

### 1. Service Account Key (Recommended for Production)

```yaml
credential:
  project_id: "my-gcp-project"
  private_key_id: "key-id"
  private_key: "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
  client_email: "service-account@project.iam.gserviceaccount.com"
  client_id: "123456789"
```

### 2. Application Default Credentials (ADC)

If `credential` is not specified, the connector uses Application Default Credentials:

- On GCE/GKE: Uses the instance/pod service account
- Local development: Uses `gcloud auth application-default login`

```bash
# Set up ADC locally
gcloud auth application-default login
```

## Features

### Phase 1 (Current Implementation)

- ✅ Extract Projects as Containers
- ✅ Extract Lakes as Domains
- ✅ Extract Zones as Sub-domains
- ⏳ Extract Assets as Data Products (in progress)
- ⏳ Extract Entities as Datasets (in progress)
- ⏳ Extract lineage relationships (in progress)
- ⏳ Create sibling relationships with native datasets (in progress)

### Phase 2 (Future)

- ⬜ Extract Entry Groups from Universal Catalog
- ⬜ Extract Entries from Universal Catalog
- ⬜ Support Custom Aspects
- ⬜ Incremental ingestion
- ⬜ Data quality metrics
- ⬜ Data profiling statistics

## Usage Examples

### Running the Ingestion

```bash
# Using a recipe file
datahub ingest -c dataplex_recipe.yml

# Or directly
datahub ingest -c '
source:
  type: dataplex
  config:
    project_id: my-project
    location: us-central1

sink:
  type: datahub-rest
  config:
    server: http://localhost:8080
'
```

### Test Connection

```bash
datahub check source-connection -c dataplex_recipe.yml
```

## Troubleshooting

### Common Issues

1. **Authentication Errors**

   ```
   Error: 403 Permission denied on resource
   ```

   Solution: Verify that your service account has the required IAM permissions.

2. **API Not Enabled**

   ```
   Error: Dataplex API has not been used in project
   ```

   Solution: Enable the Dataplex API in your GCP project:

   ```bash
   gcloud services enable dataplex.googleapis.com --project=PROJECT_ID
   ```

3. **No Resources Found**
   ```
   Warning: No lakes found in project
   ```
   Solution: Check that:
   - You have Dataplex resources in the specified location
   - Your filter patterns are not too restrictive
   - Your service account has list permissions

### Debug Mode

Enable debug logging for troubleshooting:

```yaml
source:
  type: dataplex
  config:
    # ... your config ...

# Add this to enable debug logs
pipeline_name: dataplex_ingestion
debug_mode: true
```

## Development

### Project Structure

```
dataplex/
├── __init__.py              # Package exports
├── dataplex.py              # Main source implementation
├── dataplex_config.py       # Configuration classes
├── dataplex_report.py       # Reporting and metrics
├── README.md                # This file
├── dataplex_implementation.md  # Detailed spec
└── example_code/            # Reference examples
    ├── dataplex_client.py
    └── ...
```

### Contributing

When contributing to this connector:

1. Follow the patterns established in `vertexai` and `bigquery_v2` sources
2. Add appropriate type hints to all functions
3. Update the README with new features
4. Add unit tests for new functionality
5. Run linting before committing:
   ```bash
   ./gradlew :metadata-ingestion:lintFix
   ```

## References

- [Dataplex Documentation](https://cloud.google.com/dataplex/docs)
- [Dataplex API Reference](https://cloud.google.com/dataplex/docs/reference/rest)
- [DataHub Documentation](https://datahubproject.io/docs/)
- [Implementation Spec](./dataplex_implementation.md)

## Support

For issues or questions:

- GitHub Issues: [datahub/issues](https://github.com/datahub-project/datahub/issues)
- Slack: [DataHub Community](https://slack.datahubproject.io/)

## License

This connector is part of the DataHub project and follows the same Apache 2.0 license.
