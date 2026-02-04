:::caution
The Dataplex connector will overwrite metadata from other Google Cloud source connectors (BigQuery, GCS, etc.) if they extract the same entities. If you're running multiple Google Cloud connectors, be aware that the last connector to run will determine the final metadata state for overlapping entities.
:::

### Prerequisites

Please refer to the [Dataplex documentation](https://cloud.google.com/dataplex/docs) for basic information on Google Dataplex.

#### Authentication

Google Cloud uses Application Default Credentials (ADC) for authentication. Refer to the [GCP documentation](https://cloud.google.com/docs/authentication/provide-credentials-adc) to set up ADC based on your environment. If you prefer to use a service account then use the following instructions.

#### Create a service account and assign roles

1. Setup a ServiceAccount as per [GCP docs](https://cloud.google.com/iam/docs/creating-managing-service-accounts#iam-service-accounts-create-console) and assign the previously mentioned roles to this service account.

2. Download a service account JSON keyfile.

   Example credential file:

   ```json
   {
     "type": "service_account",
     "project_id": "project-id-1234567",
     "private_key_id": "d0121d0000882411234e11166c6aaa23ed5d74e0",
     "private_key": "-----BEGIN PRIVATE KEY-----\nMIIyourkey\n-----END PRIVATE KEY-----",
     "client_email": "test@suppproject-id-1234567.iam.gserviceaccount.com",
     "client_id": "113545814931671546333",
     "auth_uri": "https://accounts.google.com/o/oauth2/auth",
     "token_uri": "https://oauth2.googleapis.com/token",
     "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
     "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test%suppproject-id-1234567.iam.gserviceaccount.com"
   }
   ```

3. To provide credentials to the source, you can either:

   Set an environment variable:

   ```sh
   $ export GOOGLE_APPLICATION_CREDENTIALS="/path/to/keyfile.json"
   ```

   _or_

   Set credential config in your source based on the credential json file. For example:

   ```yml
   credential:
     project_id: "project-id-1234567"
     private_key_id: "d0121d0000882411234e11166c6aaa23ed5d74e0"
     private_key: "-----BEGIN PRIVATE KEY-----\nMIIyourkey\n-----END PRIVATE KEY-----\n"
     client_email: "test@suppproject-id-1234567.iam.gserviceaccount.com"
     client_id: "123456678890"
   ```

#### Permissions

Grant the following permissions to the Service Account on every project where you would like to extract metadata from.

**For Universal Catalog Entries API:**

Default GCP Role: [roles/dataplex.catalogViewer](https://cloud.google.com/dataplex/docs/iam-roles#dataplex.catalogViewer)

| Permission                  | Description                           |
| --------------------------- | ------------------------------------- |
| `dataplex.entryGroups.get`  | Retrieve specific entry group details |
| `dataplex.entryGroups.list` | View all entry groups in a location   |
| `dataplex.entries.get`      | Access entry metadata and details     |
| `dataplex.entries.getData`  | View data aspects within entries      |
| `dataplex.entries.list`     | Enumerate entries within groups       |

**For lineage extraction** (optional, `include_lineage: true`):

Default GCP Role: [roles/datalineage.viewer](https://docs.cloud.google.com/iam/docs/roles-permissions/datalineage#datalineage.viewer)

| Permission                 | Description                               |
| -------------------------- | ----------------------------------------- |
| `datalineage.links.get`    | Allows a user to view lineage links       |
| `datalineage.links.search` | Allows a user to search for lineage links |

### Integration Details

The Dataplex connector extracts metadata from Google Dataplex using the **Universal Catalog Entries API**. This API extracts entries from system-managed entry groups for Google Cloud services and is the recommended approach for discovering resources across your GCP organization.

**Supported services include:**

- **BigQuery**: datasets, tables, models, routines, connections, and linked datasets
- **Cloud SQL**: instances
- **AlloyDB**: instances, databases, schemas, tables, and views
- **Spanner**: instances, databases, and tables
- **Pub/Sub**: topics and subscriptions
- **Cloud Storage**: buckets
- **Bigtable**: instances, clusters, and tables
- **Vertex AI**: models, datasets, and feature stores
- **Dataform**: repositories and workflows
- **Dataproc Metastore**: services and databases

#### Platform Alignment

Datasets discovered by Dataplex use the same URNs as native connectors (e.g., `bigquery`, `gcs`). This means:

- **No Duplication**: Dataplex and native BigQuery/GCS connectors can run together - entities discovered by both will merge
- **Native Containers**: BigQuery tables appear in their native dataset containers
- **Unified View**: Users see a single view of all datasets regardless of discovery method

#### Concept Mapping

This ingestion source maps the following Dataplex Concepts to DataHub Concepts:

| Dataplex Concept          | DataHub Concept                                                                     | Notes                                                                        |
| :------------------------ | :---------------------------------------------------------------------------------- | :--------------------------------------------------------------------------- |
| Entry (Universal Catalog) | [`Dataset`](https://docs.datahub.com/docs/generated/metamodel/entities/dataset)     | From Universal Catalog. Uses source platform URNs (e.g., `bigquery`, `gcs`). |
| BigQuery Project/Dataset  | [`Container`](https://docs.datahub.com/docs/generated/metamodel/entities/container) | Created as containers to align with native BigQuery connector.               |

#### Custom Properties

The connector adds the following custom properties to datasets:

- `dataplex_entry_id`: The entry identifier in Dataplex
- `dataplex_entry_group`: The entry group containing this entry
- `dataplex_fully_qualified_name`: The fully qualified name of the entry
- `dataplex_ingested`: Marker indicating the dataset was ingested via Dataplex

:::note
To access system-managed entry groups like `@bigquery`, use multi-region locations (`us`, `eu`, `asia`) via the `entries_location` config parameter. Regional locations (`us-central1`, etc.) only contain placeholder entries.
:::

### Filtering Configuration

Filter which datasets to ingest using regex patterns with allow/deny lists:

**Example:**

```yaml
source:
  type: dataplex
  config:
    project_ids:
      - "my-gcp-project"

    filter_config:
      entries:
        dataset_pattern:
          allow:
            - "production_.*" # Only production datasets
          deny:
            - ".*_test" # Exclude test datasets
            - ".*_temp" # Exclude temporary datasets
```

### Lineage

When `include_lineage` is enabled and proper permissions are granted, the connector extracts **table-level lineage** using the Dataplex Lineage API. Dataplex automatically tracks lineage from these Google Cloud systems:

**Supported Systems:**

- **BigQuery**: DDL (CREATE TABLE, CREATE TABLE AS SELECT, views, materialized views) and DML (SELECT, INSERT, MERGE, UPDATE, DELETE) operations
- **Cloud Data Fusion**: Pipeline executions
- **Cloud Composer**: Workflow orchestration
- **Dataflow**: Streaming and batch jobs
- **Dataproc**: Apache Spark and Apache Hive jobs (including Dataproc Serverless)
- **Vertex AI**: Models, datasets, feature store views, and feature groups

**Not Supported:**

- **Column-level lineage**: The connector extracts only table-level lineage (column-level lineage is available in Dataplex but not exposed through this connector)
- **Custom sources**: Only Google Cloud systems with automatic lineage tracking are supported
- **BigQuery Data Transfer Service**: Recurring loads are not automatically tracked

**Lineage Limitations:**

- Lineage data is retained for 30 days in Dataplex
- Lineage may take up to 24 hours to appear after job completion
- Cross-region lineage is not supported by Dataplex
- Lineage is only available for entries with active lineage tracking enabled

For more details, see [Dataplex Lineage Documentation](https://docs.cloud.google.com/dataplex/docs/about-data-lineage).

### Configuration Options

**Metadata Extraction:**

- **`include_schema`** (default: `true`): Extract column metadata and types
- **`include_lineage`** (default: `true`): Extract table-level lineage (automatically retries transient errors)

**Performance Tuning:**

- **`batch_size`** (default: `1000`): Entries per batch for memory optimization. Set to `None` to disable batching (small deployments only)

**Lineage Retry Settings** (optional):

- **`lineage_max_retries`** (default: `3`, range: `1-10`): Retry attempts for transient errors
- **`lineage_retry_backoff_multiplier`** (default: `1.0`, range: `0.1-10.0`): Backoff delay multiplier

**Example Configuration:**

```yaml
source:
  type: dataplex
  config:
    project_ids:
      - "my-gcp-project"

    # Location for entries (Universal Catalog) - defaults to "us"
    # Must be multi-region (us, eu, asia) for system entry groups like @bigquery
    entries_location: "us"

    # Metadata extraction settings
    include_schema: true # Enable schema metadata extraction (default: true)
    include_lineage: true # Enable lineage extraction with automatic retries

    # Lineage retry settings (optional, defaults shown)
    lineage_max_retries: 3 # Max retry attempts (range: 1-10)
    lineage_retry_backoff_multiplier: 1.0 # Exponential backoff multiplier (range: 0.1-10.0)
```

**Configuration for Large Deployments:**

For deployments with thousands of entries, memory optimization is important. The connector uses batched emission to keep memory bounded:

```yaml
source:
  type: dataplex
  config:
    project_ids:
      - "my-gcp-project"
    entries_location: "us"

    # Performance tuning
    batch_size: 1000 # Process and emit 1000 entries at a time to optimize memory usage
```

### Troubleshooting

#### Lineage Extraction Issues

**Automatic Retry Behavior:**

The connector automatically retries transient errors when extracting lineage:

- **Retried errors** (with exponential backoff): Timeouts (DeadlineExceeded), rate limiting (HTTP 429), service issues (HTTP 503, 500)
- **Non-retried errors** (logs warning and continues): Permission denied (HTTP 403), not found (HTTP 404), invalid argument (HTTP 400)

After exhausting retries, the connector logs a warning and continues processing other entries. You'll still get metadata even if lineage extraction fails for some entries.

**Common Issues:**

1. **Regional restrictions**: Lineage API requires multi-region location (`us`, `eu`, `asia`) rather than specific regions (`us-central1`). The connector automatically uses the `entries_location` config.
2. **Missing permissions**: Ensure service account has `roles/datalineage.viewer` role on all projects.
3. **No lineage data**: Some entries may not have lineage if they weren't created through supported systems (BigQuery DDL/DML, Cloud Data Fusion, etc.).
4. **Rate limiting**: If you encounter persistent rate limiting, increase `lineage_retry_backoff_multiplier` to add more delay between retries, or decrease `lineage_max_retries` if you prefer faster failure.
