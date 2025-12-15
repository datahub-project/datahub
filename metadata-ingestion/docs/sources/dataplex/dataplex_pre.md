Ingesting metadata from Google Dataplex requires using the **dataplex** module.

#### Prerequisites

Please refer to the [Dataplex documentation](https://cloud.google.com/dataplex/docs) for basic information on Google Dataplex.

#### Credentials to access GCP

Please read the section to understand how to set up application default credentials in the [GCP docs](https://cloud.google.com/docs/authentication/provide-credentials-adc#how-to).

##### Permissions

Grant the following permissions to the Service Account on every project where you would like to extract metadata from.

**For Universal Catalog Entries API** (default, `include_entries: true`):

Default GCP Role: [roles/dataplex.catalogViewer](https://cloud.google.com/dataplex/docs/iam-roles#dataplex.catalogViewer)

| Permission                  | Description                           |
| --------------------------- | ------------------------------------- |
| `dataplex.entryGroups.get`  | Retrieve specific entry group details |
| `dataplex.entryGroups.list` | View all entry groups in a location   |
| `dataplex.entries.get`      | Access entry metadata and details     |
| `dataplex.entries.getData`  | View data aspects within entries      |
| `dataplex.entries.list`     | Enumerate entries within groups       |

**For Lakes/Zones Entities API** (optional, `include_entities: true`):

Default GCP Role: [roles/dataplex.viewer](https://cloud.google.com/dataplex/docs/iam-roles#dataplex.viewer)

| Permission               | Description                                           |
| ------------------------ | ----------------------------------------------------- |
| `dataplex.lakes.get`     | Allows a user to view details of a specific lake      |
| `dataplex.lakes.list`    | Allows a user to view and list all lakes in a project |
| `dataplex.zones.get`     | Allows a user to view details of a specific zone      |
| `dataplex.zones.list`    | Allows a user to view and list all zones in a lake    |
| `dataplex.assets.get`    | Allows a user to view details of a specific asset     |
| `dataplex.assets.list`   | Allows a user to view and list all assets in a zone   |
| `dataplex.entities.get`  | Allows a user to view details of a specific entity    |
| `dataplex.entities.list` | Allows a user to view and list all entities in a zone |

**For lineage extraction** (optional, `include_lineage: true`):

Default GCP Role: [roles/datalineage.viewer](https://docs.cloud.google.com/iam/docs/roles-permissions/datalineage#datalineage.viewer)

| Permission                 | Description                               |
| -------------------------- | ----------------------------------------- |
| `datalineage.links.get`    | Allows a user to view lineage links       |
| `datalineage.links.search` | Allows a user to search for lineage links |

**Note:** If using both APIs, grant both sets of permissions. Most users only need `roles/dataplex.catalogViewer` for Entries API access.

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

### Integration Details

The Dataplex connector extracts metadata from Google Dataplex using two complementary APIs:

1. **Universal Catalog Entries API** (Primary, default enabled): Extracts entries from system-managed entry groups for Google Cloud services. This is the recommended approach for discovering resources across your GCP organization. Supported services include:

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

2. **Lakes/Zones Entities API** (Optional, default disabled): Extracts entities from Dataplex lakes and zones. Use this if you are using the legacy Data Catalog and need lake/zone information not available in the Entries API. See [API Selection Guide](#api-selection-guide) below for detailed guidance on when to use each API as using both APIs can cause loss of custom properties.

**Datasets are ingested using their source platform URNs** (BigQuery, GCS, etc.) to align with native source connectors.

#### Concept Mapping

This ingestion source maps the following Dataplex Concepts to DataHub Concepts:

| Dataplex Concept          | DataHub Concept                                                                     | Notes                                                                                                                                                                                                                                                                                          |
| :------------------------ | :---------------------------------------------------------------------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Entry (Universal Catalog) | [`Dataset`](https://docs.datahub.com/docs/generated/metamodel/entities/dataset)     | Metadata from Universal Catalog for Google Cloud services (BigQuery, Cloud SQL, AlloyDB, Spanner, Pub/Sub, GCS, Bigtable, Vertex AI, Dataform, Dataproc Metastore). Ingested using **source platform URNs** (e.g., `bigquery`, `gcs`, `spanner`). Schema metadata is extracted when available. |
| Entity (Lakes/Zones)      | [`Dataset`](https://docs.datahub.com/docs/generated/metamodel/entities/dataset)     | Discovered table or fileset from lakes/zones. Ingested using **source platform URNs** (e.g., `bigquery`, `gcs`). Schema metadata is extracted when available.                                                                                                                                  |
| BigQuery Project/Dataset  | [`Container`](https://docs.datahub.com/docs/generated/metamodel/entities/container) | BigQuery projects and datasets are created as containers to align with the native BigQuery connector. Dataplex-discovered BigQuery tables are linked to these containers.                                                                                                                      |
| Lake/Zone/Asset           | Custom Properties                                                                   | Dataplex hierarchy information (lake, zone, asset, zone type) is preserved as **custom properties** on datasets for traceability without creating separate containers.                                                                                                                         |

#### API Selection Guide

**When to use Entries API** (default, `include_entries: true`):

- ✅ You want to discover all BigQuery tables, Pub/Sub topics, and other Google Cloud resources
- ✅ You need comprehensive metadata from Dataplex's centralized catalog
- ✅ You want system-managed discovery without manual lake/zone configuration
- ✅ **Recommended for most users**

**When to use Entities API** (`include_entities: true`):

- Use this if you need lake/zone information that isn't available in the Entries API
- Provides Dataplex organizational context (lake, zone, asset metadata)
- Can be used alongside Entries API, but see warning below

**Important**: To access system-managed entry groups like `@bigquery` that contain BigQuery tables, you must use **multi-region locations** (`us`, `eu`, `asia`) via the `entries_location` config parameter. Regional locations (`us-central1`, etc.) only contain placeholder entries.

#### ⚠️ Using Both APIs Together - Important Behavior

When both `include_entries` and `include_entities` are enabled and they discover the **same table** (same URN), the metadata behaves as follows:

**What gets preserved:**

- ✅ Schema metadata (from Entries API - most authoritative)
- ✅ Entry-specific custom properties (`dataplex_entry_id`, `dataplex_entry_group`, `dataplex_fully_qualified_name`, etc.)

**What gets lost:**

- ❌ Entity-specific custom properties (`dataplex_lake`, `dataplex_zone`, `dataplex_zone_type`, `data_path`, `system`, `format`, `asset`)

**Why this happens:** DataHub replaces aspects at the aspect level. When the Entries API emits metadata for a dataset that was already processed by the Entities API, it completely replaces the `datasetProperties` aspect, which contains all custom properties.

**Recommendation:**

- **For most users**: Use Entries API only (default). It provides comprehensive metadata from Universal Catalog.
- **For lake/zone context**: Use Entities API only with `include_entries: false` if you specifically need Dataplex organizational metadata.
- **Using both**: Only enable both APIs if you need Entries API for some tables and Entities API for others (non-overlapping datasets). For overlapping tables, entry metadata will take precedence and entity context will be lost.

**Example showing the data loss:**

```yaml
# Entity metadata (first):
custom_properties:
  dataplex_lake: "production-lake"
  dataplex_zone: "raw-zone"
  dataplex_zone_type: "RAW"
  data_path: "gs://bucket/path"

# After Entry metadata (second) - lake/zone info is lost:
custom_properties:
  dataplex_entry_id: "abc123"
  dataplex_entry_group: "@bigquery"
  dataplex_fully_qualified_name: "bigquery:project.dataset.table"
```

#### Filtering Configuration

The connector supports filtering at multiple levels with clear separation between Entries API and Entities API filters:

**Entries API Filtering** (only applies when `include_entries=true`):

- `entries.dataset_pattern`: Filter which entry IDs to ingest from Universal Catalog
  - Supports regex patterns with allow/deny lists
  - Applies to entries discovered from system-managed entry groups like `@bigquery`

**Entities API Filtering** (only applies when `include_entities=true`):

- `entities.lake_pattern`: Filter which lakes to process
- `entities.zone_pattern`: Filter which zones to process
- `entities.dataset_pattern`: Filter which entity IDs (tables/filesets) to ingest from lakes/zones
  - Supports regex patterns with allow/deny lists

These filters are nested under `filter_config.entries` and `filter_config.entities` to make it clear which API each filter applies to. This allows you to have different filtering rules for each API when both are enabled.

**Example with filtering:**

```yaml
source:
  type: dataplex
  config:
    project_ids:
      - "my-gcp-project"
    entries_location: "us"
    include_entries: true
    include_entities: true

    filter_config:
      # Entries API filtering (Universal Catalog)
      entries:
        dataset_pattern:
          allow:
            - "bq_.*" # Allow BigQuery entries starting with bq_
            - "pubsub_.*" # Allow Pub/Sub entries
          deny:
            - ".*_test" # Deny test entries
            - ".*_temp" # Deny temporary entries

      # Entities API filtering (Lakes/Zones)
      entities:
        lake_pattern:
          allow:
            - "production-.*" # Only production lakes
        zone_pattern:
          deny:
            - ".*-sandbox" # Exclude sandbox zones
        dataset_pattern:
          allow:
            - "table_.*" # Allow entities starting with table_
            - "fileset_.*" # Allow filesets
          deny:
            - ".*_backup" # Exclude backups
```

#### Platform Alignment

The connector generates datasets that align with native source connectors:

**BigQuery Entities:**

- **URN Format**: `urn:li:dataset:(urn:li:dataPlatform:bigquery,{project}.{dataset}.{table},PROD)`
- **Container**: Linked to BigQuery dataset containers (same as BigQuery connector)
- **Platform**: `bigquery`

**GCS Entities:**

- **URN Format**: `urn:li:dataset:(urn:li:dataPlatform:gcs,{bucket}/{path},PROD)`
- **Container**: No container (same as GCS connector)
- **Platform**: `gcs`

This alignment ensures:

- **Consistency**: Dataplex-discovered entities appear alongside native BigQuery/GCS entities in the same container hierarchy
- **No Duplication**: If you run both Dataplex and BigQuery/GCS connectors, entities discovered by both will merge (same URN)
- **Unified Navigation**: Users see a single view of BigQuery datasets or GCS buckets, regardless of discovery method

#### Dataplex Context Preservation

Dataplex-specific metadata is preserved as custom properties on each dataset:

| Custom Property      | Description                                      | Example Value       |
| :------------------- | :----------------------------------------------- | :------------------ |
| `dataplex_ingested`  | Indicates this entity was discovered by Dataplex | `"true"`            |
| `dataplex_lake`      | Dataplex lake ID                                 | `"my-data-lake"`    |
| `dataplex_zone`      | Dataplex zone ID                                 | `"raw-zone"`        |
| `dataplex_entity_id` | Dataplex entity ID                               | `"customer_table"`  |
| `dataplex_zone_type` | Zone type (RAW or CURATED)                       | `"RAW"`             |
| `data_path`          | GCS path for the entity                          | `"gs://bucket/..."` |
| `system`             | Storage system (BIGQUERY, CLOUD_STORAGE)         | `"BIGQUERY"`        |
| `format`             | Data format (PARQUET, AVRO, etc.)                | `"PARQUET"`         |

These properties allow you to:

- Identify which assets were discovered through Dataplex
- Understand the Dataplex organizational structure (lakes, zones)
- Filter or search for Dataplex-managed entities
- Trace entities back to their Dataplex catalog origin

#### Lineage

When `include_lineage` is enabled and proper permissions are granted, the connector extracts **table-level lineage** using the Dataplex Lineage API. Dataplex automatically tracks lineage from these Google Cloud systems:

**Supported Systems:**

- **BigQuery**: DDL (CREATE TABLE, CREATE TABLE AS SELECT, views, materialized views) and DML (SELECT, INSERT, MERGE, UPDATE, DELETE) operations
- **Cloud Data Fusion**: Pipeline executions
- **Cloud Composer**: Workflow orchestration
- **Dataflow**: Streaming and batch jobs
- **Dataproc**: Spark and Hadoop jobs
- **Vertex AI**: ML pipeline operations

**Not Supported:**

- **Column-level lineage**: The connector extracts only table-level lineage (column-level lineage is available in Dataplex but not exposed through this connector)
- **Custom sources**: Only Google Cloud systems with automatic lineage tracking are supported
- **BigQuery Data Transfer Service**: Recurring loads are not automatically tracked

**Lineage Limitations:**

- Lineage data is retained for 30 days in Dataplex
- Lineage may take up to 24 hours to appear after job completion

For more details, see [Dataplex Lineage Documentation](https://docs.cloud.google.com/dataplex/docs/about-data-lineage).

**Metadata Extraction and Performance Configuration Options:**

- **`include_schema`** (default: `true`): Enable schema metadata extraction (columns, types, descriptions). Set to `false` to skip schema extraction for faster ingestion when only basic dataset metadata is needed. Disabling schema extraction can improve performance for large deployments
- **`include_lineage`** (default: `true`): Enable table-level lineage extraction. Lineage API calls automatically retry transient errors (timeouts, rate limits, service unavailable) with exponential backoff
- **`batch_size`** (default: `1000`): Controls batching for metadata emission and lineage extraction. Lower values reduce memory usage but may increase processing time. Set to `null` to disable batching. Recommended: `1000` for large deployments (>10k entities), `null` for small deployments (<1k entities)

**Lineage Retry Configuration:**

You can customize how the connector handles transient errors when extracting lineage:

- **`lineage_max_retries`** (default: `3`, range: `1-10`): Maximum number of retry attempts for lineage API calls when encountering transient errors (timeouts, rate limits, service unavailable). Each attempt uses exponential backoff. Higher values increase resilience but may slow down ingestion
- **`lineage_retry_backoff_multiplier`** (default: `1.0`, range: `0.1-10.0`): Multiplier for exponential backoff between retry attempts (in seconds). Wait time formula: `multiplier * (2 ^ attempt_number)`, capped between 2-10 seconds. Higher values reduce API load but increase ingestion time

**Automatic Retry Behavior:**

The connector automatically handles transient errors when extracting lineage:

**Retried Errors** (with exponential backoff):

- **Timeouts**: DeadlineExceeded errors from slow API responses
- **Rate Limiting**: HTTP 429 (TooManyRequests) errors
- **Service Issues**: HTTP 503 (ServiceUnavailable), HTTP 500 (InternalServerError)

**Non-Retried Errors** (logs warning and continues):

- **Permission Denied**: HTTP 403 - missing `datalineage.viewer` role
- **Not Found**: HTTP 404 - entity or lineage data doesn't exist
- **Invalid Argument**: HTTP 400 - incorrect API parameters (e.g., wrong region format)

**Common Configuration Issues:**

1. **Regional restrictions**: Lineage API requires multi-region location (e.g., `us`, `eu`) rather than specific regions (e.g., `us-central1`). The connector automatically converts your `location` config
2. **Missing permissions**: Ensure service account has `roles/datalineage.viewer` role on all projects
3. **No lineage data**: Some entities may not have lineage if they weren't created through supported systems (BigQuery DDL/DML, Cloud Data Fusion, etc.)
4. **Rate limiting**: If you encounter persistent rate limiting, increase `lineage_retry_backoff_multiplier` to add more delay between retries, or decrease `lineage_max_retries` if you prefer faster failure

After exhausting retries, the connector logs a warning and continues processing other entities - you'll still get metadata (lakes, zones, assets, entities, schema) even if lineage extraction fails for some entities.

**Example Configuration:**

```yaml
source:
  type: dataplex
  config:
    project_ids:
      - "my-gcp-project"

    # Location for lakes/zones/entities (if using include_entities)
    location: "us-central1"

    # Location for entries (Universal Catalog) - defaults to "us"
    # Must be multi-region (us, eu, asia) for system entry groups like @bigquery
    entries_location: "us" # Default value, can be omitted

    # API selection
    include_entries: true # Enable Universal Catalog entries (default: true)
    include_entities: false # Enable lakes/zones entities (default: false)

    # Metadata extraction settings
    include_schema: true # Enable schema metadata extraction (default: true)
    include_lineage: true # Enable lineage extraction with automatic retries

    # Lineage retry settings (optional, defaults shown)
    lineage_max_retries: 3 # Max retry attempts (range: 1-10)
    lineage_retry_backoff_multiplier: 1.0 # Exponential backoff multiplier (range: 0.1-10.0)
```

**Advanced Configuration for Large Deployments:**

For deployments with thousands of entities, memory optimization is critical. The connector uses batched emission to keep memory bounded:

```yaml
source:
  type: dataplex
  config:
    project_ids:
      - "my-gcp-project"
    location: "us-central1"
    entries_location: "us"

    # API selection
    include_entries: true
    include_entities: false

    # Memory optimization for large deployments
    batch_size:
      1000 # Batch size for metadata emission and lineage extraction
      # Entries/entities are emitted in batches of 1000 to prevent memory issues
      # Set to null to disable batching (only for small deployments <1k entities)
    max_workers: 10 # Parallelize entity extraction across zones
```

**How Batching Works:**

- Entries and entities are collected during API streaming
- When a batch reaches `batch_size` entries, it's immediately emitted to DataHub
- The batch cache is cleared to free memory
- This keeps memory usage bounded regardless of dataset size
- For deployments with 50k+ entities, batching prevents out-of-memory errors

**Lineage Limitations:**

- Dataplex does not support column-level lineage extraction
- Lineage retention period: 30 days (Dataplex limitation)
- Cross-region lineage is not supported by Dataplex
- Lineage is only available for entities with active lineage tracking enabled
  For more details on lineage limitations, refer to [GCP docs](https://docs.cloud.google.com/dataplex/docs/about-data-lineage#current-feature-limitations).

### Python Dependencies

The connector requires the following Python packages, which are automatically installed with `acryl-datahub[dataplex]`:

- `google-cloud-dataplex>=1.0.0`
- `google-cloud-datacatalog-lineage==0.2.2` (required for lineage extraction when `include_lineage: true`)
