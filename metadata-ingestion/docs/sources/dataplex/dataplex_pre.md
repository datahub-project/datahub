Ingesting metadata from Google Dataplex requires using the **dataplex** module.

#### Prerequisites

Please refer to the [Dataplex documentation](https://cloud.google.com/dataplex/docs) for basic information on Google Dataplex.

#### Credentials to access GCP

Please read the section to understand how to set up application default credentials in the [GCP docs](https://cloud.google.com/docs/authentication/provide-credentials-adc#how-to).

##### Permissions

Grant the following permissions to the Service Account on every project where you would like to extract metadata from.

Default GCP Role which contains these permissions: [roles/dataplex.viewer](https://cloud.google.com/dataplex/docs/iam-roles#dataplex.viewer)

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

**Optional permissions for lineage extraction:**

If you want to extract lineage information, you'll also need:

| Permission                 | Description                               |
| -------------------------- | ----------------------------------------- |
| `datalineage.links.get`    | Allows a user to view lineage links       |
| `datalineage.links.search` | Allows a user to search for lineage links |

Default GCP Role for lineage: [roles/datalineage.viewer](https://cloud.google.com/data-catalog/docs/how-to/lineage#required-permissions)

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

The Dataplex connector extracts metadata from Google Dataplex, including Projects, Lakes, Zones, Assets, and Entities in a given project and location.

#### Concept Mapping

This ingestion source maps the following Dataplex Concepts to DataHub Concepts:

| Dataplex Concept | DataHub Concept                                                                      | Notes                                                                                                 |
| :--------------- | :----------------------------------------------------------------------------------- | :---------------------------------------------------------------------------------------------------- |
| Project          | [`Container`](https://docs.datahub.com/docs/generated/metamodel/entities/container/) | GCP Project containing Dataplex resources                                                             |
| Lake             | [`Container`](https://docs.datahub.com/docs/generated/metamodel/entities/container/) | Data lake container (sub-container of Project)                                                        |
| Zone             | [`Container`](https://docs.datahub.com/docs/generated/metamodel/entities/container/) | Zone container under a Lake (RAW or CURATED type)                                                     |
| Asset            | [`Container`](https://docs.datahub.com/docs/generated/metamodel/entities/container/) | Asset container representing a BigQuery dataset or GCS bucket (sub-container of Zone)                 |
| Entity           | [`Dataset`](https://docs.datahub.com/docs/generated/metamodel/entities/dataset)      | Discovered table or fileset (linked to Asset container). Schema metadata is extracted when available. |

#### Hierarchy

The connector creates a hierarchical container structure:

```
Project (Container)
└── Lake (Container)
    └── Zone (Container)
        └── Asset (Container)
            └── Entity (Dataset)
```

#### Sibling Relationships

When `create_sibling_relationships` is enabled, the connector creates bidirectional sibling relationships between Dataplex entities and their source platform entities (e.g., BigQuery tables, GCS objects). This allows you to:

- Navigate between Dataplex and source platform views of the same data
- Control which entity is considered primary using `dataplex_is_primary_sibling` config
- By default, the source platform entity (BigQuery/GCS) is marked as primary

#### Lineage

When `extract_lineage` is enabled and proper permissions are granted, the connector extracts lineage information using the Dataplex Lineage API to capture relationships between:

- Entities and their transformations
- Data flows between assets
- Processing workflows

**Lineage Configuration Options:**

- **`extract_lineage`** (default: `true`): Enable table-level lineage extraction
- **`lineage_fail_fast`** (default: `false`): If true, fail immediately on lineage extraction errors. If false, log errors and continue processing. Recommended: `false` for development, `true` for production
- **`max_lineage_failures`** (default: `10`): Maximum consecutive lineage extraction failures before stopping (circuit breaker). Set to `-1` to disable circuit breaker
- **`lineage_batch_size`** (default: `1000`): Number of entities to process in each lineage extraction batch. Lower values reduce memory usage but may increase processing time. Set to `-1` to disable batching. Recommended: `1000` for large deployments (>10k entities), `-1` for small deployments

**Example Configuration:**

```yaml
source:
  type: dataplex
  config:
    project_ids:
      - "my-gcp-project"
    location: "us-central1"

    # Lineage settings
    extract_lineage: true # Enable lineage extraction

    # Error handling (optional)
    lineage_fail_fast: false # Continue on errors (development mode)
    max_lineage_failures: 10 # Circuit breaker after 10 consecutive failures

    # Memory optimization (optional)
    lineage_batch_size: 1000 # Process 1000 entities per batch
```

**Advanced Configuration for Large Deployments:**

For deployments with thousands of entities, memory optimization is critical:

```yaml
source:
  type: dataplex
  config:
    project_ids:
      - "my-gcp-project"
    location: "us-central1"

    # Memory optimization for large deployments
    lineage_batch_size: 1000 # Process 1000 entities at a time
    max_workers: 10 # Parallelize entity extraction across zones

    # Production error handling
    lineage_fail_fast: true # Fail fast on errors in production
    max_lineage_failures: 5 # Lower threshold for production
```

**Lineage Limitations:**

- Dataplex does not support column-level lineage extraction
- Lineage retention period: 30 days (Dataplex limitation)
- Cross-region lineage is not supported by Dataplex
- Lineage is only available for entities with active lineage tracking enabled

### Python Dependencies

The connector requires the following Python packages:

```bash
pip install 'google-cloud-dataplex>=1.0.0'
pip install 'google-cloud-datacatalog-lineage==0.2.2'  # Required only if extract_lineage is enabled
```
