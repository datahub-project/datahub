### Overview

The `glue` module ingests metadata from Glue into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

#### IAM permissions

For metadata extraction, grant read access to Glue catalog resources and related services used by your configuration.

Typical minimum permissions include:

- `glue:GetDatabases`
- `glue:GetDatabase`
- `glue:GetTables`
- `glue:GetTable`
- `glue:GetPartitions`
- `glue:GetJobs`
- `glue:GetJob`

If lineage or storage details are enabled, also grant corresponding S3 permissions for referenced buckets.

#### Glue Cross-account Access

When extracting from cross-account Glue catalogs, set `catalog_id` explicitly and ensure the runtime principal has access to both catalog metadata and referenced storage resources in the target account.
