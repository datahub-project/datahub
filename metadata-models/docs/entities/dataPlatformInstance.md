# Data Platform Instance

A Data Platform Instance represents a specific deployment or instance of a data platform. While a [dataPlatform](./dataPlatform.md) represents a technology type (e.g., MySQL, Snowflake, BigQuery), a dataPlatformInstance represents a particular running instance of that platform (e.g., "production-mysql-cluster", "dev-snowflake-account", "analytics-bigquery-project").

This entity is crucial for organizations that run multiple instances of the same platform technology across different environments, regions, or organizational units. It enables DataHub to distinguish between assets from different platform instances and provides a way to organize and manage platform-level metadata and credentials.

## Identity

Data Platform Instances are identified by two components:

- **Platform**: The URN of the data platform technology (e.g., `urn:li:dataPlatform:snowflake`)
- **Instance**: A unique string identifier for this specific instance (e.g., "prod-us-west-2", "dev-cluster-01")

The complete URN follows the pattern:

```
urn:li:dataPlatformInstance:(urn:li:dataPlatform:<platform>,<instance_id>)
```

### Examples

- `urn:li:dataPlatformInstance:(urn:li:dataPlatform:mysql,production-mysql-01)`
  - A production MySQL database cluster
- `urn:li:dataPlatformInstance:(urn:li:dataPlatform:snowflake,acme-prod-account)`
  - A production Snowflake account
- `urn:li:dataPlatformInstance:(urn:li:dataPlatform:bigquery,analytics-project)`
  - A BigQuery project used for analytics
- `urn:li:dataPlatformInstance:(urn:li:dataPlatform:iceberg,data-lake-warehouse)`
  - An Iceberg warehouse instance

## Important Capabilities

### Platform Instance Properties

The `dataPlatformInstanceProperties` aspect contains descriptive metadata about the platform instance:

- **name**: A display-friendly name for the instance (searchable, supports autocomplete)
- **description**: Documentation explaining the purpose, usage, or characteristics of this instance
- **customProperties**: Key-value pairs for additional custom metadata
- **externalUrl**: A link to external documentation or management console for this instance

This aspect helps users understand what each platform instance represents and how it should be used.

<details>
<summary>Python SDK: Create a platform instance with properties</summary>

```python
{{ inline /metadata-ingestion/examples/library/platform_instance_create.py show_path_as_comment }}
```

</details>

### Iceberg Warehouse Configuration

DataHub can serve as an Iceberg catalog, managing Iceberg tables through platform instances. The `icebergWarehouseInfo` aspect stores the configuration needed to manage an Iceberg warehouse:

- **dataRoot**: S3 path to the root location for table storage
- **clientId**: URN reference to the AWS access key ID secret
- **clientSecret**: URN reference to the AWS secret access key secret
- **region**: AWS region where the warehouse is located
- **role**: IAM role ARN used for credential vending
- **tempCredentialExpirationSeconds**: Expiration time for temporary credentials
- **env**: Environment/fabric type (PROD, DEV, QA, etc.)

This enables DataHub to manage Iceberg tables as a REST catalog, handling metadata operations and credential vending for data access.

The `datahub iceberg` CLI provides commands to create, update, list, and delete Iceberg warehouses. See the [Iceberg integration documentation](https://datahubproject.io/docs/generated/ingestion/sources/iceberg) for details.

### Ownership and Tags

Like other DataHub entities, platform instances support:

- **ownership**: Track who owns or manages this platform instance
- **globalTags**: Apply tags for categorization (e.g., "production", "pci-compliant", "deprecated")
- **institutionalMemory**: Add links to runbooks, documentation, or related resources

These aspects enable governance and discoverability of platform instances.

<details>
<summary>Python SDK: Add metadata to a platform instance</summary>

```python
{{ inline /metadata-ingestion/examples/library/platform_instance_add_metadata.py show_path_as_comment }}
```

</details>

### Status and Deprecation

Platform instances can be marked with status information:

- **status**: Indicates if the instance is active or has been removed
- **deprecation**: Mark instances as deprecated when they are being phased out, with optional decommission date and migration notes

This helps communicate lifecycle information about platform instances to users.

## Code Examples

### Creating a Platform Instance

The most common way to create platform instances is through the ingestion framework, which automatically creates them when the `platform_instance` configuration is specified in source configs. However, you can also create them programmatically:

<details>
<summary>Python SDK: Create a data platform instance</summary>

```python
{{ inline /metadata-ingestion/examples/library/platform_instance_create.py show_path_as_comment }}
```

</details>

### Attaching Platform Instance to Datasets

When ingesting metadata, the `dataPlatformInstance` aspect links datasets to their platform instance. This is typically done by ingestion connectors but can also be done manually:

<details>
<summary>Python SDK: Attach platform instance to a dataset</summary>

```python
{{ inline /metadata-ingestion/examples/library/dataset_attach_platform_instance.py show_path_as_comment }}
```

</details>

### Querying Platform Instances

You can retrieve platform instance information using the REST API or GraphQL:

<details>
<summary>Python SDK: Query platform instance via REST API</summary>

```python
{{ inline /metadata-ingestion/examples/library/platform_instance_query.py show_path_as_comment }}
```

</details>

<details>
<summary>REST API: Fetch platform instance entity</summary>

```bash
curl 'http://localhost:8080/entities/urn%3Ali%3AdataPlatformInstance%3A(urn%3Ali%3AdataPlatform%3Amysql%2Cproduction-cluster)'
```

</details>

<details>
<summary>GraphQL: Search for Iceberg warehouses</summary>

```graphql
query {
  search(
    input: {
      type: DATA_PLATFORM_INSTANCE
      query: "dataPlatform:iceberg"
      start: 0
      count: 10
    }
  ) {
    searchResults {
      entity {
        ... on DataPlatformInstance {
          urn
          platform {
            name
          }
          instanceId
          properties {
            name
            description
          }
        }
      }
    }
  }
}
```

</details>

## Integration Points

### Relationship to Other Entities

Platform instances are referenced by many entities through the `dataPlatformInstance` aspect:

- **Datasets**: Associate datasets with their platform instance, enabling filtering and organization by instance
- **Charts**: BI tool charts can be linked to the specific instance they query
- **Dashboards**: Dashboards are associated with platform instances
- **Data Jobs**: ETL/pipeline jobs reference the platform instance they run on
- **Data Flows**: Pipeline definitions can be associated with platform instances
- **ML Models**: Models can track which platform instance they were trained on
- **Containers**: Database schemas, folders, and other containers reference their instance
- **Assertions**: Data quality assertions can be scoped to specific instances

This creates a powerful organizational dimension across all data assets.

### Ingestion Framework Integration

Most DataHub ingestion sources support a `platform_instance` configuration parameter. When specified, the connector automatically attaches the platform instance to all ingested entities:

```yaml
source:
  type: mysql
  config:
    host_port: "mysql.prod.company.com:3306"
    platform_instance: "production-mysql-cluster"
    # ... other config
```

The platform instance is then used to:

- Distinguish assets from different instances of the same platform
- Enable instance-level filtering in the UI
- Support multi-tenant or multi-region deployments
- Organize metadata by deployment environment

### Usage in Dataset Naming

For platforms that support multiple instances, the platform instance is often incorporated into dataset names to ensure uniqueness. For example:

- Without instance: `urn:li:dataset:(urn:li:dataPlatform:mysql,db.schema.table,PROD)`
- With instance: `urn:li:dataset:(urn:li:dataPlatform:mysql,prod-cluster.db.schema.table,PROD)`

This ensures that tables with the same name across different instances have distinct URNs.

### Iceberg Catalog Integration

When DataHub serves as an Iceberg REST catalog, platform instances represent Iceberg warehouses. Each warehouse configuration includes:

- Storage credentials for S3 access
- IAM role configuration for credential vending
- Warehouse root location in object storage
- Environment designation

DataHub manages the lifecycle of Iceberg tables within these warehouses, handling:

- Table creation and metadata storage
- Temporary credential generation for read/write access
- Table discovery and lineage tracking
- Schema evolution

See the `datahub iceberg` CLI commands for managing Iceberg warehouses as platform instances.

## Notable Exceptions

### Internal vs. External Use

Data Platform Instances are categorized as "internal" entities in DataHub's entity registry, meaning they are primarily used for organization and metadata management rather than being primary discovery targets. Users typically interact with datasets, dashboards, and other assets rather than directly browsing platform instances.

However, platform instances are searchable and can be viewed in the DataHub UI when investigating asset organization or platform-level configurations.

### Platform Instance vs. Environment

Platform instances are distinct from the environment/fabric concept used in entity URNs (PROD, DEV, QA, etc.). While environment is a required part of many entity identifiers, platform instance is optional and provides a finer-grained organizational dimension.

A single platform instance typically corresponds to one environment, but you can have multiple instances within the same environment (e.g., "prod-us-west", "prod-us-east", "prod-eu-central" all in PROD environment).

### Automatic Instance Creation

Platform instances are typically created implicitly during ingestion rather than being explicitly defined beforehand. When an ingestion source references a platform instance that doesn't exist, DataHub will automatically create a basic platform instance entity. You can then enrich it with additional metadata like properties, ownership, and tags.

### Limited GraphQL Search

Unlike primary entities like datasets and dashboards, platform instances have limited search functionality in GraphQL. The `search` query with `type: DATA_PLATFORM_INSTANCE` is supported, but some advanced search features may not be fully implemented. REST API access provides full functionality.

### Immutable Key Components

Once created, a platform instance's key components (platform URN and instance ID) cannot be changed. If you need to rename an instance, you must create a new platform instance entity and migrate references from the old one.
