# Data Platform

A Data Platform is a metadata entity that represents a source system, technology, or tool that contains and manages data assets. Data Platforms are the foundational building blocks in DataHub's metadata model, serving as the namespace and classification system for all datasets, dashboards, charts, jobs, and other data assets.

Examples of data platforms include databases (MySQL, PostgreSQL, Oracle), data warehouses (Snowflake, BigQuery, Redshift), BI tools (Looker, Tableau, Power BI), data lakes (S3, HDFS), message brokers (Kafka), and many other systems where data resides or flows through.

## Identity

A Data Platform is uniquely identified by a single component:

- **platformName**: The standardized name of the technology (e.g., "mysql", "snowflake", "looker", "kafka")

The URN structure for a Data Platform is:

```
urn:li:dataPlatform:<platformName>
```

### Examples

```
urn:li:dataPlatform:mysql
urn:li:dataPlatform:snowflake
urn:li:dataPlatform:bigquery
urn:li:dataPlatform:looker
urn:li:dataPlatform:kafka
urn:li:dataPlatform:s3
urn:li:dataPlatform:dbt
```

### Platform Name Conventions

Platform names follow these conventions:

- **Lowercase**: All platform names are lowercase (e.g., "bigquery" not "BigQuery")
- **No spaces**: Use hyphens for multi-word names (e.g., "azure-ad" not "Azure AD")
- **Technology-specific**: Name represents the specific technology, not a vendor or product line
- **Standardized**: DataHub maintains a canonical list of platform names to ensure consistency

The complete list of officially supported data platforms is maintained in DataHub's [data-platforms.yaml](https://github.com/datahub-project/datahub/blob/master/metadata-service/configuration/src/main/resources/bootstrap_mcps/data-platforms.yaml) bootstrap configuration.

### Custom Platforms

While DataHub ships with 100+ pre-defined platforms, you can create custom platform entities for:

- In-house or proprietary data systems
- New technologies not yet in the official list
- Logical groupings of related systems
- Platform variations with specific configurations

When creating custom platforms, follow the naming conventions above and ensure uniqueness across your DataHub instance.

## Important Capabilities

### Platform Information

The `dataPlatformInfo` aspect contains the core metadata about a data platform:

- **name**: The canonical name of the platform (max 15 characters, searchable)
- **displayName**: A user-friendly display name for the platform (e.g., "Google BigQuery" for platform "bigquery")
- **type**: The category of platform from the PlatformType enumeration
- **datasetNameDelimiter**: The character used to separate components in dataset names (e.g., "." for Oracle, "/" for HDFS)
- **logoUrl**: Optional URL to a logo image representing the platform

#### Platform Types

Data platforms are classified into these categories:

- **RELATIONAL_DB**: Traditional relational databases (MySQL, PostgreSQL, Oracle, SQL Server)
- **OLAP_DATASTORE**: Online analytical processing systems (Pinot, Druid, ClickHouse)
- **KEY_VALUE_STORE**: Key-value and NoSQL databases (Redis, DynamoDB, Cassandra)
- **SEARCH_ENGINE**: Search and indexing platforms (Elasticsearch, Solr)
- **MESSAGE_BROKER**: Event streaming and messaging systems (Kafka, Pulsar, RabbitMQ)
- **FILE_SYSTEM**: Distributed file systems (HDFS, NFS, local file systems)
- **OBJECT_STORE**: Cloud object storage (S3, GCS, Azure Blob Storage)
- **QUERY_ENGINE**: SQL query engines (Presto, Trino, Athena, Hive)
- **OTHERS**: Other platform types including BI tools, orchestrators, data catalogs, and specialized systems

The platform type helps DataHub understand how to interact with the platform and what kinds of metadata are expected.

The following code snippet shows you how to create a custom Data Platform.

<details>
<summary>Python SDK: Create a Data Platform</summary>

```python
{{ inline /metadata-ingestion/examples/library/data_platform_create.py show_path_as_comment }}
```

</details>

### Dataset Name Delimiter

The `datasetNameDelimiter` field is critical for understanding dataset naming conventions on each platform:

- **"."**: Used by most relational databases (e.g., `database.schema.table` in PostgreSQL)
- **"/"**: Used by file systems (e.g., `/data/warehouse/customers` in HDFS)
- **""** (empty string): Used when dataset names are flat without hierarchy

This delimiter helps DataHub:

- Parse dataset names correctly
- Build browse paths for hierarchical navigation
- Display dataset names in a user-friendly format
- Generate proper URNs for datasets on the platform

### Logo URL

Platforms can have custom logos displayed in the DataHub UI through the `logoUrl` field. This helps users quickly recognize platforms visually. DataHub ships with built-in logos for all officially supported platforms, but custom platforms can specify their own logo URLs.

## Integration Points

### Relationship with Datasets

Data Platforms are the parent entity for all datasets. Every dataset URN includes a platform reference:

```
urn:li:dataset:(urn:li:dataPlatform:snowflake,database.schema.table,PROD)
```

This relationship enables:

- **Discovery**: Find all datasets belonging to a specific platform
- **Governance**: Apply platform-level policies and access controls
- **Lineage**: Track data movement between different platforms
- **Inventory**: Understand the distribution of data across your technology stack

### Relationship with Other Entities

Beyond datasets, platforms are referenced by many other entity types:

- **Dashboards**: BI dashboards from platforms like Looker, Tableau, Superset
- **Charts**: Visualizations from BI platforms
- **DataFlows**: Orchestration workflows from Airflow, Prefect, Dagster
- **DataJobs**: Individual tasks or jobs within data pipelines
- **ML Models**: Machine learning models from platforms like SageMaker, MLflow
- **DataProcesses**: Streaming processes from platforms like Flink, Spark

All of these entities include a platform reference in their URNs, creating a comprehensive technology inventory.

### Platform Instances

For organizations running multiple instances of the same platform (e.g., multiple Snowflake accounts, multiple production BigQuery projects), DataHub provides a `dataPlatformInstance` entity. This allows distinguishing between:

- Platform: The technology itself (e.g., "snowflake")
- Platform Instance: A specific deployment of that technology (e.g., "snowflake-prod-us-west", "snowflake-dev-eu")

Platform instances reference their parent platform and add deployment-specific metadata like environment, region, or account information.

<details>
<summary>Python SDK: Create a Platform Instance</summary>

```python
{{ inline /metadata-ingestion/examples/library/platform_instance_create.py show_path_as_comment }}
```

</details>

### Ingestion Sources

Data Platforms are typically created automatically through two mechanisms:

1. **Bootstrap Data**: When DataHub starts, it loads ~100 pre-defined platforms from the bootstrap configuration
2. **Ingestion Connectors**: When you run an ingestion source (e.g., the Snowflake connector), it automatically creates platform references for any assets it ingests

You rarely need to manually create platform entities unless you're adding a custom or in-house system not covered by the standard bootstrap list.

### Search and Discovery

Platforms are fully searchable in DataHub:

- **Platform Name Search**: Find platforms by their canonical name
- **Display Name Search**: Search using user-friendly display names
- **Platform Filtering**: Filter datasets and other entities by platform
- **Platform Browse**: Navigate through entities organized by platform

This enables users to explore the data landscape from a platform-centric view.

### GraphQL API

Data Platforms can be queried through the GraphQL API to retrieve:

- Platform metadata (name, type, logo)
- Counts of entities on the platform
- Platform configuration information

While platforms are rarely created via GraphQL (since they're mostly bootstrap data), the API enables programmatic access to platform information.

## Notable Exceptions

### Bootstrap vs Custom Platforms

DataHub makes a distinction between:

- **Bootstrap Platforms**: The ~100 platforms loaded at startup, maintained by the DataHub project
- **Custom Platforms**: Platforms you create for your own use cases

Bootstrap platforms:

- Have official logos and display names
- Are maintained across DataHub upgrades
- Are tested with DataHub's ingestion connectors
- Follow consistent naming conventions

Custom platforms:

- May not have logos (unless you provide logoUrl)
- Are your responsibility to maintain
- May not have pre-built ingestion connectors
- Should follow the same naming conventions to avoid confusion

### Platform Name Immutability

Once a platform is created and assets are associated with it, the platform name becomes immutable for practical purposes. Changing a platform name would require:

- Migrating all dataset URNs that reference the platform
- Updating all lineage edges
- Re-ingesting all metadata from that platform

If you need to rename a platform, it's generally easier to:

1. Create a new platform with the desired name
2. Re-ingest all assets using the new platform name
3. Deprecate or delete the old platform

### Platform Name Length Limit

Platform names are limited to 15 characters (enforced by `@validate.strlen.max = 15`). This is a legacy constraint that ensures platform names are concise and fit well in URNs and UI displays.

If your platform's natural name exceeds 15 characters, use an abbreviation or acronym:

- "azure-synapse" → "synapse"
- "amazon-redshift" → "redshift"
- "google-bigquery" → "bigquery"

The full name can go in the `displayName` field without length restrictions.

### Case Sensitivity

Platform names are case-sensitive in URNs but conventionally always lowercase. Avoid creating platforms that differ only in case (e.g., "MySQL" and "mysql") as this will cause confusion and potential URN conflicts.

### Platform Types Are Descriptive, Not Functional

The `type` field (FILE_SYSTEM, RELATIONAL_DB, etc.) is primarily for classification and display purposes. DataHub does not enforce different behaviors based on platform type. Two platforms of different types are treated the same way by the system.

However, connectors and ingestion logic may use platform type to determine:

- What metadata to extract
- How to parse dataset names
- What lineage patterns to expect
- What profiling or usage collection is possible

## Common Platform Categories

### Databases and Data Warehouses

**RELATIONAL_DB platforms**: MySQL, PostgreSQL, Oracle, SQL Server, MariaDB, DB2

**OLAP_DATASTORE platforms**: Snowflake, BigQuery, Redshift, Clickhouse, Pinot, Druid

These platforms typically have:

- Hierarchical dataset names (database.schema.table)
- "." as the delimiter
- Schema metadata (columns, types, constraints)
- Query log metadata for usage and lineage

### Business Intelligence Tools

**OTHERS platform type** (BI-specific): Looker, Tableau, Power BI, Metabase, Superset, Mode

These platforms typically have:

- Dashboard and chart entities
- Lineage from dashboards to underlying datasets
- Usage statistics for views and interactions
- Metadata about report authors and schedules

### Data Lakes and Object Stores

**OBJECT_STORE platforms**: S3, GCS, Azure Blob Storage, MinIO

**FILE_SYSTEM platforms**: HDFS, NFS, Azure Data Lake

These platforms typically have:

- "/" as the delimiter
- File-based datasets (Parquet, Avro, CSV, JSON)
- Path-based naming conventions
- Schema-on-read metadata

### Streaming Platforms

**MESSAGE_BROKER platforms**: Kafka, Pulsar, Kinesis, Event Hubs, RabbitMQ

These platforms typically have:

- Topic or stream datasets
- Schema registry integration
- Real-time lineage tracking
- Throughput and lag metrics

### Orchestration and Pipeline Tools

**OTHERS platform type** (orchestration-specific): Airflow, Prefect, Dagster, Luigi, Argo

These platforms typically have:

- DataFlow (pipeline) and DataJob (task) entities
- DAG structures and dependencies
- Execution statistics and run history
- Lineage based on job dependencies

### Transformation Tools

**OTHERS platform type** (transformation-specific): dbt, Spark, Flink, Databricks

These platforms typically have:

- Strong column-level lineage
- Transformation logic metadata
- Model/job entities
- Test and documentation metadata

## Use Cases

Data Platforms enable several critical use cases in DataHub:

1. **Technology Inventory**: Understand what platforms your organization uses and how extensively
2. **Platform Governance**: Apply policies and access controls at the platform level
3. **Migration Planning**: Identify datasets to migrate when consolidating or replacing platforms
4. **Cost Optimization**: Track which platforms host the most/least used data
5. **Multi-Platform Lineage**: Trace data flows across different technologies in your data ecosystem
6. **Technology Standardization**: Enforce or monitor adoption of approved platforms
7. **Impact Analysis**: Understand the blast radius of platform outages or changes
8. **Discovery Navigation**: Browse and filter assets by platform for easier discovery

By establishing platforms as first-class entities, DataHub provides a comprehensive view of your organization's data technology landscape.
