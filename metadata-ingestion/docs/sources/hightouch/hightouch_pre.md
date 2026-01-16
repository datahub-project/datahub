### Capabilities

This connector extracts the following:

- Metadata for Hightouch sources, models, syncs, and destinations
- Lineage information between sources, models, syncs, and destinations
- Sync run execution history with detailed statistics
- Column-level lineage from field mappings

| Capability               | Status | Notes                                                                             |
| ------------------------ | ------ | --------------------------------------------------------------------------------- |
| Platform Instance        | ✅     | Enabled by default                                                                |
| Lineage (Coarse-grained) | ✅     | Table-to-table lineage showing data flow from source → model → sync → destination |
| Lineage (Fine-grained)   | ✅     | Column-to-column lineage via field mappings and SQL parsing                       |
| Deletion Detection       | ✅     | Enabled via stateful ingestion for automatic cleanup of stale entities            |
| Tags                     | ✅     | Model tags are captured as custom properties                                      |
| Containers               | ✅     | Hierarchical organization via workspaces and folders                              |
| Siblings                 | ✅     | Sibling relationships between Hightouch models and upstream tables (optional)     |
| Schema Normalization     | ✅     | Automatic casing normalization to match upstream table schemas                    |
| SQL Parsing              | ✅     | Extracts upstream dependencies from raw_sql models                                |
| Owners                   | 🚫     | Ownership information not exposed by Hightouch API                                |

### Integration Details

This source extracts the following metadata from Hightouch:

- **Sources** - Database and warehouse connections (as DataHub Dataset references)
- **Models** - SQL queries and transformations (optionally as DataHub Datasets)
- **Syncs** - Data pipelines from models to destinations (as DataHub DataJobs)
- **Destinations** - Target systems like Salesforce, HubSpot, etc. (as DataHub Dataset references)
- **Sync Runs** - Execution history with detailed statistics (as DataHub DataProcessInstances)
- **Lineage** - Complete data flow from sources through models and syncs to destinations
- **Column-level Lineage** - Field mappings between source and destination systems

## Prerequisites

### 1. Generate a Hightouch API Key

1. Log in to your Hightouch account
2. Navigate to **Settings** → **API Keys**
3. Click **Create API Key**
4. Give your key a descriptive name (e.g., "DataHub Integration")
5. Select the appropriate permissions:
   - **Read access** to: Sources, Models, Syncs, Destinations, Sync Runs
6. Copy the generated API key (you won't be able to see it again)
7. Store the API key securely - you'll use it in the DataHub recipe configuration

### 2. Required Permissions

The API key needs read access to:

- Sources
- Models
- Syncs
- Destinations
- Sync Runs (for execution history)

## Configuration Notes

### Model Ingestion Options

You can configure how Hightouch models are represented in DataHub:

- **Option 1 (Recommended)**: Set `emit_models_as_datasets: true`

  - Models appear as separate Dataset entities in DataHub with platform "hightouch"
  - Lineage flows: Source Tables → Hightouch Models → Syncs → Destination Tables
  - Provides visibility into intermediate transformations

- **Option 2**: Set `emit_models_as_datasets: false`
  - Models are not created as separate entities
  - Lineage flows directly: Source Tables → Syncs → Destination Tables
  - Simpler lineage graph, but less visibility into transformations

### Lineage Features

The connector extracts comprehensive lineage:

1. **Coarse-grained Lineage**: Table-to-table relationships showing data flow
2. **Fine-grained Lineage**: Column-to-column mappings extracted from sync field mappings
3. **Multi-hop Lineage**: Complete path from source databases through models to destinations

## Concept Mapping

| Hightouch Object | DataHub Entity                                                           | Description                                                   |
| ---------------- | ------------------------------------------------------------------------ | ------------------------------------------------------------- |
| `Source`         | [Dataset](../../metamodel/entities/dataset.md)                           | Source database/warehouse (referenced as input)               |
| `Model`          | [Dataset](../../metamodel/entities/dataset.md)                           | SQL query or transformation (optional, platform: "hightouch") |
| `Sync`           | [Data Job](../../metamodel/entities/dataJob.md)                          | Data pipeline that moves data from model to destination       |
| `Destination`    | [Dataset](../../metamodel/entities/dataset.md)                           | Target system (referenced as output)                          |
| `Sync Run`       | [Data Process Instance](../../metamodel/entities/dataProcessInstance.md) | Execution instance with statistics                            |
| `Workspace`      | Platform Instance                                                        | Hightouch workspace (optional grouping)                       |

### Compatibility

This connector is compatible with Hightouch API v1 and supports lineage extraction for a wide variety of source and destination types. The connector automatically maps Hightouch source and destination types to DataHub platform names for proper lineage tracking.

#### Supported Sources

The connector recognizes and creates lineage for the following source types:

<details>
<summary><b>Data Warehouses</b></summary>

- Snowflake
- Google BigQuery
- Amazon Redshift
- Databricks
- Azure Synapse Analytics
- Amazon Athena

</details>

<details>
<summary><b>Databases</b></summary>

- PostgreSQL
- MySQL
- Microsoft SQL Server / Azure SQL
- Oracle
- MongoDB
- Amazon DynamoDB

</details>

<details>
<summary><b>BI & Analytics Tools</b></summary>

- Looker
- Tableau
- Metabase
- Mode
- Sigma

</details>

<details>
<summary><b>Cloud Storage</b></summary>

- Amazon S3
- Google Cloud Storage (GCS)
- Azure Blob Storage
- Azure Data Lake Storage (ADLS / ADLS Gen2)

</details>

<details>
<summary><b>SaaS Applications</b></summary>

- Salesforce
- Google Sheets
- Airtable
- Google Analytics
- HubSpot

</details>

#### Supported Destinations

The connector recognizes and creates lineage for the following destination types:

<details>
<summary><b>Data Warehouses & Databases</b></summary>

- Snowflake
- Google BigQuery
- Amazon Redshift
- Databricks
- PostgreSQL
- MySQL
- Microsoft SQL Server

</details>

<details>
<summary><b>Cloud Storage</b></summary>

- Amazon S3
- Google Cloud Storage (GCS)
- Azure Blob Storage
- Azure Data Lake Storage (ADLS / ADLS Gen2)

</details>

<details>
<summary><b>CRM & Sales</b></summary>

- Salesforce
- HubSpot
- Zendesk
- Pipedrive
- Outreach
- Salesloft

</details>

<details>
<summary><b>Marketing Automation</b></summary>

- Braze
- Iterable
- Customer.io
- Marketo
- Klaviyo
- Mailchimp
- ActiveCampaign
- Eloqua
- SendGrid

</details>

<details>
<summary><b>Analytics & Product</b></summary>

- Segment
- Mixpanel
- Amplitude
- Google Analytics
- Heap
- Pendo
- Intercom

</details>

<details>
<summary><b>Advertising Platforms</b></summary>

- Facebook Ads
- Google Ads
- LinkedIn Ads
- Snapchat Ads
- TikTok Ads
- Pinterest Ads
- Twitter Ads

</details>

<details>
<summary><b>Customer Support</b></summary>

- Zendesk
- Intercom
- Freshdesk
- Kustomer

</details>

<details>
<summary><b>Collaboration & Productivity</b></summary>

- Google Sheets
- Airtable
- Slack

</details>

<details>
<summary><b>Payment & Finance</b></summary>

- Stripe
- Chargebee

</details>

<details>
<summary><b>Other Integrations</b></summary>

- HTTP/Webhook endpoints
- Custom destinations

</details>

:::note
If your source or destination type is not automatically recognized, you can manually configure the platform mapping using the `sources_to_platform_instance` and `destinations_to_platform_instance` configuration options. For the complete and most up-to-date list of supported integrations, refer to the [Hightouch documentation](https://hightouch.com/docs/destinations/overview/).
:::

## Advanced Configuration

### Working with Platform Instances

If you have multiple instances of source or destination systems, configure platform instances to generate correct lineage edges.

#### Example: Multiple Snowflake Sources

```yaml
sources_to_platform_instance:
  # Key is the Hightouch source ID (visible in Hightouch URL)
  "12345":
    platform: "snowflake"
    platform_instance: "prod-snowflake"
    env: "PROD"
    database: "analytics"

  "67890":
    platform: "snowflake"
    platform_instance: "dev-snowflake"
    env: "DEV"
    database: "analytics_dev"
```

#### Example: Multiple Salesforce Destinations

```yaml
destinations_to_platform_instance:
  # Key is the Hightouch destination ID
  "dest_123":
    platform: "salesforce"
    platform_instance: "prod-salesforce"
    env: "PROD"

  "dest_456":
    platform: "salesforce"
    platform_instance: "sandbox-salesforce"
    env: "QA"
```

### Finding Source and Destination IDs

1. **In Hightouch UI**: Navigate to the source or destination, and check the URL

   - URL format: `https://app.hightouch.com/[workspace]/sources/[source_id]`
   - Example: `https://app.hightouch.com/my-workspace/sources/12345` → source_id is `12345`

2. **Via API**: Use the Hightouch API to list sources and destinations:
   ```bash
   curl -H "Authorization: Bearer YOUR_API_KEY" \
        https://api.hightouch.com/api/v1/sources
   ```

### Filtering Syncs and Models

Control which syncs and models are ingested using regex patterns:

```yaml
# Include only production syncs
sync_patterns:
  allow:
    - "prod-.*"
    - "production-.*"
  deny:
    - "test-.*"
    - ".*-staging"

# Include only specific models
model_patterns:
  allow:
    - "customer-.*"
    - "product-.*"
  deny:
    - ".*-draft"
```

### Controlling Sync Run History

Limit the number of historical sync runs ingested:

```yaml
include_sync_runs: true
max_sync_runs_per_sync: 10 # Last 10 runs per sync
```

Set `include_sync_runs: false` to skip sync run history entirely.

### Container Organization

The connector organizes Hightouch entities hierarchically using DataHub containers:

```yaml
extract_workspaces_to_containers: true # Enable container organization (default: true)
```

When enabled, the connector creates a hierarchy:

- **Workspaces** → Top-level containers for Hightouch workspaces (using workspace names when available from API, otherwise workspace IDs)
- **Folders** → Sub-containers for model folders (displayed as folder IDs since the Hightouch API does not provide folder names)
- **Models & Syncs** → Placed within their respective workspace/folder containers

This provides better organization in the DataHub UI, especially for large Hightouch deployments. Folders are typically organized by destination type (e.g., Salesforce, Mixpanel, Intercom).

### Sibling Relationships

For table-type models and simple raw_sql models, you can establish sibling relationships between the Hightouch model and its upstream source table:

```yaml
include_table_lineage_to_sibling: true # Create sibling relationships (default: false)
```

When enabled:

- The Hightouch model is marked as the **primary** sibling
- The upstream source table is linked as a **secondary** sibling
- This allows you to view both representations of the same data in DataHub
- Requires `sources_to_platform_instance` configuration for proper URN matching

**Note**: Sibling aspects are only emitted on the upstream table if it already exists in DataHub (has been ingested), preventing the creation of "ghost" entities.

### SQL Parsing & Lineage

The connector now includes SQL parsing capabilities for `raw_sql` models:

- Automatically extracts upstream table dependencies from SQL queries
- Creates column-level lineage for table-type models
- Normalizes column casing to match upstream table schemas (e.g., Snowflake lowercasing)
- Supports schema preloading from DataHub for accurate lineage resolution

For best results:

- Ensure upstream source tables are already ingested into DataHub
- Configure `sources_to_platform_instance` for accurate URN generation
- Enable a DataHub graph connection for schema resolution

## Current Limitations

1. **User Ownership**: Hightouch API does not currently expose creator/owner information for syncs, so ownership metadata is not extracted.

2. **API Rate Limits**: The Hightouch API has rate limits. The connector implements retry logic with exponential backoff, but very large workspaces with thousands of syncs may need to be ingested in batches.

3. **Supported Sources**: Platform detection works for major databases (Snowflake, BigQuery, Redshift, Postgres, etc.). Custom or less common sources may need manual platform mapping.

4. **Complex SQL Queries**: SQL parsing works best for simple queries. Complex multi-table JOINs, CTEs, and subqueries may not produce complete column-level lineage.

## Troubleshooting

### Authentication Errors

```
Error: 401 Unauthorized
```

**Solution**: Verify your API key is correct and has not expired. Generate a new key if needed.

### Missing Lineage

If lineage is not appearing correctly:

1. Verify source and destination IDs are configured in platform instance mappings
2. Check that `emit_models_as_datasets` is set to your preferred mode
3. Ensure source and destination datasets already exist in DataHub (or ingest them separately)

### Rate Limiting

```
Error: 429 Too Many Requests
```

**Solution**: The connector automatically retries with backoff. For large workspaces, consider:

- Using filtering patterns to reduce the number of syncs processed
- Reducing `max_sync_runs_per_sync` to limit API calls
