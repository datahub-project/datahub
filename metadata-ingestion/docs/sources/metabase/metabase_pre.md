### Prerequisites

To use this connector, you'll need:

- Metabase version v0.41+ (Models require v0.41+)
- Authentication credentials (either username/password or API key - **API key is recommended**)
- Appropriate permissions to access the Metabase API

### Authentication

DataHub supports two authentication methods:

1. **API Key** (Recommended)
2. **Username/Password**

To create an API key in Metabase:

1. Navigate to your Metabase instance
2. Click on your profile in the top right
3. Go to "Account Settings"
4. Click on "API Keys" tab
5. Generate a new API key

The API key method is more secure and doesn't require managing passwords.

### Integration Details

This plugin extracts Charts, Dashboards, Models, and their associated metadata from Metabase. It supports comprehensive lineage extraction, including table-to-chart, table-to-dashboard, and nested query lineage.

#### Entities Extracted

The connector extracts the following Metabase entities into DataHub:

- **Dashboards** → DataHub Dashboards
- **Charts (Cards/Questions)** → DataHub Charts
- **Models** → DataHub Datasets (with "Model" and "View" subtypes)
- **Collections** → DataHub Tags

#### Dashboard Extraction

Dashboards are extracted using the [/api/dashboard](https://www.metabase.com/docs/latest/api/dashboard) endpoint.

**Extracted Information:**

- Title and description
- Owner and last modified user
- Link to the dashboard in Metabase
- Associated charts (via chartEdges)
- **Lineage to upstream database tables** (via datasetEdges)
- Collection tags for organization

**Dashboard Lineage:** The connector automatically aggregates table dependencies from all charts within a dashboard, creating direct table-to-dashboard lineage. This enables:

- Impact analysis: See which dashboards are affected when a table changes
- Data discovery: Find dashboards consuming specific datasets
- Dependency tracking: Understand the complete data flow

#### Chart Extraction

Charts (Metabase Cards/Questions) are extracted using the [/api/card](https://www.metabase.com/docs/latest/api-documentation.html#card) endpoint.

**Extracted Information:**

- Title and description
- Owner and last modified user
- Link to the chart in Metabase
- Chart type (bar, line, table, pie, etc.)
- Source dataset lineage
- SQL query (for native queries)
- Collection tags

**Custom Properties:**

| Property     | Description                                     |
| ------------ | ----------------------------------------------- |
| `Dimensions` | Column names used in the visualization          |
| `Filters`    | Any filters applied to the chart                |
| `Metrics`    | Columns used for aggregation (COUNT, SUM, etc.) |

#### Lineage Extraction

The connector provides comprehensive lineage extraction with support for multiple query types:

##### 1. Native SQL Query Lineage

- Parses SQL queries using DataHub's SQL parser
- Extracts table references from SELECT, JOIN, and subqueries
- Handles Metabase template variables (`{{variable}}`) and optional clauses (`[[WHERE ...]]`)
- Creates lineage from source tables to charts

##### 2. Query Builder Lineage

- Extracts source tables from Metabase's visual query builder
- Maps `source-table` IDs to actual database tables
- Creates lineage from tables to charts

##### 3. Nested Query Lineage

- Handles charts built on top of other charts (using `card__` references)
- Recursively resolves the chain to find ultimate source tables
- Includes protection against circular references (max depth: 5)
- Useful for multi-layered analysis workflows

##### 4. Dashboard-Level Lineage

- Aggregates lineage from all charts in a dashboard
- Creates direct table-to-dashboard edges (via `datasetEdges`)
- Automatically deduplicates tables referenced by multiple charts
- Enables dashboard-level impact analysis

##### 5. Model Lineage

- Extracts upstream table dependencies for Metabase Models
- Models appear as Dataset entities with lineage to their source tables
- Supports both SQL-based and query builder-based models

#### Collection Tags

Metabase Collections are mapped to DataHub tags for better organization and discoverability:

- Collection names are automatically converted to tags (e.g., "Sales Dashboard" → `metabase_collection_sales_dashboard`)
- Special characters are sanitized: `"Sales & Marketing"` → `metabase_collection_sales_marketing`, `"Q1/Q2 Reports"` → `metabase_collection_q1q2_reports`
- Only alphanumeric characters and underscores are kept in tag names
- Tags are applied to dashboards, charts, and models within that collection
- Enables filtering by collection in DataHub
- Helps organize assets by team, department, or project

**Configuration:** Set `extract_collections_as_tags: false` to disable this feature.

#### Metabase Models

Metabase Models (introduced in v0.41) are saved questions that can be used as data sources for other questions. The connector extracts Models as DataHub Dataset entities:

**Model as Datasets:**

- URN format: `urn:li:dataset:(urn:li:dataPlatform:metabase,model.{model_id},PROD)`
- SubTypes: `["Model", "View"]` to indicate virtual dataset nature
- ViewProperties: Preserves the underlying SQL query
- Upstream lineage: Connects models to their source tables
- Collection tags: Applied based on model's collection membership

**Why Extract Models:**

- **Lineage tracking**: See dependencies between models and source tables
- **Impact analysis**: Understand which models are affected by table changes
- **Documentation**: Model queries and descriptions are preserved
- **Discovery**: Find reusable data models in your organization

**Configuration:** Set `extract_models: false` to disable model extraction.

#### Collections

The [/api/collection](https://www.metabase.com/docs/latest/api/collection) endpoint is used to:

- Retrieve available collections
- List dashboards within each collection via [/api/collection/{id}/items?models=dashboard](https://www.metabase.com/docs/latest/api/collection#get-apicollectioniditems)
- Map collections to tags on assets

### Concept Mapping

This ingestion source maps the following Source System Concepts to DataHub Concepts:

| Source Concept | DataHub Concept                                               | Notes                        |
| -------------- | ------------------------------------------------------------- | ---------------------------- |
| `"Metabase"`   | [Data Platform](../../metamodel/entities/dataPlatform.md)     |                              |
| Dashboard      | [Dashboard](../../metamodel/entities/dashboard.md)            |                              |
| Card/Question  | [Chart](../../metamodel/entities/chart.md)                    |                              |
| Model          | [Dataset](../../metamodel/entities/dataset.md)                | SubTypes `["Model", "View"]` |
| Collection     | [Tag](../../metamodel/entities/tag.md)                        | Optionally Extracted         |
| Database Table | [Dataset](../../metamodel/entities/dataset.md)                | From connected database      |
| User           | [User (a.k.a CorpUser)](../../metamodel/entities/corpuser.md) | Ownership information        |

### Configuration Examples

#### Enable all features (default)

```yaml
source:
  type: metabase
  config:
    connect_uri: https://metabase.company.com
    api_key: your-api-key # Recommended over username/password
    extract_collections_as_tags: true # Map collections to tags
    extract_models: true # Extract Metabase Models as datasets
```

#### Using username/password authentication

```yaml
source:
  type: metabase
  config:
    connect_uri: https://metabase.company.com
    username: datahub_user
    password: secure_password
    extract_collections_as_tags: true
    extract_models: true
```

#### Disable optional features

```yaml
source:
  type: metabase
  config:
    connect_uri: https://metabase.company.com
    api_key: your-api-key
    extract_collections_as_tags: false # Don't create tags
    extract_models: false # Don't extract models
```

#### Exclude other user collections

```yaml
source:
  type: metabase
  config:
    connect_uri: https://metabase.company.com
    api_key: your-api-key
    exclude_other_user_collections: true # Only your collections
```

### Advanced Configuration

#### Custom Platform Mapping

Metabase databases will be mapped to a DataHub platform based on the engine listed in the [api/database](https://www.metabase.com/docs/latest/api-documentation.html#database) response. This mapping can be customized by using the `engine_platform_map` config option.

For example, to map databases using the `athena` engine to the underlying datasets in the `glue` platform:

```yaml
source:
  type: metabase
  config:
    connect_uri: https://metabase.company.com
    api_key: your-api-key
    engine_platform_map:
      athena: glue
```

#### Database Name Override

DataHub will try to determine database name from Metabase [api/database](https://www.metabase.com/docs/latest/api-documentation.html#database) payload. However, the name can be overridden from `database_alias_map` for a given database connected to Metabase:

```yaml
source:
  type: metabase
  config:
    connect_uri: https://metabase.company.com
    api_key: your-api-key
    database_alias_map:
      postgres: my_custom_db_name
```

#### Platform Instance Mapping

If several platform instances with the same platform (e.g., from several distinct ClickHouse clusters) are present in DataHub, the mapping between database id in Metabase and platform instance in DataHub may be configured:

```yaml
source:
  type: metabase
  config:
    connect_uri: https://metabase.company.com
    api_key: your-api-key
    database_id_to_instance_map:
      "42": platform_instance_in_datahub
```

**Note:** The key in this map must be a string, not integer, although Metabase API provides `id` as a number.

If `database_id_to_instance_map` is not specified, `platform_instance_map` is used for platform instance mapping:

```yaml
source:
  type: metabase
  config:
    connect_uri: https://metabase.company.com
    api_key: your-api-key
    platform_instance_map:
      clickhouse: my_clickhouse_instance
```

### Compatibility

- Tested with Metabase v0.48.3
- Works with various database backends: PostgreSQL, MySQL, BigQuery, Snowflake, Redshift, and more
- Supports both username/password and API key authentication (API key recommended)
