### Concept Mapping

| Source Concept              | DataHub Concept                                           | Notes                                                                    |
| --------------------------- | --------------------------------------------------------- | ------------------------------------------------------------------------ |
| `"grafana"`                 | [Data Platform](../../metamodel/entities/dataPlatform.md) |                                                                          |
| Grafana Folder              | [Container](../../metamodel/entities/container.md)        | Subtype `Folder`                                                         |
| Grafana Dashboard           | [Container](../../metamodel/entities/container.md)        | Subtype `Dashboard`                                                      |
| Grafana Panel/Visualization | [Chart](../../metamodel/entities/chart.md)                | Various types mapped based on panel type (e.g., graph → LINE, pie → PIE) |
| Grafana Data Source         | [Dataset](../../metamodel/entities/dataset.md)            | Created for each panel's data source                                     |
| Dashboard Owner             | [Corp User](../../metamodel/entities/corpuser.md)         | Derived from dashboard UID and creator                                   |
| Dashboard Tags              | [Tag](../../metamodel/entities/tag.md)                    | Supports both simple tags and key:value tags                             |

### Compatibility

The connector supports extracting metadata from any Grafana instance accessible via API. For SQL-based data sources, column-level lineage can be extracted when the queries are parseable. The connector supports various panel types and their transformations, and can work with both standalone Grafana instances and those integrated with other platforms.

For optimal lineage extraction from SQL-based data sources:

- Database/schema information should be properly configured in the connection settings
- The platform mapping (`connection_to_platform_map`) should be configured to match your data sources

### Prerequisites:

The Grafana source supports two extraction modes based on your permission level:

#### Enhanced Mode (Default)

For full metadata extraction including lineage, containers, and detailed panel information:

1. A running Grafana instance
2. A service account token with **Admin permissions** to:
   - Read dashboards and folders
   - Access data source configurations
   - View user information
   - Access detailed dashboard metadata
   - Read panel configurations and transformations

#### Basic Mode (Limited Permissions)

For users with limited permissions who only need basic dashboard metadata:

1. A running Grafana instance
2. A service account token with **Viewer permissions** to:
   - Read dashboards (via `/api/search` endpoint)
   - Basic dashboard metadata access

To enable basic mode, set `basic_mode: true` in your configuration. This provides backwards compatibility with the original simple connector behavior.

**Note:** Basic mode extracts only dashboard entities without folder hierarchy, panel details, lineage information, or schema metadata. It's recommended to use enhanced mode when possible for complete metadata extraction.

#### Configuration Examples

Enhanced Mode (Default):

```yaml
source:
  type: grafana
  config:
    url: "https://grafana.company.com"
    service_account_token: "your_admin_token"
    # basic_mode: false  # Default - full extraction
```

Basic Mode (Limited Permissions):

```yaml
source:
  type: grafana
  config:
    url: "https://grafana.company.com"
    service_account_token: "your_viewer_token"
    basic_mode: true # Enable basic mode for limited permissions
```

#### Lineage Configuration

The Grafana source can extract lineage information between charts and their data sources. You can control lineage extraction using these configuration options:

```yaml
source:
  type: grafana
  config:
    url: "https://grafana.company.com"
    service_account_token: "your_token"

    # Lineage extraction (default: true)
    include_lineage: true

    # Column-level lineage from SQL queries (default: true)
    # Only applicable when include_lineage is true
    include_column_lineage: true

    # Platform mappings for lineage extraction
    connection_to_platform_map:
      postgres_datasource_uid:
        platform: postgres
        platform_instance: my_postgres
        env: PROD
        database: analytics
        database_schema: public
```

**Lineage Features:**

- **Dataset-level lineage**: Links charts to their underlying data sources
- **Column-level lineage**: Extracts field-to-field relationships from SQL queries
- **Platform mapping**: Maps Grafana data sources to their actual platforms for accurate lineage
- **SQL parsing**: Supports parsing of SQL queries for detailed lineage extraction

**Performance Note:** Lineage extraction can be disabled (`include_lineage: false`) to improve ingestion performance when lineage information is not needed.
