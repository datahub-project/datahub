### Overview

The `grafana` module ingests metadata from Grafana into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

#### Compatibility

Supports any Grafana instance accessible via API. Extracts column-level lineage from parseable SQL queries in data sources.

For optimal SQL lineage extraction:

- Configure database/schema information in data source connection settings
- Set `connection_to_platform_map` to match your data sources

#### Extracted Metadata Scope

The connector extracts metadata from Grafana APIs with support for:

- Folder and dashboard container hierarchy
- Panel and visualization entities (chart modeling)
- Data source references for dataset linking
- Dashboard ownership and tags
- Optional table/column lineage from parseable SQL-based panels

### Prerequisites

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
