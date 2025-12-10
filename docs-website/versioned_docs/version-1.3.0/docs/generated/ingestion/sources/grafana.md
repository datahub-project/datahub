---
sidebar_position: 28
title: Grafana
slug: /generated/ingestion/sources/grafana
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/ingestion/sources/grafana.md
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Grafana
![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Column-level Lineage | ✅ | Enabled by default. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default. |
| Extract Ownership | ✅ | Enabled by default. |
| Extract Tags | ✅ | Enabled by default. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Table-Level Lineage | ✅ | Enabled by default. |


This plugin extracts metadata from Grafana and ingests it into DataHub. It connects to Grafana's API
to extract metadata about dashboards, charts, and data sources. The following types of metadata are extracted:

- Container Entities:
    - Folders: Top-level organizational units in Grafana
    - Dashboards: Collections of panels and charts
    - The full container hierarchy is preserved (Folders -> Dashboards -> Charts/Datasets)

- Charts and Visualizations:
    - All panel types (graphs, tables, stat panels, etc.)
    - Chart configuration and properties
    - Links to the original Grafana UI
    - Custom properties including panel types and data source information
    - Input fields and schema information when available

- Data Sources and Datasets:
    - Physical datasets representing Grafana's data sources
    - Dataset schema information extracted from queries and panel configurations
    - Support for various data source types (SQL, Prometheus, etc.)
    - Custom properties including data source type and configuration

- Lineage Information:
    - Dataset-level lineage showing relationships between:
        - Source data systems and Grafana datasets
        - Grafana datasets and charts
    - Column-level lineage for SQL-based data sources
    - Support for external source systems through configurable platform mappings

- Tags and Ownership:
    - Dashboard and chart tags
    - Ownership information derived from:
        - Dashboard creators
        - Technical owners based on dashboard UIDs
        - Custom ownership assignments

The source supports the following capabilities:
- Platform instance support for multi-Grafana deployments
- Stateful ingestion with support for soft-deletes
- Fine-grained lineage at both dataset and column levels
- Automated tag extraction
- Support for both HTTP and HTTPS connections with optional SSL verification

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

### CLI based Ingestion

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: grafana
  config:
    # Coordinates
    platform_instance: production # optional
    env: PROD # optional
    url: https://grafana.company.com
    service_account_token: ${GRAFANA_SERVICE_ACCOUNT_TOKEN}

    # SSL verification for HTTPS connections
    verify_ssl: true # optional, default is true

    # Source type mapping for lineage
    connection_to_platform_map:
      postgres:
        platform: postgres
        database: grafana # optional
        database_schema: grafana # optional
        platform_instance: database_2 # optional
        env: PROD # optional
      mysql_uid_1: # Grafana datasource UID
        platform: mysql
        platform_instance: database_1 # optional
        database: my_database # optional
sink:
  # sink configs

```

### Config Details
<Tabs>
                <TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">service_account_token</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string(password)</span></div> | Service account token for Grafana  |
| <div className="path-line"><span className="path-main">url</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Grafana URL in the format http://your-grafana-instance with no trailing slash  |
| <div className="path-line"><span className="path-main">basic_mode</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Enable basic extraction mode for users with limited permissions. In basic mode, only dashboard metadata is extracted without detailed panel information, lineage, or folder hierarchy. This requires only basic dashboard read permissions. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_column_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to extract column-level lineage from SQL queries. Only applicable when include_lineage is enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to extract lineage between charts and data sources. When enabled, the source will parse SQL queries and datasource configurations to build lineage relationships. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">ingest_owners</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to ingest dashboard ownership information <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">ingest_tags</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to ingest dashboard and chart tags <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">page_size</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of items to fetch per API call when paginating through folders and dashboards <div className="default-line default-line-with-docs">Default: <span className="default-value">100</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">platform_instance_map</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | A holder for platform -> platform_instance mappings to generate correct dataset urns <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">verify_ssl</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to verify SSL certificates when connecting to Grafana <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">connection_to_platform_map</span></div> <div className="type-name-line"><span className="type-name">map(str,PlatformConnectionConfig)</span></div> | Platform connection configuration for mapping Grafana datasources to their actual platforms.  |
| <div className="path-line"><span className="path-prefix">connection_to_platform_map.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">connection_to_platform_map.`key`.</span><span className="path-main">platform</span>&nbsp;<abbr title="Required if connection_to_platform_map is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | The platform name (e.g., 'postgres', 'mysql', 'snowflake')  |
| <div className="path-line"><span className="path-prefix">connection_to_platform_map.`key`.</span><span className="path-main">database</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Default database name <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">connection_to_platform_map.`key`.</span><span className="path-main">database_schema</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Default schema name <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">connection_to_platform_map.`key`.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">dashboard_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">dashboard_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">folder_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">folder_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulIngestionConfig, null</span></div> | Stateful Ingestion Config <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |

</div>


</TabItem>
<TabItem value="schema" label="Schema">

The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.


```javascript
{
  "$defs": {
    "AllowDenyPattern": {
      "additionalProperties": false,
      "description": "A class to store allow deny regexes",
      "properties": {
        "allow": {
          "default": [
            ".*"
          ],
          "description": "List of regex patterns to include in ingestion",
          "items": {
            "type": "string"
          },
          "title": "Allow",
          "type": "array"
        },
        "deny": {
          "default": [],
          "description": "List of regex patterns to exclude from ingestion.",
          "items": {
            "type": "string"
          },
          "title": "Deny",
          "type": "array"
        },
        "ignoreCase": {
          "anyOf": [
            {
              "type": "boolean"
            },
            {
              "type": "null"
            }
          ],
          "default": true,
          "description": "Whether to ignore case sensitivity during pattern matching.",
          "title": "Ignorecase"
        }
      },
      "title": "AllowDenyPattern",
      "type": "object"
    },
    "PlatformConnectionConfig": {
      "additionalProperties": false,
      "description": "Platform connection configuration for mapping Grafana datasources to their actual platforms.",
      "properties": {
        "platform_instance": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details.",
          "title": "Platform Instance"
        },
        "env": {
          "default": "PROD",
          "description": "The environment that all assets produced by this connector belong to",
          "title": "Env",
          "type": "string"
        },
        "platform": {
          "description": "The platform name (e.g., 'postgres', 'mysql', 'snowflake')",
          "title": "Platform",
          "type": "string"
        },
        "database": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Default database name",
          "title": "Database"
        },
        "database_schema": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Default schema name",
          "title": "Database Schema"
        }
      },
      "required": [
        "platform"
      ],
      "title": "PlatformConnectionConfig",
      "type": "object"
    },
    "StatefulIngestionConfig": {
      "additionalProperties": false,
      "description": "Basic Stateful Ingestion Specific Configuration for any source.",
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False",
          "title": "Enabled",
          "type": "boolean"
        }
      },
      "title": "StatefulIngestionConfig",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "description": "Configuration for Grafana source",
  "properties": {
    "env": {
      "default": "PROD",
      "description": "The environment that all assets produced by this connector belong to",
      "title": "Env",
      "type": "string"
    },
    "platform_instance": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details.",
      "title": "Platform Instance"
    },
    "stateful_ingestion": {
      "anyOf": [
        {
          "$ref": "#/$defs/StatefulIngestionConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Stateful Ingestion Config"
    },
    "platform_instance_map": {
      "anyOf": [
        {
          "additionalProperties": {
            "type": "string"
          },
          "type": "object"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "A holder for platform -> platform_instance mappings to generate correct dataset urns",
      "title": "Platform Instance Map"
    },
    "url": {
      "description": "Grafana URL in the format http://your-grafana-instance with no trailing slash",
      "title": "Url",
      "type": "string"
    },
    "service_account_token": {
      "description": "Service account token for Grafana",
      "format": "password",
      "title": "Service Account Token",
      "type": "string",
      "writeOnly": true
    },
    "verify_ssl": {
      "default": true,
      "description": "Whether to verify SSL certificates when connecting to Grafana",
      "title": "Verify Ssl",
      "type": "boolean"
    },
    "page_size": {
      "default": 100,
      "description": "Number of items to fetch per API call when paginating through folders and dashboards",
      "title": "Page Size",
      "type": "integer"
    },
    "basic_mode": {
      "default": false,
      "description": "Enable basic extraction mode for users with limited permissions. In basic mode, only dashboard metadata is extracted without detailed panel information, lineage, or folder hierarchy. This requires only basic dashboard read permissions.",
      "title": "Basic Mode",
      "type": "boolean"
    },
    "dashboard_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex pattern to filter dashboards for ingestion"
    },
    "folder_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex pattern to filter folders for ingestion"
    },
    "ingest_tags": {
      "default": true,
      "description": "Whether to ingest dashboard and chart tags",
      "title": "Ingest Tags",
      "type": "boolean"
    },
    "ingest_owners": {
      "default": true,
      "description": "Whether to ingest dashboard ownership information",
      "title": "Ingest Owners",
      "type": "boolean"
    },
    "include_lineage": {
      "default": true,
      "description": "Whether to extract lineage between charts and data sources. When enabled, the source will parse SQL queries and datasource configurations to build lineage relationships.",
      "title": "Include Lineage",
      "type": "boolean"
    },
    "include_column_lineage": {
      "default": true,
      "description": "Whether to extract column-level lineage from SQL queries. Only applicable when include_lineage is enabled.",
      "title": "Include Column Lineage",
      "type": "boolean"
    },
    "connection_to_platform_map": {
      "additionalProperties": {
        "$ref": "#/$defs/PlatformConnectionConfig"
      },
      "description": "Map of Grafana datasource types/UIDs to platform connection configs for lineage extraction",
      "title": "Connection To Platform Map",
      "type": "object"
    }
  },
  "required": [
    "url",
    "service_account_token"
  ],
  "title": "GrafanaSourceConfig",
  "type": "object"
}
```


</TabItem>
</Tabs>


### Code Coordinates
- Class Name: `datahub.ingestion.source.grafana.grafana_source.GrafanaSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/grafana/grafana_source.py)


<h2>Questions</h2>

If you've got any questions on configuring ingestion for Grafana, feel free to ping us on [our Slack](https://datahub.com/slack).
