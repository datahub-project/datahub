### Concept Mapping

| Source Concept | DataHub Concept                                           | Notes |
| -------------- |-----------------------------------------------------------| ----- |
| `"grafana"` | [Data Platform](../../metamodel/entities/dataPlatform.md) | |
| Grafana Folder | [Container](../../metamodel/entities/container.md)        | Subtype `Folder` |
| Grafana Dashboard | [Container](../../metamodel/entities/container.md)        | Subtype `Dashboard` |
| Grafana Panel/Visualization | [Chart](../../metamodel/entities/chart.md)                | Various types mapped based on panel type (e.g., graph → LINE, pie → PIE) |
| Grafana Data Source | [Dataset](../../metamodel/entities/dataset.md)            | Created for each panel's data source |
| Dashboard Owner | [Corp User](../../metamodel/entities/corpuser.md)         | Derived from dashboard UID and creator |
| Dashboard Tags | [Tag](../../metamodel/entities/tag.md)                    | Supports both simple tags and key:value tags |

### Compatibility

The connector supports extracting metadata from any Grafana instance accessible via API. For SQL-based data sources, column-level lineage can be extracted when the queries are parseable. The connector supports various panel types and their transformations, and can work with both standalone Grafana instances and those integrated with other platforms.

For optimal lineage extraction from SQL-based data sources:
- Queries should be well-formed and complete
- Database/schema information should be properly configured in the connection settings
- The platform mapping should be configured to match your data sources

### Prerequisites:
1. A running Grafana instance
2. A service account token with permissions to:
   - Read dashboards and folders
   - Access data source configurations
   - View user information