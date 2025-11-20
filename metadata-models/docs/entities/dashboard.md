# Dashboard

Dashboards are collections of visualizations and charts that provide insights into data. In DataHub, dashboards represent collections of charts (visualizations) typically found in Business Intelligence (BI) platforms and data visualization tools. Dashboards are typically ingested from platforms like Looker, Tableau, PowerBI, Superset, Mode, Grafana, and other BI tools.

## Identity

Dashboards are identified by two pieces of information:

- **The platform that they belong to**: This is the specific BI tool or dashboarding platform that hosts the dashboard. Examples include `looker`, `tableau`, `powerbi`, `superset`, `mode`, etc. This corresponds to the `dashboardTool` field in the dashboard's key aspect.
- **The dashboard identifier in the specific platform**: Each platform has its own way of uniquely identifying dashboards within its system. For example, Looker uses identifiers like `dashboards.999999`, while PowerBI might use GUIDs, and Tableau uses site-relative paths.

An example of a dashboard identifier is `urn:li:dashboard:(looker,dashboards.999999)`.

For platforms with multiple instances (e.g., separate Looker deployments for different environments), the URN can include a platform instance identifier: `urn:li:dashboard:(looker,dashboards.999999,prod-instance)`.

## Important Capabilities

### Dashboard Information and Metadata

The core metadata about a dashboard is stored in the `dashboardInfo` aspect. This includes:

- **Title**: The display name of the dashboard (searchable, auto-complete enabled)
- **Description**: A detailed description of what the dashboard shows
- **Dashboard URL**: A link to view the dashboard in its native platform
- **Access Level**: The access level of the dashboard (PUBLIC or PRIVATE)
- **Last Modified**: Audit stamps tracking when the dashboard was created, modified, or deleted
- **Last Refreshed**: Timestamp of when the dashboard data was last refreshed, including who refreshed it
- **Custom Properties**: Platform-specific metadata as key-value pairs

The following code snippet shows you how to create a dashboard with basic information.

<details>
<summary>Python SDK: Create a dashboard</summary>

```python
{{ inline /metadata-ingestion/examples/library/dashboard_create_simple.py show_path_as_comment }}
```

</details>

For more complex dashboard creation with charts and datasets:

<details>
<summary>Python SDK: Create a dashboard with charts and datasets</summary>

```python
{{ inline /metadata-ingestion/examples/library/dashboard_create_complex.py show_path_as_comment }}
```

</details>

### Container Relationships - Charts in Dashboards

Dashboards act as containers for charts. The `dashboardInfo` aspect's `chartEdges` field tracks which charts belong to the dashboard, creating a hierarchical relationship:

```
Dashboard (Container)
├── Chart 1 (Bar Chart)
├── Chart 2 (Pie Chart)
└── Chart 3 (Line Chart)
```

Each chart edge includes:

- **Destination URN**: The URN of the chart
- **Created**: Audit stamps for when the chart was added
- **Last Modified**: Audit stamps for when the relationship was modified
- **Properties**: Optional custom properties for the relationship

This relationship enables:

- **Navigation**: Users can browse from dashboards to their constituent charts
- **Dependency Tracking**: Understanding which charts belong to which dashboards
- **Metadata Propagation**: Ownership and tags can be inherited from dashboard to charts
- **Impact Analysis**: When a dashboard is deprecated, identify all affected charts

<details>
<summary>Python SDK: Add charts to a dashboard</summary>

```python
{{ inline /metadata-ingestion/examples/library/dashboard_add_charts.py show_path_as_comment }}
```

</details>

### Data Lineage - Dataset Dependencies

Dashboards often consume data from one or more datasets. The `dashboardInfo` aspect's `datasetEdges` field tracks these relationships, creating `Consumes` relationships in the metadata graph. This enables:

- **Impact Analysis**: Understanding which dashboards are affected when a dataset changes
- **Data Lineage Visualization**: Showing the flow of data from source datasets through to dashboards
- **Dependency Tracking**: Identifying all upstream dependencies for a dashboard
- **Compliance**: Tracking which dashboards use sensitive or regulated data

<details>
<summary>Python SDK: Add dataset lineage to a dashboard</summary>

```python
{{ inline /metadata-ingestion/examples/library/dashboard_add_lineage.py show_path_as_comment }}
```

</details>

### Nested Dashboards

Some platforms support dashboards that contain other dashboards (e.g., PowerBI Apps that contain multiple dashboards). The `dashboardInfo` aspect's `dashboards` field tracks these nested relationships, enabling hierarchical dashboard organization:

```
PowerBI App (Dashboard)
├── Sales Dashboard (Dashboard)
│   ├── Chart 1
│   └── Chart 2
└── Marketing Dashboard (Dashboard)
    ├── Chart 3
    └── Chart 4
```

This is particularly useful for platforms like PowerBI where Apps act as top-level containers for multiple dashboards.

### Field-Level Lineage

The `inputFields` aspect (optional) provides fine-grained tracking of which specific dataset fields (columns) are referenced by the dashboard. Each input field creates a `consumesField` relationship to a `schemaField` entity, enabling:

- **Column-Level Impact Analysis**: Understanding which dashboards use a specific column
- **Data Sensitivity Tracking**: Identifying dashboards that display sensitive fields
- **Schema Change Impact**: Predicting the effect of schema changes on dashboards

### Editable Properties

DataHub separates metadata that comes from ingestion sources (in `dashboardInfo`) from metadata that users edit in the DataHub UI (in `editableDashboardProperties`). This separation ensures that:

- User edits are preserved across ingestion runs
- Source system metadata remains authoritative for its fields
- Users can enhance metadata without interfering with automated ingestion

The `editableDashboardProperties` aspect currently supports:

- **Description**: A user-provided description that supplements or overrides the ingested description

<details>
<summary>Python SDK: Update editable dashboard properties</summary>

```python
{{ inline /metadata-ingestion/examples/library/dashboard_update_editable.py show_path_as_comment }}
```

</details>

### Tags and Glossary Terms

Dashboards can have Tags or Terms attached to them. Read [this blog](https://medium.com/datahub-project/tags-and-terms-two-powerful-datahub-features-used-in-two-different-scenarios-b5b4791e892e) to understand the difference between tags and terms.

#### Adding Tags to a Dashboard

Tags are added to dashboards using the `globalTags` aspect.

<details>
<summary>Python SDK: Add a tag to a dashboard</summary>

```python
{{ inline /metadata-ingestion/examples/library/dashboard_add_tag.py show_path_as_comment }}
```

</details>

#### Adding Glossary Terms to a Dashboard

Glossary terms are added using the `glossaryTerms` aspect.

<details>
<summary>Python SDK: Add a glossary term to a dashboard</summary>

```python
{{ inline /metadata-ingestion/examples/library/dashboard_add_term.py show_path_as_comment }}
```

</details>

### Ownership

Ownership is associated with a dashboard using the `ownership` aspect. Owners can be of different types such as `DATAOWNER`, `TECHNICAL_OWNER`, `BUSINESS_OWNER`, etc. Ownership can be inherited from source systems or added in DataHub.

<details>
<summary>Python SDK: Add an owner to a dashboard</summary>

```python
{{ inline /metadata-ingestion/examples/library/dashboard_add_owner.py show_path_as_comment }}
```

</details>

### Usage Statistics

Dashboards can track usage metrics through the `dashboardUsageStatistics` aspect (experimental). This timeseries aspect captures:

- **Views Count**: Total number of times the dashboard has been viewed
- **Executions Count**: Number of times the dashboard has been refreshed or synced
- **Unique User Count**: Number of distinct users who viewed the dashboard
- **Per-User Counts**: Detailed usage breakdown by individual users
- **Favorites Count**: Number of users who favorited the dashboard
- **Last Viewed At**: Timestamp of the most recent view
- **Event Granularity**: Bucketing for statistics (daily, hourly)

Usage statistics help identify:

- Popular dashboards that might need performance optimization
- Unused dashboards that could be deprecated
- User engagement patterns
- Dashboard adoption metrics

<details>
<summary>Python SDK: Add usage statistics to a dashboard</summary>

```python
{{ inline /metadata-ingestion/examples/library/dashboard_usage.py show_path_as_comment }}
```

</details>

### Organizational Context

#### Domains

Dashboards can be organized into Domains (business areas or data products) using the `domains` aspect. This helps with:

- Organizing dashboards by business function
- Access control and governance
- Discovery by domain experts

#### Container Hierarchy

Dashboards can belong to higher-level containers (e.g., a Tableau project or PowerBI workspace). The `container` aspect tracks this relationship, creating a hierarchical structure:

```
Tableau Project (Container)
├── Sales Dashboard
├── Marketing Dashboard
└── Finance Dashboard
```

This hierarchy is important for:

- Navigating related dashboards
- Understanding organizational structure
- Propagating metadata (like ownership) from container to dashboards

### Embedding and External URLs

The `embed` aspect stores URLs that allow embedding the dashboard in external applications or viewing it in its native platform. The `dashboardUrl` field in `dashboardInfo` provides a direct link to the dashboard. This supports:

- Embedding dashboards in wikis or documentation
- Deep linking to the dashboard in the BI tool
- Integration with external portals

### Dashboard Subtypes

Dashboards from different platforms may have platform-specific subtypes defined in the `subTypes` aspect. Examples include:

- **PowerBI**: `PowerBI App` (a container for multiple PowerBI dashboards)
- **Looker**: Different Looker dashboard types
- **Tableau**: Workbook types

Subtypes help users understand the platform-specific nature of the dashboard.

## Integration with External Systems

### Ingestion from BI Platforms

Dashboards are typically ingested automatically from BI platforms using DataHub's ingestion connectors:

- **Looker**: Ingests dashboards and their elements (charts/looks)
- **Tableau**: Ingests dashboards (workbooks) and their sheets
- **PowerBI**: Ingests reports and apps (which can contain multiple dashboards)
- **Superset**: Ingests dashboards with their charts
- **Mode**: Ingests reports (dashboards) and queries
- **Grafana**: Ingests dashboards with their panels

Each connector maps platform-specific dashboard metadata to DataHub's standardized dashboard model, including:

- Dashboard title, description, and URL
- Container relationships (charts within dashboards)
- Dataset dependencies (lineage)
- Ownership information
- Custom platform-specific properties

### Querying Dashboard Information

You can retrieve dashboard information using DataHub's REST API:

<details>
<summary>Fetch dashboard entity snapshot</summary>

```bash
curl 'http://localhost:8080/entities/urn%3Ali%3Adashboard%3A(looker,dashboards.999999)'
```

The response includes all aspects of the dashboard, including:

- Dashboard information (title, description, URL)
- Charts contained in the dashboard
- Datasets consumed by the dashboard
- Ownership, tags, and terms
- Usage statistics
- And all other configured aspects

</details>

### Relationships API

You can query dashboard relationships to understand its connections to other entities:

<details>
<summary>Find charts contained in a dashboard</summary>

```bash
curl 'http://localhost:8080/relationships?direction=OUTGOING&urn=urn%3Ali%3Adashboard%3A(looker,dashboards.999999)&types=Contains'
```

This returns all charts that belong to this dashboard.

</details>

<details>
<summary>Find datasets consumed by a dashboard</summary>

```bash
curl 'http://localhost:8080/relationships?direction=OUTGOING&urn=urn%3Ali%3Adashboard%3A(looker,dashboards.999999)&types=Consumes'
```

This returns all datasets that this dashboard depends on.

</details>

<details>
<summary>Find which dashboards contain a specific chart</summary>

```bash
curl 'http://localhost:8080/relationships?direction=INCOMING&urn=urn%3Ali%3Achart%3A(looker,look%2F1234)&types=Contains'
```

This returns all dashboards that contain the specified chart.

</details>

## GraphQL API

Dashboards are fully supported in DataHub's GraphQL API, which provides:

- **Queries**: Search, browse, and retrieve dashboards
- **Mutations**: Create, update, and delete dashboards
- **Faceted Search**: Filter dashboards by tool, access level, and other attributes
- **Lineage Queries**: Traverse upstream (datasets) and downstream relationships
- **Batch Loading**: Efficiently load multiple dashboards
- **Usage Statistics**: Query dashboard usage metrics

The GraphQL `Dashboard` type includes all standard metadata fields plus dashboard-specific properties like charts, datasets, and usage statistics.

## Notable Exceptions

### Platform-Specific Variations

Different BI platforms have different concepts that map to dashboards:

- **Tableau**: A Tableau "workbook" maps to a DataHub dashboard entity. A workbook contains multiple "sheets" (charts). Tableau also has "dashboards" within workbooks that are collections of sheets - these are also modeled as dashboard entities in DataHub.

- **PowerBI**: PowerBI has "reports" (which map to dashboards) and "apps" (which are containers for multiple reports). Apps are also modeled as dashboards but with a subtype distinguishing them.

- **Looker**: Looker dashboards contain "elements" which can be charts, looks, or text elements. Each visual element becomes a chart in DataHub.

- **Superset**: Superset dashboards contain "slices" (charts). The relationship is straightforward dashboard-to-chart.

### Deprecated Fields

The `dashboardInfo.charts` and `dashboardInfo.datasets` fields are deprecated in favor of `dashboardInfo.chartEdges` and `dashboardInfo.datasetEdges`. The `Edges` versions provide richer relationship metadata including timestamps and actors for when relationships were created or modified.

### Container vs Parent

Dashboards serve dual roles:

- They are **containers** for charts (via `chartEdges`)
- They can have **parent containers** themselves (e.g., a Tableau project or PowerBI workspace)

This two-level hierarchy is important for understanding organizational structure.

## Related Entities

Dashboards frequently interact with these other DataHub entities:

- **Chart**: Dashboards contain charts (visual elements)
- **Dataset**: Dashboards consume data from datasets
- **SchemaField**: Dashboards may reference specific fields/columns through field-level lineage
- **DataPlatform**: Dashboards are associated with a specific BI platform
- **Container**: Dashboards can belong to higher-level containers (projects, workspaces)
- **Domain**: Dashboards can be organized into business domains
- **GlossaryTerm**: Dashboards can be annotated with business terms
- **Tag**: Dashboards can be tagged for classification and discovery
- **CorpUser / CorpGroup**: Dashboards have owners and are used by users
