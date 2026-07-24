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


**Python SDK: Create a dashboard**

```python
# Inlined from /metadata-ingestion/examples/library/dashboard_create_simple.py
from datahub.metadata.urns import TagUrn
from datahub.sdk import Dashboard, DataHubClient

client = DataHubClient.from_env()

dashboard = Dashboard(
    name="example_dashboard",
    platform="looker",
    description="looker dashboard for production",
    tags=[TagUrn(name="production"), TagUrn(name="data_engineering")],
)

client.entities.upsert(dashboard)

```



For more complex dashboard creation with charts and datasets:


**Python SDK: Create a dashboard with charts and datasets**

```python
# Inlined from /metadata-ingestion/examples/library/dashboard_create_complex.py
from datahub.metadata.urns import TagUrn
from datahub.sdk import Chart, Dashboard, DataHubClient, Dataset

client = DataHubClient.from_env()
dashboard1 = Dashboard(
    name="example_dashboard_2",
    platform="looker",
    description="looker dashboard for production",
)
chart = Chart(
    name="example_chart",
    platform="looker",
    description="looker chart for production",
)

input_dataset = Dataset(
    name="example_dataset5",
    platform="snowflake",
    description="snowflake dataset for production",
)


dashboard2 = Dashboard(
    name="example_dashboard",
    platform="looker",
    description="looker dashboard for production",
    tags=[TagUrn(name="production"), TagUrn(name="data_engineering")],
    input_datasets=[input_dataset.urn],
    charts=[chart.urn],
    dashboards=[dashboard1.urn],
)


client.entities.upsert(dashboard1)
client.entities.upsert(chart)
client.entities.upsert(input_dataset)

client.entities.upsert(dashboard2)

```



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


**Python SDK: Add charts to a dashboard**

```python
# Inlined from /metadata-ingestion/examples/library/dashboard_add_charts.py
from datahub.sdk import Chart, Dashboard, DataHubClient

client = DataHubClient.from_env()

# Create charts that belong to the dashboard
chart1 = Chart(platform="looker", name="sales_by_region_chart")
chart2 = Chart(platform="looker", name="revenue_trend_chart")
chart3 = Chart(platform="looker", name="customer_count_chart")

# Create dashboard with charts
dashboard = Dashboard(
    platform="looker",
    name="sales_dashboard",
    display_name="Sales Overview Dashboard",
    description="Comprehensive sales analytics dashboard",
)

# Add charts to the dashboard
dashboard.add_chart(chart1)
dashboard.add_chart(chart2)
dashboard.add_chart(chart3)

# Upsert the dashboard (this will also create the chart relationships)
client.entities.upsert(dashboard)

```



### Data Lineage - Dataset Dependencies

Dashboards often consume data from one or more datasets. The `dashboardInfo` aspect's `datasetEdges` field tracks these relationships, creating `Consumes` relationships in the metadata graph. This enables:

- **Impact Analysis**: Understanding which dashboards are affected when a dataset changes
- **Data Lineage Visualization**: Showing the flow of data from source datasets through to dashboards
- **Dependency Tracking**: Identifying all upstream dependencies for a dashboard
- **Compliance**: Tracking which dashboards use sensitive or regulated data


**Python SDK: Add dataset lineage to a dashboard**

```python
# Inlined from /metadata-ingestion/examples/library/dashboard_add_lineage.py
from datahub.sdk import Dashboard, DataHubClient, Dataset

client = DataHubClient.from_env()

# Define the source datasets
sales_dataset = Dataset(platform="bigquery", name="project.dataset.sales")
customer_dataset = Dataset(platform="bigquery", name="project.dataset.customers")
products_dataset = Dataset(platform="bigquery", name="project.dataset.products")

# Create dashboard with lineage to upstream datasets
dashboard = Dashboard(
    platform="looker",
    name="sales_dashboard",
    display_name="Sales Overview Dashboard",
    description="Dashboard showing sales metrics across regions and products",
)

# Add dataset dependencies
dashboard.add_input_dataset(sales_dataset)
dashboard.add_input_dataset(customer_dataset)
dashboard.add_input_dataset(products_dataset)

# Upsert the dashboard (this will create the Consumes relationships)
client.entities.upsert(dashboard)

```



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


**Python SDK: Update editable dashboard properties**

```python
# Inlined from /metadata-ingestion/examples/library/dashboard_update_editable.py
import datahub.metadata.schema_classes as models
from datahub.sdk import DashboardUrn, DataHubClient

client = DataHubClient.from_env()

dashboard_urn = DashboardUrn("looker", "dashboards.999999")

dashboard = client.entities.get(dashboard_urn)

current_editable = dashboard._get_aspect(models.EditableDashboardPropertiesClass)

if current_editable:
    current_editable.description = "Updated description added via DataHub UI"
else:
    current_editable = models.EditableDashboardPropertiesClass(
        description="Updated description added via DataHub UI"
    )
    dashboard._set_aspect(current_editable)

client.entities.update(dashboard)
print(f"Updated editable properties for dashboard {dashboard_urn}")

```



### Tags and Glossary Terms

Dashboards can have Tags or Terms attached to them. Read [this blog](https://medium.com/datahub-project/tags-and-terms-two-powerful-datahub-features-used-in-two-different-scenarios-b5b4791e892e) to understand the difference between tags and terms.

#### Adding Tags to a Dashboard

Tags are added to dashboards using the `globalTags` aspect.


**Python SDK: Add a tag to a dashboard**

```python
# Inlined from /metadata-ingestion/examples/library/dashboard_add_tag.py
from datahub.sdk import DashboardUrn, DataHubClient, TagUrn

client = DataHubClient.from_env()

dashboard = client.entities.get(DashboardUrn("looker", "dashboards.999999"))

dashboard.add_tag(TagUrn("Production"))

client.entities.update(dashboard)

print(f"Added tag {TagUrn('Production')} to dashboard {dashboard.urn}")

```



#### Adding Glossary Terms to a Dashboard

Glossary terms are added using the `glossaryTerms` aspect.


**Python SDK: Add a glossary term to a dashboard**

```python
# Inlined from /metadata-ingestion/examples/library/dashboard_add_term.py
from datahub.metadata.urns import GlossaryTermUrn
from datahub.sdk import DashboardUrn, DataHubClient

client = DataHubClient.from_env()

dashboard = client.entities.get(DashboardUrn("looker", "dashboards.999999"))

dashboard.add_term(GlossaryTermUrn("SalesMetrics"))

client.entities.update(dashboard)

print(f"Added term {GlossaryTermUrn('SalesMetrics')} to dashboard {dashboard.urn}")

```



### Ownership

Ownership is associated with a dashboard using the `ownership` aspect. Owners can be of different types such as `DATAOWNER`, `TECHNICAL_OWNER`, `BUSINESS_OWNER`, etc. Ownership can be inherited from source systems or added in DataHub.


**Python SDK: Add an owner to a dashboard**

```python
# Inlined from /metadata-ingestion/examples/library/dashboard_add_owner.py
from datahub.sdk import CorpUserUrn, DashboardUrn, DataHubClient

client = DataHubClient.from_env()

dashboard = client.entities.get(DashboardUrn("looker", "dashboards.999999"))

dashboard.add_owner(CorpUserUrn("jdoe"))

client.entities.update(dashboard)

print(f"Added owner {CorpUserUrn('jdoe')} to dashboard {dashboard.urn}")

```



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


**Python SDK: Add usage statistics to a dashboard**

```python
# Inlined from /metadata-ingestion/examples/library/dashboard_usage.py
from datetime import datetime
from typing import List

import datahub.metadata.schema_classes as models
from datahub.metadata.urns import CorpUserUrn
from datahub.sdk import DataHubClient
from datahub.sdk.dashboard import Dashboard

client = DataHubClient.from_env()

# Day 1: User activity and absolute usage statistics
usage_day_1_user_counts: List[models.DashboardUserUsageCountsClass] = [
    models.DashboardUserUsageCountsClass(
        user=str(CorpUserUrn("user1")),
        executionsCount=3,
        usageCount=3,
    ),
    models.DashboardUserUsageCountsClass(
        user=str(CorpUserUrn("user2")),
        executionsCount=2,
        usageCount=2,
    ),
]

usage_day_1_stats = models.DashboardUsageStatisticsClass(
    timestampMillis=round(
        datetime.strptime("2022-02-09", "%Y-%m-%d").timestamp() * 1000
    ),
    eventGranularity=models.TimeWindowSizeClass(unit=models.CalendarIntervalClass.DAY),
    uniqueUserCount=2,
    executionsCount=5,
    userCounts=usage_day_1_user_counts,
)

absolute_usage_day_1_stats = models.DashboardUsageStatisticsClass(
    timestampMillis=round(
        datetime.strptime("2022-02-09", "%Y-%m-%d").timestamp() * 1000
    ),
    favoritesCount=100,
    viewsCount=25,
    lastViewedAt=round(
        datetime.strptime("2022-02-09 04:45:30", "%Y-%m-%d %H:%M:%S").timestamp() * 1000
    ),
)

dashboard = Dashboard(
    platform="looker",
    name="dashboards.999999",
    extra_aspects=[usage_day_1_stats, absolute_usage_day_1_stats],
)

client.entities.update(dashboard)

# Day 2: Updated user activity and absolute usage statistics
usage_day_2_user_counts: List[models.DashboardUserUsageCountsClass] = [
    models.DashboardUserUsageCountsClass(
        user=str(CorpUserUrn("user1")),
        executionsCount=4,
        usageCount=4,
    ),
    models.DashboardUserUsageCountsClass(
        user=str(CorpUserUrn("user2")),
        executionsCount=6,
        usageCount=6,
    ),
]

usage_day_2_stats = models.DashboardUsageStatisticsClass(
    timestampMillis=round(
        datetime.strptime("2022-02-10", "%Y-%m-%d").timestamp() * 1000
    ),
    eventGranularity=models.TimeWindowSizeClass(unit=models.CalendarIntervalClass.DAY),
    uniqueUserCount=2,
    executionsCount=10,
    userCounts=usage_day_2_user_counts,
)

absolute_usage_day_2_stats = models.DashboardUsageStatisticsClass(
    timestampMillis=round(
        datetime.strptime("2022-02-10", "%Y-%m-%d").timestamp() * 1000
    ),
    favoritesCount=100,
    viewsCount=27,
    lastViewedAt=round(
        datetime.strptime("2022-02-10 10:45:30", "%Y-%m-%d %H:%M:%S").timestamp() * 1000
    ),
)

dashboard = Dashboard(
    platform="looker",
    name="dashboards.999999",
    extra_aspects=[usage_day_2_stats, absolute_usage_day_2_stats],
)

client.entities.update(dashboard)

# Day 3: Single user activity and absolute usage statistics
usage_day_3_user_counts: List[models.DashboardUserUsageCountsClass] = [
    models.DashboardUserUsageCountsClass(
        user=str(CorpUserUrn("user1")),
        executionsCount=2,
        usageCount=2,
    ),
]

usage_day_3_stats = models.DashboardUsageStatisticsClass(
    timestampMillis=round(
        datetime.strptime("2022-02-11", "%Y-%m-%d").timestamp() * 1000
    ),
    eventGranularity=models.TimeWindowSizeClass(unit=models.CalendarIntervalClass.DAY),
    uniqueUserCount=1,
    executionsCount=2,
    userCounts=usage_day_3_user_counts,
)

absolute_usage_day_3_stats = models.DashboardUsageStatisticsClass(
    timestampMillis=round(
        datetime.strptime("2022-02-11", "%Y-%m-%d").timestamp() * 1000
    ),
    favoritesCount=102,
    viewsCount=30,
    lastViewedAt=round(
        datetime.strptime("2022-02-11 02:45:30", "%Y-%m-%d %H:%M:%S").timestamp() * 1000
    ),
)

dashboard = Dashboard(
    platform="looker",
    name="dashboards.999999",
    extra_aspects=[usage_day_3_stats, absolute_usage_day_3_stats],
)

client.entities.update(dashboard)

```



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


**Fetch dashboard entity snapshot**

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



### Relationships API

You can query dashboard relationships to understand its connections to other entities:


**Find charts contained in a dashboard**

```bash
curl 'http://localhost:8080/relationships?direction=OUTGOING&urn=urn%3Ali%3Adashboard%3A(looker,dashboards.999999)&types=Contains'
```

This returns all charts that belong to this dashboard.




**Find datasets consumed by a dashboard**

```bash
curl 'http://localhost:8080/relationships?direction=OUTGOING&urn=urn%3Ali%3Adashboard%3A(looker,dashboards.999999)&types=Consumes'
```

This returns all datasets that this dashboard depends on.




**Find which dashboards contain a specific chart**

```bash
curl 'http://localhost:8080/relationships?direction=INCOMING&urn=urn%3Ali%3Achart%3A(looker,look%2F1234)&types=Contains'
```

This returns all dashboards that contain the specified chart.



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



## Technical Reference Guide

The sections above provide an overview of how to use this entity. The following sections provide detailed technical information about how metadata is stored and represented in DataHub.

**Aspects** are the individual pieces of metadata that can be attached to an entity. Each aspect contains specific information (like ownership, tags, or properties) and is stored as a separate record, allowing for flexible and incremental metadata updates.

**Relationships** show how this entity connects to other entities in the metadata graph. These connections are derived from the fields within each aspect and form the foundation of DataHub's knowledge graph.

### Reading the Field Tables

Each aspect's field table includes an **Annotations** column that provides additional metadata about how fields are used:

- **⚠️ Deprecated**: This field is deprecated and may be removed in a future version. Check the description for the recommended alternative
- **Searchable**: This field is indexed and can be searched in DataHub's search interface
- **Searchable (fieldname)**: When the field name in parentheses is shown, it indicates the field is indexed under a different name in the search index. For example, `dashboardTool` is indexed as `tool`
- **→ RelationshipName**: This field creates a relationship to another entity. The arrow indicates this field contains a reference (URN) to another entity, and the name indicates the type of relationship (e.g., `→ Contains`, `→ OwnedBy`)

Fields with complex types (like `Edge`, `AuditStamp`) link to their definitions in the [Common Types](#common-types) section below.

### Aspects

#### dashboardKey
Key for a Dashboard



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| dashboardTool | string | ✓ | The name of the dashboard tool such as looker, redash etc. | Searchable (tool) |
| dashboardId | string | ✓ | Unique id for the dashboard. This id should be globally unique for a dashboarding tool even when ... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dashboardKey"
  },
  "name": "DashboardKey",
  "namespace": "com.linkedin.metadata.key",
  "fields": [
    {
      "Searchable": {
        "boostScore": 4.0,
        "fieldName": "tool",
        "fieldType": "TEXT_PARTIAL"
      },
      "type": "string",
      "name": "dashboardTool",
      "doc": "The name of the dashboard tool such as looker, redash etc."
    },
    {
      "type": "string",
      "name": "dashboardId",
      "doc": "Unique id for the dashboard. This id should be globally unique for a dashboarding tool even when there are multiple deployments of it. As an example, dashboard URL could be used here for Looker such as 'looker.linkedin.com/dashboards/1234'"
    }
  ],
  "doc": "Key for a Dashboard"
}
```





#### dashboardInfo
Information about a dashboard



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| customProperties | map | ✓ | Custom property bag. | Searchable |
| externalUrl | string |  | URL where the reference exist | Searchable |
| title | string | ✓ | Title of the dashboard | Searchable |
| description | string | ✓ | Detailed description about the dashboard | Searchable |
| charts | string[] | ✓ | Charts in a dashboard Deprecated! Use chartEdges instead. | ⚠️ Deprecated, → Contains |
| chartEdges | [Edge](#edge)[] |  | Charts in a dashboard | → Contains |
| datasets | string[] | ✓ | Datasets consumed by a dashboard Deprecated! Use datasetEdges instead. | ⚠️ Deprecated, → Consumes |
| datasetEdges | [Edge](#edge)[] |  | Datasets consumed by a dashboard | → Consumes |
| dashboards | [Edge](#edge)[] | ✓ | Dashboards included by this dashboard. Some dashboard entities (e.g. PowerBI Apps) can contain ot... | → DashboardContainsDashboard |
| lastModified | [ChangeAuditStamps](#changeauditstamps) | ✓ | Captures information about who created/last modified/deleted this dashboard and when | Searchable |
| dashboardUrl | string |  | URL for the dashboard. This could be used as an external link on DataHub to allow users access/vi... | Searchable |
| access | AccessLevel |  | Access level for the dashboard | Searchable |
| lastRefreshed | long |  | The time when this dashboard last refreshed |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dashboardInfo"
  },
  "name": "DashboardInfo",
  "namespace": "com.linkedin.dashboard",
  "fields": [
    {
      "Searchable": {
        "/*": {
          "fieldType": "TEXT",
          "queryByDefault": true
        }
      },
      "type": {
        "type": "map",
        "values": "string"
      },
      "name": "customProperties",
      "default": {},
      "doc": "Custom property bag."
    },
    {
      "Searchable": {
        "fieldType": "KEYWORD"
      },
      "java": {
        "class": "com.linkedin.common.url.Url",
        "coercerClass": "com.linkedin.common.url.UrlCoercer"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "externalUrl",
      "default": null,
      "doc": "URL where the reference exist"
    },
    {
      "Searchable": {
        "boostScore": 10.0,
        "enableAutocomplete": true,
        "fieldNameAliases": [
          "_entityName"
        ],
        "fieldType": "WORD_GRAM",
        "searchLabel": "entityName",
        "searchTier": 1
      },
      "type": "string",
      "name": "title",
      "doc": "Title of the dashboard"
    },
    {
      "Searchable": {
        "fieldType": "TEXT",
        "hasValuesFieldName": "hasDescription",
        "sanitizeRichText": true,
        "searchTier": 2
      },
      "type": "string",
      "name": "description",
      "doc": "Detailed description about the dashboard"
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "chart"
          ],
          "isLineage": true,
          "name": "Contains"
        }
      },
      "deprecated": true,
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "charts",
      "default": [],
      "doc": "Charts in a dashboard\nDeprecated! Use chartEdges instead."
    },
    {
      "Relationship": {
        "/*/destinationUrn": {
          "createdActor": "chartEdges/*/created/actor",
          "createdOn": "chartEdges/*/created/time",
          "entityTypes": [
            "chart"
          ],
          "isLineage": true,
          "name": "Contains",
          "properties": "chartEdges/*/properties",
          "updatedActor": "chartEdges/*/lastModified/actor",
          "updatedOn": "chartEdges/*/lastModified/time"
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "Edge",
            "namespace": "com.linkedin.common",
            "fields": [
              {
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": [
                  "null",
                  "string"
                ],
                "name": "sourceUrn",
                "default": null,
                "doc": "Urn of the source of this relationship edge.\nIf not specified, assumed to be the entity that this aspect belongs to."
              },
              {
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": "string",
                "name": "destinationUrn",
                "doc": "Urn of the destination of this relationship edge."
              },
              {
                "type": [
                  "null",
                  {
                    "type": "record",
                    "name": "AuditStamp",
                    "namespace": "com.linkedin.common",
                    "fields": [
                      {
                        "type": "long",
                        "name": "time",
                        "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
                      },
                      {
                        "java": {
                          "class": "com.linkedin.common.urn.Urn"
                        },
                        "type": "string",
                        "name": "actor",
                        "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
                      },
                      {
                        "java": {
                          "class": "com.linkedin.common.urn.Urn"
                        },
                        "type": [
                          "null",
                          "string"
                        ],
                        "name": "impersonator",
                        "default": null,
                        "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
                      },
                      {
                        "type": [
                          "null",
                          "string"
                        ],
                        "name": "message",
                        "default": null,
                        "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
                      }
                    ],
                    "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
                  }
                ],
                "name": "created",
                "default": null,
                "doc": "Audit stamp containing who created this relationship edge and when"
              },
              {
                "type": [
                  "null",
                  "com.linkedin.common.AuditStamp"
                ],
                "name": "lastModified",
                "default": null,
                "doc": "Audit stamp containing who last modified this relationship edge and when"
              },
              {
                "type": [
                  "null",
                  {
                    "type": "map",
                    "values": "string"
                  }
                ],
                "name": "properties",
                "default": null,
                "doc": "A generic properties bag that allows us to store specific information on this graph edge."
              }
            ],
            "doc": "A common structure to represent all edges to entities when used inside aspects as collections\nThis ensures that all edges have common structure around audit-stamps and will support PATCH, time-travel automatically."
          }
        }
      ],
      "name": "chartEdges",
      "default": null,
      "doc": "Charts in a dashboard"
    },
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "dataset"
          ],
          "isLineage": true,
          "name": "Consumes"
        }
      },
      "deprecated": true,
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "datasets",
      "default": [],
      "doc": "Datasets consumed by a dashboard\nDeprecated! Use datasetEdges instead."
    },
    {
      "Relationship": {
        "/*/destinationUrn": {
          "createdActor": "datasetEdges/*/created/actor",
          "createdOn": "datasetEdges/*/created/time",
          "entityTypes": [
            "dataset"
          ],
          "isLineage": true,
          "name": "Consumes",
          "properties": "datasetEdges/*/properties",
          "updatedActor": "datasetEdges/*/lastModified/actor",
          "updatedOn": "datasetEdges/*/lastModified/time"
        }
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": "com.linkedin.common.Edge"
        }
      ],
      "name": "datasetEdges",
      "default": null,
      "doc": "Datasets consumed by a dashboard"
    },
    {
      "Relationship": {
        "/*/destinationUrn": {
          "createdActor": "dashboards/*/created/actor",
          "createdOn": "dashboards/*/created/time",
          "entityTypes": [
            "dashboard"
          ],
          "isLineage": true,
          "name": "DashboardContainsDashboard",
          "properties": "dashboards/*/properties",
          "updatedActor": "dashboards/*/lastModified/actor",
          "updatedOn": "dashboards/*/lastModified/time"
        }
      },
      "type": {
        "type": "array",
        "items": "com.linkedin.common.Edge"
      },
      "name": "dashboards",
      "default": [],
      "doc": "Dashboards included by this dashboard.\nSome dashboard entities (e.g. PowerBI Apps) can contain other dashboards.\n\nThe Edge's sourceUrn should never be set, as it will always be the base dashboard."
    },
    {
      "Searchable": {
        "/lastModified/time": {
          "fieldName": "lastModifiedAt",
          "fieldType": "DATETIME",
          "searchLabel": "lastModifiedAt"
        }
      },
      "type": {
        "type": "record",
        "name": "ChangeAuditStamps",
        "namespace": "com.linkedin.common",
        "fields": [
          {
            "type": "com.linkedin.common.AuditStamp",
            "name": "created",
            "default": {
              "actor": "urn:li:corpuser:unknown",
              "impersonator": null,
              "time": 0,
              "message": null
            },
            "doc": "An AuditStamp corresponding to the creation of this resource/association/sub-resource. A value of 0 for time indicates missing data."
          },
          {
            "type": "com.linkedin.common.AuditStamp",
            "name": "lastModified",
            "default": {
              "actor": "urn:li:corpuser:unknown",
              "impersonator": null,
              "time": 0,
              "message": null
            },
            "doc": "An AuditStamp corresponding to the last modification of this resource/association/sub-resource. If no modification has happened since creation, lastModified should be the same as created. A value of 0 for time indicates missing data."
          },
          {
            "type": [
              "null",
              "com.linkedin.common.AuditStamp"
            ],
            "name": "deleted",
            "default": null,
            "doc": "An AuditStamp corresponding to the deletion of this resource/association/sub-resource. Logically, deleted MUST have a later timestamp than creation. It may or may not have the same time as lastModified depending upon the resource/association/sub-resource semantics."
          }
        ],
        "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into various lifecycle stages, and who acted to move it into those lifecycle stages. The recommended best practice is to include this record in your record schema, and annotate its fields as @readOnly in your resource. See https://github.com/linkedin/rest.li/wiki/Validation-in-Rest.li#restli-validation-annotations"
      },
      "name": "lastModified",
      "doc": "Captures information about who created/last modified/deleted this dashboard and when"
    },
    {
      "Searchable": {
        "fieldType": "KEYWORD",
        "searchTier": 4
      },
      "java": {
        "class": "com.linkedin.common.url.Url",
        "coercerClass": "com.linkedin.common.url.UrlCoercer"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "dashboardUrl",
      "default": null,
      "doc": "URL for the dashboard. This could be used as an external link on DataHub to allow users access/view the dashboard"
    },
    {
      "Searchable": {
        "addToFilters": true,
        "fieldType": "KEYWORD",
        "filterNameOverride": "Access Level",
        "searchTier": 4
      },
      "type": [
        "null",
        {
          "type": "enum",
          "symbolDocs": {
            "PRIVATE": "Private availability to certain set of users",
            "PUBLIC": "Publicly available access level"
          },
          "name": "AccessLevel",
          "namespace": "com.linkedin.common",
          "symbols": [
            "PUBLIC",
            "PRIVATE"
          ],
          "doc": "The various access levels"
        }
      ],
      "name": "access",
      "default": null,
      "doc": "Access level for the dashboard"
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "lastRefreshed",
      "default": null,
      "doc": "The time when this dashboard last refreshed"
    }
  ],
  "doc": "Information about a dashboard"
}
```





#### editableDashboardProperties
Stores editable changes made to properties. This separates changes made from
ingestion pipelines and edits in the UI to avoid accidental overwrites of user-provided data by ingestion pipelines



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| created | [AuditStamp](#auditstamp) | ✓ | An AuditStamp corresponding to the creation of this resource/association/sub-resource. A value of... |  |
| lastModified | [AuditStamp](#auditstamp) | ✓ | An AuditStamp corresponding to the last modification of this resource/association/sub-resource. I... |  |
| deleted | [AuditStamp](#auditstamp) |  | An AuditStamp corresponding to the deletion of this resource/association/sub-resource. Logically,... |  |
| description | string |  | Edited documentation of the dashboard | Searchable (editedDescription) |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "editableDashboardProperties"
  },
  "name": "EditableDashboardProperties",
  "namespace": "com.linkedin.dashboard",
  "fields": [
    {
      "type": {
        "type": "record",
        "name": "AuditStamp",
        "namespace": "com.linkedin.common",
        "fields": [
          {
            "type": "long",
            "name": "time",
            "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
          },
          {
            "java": {
              "class": "com.linkedin.common.urn.Urn"
            },
            "type": "string",
            "name": "actor",
            "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
          },
          {
            "java": {
              "class": "com.linkedin.common.urn.Urn"
            },
            "type": [
              "null",
              "string"
            ],
            "name": "impersonator",
            "default": null,
            "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
          },
          {
            "type": [
              "null",
              "string"
            ],
            "name": "message",
            "default": null,
            "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
          }
        ],
        "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
      },
      "name": "created",
      "default": {
        "actor": "urn:li:corpuser:unknown",
        "impersonator": null,
        "time": 0,
        "message": null
      },
      "doc": "An AuditStamp corresponding to the creation of this resource/association/sub-resource. A value of 0 for time indicates missing data."
    },
    {
      "type": "com.linkedin.common.AuditStamp",
      "name": "lastModified",
      "default": {
        "actor": "urn:li:corpuser:unknown",
        "impersonator": null,
        "time": 0,
        "message": null
      },
      "doc": "An AuditStamp corresponding to the last modification of this resource/association/sub-resource. If no modification has happened since creation, lastModified should be the same as created. A value of 0 for time indicates missing data."
    },
    {
      "type": [
        "null",
        "com.linkedin.common.AuditStamp"
      ],
      "name": "deleted",
      "default": null,
      "doc": "An AuditStamp corresponding to the deletion of this resource/association/sub-resource. Logically, deleted MUST have a later timestamp than creation. It may or may not have the same time as lastModified depending upon the resource/association/sub-resource semantics."
    },
    {
      "Searchable": {
        "fieldName": "editedDescription",
        "fieldType": "TEXT",
        "sanitizeRichText": true,
        "searchTier": 2
      },
      "type": [
        "null",
        "string"
      ],
      "name": "description",
      "default": null,
      "doc": "Edited documentation of the dashboard"
    }
  ],
  "doc": "Stores editable changes made to properties. This separates changes made from\ningestion pipelines and edits in the UI to avoid accidental overwrites of user-provided data by ingestion pipelines"
}
```





#### ownership
Ownership information of an entity.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| owners | Owner[] | ✓ | List of owners of the entity. |  |
| ownerTypes | map |  | Ownership type to Owners map, populated via mutation hook. | Searchable |
| lastModified | [AuditStamp](#auditstamp) | ✓ | Audit stamp containing who last modified the record and when. A value of 0 in the time field indi... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "ownership"
  },
  "name": "Ownership",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "Owner",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "Relationship": {
                "entityTypes": [
                  "corpuser",
                  "corpGroup"
                ],
                "name": "OwnedBy"
              },
              "Searchable": {
                "addToFilters": true,
                "fieldName": "owners",
                "fieldType": "URN",
                "filterNameOverride": "Owned By",
                "hasValuesFieldName": "hasOwners",
                "queryByDefault": false,
                "searchTier": 2
              },
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "owner",
              "doc": "Owner URN, e.g. urn:li:corpuser:ldap, urn:li:corpGroup:group_name, and urn:li:multiProduct:mp_name\n(Caveat: only corpuser is currently supported in the frontend.)"
            },
            {
              "deprecated": true,
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "BUSINESS_OWNER": "A person or group who is responsible for logical, or business related, aspects of the asset.",
                  "CONSUMER": "A person, group, or service that consumes the data\nDeprecated! Use TECHNICAL_OWNER or BUSINESS_OWNER instead.",
                  "CUSTOM": "Set when ownership type is unknown or a when new one is specified as an ownership type entity for which we have no\nenum value for. This is used for backwards compatibility",
                  "DATAOWNER": "A person or group that is owning the data\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "DATA_STEWARD": "A steward, expert, or delegate responsible for the asset.",
                  "DELEGATE": "A person or a group that overseas the operation, e.g. a DBA or SRE.\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "DEVELOPER": "A person or group that is in charge of developing the code\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "NONE": "No specific type associated to the owner.",
                  "PRODUCER": "A person, group, or service that produces/generates the data\nDeprecated! Use TECHNICAL_OWNER instead.",
                  "STAKEHOLDER": "A person or a group that has direct business interest\nDeprecated! Use TECHNICAL_OWNER, BUSINESS_OWNER, or STEWARD instead.",
                  "TECHNICAL_OWNER": "person or group who is responsible for technical aspects of the asset."
                },
                "deprecatedSymbols": {
                  "CONSUMER": true,
                  "DATAOWNER": true,
                  "DELEGATE": true,
                  "DEVELOPER": true,
                  "PRODUCER": true,
                  "STAKEHOLDER": true
                },
                "name": "OwnershipType",
                "namespace": "com.linkedin.common",
                "symbols": [
                  "CUSTOM",
                  "TECHNICAL_OWNER",
                  "BUSINESS_OWNER",
                  "DATA_STEWARD",
                  "NONE",
                  "DEVELOPER",
                  "DATAOWNER",
                  "DELEGATE",
                  "PRODUCER",
                  "CONSUMER",
                  "STAKEHOLDER"
                ],
                "doc": "Asset owner types"
              },
              "name": "type",
              "doc": "The type of the ownership"
            },
            {
              "Relationship": {
                "entityTypes": [
                  "ownershipType"
                ],
                "name": "ownershipType"
              },
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "typeUrn",
              "default": null,
              "doc": "The type of the ownership\nUrn of type O"
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "OwnershipSource",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": {
                        "type": "enum",
                        "symbolDocs": {
                          "AUDIT": "Auditing system or audit logs",
                          "DATABASE": "Database, e.g. GRANTS table",
                          "FILE_SYSTEM": "File system, e.g. file/directory owner",
                          "ISSUE_TRACKING_SYSTEM": "Issue tracking system, e.g. Jira",
                          "MANUAL": "Manually provided by a user",
                          "OTHER": "Other sources",
                          "SERVICE": "Other ownership-like service, e.g. Nuage, ACL service etc",
                          "SOURCE_CONTROL": "SCM system, e.g. GIT, SVN"
                        },
                        "name": "OwnershipSourceType",
                        "namespace": "com.linkedin.common",
                        "symbols": [
                          "AUDIT",
                          "DATABASE",
                          "FILE_SYSTEM",
                          "ISSUE_TRACKING_SYSTEM",
                          "MANUAL",
                          "SERVICE",
                          "SOURCE_CONTROL",
                          "OTHER"
                        ]
                      },
                      "name": "type",
                      "doc": "The type of the source"
                    },
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "url",
                      "default": null,
                      "doc": "A reference URL for the source"
                    }
                  ],
                  "doc": "Source/provider of the ownership information"
                }
              ],
              "name": "source",
              "default": null,
              "doc": "Source information for the ownership"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "ownerAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "ownerAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "ownerAttributionDates",
                  "fieldType": "DATETIME",
                  "queryByDefault": false
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "MetadataAttribution",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When this metadata was updated."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "source",
                      "default": null,
                      "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                    },
                    {
                      "type": {
                        "type": "map",
                        "values": "string"
                      },
                      "name": "sourceDetail",
                      "default": {},
                      "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                    }
                  ],
                  "doc": "Information about who, why, and how this metadata was applied"
                }
              ],
              "name": "attribution",
              "default": null,
              "doc": "Information about who, why, and how this metadata was applied"
            }
          ],
          "doc": "Ownership information"
        }
      },
      "name": "owners",
      "doc": "List of owners of the entity."
    },
    {
      "Searchable": {
        "/$key": {
          "fieldType": "MAP_ARRAY",
          "queryByDefault": false
        }
      },
      "type": [
        {
          "type": "map",
          "values": {
            "type": "array",
            "items": "string"
          }
        },
        "null"
      ],
      "name": "ownerTypes",
      "default": {},
      "doc": "Ownership type to Owners map, populated via mutation hook."
    },
    {
      "type": {
        "type": "record",
        "name": "AuditStamp",
        "namespace": "com.linkedin.common",
        "fields": [
          {
            "type": "long",
            "name": "time",
            "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
          },
          {
            "java": {
              "class": "com.linkedin.common.urn.Urn"
            },
            "type": "string",
            "name": "actor",
            "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
          },
          {
            "java": {
              "class": "com.linkedin.common.urn.Urn"
            },
            "type": [
              "null",
              "string"
            ],
            "name": "impersonator",
            "default": null,
            "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
          },
          {
            "type": [
              "null",
              "string"
            ],
            "name": "message",
            "default": null,
            "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
          }
        ],
        "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
      },
      "name": "lastModified",
      "default": {
        "actor": "urn:li:corpuser:unknown",
        "impersonator": null,
        "time": 0,
        "message": null
      },
      "doc": "Audit stamp containing who last modified the record and when. A value of 0 in the time field indicates missing data."
    }
  ],
  "doc": "Ownership information of an entity."
}
```





#### status
The lifecycle status metadata of an entity, e.g. dataset, metric, feature, etc.
This aspect is used to represent soft deletes conventionally.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| removed | boolean | ✓ | Whether the entity has been removed (soft-deleted). Kept for backward compatibility. When lifecyc... | Searchable |
| lifecycleStage | string |  | The lifecycle stage of the entity, referencing a lifecycleStageType entity. When null, the entity... | Searchable |
| lifecycleLastUpdated | [AuditStamp](#auditstamp) |  | Attribution for the lifecycle stage transition — who moved the entity into its current stage and ... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "status"
  },
  "name": "Status",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "fieldType": "BOOLEAN"
      },
      "type": "boolean",
      "name": "removed",
      "default": false,
      "doc": "Whether the entity has been removed (soft-deleted).\nKept for backward compatibility. When lifecycleStage is set to a stage\nwith hideInSearch=true, this field is NOT automatically synced \u2014 the\nsearch layer uses lifecycleStage settings directly."
    },
    {
      "Searchable": {
        "fieldType": "KEYWORD",
        "queryByDefault": false
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "lifecycleStage",
      "default": null,
      "doc": "The lifecycle stage of the entity, referencing a lifecycleStageType entity.\nWhen null, the entity is in its default active state (visible in search).\nWhen set, the referenced lifecycle stage's settings determine behavior\n(e.g., hideInSearch=true excludes the entity from default search results).\n\nUsers can override default filtering by explicitly filtering on this field."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "AuditStamp",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "type": "long",
              "name": "time",
              "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "actor",
              "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "impersonator",
              "default": null,
              "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "message",
              "default": null,
              "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
            }
          ],
          "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
        }
      ],
      "name": "lifecycleLastUpdated",
      "default": null,
      "doc": "Attribution for the lifecycle stage transition \u2014 who moved the entity\ninto its current stage and when. Populated automatically by the\nsetLifecycleStage mutation; should be set by any code path that\nwrites the lifecycleStage field."
    }
  ],
  "doc": "The lifecycle status metadata of an entity, e.g. dataset, metric, feature, etc.\nThis aspect is used to represent soft deletes conventionally."
}
```





#### globalTags
Tag aspect used for applying tags to an entity



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| tags | TagAssociation[] | ✓ | Tags associated with a given entity | Searchable, → TaggedWith |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "globalTags"
  },
  "name": "GlobalTags",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Relationship": {
        "/*/tag": {
          "entityTypes": [
            "tag"
          ],
          "name": "TaggedWith"
        }
      },
      "Searchable": {
        "/*/tag": {
          "addToFilters": true,
          "boostScore": 0.5,
          "fieldName": "tags",
          "fieldType": "URN",
          "filterNameOverride": "Tagged With",
          "hasValuesFieldName": "hasTags",
          "queryByDefault": true,
          "searchTier": 2
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "TagAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.TagUrn"
              },
              "type": "string",
              "name": "tag",
              "doc": "Urn of the applied tag"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "context",
              "default": null,
              "doc": "Additional context about the association"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "tagAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "tagAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "tagAttributionDates",
                  "fieldType": "DATETIME",
                  "queryByDefault": false
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "MetadataAttribution",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When this metadata was updated."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "source",
                      "default": null,
                      "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                    },
                    {
                      "type": {
                        "type": "map",
                        "values": "string"
                      },
                      "name": "sourceDetail",
                      "default": {},
                      "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                    }
                  ],
                  "doc": "Information about who, why, and how this metadata was applied"
                }
              ],
              "name": "attribution",
              "default": null,
              "doc": "Information about who, why, and how this metadata was applied"
            }
          ],
          "doc": "Properties of an applied tag. For now, just an Urn. In the future we can extend this with other properties, e.g.\npropagation parameters."
        }
      },
      "name": "tags",
      "doc": "Tags associated with a given entity"
    }
  ],
  "doc": "Tag aspect used for applying tags to an entity"
}
```





#### browsePaths
Shared aspect containing Browse Paths to be indexed for an entity.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| paths | string[] | ✓ | A list of valid browse paths for the entity.  Browse paths are expected to be forward slash-separ... | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "browsePaths"
  },
  "name": "BrowsePaths",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "/*": {
          "fieldName": "browsePaths",
          "fieldType": "BROWSE_PATH"
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "paths",
      "doc": "A list of valid browse paths for the entity.\n\nBrowse paths are expected to be forward slash-separated strings. For example: 'prod/snowflake/datasetName'"
    }
  ],
  "doc": "Shared aspect containing Browse Paths to be indexed for an entity."
}
```





#### glossaryTerms
Related business terms information



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| terms | GlossaryTermAssociation[] | ✓ | The related business terms |  |
| auditStamp | [AuditStamp](#auditstamp) | ✓ | Audit stamp containing who reported the related business term |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "glossaryTerms"
  },
  "name": "GlossaryTerms",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "GlossaryTermAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "Relationship": {
                "entityTypes": [
                  "glossaryTerm"
                ],
                "name": "TermedWith"
              },
              "Searchable": {
                "addToFilters": true,
                "fieldName": "glossaryTerms",
                "fieldType": "URN",
                "filterNameOverride": "Glossary Term",
                "hasValuesFieldName": "hasGlossaryTerms",
                "includeSystemModifiedAt": true,
                "systemModifiedAtFieldName": "termsModifiedAt"
              },
              "java": {
                "class": "com.linkedin.common.urn.GlossaryTermUrn"
              },
              "type": "string",
              "name": "urn",
              "doc": "Urn of the applied glossary term"
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "actor",
              "default": null,
              "doc": "The user URN which will be credited for adding associating this term to the entity"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "context",
              "default": null,
              "doc": "Additional context about the association"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "termAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "termAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "termAttributionDates",
                  "fieldType": "DATETIME",
                  "queryByDefault": false
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "MetadataAttribution",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When this metadata was updated."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "source",
                      "default": null,
                      "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                    },
                    {
                      "type": {
                        "type": "map",
                        "values": "string"
                      },
                      "name": "sourceDetail",
                      "default": {},
                      "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                    }
                  ],
                  "doc": "Information about who, why, and how this metadata was applied"
                }
              ],
              "name": "attribution",
              "default": null,
              "doc": "Information about who, why, and how this metadata was applied"
            }
          ],
          "doc": "Properties of an applied glossary term."
        }
      },
      "name": "terms",
      "doc": "The related business terms"
    },
    {
      "type": {
        "type": "record",
        "name": "AuditStamp",
        "namespace": "com.linkedin.common",
        "fields": [
          {
            "type": "long",
            "name": "time",
            "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
          },
          {
            "java": {
              "class": "com.linkedin.common.urn.Urn"
            },
            "type": "string",
            "name": "actor",
            "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
          },
          {
            "java": {
              "class": "com.linkedin.common.urn.Urn"
            },
            "type": [
              "null",
              "string"
            ],
            "name": "impersonator",
            "default": null,
            "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
          },
          {
            "type": [
              "null",
              "string"
            ],
            "name": "message",
            "default": null,
            "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
          }
        ],
        "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
      },
      "name": "auditStamp",
      "doc": "Audit stamp containing who reported the related business term"
    }
  ],
  "doc": "Related business terms information"
}
```





#### institutionalMemory
Institutional memory of an entity. This is a way to link to relevant documentation and provide description of the documentation. Institutional or tribal knowledge is very important for users to leverage the entity.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| elements | InstitutionalMemoryMetadata[] | ✓ | List of records that represent institutional memory of an entity. Each record consists of a link,... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "institutionalMemory"
  },
  "name": "InstitutionalMemory",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "InstitutionalMemoryMetadata",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.url.Url",
                "coercerClass": "com.linkedin.common.url.UrlCoercer"
              },
              "type": "string",
              "name": "url",
              "doc": "Link to an engineering design document or a wiki page."
            },
            {
              "type": "string",
              "name": "description",
              "doc": "Description of the link."
            },
            {
              "type": {
                "type": "record",
                "name": "AuditStamp",
                "namespace": "com.linkedin.common",
                "fields": [
                  {
                    "type": "long",
                    "name": "time",
                    "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
                  },
                  {
                    "java": {
                      "class": "com.linkedin.common.urn.Urn"
                    },
                    "type": "string",
                    "name": "actor",
                    "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
                  },
                  {
                    "java": {
                      "class": "com.linkedin.common.urn.Urn"
                    },
                    "type": [
                      "null",
                      "string"
                    ],
                    "name": "impersonator",
                    "default": null,
                    "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
                  },
                  {
                    "type": [
                      "null",
                      "string"
                    ],
                    "name": "message",
                    "default": null,
                    "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
                  }
                ],
                "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
              },
              "name": "createStamp",
              "doc": "Audit stamp associated with creation of this record"
            },
            {
              "type": [
                "null",
                "com.linkedin.common.AuditStamp"
              ],
              "name": "updateStamp",
              "default": null,
              "doc": "Audit stamp associated with updation of this record"
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "InstitutionalMemoryMetadataSettings",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "boolean",
                      "name": "showInAssetPreview",
                      "default": false,
                      "doc": "Show record in asset preview like on entity header and search previews"
                    }
                  ],
                  "doc": "Settings related to a record of InstitutionalMemoryMetadata"
                }
              ],
              "name": "settings",
              "default": null,
              "doc": "Settings for this record"
            }
          ],
          "doc": "Metadata corresponding to a record of institutional memory."
        }
      },
      "name": "elements",
      "doc": "List of records that represent institutional memory of an entity. Each record consists of a link, description, creator and timestamps associated with that record."
    }
  ],
  "doc": "Institutional memory of an entity. This is a way to link to relevant documentation and provide description of the documentation. Institutional or tribal knowledge is very important for users to leverage the entity."
}
```





#### dataPlatformInstance
The specific instance of the data platform that this entity belongs to



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| platform | string | ✓ | Data Platform | Searchable |
| instance | string |  | Instance of the data platform (e.g. db instance) | Searchable (platformInstance) |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dataPlatformInstance"
  },
  "name": "DataPlatformInstance",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "addToFilters": true,
        "fieldType": "URN",
        "filterNameOverride": "Platform"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "platform",
      "doc": "Data Platform"
    },
    {
      "Searchable": {
        "addToFilters": true,
        "fieldName": "platformInstance",
        "fieldType": "URN",
        "filterNameOverride": "Platform Instance"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "instance",
      "default": null,
      "doc": "Instance of the data platform (e.g. db instance)"
    }
  ],
  "doc": "The specific instance of the data platform that this entity belongs to"
}
```





#### browsePathsV2
Shared aspect containing a Browse Path to be indexed for an entity.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| path | BrowsePathEntry[] | ✓ | A valid browse path for the entity. This field is provided by DataHub by default. This aspect is ... | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "browsePathsV2"
  },
  "name": "BrowsePathsV2",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "/*/id": {
          "fieldName": "browsePathV2",
          "fieldType": "BROWSE_PATH_V2"
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "BrowsePathEntry",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "type": "string",
              "name": "id",
              "doc": "The ID of the browse path entry. This is what gets stored in the index.\nIf there's an urn associated with this entry, id and urn will be the same"
            },
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": [
                "null",
                "string"
              ],
              "name": "urn",
              "default": null,
              "doc": "Optional urn pointing to some entity in DataHub"
            }
          ],
          "doc": "Represents a single level in an entity's browsePathV2"
        }
      },
      "name": "path",
      "doc": "A valid browse path for the entity. This field is provided by DataHub by default.\nThis aspect is a newer version of browsePaths where we can encode more information in the path.\nThis path is also based on containers for a given entity if it has containers.\n\nThis is stored in elasticsearch as unit-separator delimited strings and only includes platform specific folders or containers.\nThese paths should not include high level info captured elsewhere ie. Platform and Environment."
    }
  ],
  "doc": "Shared aspect containing a Browse Path to be indexed for an entity."
}
```





#### domains
Links from an Asset to its Domains



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| domains | string[] | ✓ | The Domains attached to an Asset | Searchable, → AssociatedWith |
| domainAssociations | DomainAssociation[] |  | Additional per-domain association metadata such as attribution and propagation source. A superset... |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "domains",
    "schemaVersion": 2
  },
  "name": "Domains",
  "namespace": "com.linkedin.domain",
  "fields": [
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "domain"
          ],
          "name": "AssociatedWith"
        }
      },
      "Searchable": {
        "/*": {
          "addToFilters": true,
          "fieldName": "domains",
          "fieldType": "URN",
          "filterNameOverride": "Domain",
          "hasValuesFieldName": "hasDomain"
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "domains",
      "doc": "The Domains attached to an Asset"
    },
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "DomainAssociation",
            "namespace": "com.linkedin.domain",
            "fields": [
              {
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": "string",
                "name": "domain",
                "doc": "Urn of the associated domain. Corresponds to an entry in the parallel domains array."
              },
              {
                "type": [
                  "null",
                  "string"
                ],
                "name": "context",
                "default": null,
                "doc": "Additional context about the association"
              },
              {
                "Searchable": {
                  "/actor": {
                    "fieldName": "domainAttributionActors",
                    "fieldType": "URN",
                    "queryByDefault": false
                  },
                  "/source": {
                    "fieldName": "domainAttributionSources",
                    "fieldType": "URN",
                    "queryByDefault": false
                  },
                  "/time": {
                    "fieldName": "domainAttributionDates",
                    "fieldType": "DATETIME",
                    "queryByDefault": false
                  }
                },
                "type": [
                  "null",
                  {
                    "type": "record",
                    "name": "MetadataAttribution",
                    "namespace": "com.linkedin.common",
                    "fields": [
                      {
                        "type": "long",
                        "name": "time",
                        "doc": "When this metadata was updated."
                      },
                      {
                        "java": {
                          "class": "com.linkedin.common.urn.Urn"
                        },
                        "type": "string",
                        "name": "actor",
                        "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                      },
                      {
                        "java": {
                          "class": "com.linkedin.common.urn.Urn"
                        },
                        "type": [
                          "null",
                          "string"
                        ],
                        "name": "source",
                        "default": null,
                        "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                      },
                      {
                        "type": {
                          "type": "map",
                          "values": "string"
                        },
                        "name": "sourceDetail",
                        "default": {},
                        "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                      }
                    ],
                    "doc": "Information about who, why, and how this metadata was applied"
                  }
                ],
                "name": "attribution",
                "default": null,
                "doc": "Information about who, why, and how this domain was applied.\nsourceDetail may carry flags such as 'propagated'='true' when set via glossary tree propagation."
              }
            ],
            "doc": "Properties of an applied domain association."
          }
        }
      ],
      "name": "domainAssociations",
      "default": null,
      "doc": "Additional per-domain association metadata such as attribution and propagation source.\nA superset of the domains field; entries correspond by domain URN.\nInitial migration handled by the DomainsMigrationMutator;\nthe two fields are kept in sync via the DomainsSyncMutationHook."
    }
  ],
  "doc": "Links from an Asset to its Domains"
}
```





#### applications
Links from an Asset to its Applications



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| applications | string[] | ✓ | The Applications attached to an Asset | Searchable, → AssociatedWith |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "applications"
  },
  "name": "Applications",
  "namespace": "com.linkedin.application",
  "fields": [
    {
      "Relationship": {
        "/*": {
          "entityTypes": [
            "application"
          ],
          "name": "AssociatedWith"
        }
      },
      "Searchable": {
        "/*": {
          "addToFilters": true,
          "fieldName": "applications",
          "fieldType": "URN",
          "filterNameOverride": "Application",
          "hasValuesFieldName": "hasApplication"
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "applications",
      "doc": "The Applications attached to an Asset"
    }
  ],
  "doc": "Links from an Asset to its Applications"
}
```





#### container
Link from an asset to its parent container



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| container | string | ✓ | The parent container of an asset | Searchable, → IsPartOf |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "container"
  },
  "name": "Container",
  "namespace": "com.linkedin.container",
  "fields": [
    {
      "Relationship": {
        "entityTypes": [
          "container"
        ],
        "name": "IsPartOf"
      },
      "Searchable": {
        "addToFilters": true,
        "fieldName": "container",
        "fieldType": "URN",
        "filterNameOverride": "Container",
        "hasValuesFieldName": "hasContainer"
      },
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "container",
      "doc": "The parent container of an asset"
    }
  ],
  "doc": "Link from an asset to its parent container"
}
```





#### deprecation
Deprecation status of an entity



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| deprecated | boolean | ✓ | Whether the entity is deprecated. | Searchable |
| decommissionTime | long |  | The time user plan to decommission this entity. |  |
| note | string | ✓ | Additional information about the entity deprecation plan, such as the wiki, doc, RB. |  |
| actor | string | ✓ | The user URN which will be credited for modifying this deprecation content. |  |
| replacement | string |  |  |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "deprecation"
  },
  "name": "Deprecation",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "addToFilters": true,
        "fieldType": "BOOLEAN",
        "filterNameOverride": "Deprecated",
        "weightsPerFieldValue": {
          "true": 0.5
        }
      },
      "type": "boolean",
      "name": "deprecated",
      "doc": "Whether the entity is deprecated."
    },
    {
      "type": [
        "null",
        "long"
      ],
      "name": "decommissionTime",
      "default": null,
      "doc": "The time user plan to decommission this entity."
    },
    {
      "type": "string",
      "name": "note",
      "doc": "Additional information about the entity deprecation plan, such as the wiki, doc, RB."
    },
    {
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": "string",
      "name": "actor",
      "doc": "The user URN which will be credited for modifying this deprecation content."
    },
    {
      "java": {
        "class": "com.linkedin.common.urn.Urn"
      },
      "type": [
        "null",
        "string"
      ],
      "name": "replacement",
      "default": null
    }
  ],
  "doc": "Deprecation status of an entity"
}
```





#### inputFields
Information about the fields a chart or dashboard references



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| fields | InputField[] | ✓ | List of fields being referenced |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "inputFields"
  },
  "name": "InputFields",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "InputField",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "Relationship": {
                "entityTypes": [
                  "schemaField"
                ],
                "name": "consumesField"
              },
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "schemaFieldUrn",
              "doc": "Urn of the schema being referenced for lineage purposes"
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "SchemaField",
                  "namespace": "com.linkedin.schema",
                  "fields": [
                    {
                      "Searchable": {
                        "boostScore": 1.0,
                        "fieldName": "fieldPaths",
                        "fieldType": "TEXT",
                        "queryByDefault": "true"
                      },
                      "type": "string",
                      "name": "fieldPath",
                      "doc": "Flattened name of the field. Field is computed from jsonPath field."
                    },
                    {
                      "Deprecated": true,
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "jsonPath",
                      "default": null,
                      "doc": "Flattened name of a field in JSON Path notation."
                    },
                    {
                      "type": "boolean",
                      "name": "nullable",
                      "default": false,
                      "doc": "Indicates if this field is optional or nullable"
                    },
                    {
                      "Searchable": {
                        "boostScore": 0.1,
                        "fieldName": "fieldDescriptions",
                        "fieldType": "TEXT",
                        "sanitizeRichText": true
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "description",
                      "default": null,
                      "doc": "Description"
                    },
                    {
                      "Deprecated": true,
                      "Searchable": {
                        "boostScore": 0.2,
                        "fieldName": "fieldLabels",
                        "fieldType": "TEXT"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "label",
                      "default": null,
                      "doc": "Label of the field. Provides a more human-readable name for the field than field path. Some sources will\nprovide this metadata but not all sources have the concept of a label. If just one string is associated with\na field in a source, that is most likely a description.\n\nNote that this field is deprecated and is not surfaced in the UI."
                    },
                    {
                      "type": [
                        "null",
                        {
                          "type": "record",
                          "name": "AuditStamp",
                          "namespace": "com.linkedin.common",
                          "fields": [
                            {
                              "type": "long",
                              "name": "time",
                              "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
                            },
                            {
                              "java": {
                                "class": "com.linkedin.common.urn.Urn"
                              },
                              "type": "string",
                              "name": "actor",
                              "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
                            },
                            {
                              "java": {
                                "class": "com.linkedin.common.urn.Urn"
                              },
                              "type": [
                                "null",
                                "string"
                              ],
                              "name": "impersonator",
                              "default": null,
                              "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
                            },
                            {
                              "type": [
                                "null",
                                "string"
                              ],
                              "name": "message",
                              "default": null,
                              "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
                            }
                          ],
                          "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
                        }
                      ],
                      "name": "created",
                      "default": null,
                      "doc": "An AuditStamp corresponding to the creation of this schema field."
                    },
                    {
                      "type": [
                        "null",
                        "com.linkedin.common.AuditStamp"
                      ],
                      "name": "lastModified",
                      "default": null,
                      "doc": "An AuditStamp corresponding to the last modification of this schema field."
                    },
                    {
                      "type": {
                        "type": "record",
                        "name": "SchemaFieldDataType",
                        "namespace": "com.linkedin.schema",
                        "fields": [
                          {
                            "type": [
                              {
                                "type": "record",
                                "name": "BooleanType",
                                "namespace": "com.linkedin.schema",
                                "fields": [],
                                "doc": "Boolean field type."
                              },
                              {
                                "type": "record",
                                "name": "FixedType",
                                "namespace": "com.linkedin.schema",
                                "fields": [],
                                "doc": "Fixed field type."
                              },
                              {
                                "type": "record",
                                "name": "StringType",
                                "namespace": "com.linkedin.schema",
                                "fields": [],
                                "doc": "String field type."
                              },
                              {
                                "type": "record",
                                "name": "BytesType",
                                "namespace": "com.linkedin.schema",
                                "fields": [],
                                "doc": "Bytes field type."
                              },
                              {
                                "type": "record",
                                "name": "NumberType",
                                "namespace": "com.linkedin.schema",
                                "fields": [],
                                "doc": "Number data type: long, integer, short, etc.."
                              },
                              {
                                "type": "record",
                                "name": "DateType",
                                "namespace": "com.linkedin.schema",
                                "fields": [],
                                "doc": "Date field type."
                              },
                              {
                                "type": "record",
                                "name": "TimeType",
                                "namespace": "com.linkedin.schema",
                                "fields": [],
                                "doc": "Time field type. This should also be used for datetimes."
                              },
                              {
                                "type": "record",
                                "name": "EnumType",
                                "namespace": "com.linkedin.schema",
                                "fields": [],
                                "doc": "Enum field type."
                              },
                              {
                                "type": "record",
                                "name": "NullType",
                                "namespace": "com.linkedin.schema",
                                "fields": [],
                                "doc": "Null field type."
                              },
                              {
                                "type": "record",
                                "name": "MapType",
                                "namespace": "com.linkedin.schema",
                                "fields": [
                                  {
                                    "type": [
                                      "null",
                                      "string"
                                    ],
                                    "name": "keyType",
                                    "default": null,
                                    "doc": "Key type in a map"
                                  },
                                  {
                                    "type": [
                                      "null",
                                      "string"
                                    ],
                                    "name": "valueType",
                                    "default": null,
                                    "doc": "Type of the value in a map"
                                  }
                                ],
                                "doc": "Map field type."
                              },
                              {
                                "type": "record",
                                "name": "ArrayType",
                                "namespace": "com.linkedin.schema",
                                "fields": [
                                  {
                                    "type": [
                                      "null",
                                      {
                                        "type": "array",
                                        "items": "string"
                                      }
                                    ],
                                    "name": "nestedType",
                                    "default": null,
                                    "doc": "List of types this array holds."
                                  }
                                ],
                                "doc": "Array field type."
                              },
                              {
                                "type": "record",
                                "name": "UnionType",
                                "namespace": "com.linkedin.schema",
                                "fields": [
                                  {
                                    "type": [
                                      "null",
                                      {
                                        "type": "array",
                                        "items": "string"
                                      }
                                    ],
                                    "name": "nestedTypes",
                                    "default": null,
                                    "doc": "List of types in union type."
                                  }
                                ],
                                "doc": "Union field type."
                              },
                              {
                                "type": "record",
                                "name": "RecordType",
                                "namespace": "com.linkedin.schema",
                                "fields": [],
                                "doc": "Record field type."
                              }
                            ],
                            "name": "type",
                            "doc": "Data platform specific types"
                          }
                        ],
                        "doc": "Schema field data types"
                      },
                      "name": "type",
                      "doc": "Platform independent field type of the field."
                    },
                    {
                      "type": "string",
                      "name": "nativeDataType",
                      "doc": "The native type of the field in the dataset's platform as declared by platform schema."
                    },
                    {
                      "type": "boolean",
                      "name": "recursive",
                      "default": false,
                      "doc": "There are use cases when a field in type B references type A. A field in A references field of type B. In such cases, we will mark the first field as recursive."
                    },
                    {
                      "Relationship": {
                        "/tags/*/tag": {
                          "entityTypes": [
                            "tag"
                          ],
                          "name": "SchemaFieldTaggedWith"
                        }
                      },
                      "Searchable": {
                        "/tags/*/attribution/actor": {
                          "fieldName": "fieldTagAttributionActors",
                          "fieldType": "URN",
                          "queryByDefault": false
                        },
                        "/tags/*/attribution/source": {
                          "fieldName": "fieldTagAttributionSources",
                          "fieldType": "URN",
                          "queryByDefault": false
                        },
                        "/tags/*/attribution/time": {
                          "fieldName": "fieldTagAttributionDates",
                          "fieldType": "DATETIME",
                          "queryByDefault": false
                        },
                        "/tags/*/tag": {
                          "boostScore": 0.5,
                          "fieldName": "fieldTags",
                          "fieldType": "URN"
                        }
                      },
                      "type": [
                        "null",
                        {
                          "type": "record",
                          "Aspect": {
                            "name": "globalTags"
                          },
                          "name": "GlobalTags",
                          "namespace": "com.linkedin.common",
                          "fields": [
                            {
                              "Relationship": {
                                "/*/tag": {
                                  "entityTypes": [
                                    "tag"
                                  ],
                                  "name": "TaggedWith"
                                }
                              },
                              "Searchable": {
                                "/*/tag": {
                                  "addToFilters": true,
                                  "boostScore": 0.5,
                                  "fieldName": "tags",
                                  "fieldType": "URN",
                                  "filterNameOverride": "Tagged With",
                                  "hasValuesFieldName": "hasTags",
                                  "queryByDefault": true,
                                  "searchTier": 2
                                }
                              },
                              "type": {
                                "type": "array",
                                "items": {
                                  "type": "record",
                                  "name": "TagAssociation",
                                  "namespace": "com.linkedin.common",
                                  "fields": [
                                    {
                                      "java": {
                                        "class": "com.linkedin.common.urn.TagUrn"
                                      },
                                      "type": "string",
                                      "name": "tag",
                                      "doc": "Urn of the applied tag"
                                    },
                                    {
                                      "type": [
                                        "null",
                                        "string"
                                      ],
                                      "name": "context",
                                      "default": null,
                                      "doc": "Additional context about the association"
                                    },
                                    {
                                      "Searchable": {
                                        "/actor": {
                                          "fieldName": "tagAttributionActors",
                                          "fieldType": "URN",
                                          "queryByDefault": false
                                        },
                                        "/source": {
                                          "fieldName": "tagAttributionSources",
                                          "fieldType": "URN",
                                          "queryByDefault": false
                                        },
                                        "/time": {
                                          "fieldName": "tagAttributionDates",
                                          "fieldType": "DATETIME",
                                          "queryByDefault": false
                                        }
                                      },
                                      "type": [
                                        "null",
                                        {
                                          "type": "record",
                                          "name": "MetadataAttribution",
                                          "namespace": "com.linkedin.common",
                                          "fields": [
                                            {
                                              "type": "long",
                                              "name": "time",
                                              "doc": "When this metadata was updated."
                                            },
                                            {
                                              "java": {
                                                "class": "com.linkedin.common.urn.Urn"
                                              },
                                              "type": "string",
                                              "name": "actor",
                                              "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                                            },
                                            {
                                              "java": {
                                                "class": "com.linkedin.common.urn.Urn"
                                              },
                                              "type": [
                                                "null",
                                                "string"
                                              ],
                                              "name": "source",
                                              "default": null,
                                              "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                                            },
                                            {
                                              "type": {
                                                "type": "map",
                                                "values": "string"
                                              },
                                              "name": "sourceDetail",
                                              "default": {},
                                              "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                                            }
                                          ],
                                          "doc": "Information about who, why, and how this metadata was applied"
                                        }
                                      ],
                                      "name": "attribution",
                                      "default": null,
                                      "doc": "Information about who, why, and how this metadata was applied"
                                    }
                                  ],
                                  "doc": "Properties of an applied tag. For now, just an Urn. In the future we can extend this with other properties, e.g.\npropagation parameters."
                                }
                              },
                              "name": "tags",
                              "doc": "Tags associated with a given entity"
                            }
                          ],
                          "doc": "Tag aspect used for applying tags to an entity"
                        }
                      ],
                      "name": "globalTags",
                      "default": null,
                      "doc": "Tags associated with the field"
                    },
                    {
                      "Relationship": {
                        "/terms/*/urn": {
                          "entityTypes": [
                            "glossaryTerm"
                          ],
                          "name": "SchemaFieldWithGlossaryTerm"
                        }
                      },
                      "Searchable": {
                        "/terms/*/attribution/actor": {
                          "fieldName": "fieldTermAttributionActors",
                          "fieldType": "URN",
                          "queryByDefault": false
                        },
                        "/terms/*/attribution/source": {
                          "fieldName": "fieldTermAttributionSources",
                          "fieldType": "URN",
                          "queryByDefault": false
                        },
                        "/terms/*/attribution/time": {
                          "fieldName": "fieldTermAttributionDates",
                          "fieldType": "DATETIME",
                          "queryByDefault": false
                        },
                        "/terms/*/urn": {
                          "boostScore": 0.5,
                          "fieldName": "fieldGlossaryTerms",
                          "fieldType": "URN"
                        }
                      },
                      "type": [
                        "null",
                        {
                          "type": "record",
                          "Aspect": {
                            "name": "glossaryTerms"
                          },
                          "name": "GlossaryTerms",
                          "namespace": "com.linkedin.common",
                          "fields": [
                            {
                              "type": {
                                "type": "array",
                                "items": {
                                  "type": "record",
                                  "name": "GlossaryTermAssociation",
                                  "namespace": "com.linkedin.common",
                                  "fields": [
                                    {
                                      "Relationship": {
                                        "entityTypes": [
                                          "glossaryTerm"
                                        ],
                                        "name": "TermedWith"
                                      },
                                      "Searchable": {
                                        "addToFilters": true,
                                        "fieldName": "glossaryTerms",
                                        "fieldType": "URN",
                                        "filterNameOverride": "Glossary Term",
                                        "hasValuesFieldName": "hasGlossaryTerms",
                                        "includeSystemModifiedAt": true,
                                        "systemModifiedAtFieldName": "termsModifiedAt"
                                      },
                                      "java": {
                                        "class": "com.linkedin.common.urn.GlossaryTermUrn"
                                      },
                                      "type": "string",
                                      "name": "urn",
                                      "doc": "Urn of the applied glossary term"
                                    },
                                    {
                                      "java": {
                                        "class": "com.linkedin.common.urn.Urn"
                                      },
                                      "type": [
                                        "null",
                                        "string"
                                      ],
                                      "name": "actor",
                                      "default": null,
                                      "doc": "The user URN which will be credited for adding associating this term to the entity"
                                    },
                                    {
                                      "type": [
                                        "null",
                                        "string"
                                      ],
                                      "name": "context",
                                      "default": null,
                                      "doc": "Additional context about the association"
                                    },
                                    {
                                      "Searchable": {
                                        "/actor": {
                                          "fieldName": "termAttributionActors",
                                          "fieldType": "URN",
                                          "queryByDefault": false
                                        },
                                        "/source": {
                                          "fieldName": "termAttributionSources",
                                          "fieldType": "URN",
                                          "queryByDefault": false
                                        },
                                        "/time": {
                                          "fieldName": "termAttributionDates",
                                          "fieldType": "DATETIME",
                                          "queryByDefault": false
                                        }
                                      },
                                      "type": [
                                        "null",
                                        "com.linkedin.common.MetadataAttribution"
                                      ],
                                      "name": "attribution",
                                      "default": null,
                                      "doc": "Information about who, why, and how this metadata was applied"
                                    }
                                  ],
                                  "doc": "Properties of an applied glossary term."
                                }
                              },
                              "name": "terms",
                              "doc": "The related business terms"
                            },
                            {
                              "type": "com.linkedin.common.AuditStamp",
                              "name": "auditStamp",
                              "doc": "Audit stamp containing who reported the related business term"
                            }
                          ],
                          "doc": "Related business terms information"
                        }
                      ],
                      "name": "glossaryTerms",
                      "default": null,
                      "doc": "Glossary terms associated with the field"
                    },
                    {
                      "type": "boolean",
                      "name": "isPartOfKey",
                      "default": false,
                      "doc": "For schema fields that are part of complex keys, set this field to true\nWe do this to easily distinguish between value and key fields"
                    },
                    {
                      "type": [
                        "null",
                        "boolean"
                      ],
                      "name": "isPartitioningKey",
                      "default": null,
                      "doc": "For Datasets which are partitioned, this determines the partitioning key.\nNote that multiple columns can be part of a partitioning key, but currently we do not support\nrendering the ordered partitioning key."
                    },
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "jsonProps",
                      "default": null,
                      "doc": "For schema fields that have other properties that are not modeled explicitly,\nuse this field to serialize those properties into a JSON string"
                    }
                  ],
                  "doc": "SchemaField to describe metadata related to dataset schema."
                }
              ],
              "name": "schemaField",
              "default": null,
              "doc": "Copied version of the referenced schema field object for indexing purposes"
            }
          ],
          "doc": "Information about a field a chart or dashboard references"
        }
      },
      "name": "fields",
      "doc": "List of fields being referenced"
    }
  ],
  "doc": "Information about the fields a chart or dashboard references"
}
```





#### subTypes
Sub Types. Use this aspect to specialize a generic Entity
e.g. Making a Dataset also be a View or also be a LookerExplore



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| typeNames | string[] | ✓ | The names of the specific types. | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "subTypes"
  },
  "name": "SubTypes",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "/*": {
          "addToFilters": true,
          "fieldType": "KEYWORD",
          "filterNameOverride": "Sub Type",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "typeNames",
      "doc": "The names of the specific types."
    }
  ],
  "doc": "Sub Types. Use this aspect to specialize a generic Entity\ne.g. Making a Dataset also be a View or also be a LookerExplore"
}
```





#### embed
Information regarding rendering an embed for an asset.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| renderUrl | string |  | An embed URL to be rendered inside of an iframe. |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "embed"
  },
  "name": "Embed",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": [
        "null",
        "string"
      ],
      "name": "renderUrl",
      "default": null,
      "doc": "An embed URL to be rendered inside of an iframe."
    }
  ],
  "doc": "Information regarding rendering an embed for an asset."
}
```





#### structuredProperties
Properties about an entity governed by StructuredPropertyDefinition



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| properties | StructuredPropertyValueAssignment[] | ✓ | Custom property bag. |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "structuredProperties"
  },
  "name": "StructuredProperties",
  "namespace": "com.linkedin.structured",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "StructuredPropertyValueAssignment",
          "namespace": "com.linkedin.structured",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "propertyUrn",
              "doc": "The property that is being assigned a value."
            },
            {
              "type": {
                "type": "array",
                "items": [
                  "string",
                  "double"
                ]
              },
              "name": "values",
              "doc": "The value assigned to the property."
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "AuditStamp",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "impersonator",
                      "default": null,
                      "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
                    },
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "message",
                      "default": null,
                      "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
                    }
                  ],
                  "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
                }
              ],
              "name": "created",
              "default": null,
              "doc": "Audit stamp containing who created this relationship edge and when"
            },
            {
              "type": [
                "null",
                "com.linkedin.common.AuditStamp"
              ],
              "name": "lastModified",
              "default": null,
              "doc": "Audit stamp containing who last modified this relationship edge and when"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "structuredPropertyAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "structuredPropertyAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "structuredPropertyAttributionDates",
                  "fieldType": "DATETIME",
                  "queryByDefault": false
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "MetadataAttribution",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When this metadata was updated."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "source",
                      "default": null,
                      "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                    },
                    {
                      "type": {
                        "type": "map",
                        "values": "string"
                      },
                      "name": "sourceDetail",
                      "default": {},
                      "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                    }
                  ],
                  "doc": "Information about who, why, and how this metadata was applied"
                }
              ],
              "name": "attribution",
              "default": null,
              "doc": "Information about who, why, and how this metadata was applied"
            }
          ]
        }
      },
      "name": "properties",
      "doc": "Custom property bag."
    }
  ],
  "doc": "Properties about an entity governed by StructuredPropertyDefinition"
}
```





#### incidentsSummary
Summary related incidents on an entity.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| resolvedIncidents | string[] | ✓ | Resolved incidents for an asset Deprecated! Use the richer resolvedIncidentsDetails instead. | ⚠️ Deprecated |
| activeIncidents | string[] | ✓ | Active incidents for an asset Deprecated! Use the richer activeIncidentsDetails instead. | ⚠️ Deprecated |
| resolvedIncidentDetails | [IncidentSummaryDetails](#incidentsummarydetails)[] | ✓ | Summary details about the set of resolved incidents | Searchable, → ResolvedIncidents |
| activeIncidentDetails | [IncidentSummaryDetails](#incidentsummarydetails)[] | ✓ | Summary details about the set of active incidents | Searchable, → ActiveIncidents |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "incidentsSummary"
  },
  "name": "IncidentsSummary",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "deprecated": true,
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "resolvedIncidents",
      "default": [],
      "doc": "Resolved incidents for an asset\nDeprecated! Use the richer resolvedIncidentsDetails instead."
    },
    {
      "deprecated": true,
      "type": {
        "type": "array",
        "items": "string"
      },
      "name": "activeIncidents",
      "default": [],
      "doc": "Active incidents for an asset\nDeprecated! Use the richer activeIncidentsDetails instead."
    },
    {
      "Relationship": {
        "/*/urn": {
          "entityTypes": [
            "incident"
          ],
          "name": "ResolvedIncidents"
        }
      },
      "Searchable": {
        "/*/createdAt": {
          "fieldName": "resolvedIncidentCreatedTimes",
          "fieldType": "DATETIME"
        },
        "/*/priority": {
          "fieldName": "resolvedIncidentPriorities",
          "fieldType": "COUNT"
        },
        "/*/resolvedAt": {
          "fieldName": "resolvedIncidentResolvedTimes",
          "fieldType": "DATETIME"
        },
        "/*/type": {
          "fieldName": "resolvedIncidentTypes",
          "fieldType": "KEYWORD"
        },
        "/*/urn": {
          "fieldName": "resolvedIncidents",
          "fieldType": "URN",
          "hasValuesFieldName": "hasResolvedIncidents",
          "numValuesFieldName": "numResolvedIncidents",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "IncidentSummaryDetails",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "urn",
              "doc": "The urn of the incident"
            },
            {
              "type": "string",
              "name": "type",
              "doc": "The type of an incident"
            },
            {
              "type": "long",
              "name": "createdAt",
              "doc": "The time at which the incident was raised in milliseconds since epoch."
            },
            {
              "type": [
                "null",
                "long"
              ],
              "name": "resolvedAt",
              "default": null,
              "doc": "The time at which the incident was marked as resolved in milliseconds since epoch. Null if the incident is still active."
            },
            {
              "type": [
                "null",
                "int"
              ],
              "name": "priority",
              "default": null,
              "doc": "The priority of the incident"
            }
          ],
          "doc": "Summary statistics about incidents on an entity."
        }
      },
      "name": "resolvedIncidentDetails",
      "default": [],
      "doc": "Summary details about the set of resolved incidents"
    },
    {
      "Relationship": {
        "/*/urn": {
          "entityTypes": [
            "incident"
          ],
          "name": "ActiveIncidents"
        }
      },
      "Searchable": {
        "/*/createdAt": {
          "fieldName": "activeIncidentCreatedTimes",
          "fieldType": "DATETIME"
        },
        "/*/priority": {
          "fieldName": "activeIncidentPriorities",
          "fieldType": "COUNT"
        },
        "/*/type": {
          "fieldName": "activeIncidentTypes",
          "fieldType": "KEYWORD"
        },
        "/*/urn": {
          "addHasValuesToFilters": true,
          "fieldName": "activeIncidents",
          "fieldType": "URN",
          "hasValuesFieldName": "hasActiveIncidents",
          "numValuesFieldName": "numActiveIncidents",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "com.linkedin.common.IncidentSummaryDetails"
      },
      "name": "activeIncidentDetails",
      "default": [],
      "doc": "Summary details about the set of active incidents"
    }
  ],
  "doc": "Summary related incidents on an entity."
}
```





#### forms
Forms that are assigned to this entity to be filled out



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| incompleteForms | [FormAssociation](#formassociation)[] | ✓ | All incomplete forms assigned to the entity. | Searchable |
| completedForms | [FormAssociation](#formassociation)[] | ✓ | All complete forms assigned to the entity. | Searchable |
| verifications | FormVerificationAssociation[] | ✓ | Verifications that have been applied to the entity via completed forms. | Searchable |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "forms"
  },
  "name": "Forms",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "Searchable": {
        "/*/completedPrompts/*/id": {
          "fieldName": "incompleteFormsCompletedPromptIds",
          "fieldType": "KEYWORD",
          "queryByDefault": false
        },
        "/*/completedPrompts/*/lastModified/time": {
          "fieldName": "incompleteFormsCompletedPromptResponseTimes",
          "fieldType": "DATETIME",
          "queryByDefault": false
        },
        "/*/incompletePrompts/*/id": {
          "fieldName": "incompleteFormsIncompletePromptIds",
          "fieldType": "KEYWORD",
          "queryByDefault": false
        },
        "/*/urn": {
          "fieldName": "incompleteForms",
          "fieldType": "URN",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "FormAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "urn",
              "doc": "Urn of the applied form"
            },
            {
              "type": {
                "type": "array",
                "items": {
                  "type": "record",
                  "name": "FormPromptAssociation",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "string",
                      "name": "id",
                      "doc": "The id for the prompt. This must be GLOBALLY UNIQUE."
                    },
                    {
                      "type": {
                        "type": "record",
                        "name": "AuditStamp",
                        "namespace": "com.linkedin.common",
                        "fields": [
                          {
                            "type": "long",
                            "name": "time",
                            "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
                          },
                          {
                            "java": {
                              "class": "com.linkedin.common.urn.Urn"
                            },
                            "type": "string",
                            "name": "actor",
                            "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
                          },
                          {
                            "java": {
                              "class": "com.linkedin.common.urn.Urn"
                            },
                            "type": [
                              "null",
                              "string"
                            ],
                            "name": "impersonator",
                            "default": null,
                            "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
                          },
                          {
                            "type": [
                              "null",
                              "string"
                            ],
                            "name": "message",
                            "default": null,
                            "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
                          }
                        ],
                        "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
                      },
                      "name": "lastModified",
                      "doc": "The last time this prompt was touched for the entity (set, unset)"
                    },
                    {
                      "type": [
                        "null",
                        {
                          "type": "record",
                          "name": "FormPromptFieldAssociations",
                          "namespace": "com.linkedin.common",
                          "fields": [
                            {
                              "type": [
                                "null",
                                {
                                  "type": "array",
                                  "items": {
                                    "type": "record",
                                    "name": "FieldFormPromptAssociation",
                                    "namespace": "com.linkedin.common",
                                    "fields": [
                                      {
                                        "type": "string",
                                        "name": "fieldPath",
                                        "doc": "The field path on a schema field."
                                      },
                                      {
                                        "type": "com.linkedin.common.AuditStamp",
                                        "name": "lastModified",
                                        "doc": "The last time this prompt was touched for the field on the entity (set, unset)"
                                      }
                                    ],
                                    "doc": "Information about the status of a particular prompt for a specific schema field\non an entity."
                                  }
                                }
                              ],
                              "name": "completedFieldPrompts",
                              "default": null,
                              "doc": "A list of field-level prompt associations that are not yet complete for this form."
                            },
                            {
                              "type": [
                                "null",
                                {
                                  "type": "array",
                                  "items": "com.linkedin.common.FieldFormPromptAssociation"
                                }
                              ],
                              "name": "incompleteFieldPrompts",
                              "default": null,
                              "doc": "A list of field-level prompt associations that are complete for this form."
                            }
                          ],
                          "doc": "Information about the field-level prompt associations on a top-level prompt association."
                        }
                      ],
                      "name": "fieldAssociations",
                      "default": null,
                      "doc": "Optional information about the field-level prompt associations."
                    }
                  ],
                  "doc": "Information about the status of a particular prompt.\nNote that this is where we can add additional information about individual responses:\nactor, timestamp, and the response itself."
                }
              },
              "name": "incompletePrompts",
              "default": [],
              "doc": "A list of prompts that are not yet complete for this form."
            },
            {
              "type": {
                "type": "array",
                "items": "com.linkedin.common.FormPromptAssociation"
              },
              "name": "completedPrompts",
              "default": [],
              "doc": "A list of prompts that have been completed for this form."
            }
          ],
          "doc": "Properties of an applied form."
        }
      },
      "name": "incompleteForms",
      "doc": "All incomplete forms assigned to the entity."
    },
    {
      "Searchable": {
        "/*/completedPrompts/*/id": {
          "fieldName": "completedFormsCompletedPromptIds",
          "fieldType": "KEYWORD",
          "queryByDefault": false
        },
        "/*/completedPrompts/*/lastModified/time": {
          "fieldName": "completedFormsCompletedPromptResponseTimes",
          "fieldType": "DATETIME",
          "queryByDefault": false
        },
        "/*/incompletePrompts/*/id": {
          "fieldName": "completedFormsIncompletePromptIds",
          "fieldType": "KEYWORD",
          "queryByDefault": false
        },
        "/*/urn": {
          "fieldName": "completedForms",
          "fieldType": "URN",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "com.linkedin.common.FormAssociation"
      },
      "name": "completedForms",
      "doc": "All complete forms assigned to the entity."
    },
    {
      "Searchable": {
        "/*/form": {
          "fieldName": "verifiedForms",
          "fieldType": "URN",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "FormVerificationAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "form",
              "doc": "The urn of the form that granted this verification."
            },
            {
              "type": [
                "null",
                "com.linkedin.common.AuditStamp"
              ],
              "name": "lastModified",
              "default": null,
              "doc": "An audit stamp capturing who and when verification was applied for this form."
            }
          ],
          "doc": "An association between a verification and an entity that has been granted\nvia completion of one or more forms of type 'VERIFICATION'."
        }
      },
      "name": "verifications",
      "default": [],
      "doc": "Verifications that have been applied to the entity via completed forms."
    }
  ],
  "doc": "Forms that are assigned to this entity to be filled out"
}
```





#### testResults
Information about a Test Result



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| failing | [TestResult](#testresult)[] | ✓ | Results that are failing | Searchable, → IsFailing |
| passing | [TestResult](#testresult)[] | ✓ | Results that are passing | Searchable, → IsPassing |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "testResults"
  },
  "name": "TestResults",
  "namespace": "com.linkedin.test",
  "fields": [
    {
      "Relationship": {
        "/*/test": {
          "entityTypes": [
            "test"
          ],
          "name": "IsFailing"
        }
      },
      "Searchable": {
        "/*/test": {
          "fieldName": "failingTests",
          "fieldType": "URN",
          "hasValuesFieldName": "hasFailingTests",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "TestResult",
          "namespace": "com.linkedin.test",
          "fields": [
            {
              "java": {
                "class": "com.linkedin.common.urn.Urn"
              },
              "type": "string",
              "name": "test",
              "doc": "The urn of the test"
            },
            {
              "type": {
                "type": "enum",
                "symbolDocs": {
                  "FAILURE": " The Test Failed",
                  "SUCCESS": " The Test Succeeded"
                },
                "name": "TestResultType",
                "namespace": "com.linkedin.test",
                "symbols": [
                  "SUCCESS",
                  "FAILURE"
                ]
              },
              "name": "type",
              "doc": "The type of the result"
            },
            {
              "type": [
                "null",
                "string"
              ],
              "name": "testDefinitionMd5",
              "default": null,
              "doc": "The md5 of the test definition that was used to compute this result.\nSee TestInfo.testDefinition.md5 for more information."
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "AuditStamp",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When did the resource/association/sub-resource move into the specific lifecycle stage represented by this AuditEvent."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) which will be credited for moving the resource/association/sub-resource into the specific lifecycle stage. It is also the one used to authorize the change."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "impersonator",
                      "default": null,
                      "doc": "The entity (e.g. a service URN) which performs the change on behalf of the Actor and must be authorized to act as the Actor."
                    },
                    {
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "message",
                      "default": null,
                      "doc": "Additional context around how DataHub was informed of the particular change. For example: was the change created by an automated process, or manually."
                    }
                  ],
                  "doc": "Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage."
                }
              ],
              "name": "lastComputed",
              "default": null,
              "doc": "The audit stamp of when the result was computed, including the actor who computed it."
            }
          ],
          "doc": "Information about a Test Result"
        }
      },
      "name": "failing",
      "doc": "Results that are failing"
    },
    {
      "Relationship": {
        "/*/test": {
          "entityTypes": [
            "test"
          ],
          "name": "IsPassing"
        }
      },
      "Searchable": {
        "/*/test": {
          "fieldName": "passingTests",
          "fieldType": "URN",
          "hasValuesFieldName": "hasPassingTests",
          "queryByDefault": false
        }
      },
      "type": {
        "type": "array",
        "items": "com.linkedin.test.TestResult"
      },
      "name": "passing",
      "doc": "Results that are passing"
    }
  ],
  "doc": "Information about a Test Result"
}
```





#### documentation
Aspect used for storing all applicable documentations on assets.
This aspect supports multiple documentations from different sources.
There is an implicit assumption that there is only one documentation per
   source.
For example, if there are two documentations from the same source, the
   latest one will overwrite the previous one.
If there are two documentations from different sources, both will be
   stored.
Future evolution considerations:
The first entity that uses this aspect is Schema Field. We will expand this
    aspect to other entities eventually.
The values of the documentation are not currently searchable. This will be
    changed once this aspect develops opinion on which documentation entry is
    the authoritative one.
Ensuring that there is only one documentation per source is a business
    rule that is not enforced by the aspect yet. This will currently be enforced by the
    application that uses this aspect. We will eventually enforce this rule in
    the aspect using AspectMutators.



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| documentations | DocumentationAssociation[] | ✓ | Documentations associated with this asset. We could be receiving docs from different sources |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "documentation"
  },
  "name": "Documentation",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "DocumentationAssociation",
          "namespace": "com.linkedin.common",
          "fields": [
            {
              "type": "string",
              "name": "documentation",
              "doc": "Description of this asset"
            },
            {
              "Searchable": {
                "/actor": {
                  "fieldName": "documentationAttributionActors",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/source": {
                  "fieldName": "documentationAttributionSources",
                  "fieldType": "URN",
                  "queryByDefault": false
                },
                "/time": {
                  "fieldName": "documentationAttributionDates",
                  "fieldType": "DATETIME",
                  "queryByDefault": false
                }
              },
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "MetadataAttribution",
                  "namespace": "com.linkedin.common",
                  "fields": [
                    {
                      "type": "long",
                      "name": "time",
                      "doc": "When this metadata was updated."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": "string",
                      "name": "actor",
                      "doc": "The entity (e.g. a member URN) responsible for applying the assocated metadata. This can\neither be a user (in case of UI edits) or the datahub system for automation."
                    },
                    {
                      "java": {
                        "class": "com.linkedin.common.urn.Urn"
                      },
                      "type": [
                        "null",
                        "string"
                      ],
                      "name": "source",
                      "default": null,
                      "doc": "The DataHub source responsible for applying the associated metadata. This will only be filled out\nwhen a DataHub source is responsible. This includes the specific metadata test urn, the automation urn."
                    },
                    {
                      "type": {
                        "type": "map",
                        "values": "string"
                      },
                      "name": "sourceDetail",
                      "default": {},
                      "doc": "The details associated with why this metadata was applied. For example, this could include\nthe actual regex rule, sql statement, ingestion pipeline ID, etc."
                    }
                  ],
                  "doc": "Information about who, why, and how this metadata was applied"
                }
              ],
              "name": "attribution",
              "default": null,
              "doc": "Information about who, why, and how this metadata was applied"
            }
          ],
          "doc": "Properties of applied documentation including the attribution of the doc"
        }
      },
      "name": "documentations",
      "doc": "Documentations associated with this asset. We could be receiving docs from different sources"
    }
  ],
  "doc": "Aspect used for storing all applicable documentations on assets.\nThis aspect supports multiple documentations from different sources.\nThere is an implicit assumption that there is only one documentation per\n   source.\nFor example, if there are two documentations from the same source, the\n   latest one will overwrite the previous one.\nIf there are two documentations from different sources, both will be\n   stored.\nFuture evolution considerations:\nThe first entity that uses this aspect is Schema Field. We will expand this\n    aspect to other entities eventually.\nThe values of the documentation are not currently searchable. This will be\n    changed once this aspect develops opinion on which documentation entry is\n    the authoritative one.\nEnsuring that there is only one documentation per source is a business\n    rule that is not enforced by the aspect yet. This will currently be enforced by the\n    application that uses this aspect. We will eventually enforce this rule in\n    the aspect using AspectMutators."
}
```





#### access
Aspect used for associating roles to a dataset or any asset



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| roles | RoleAssociation[] |  | List of Roles which needs to be associated |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "access"
  },
  "name": "Access",
  "namespace": "com.linkedin.common",
  "fields": [
    {
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "RoleAssociation",
            "namespace": "com.linkedin.common",
            "fields": [
              {
                "Relationship": {
                  "entityTypes": [
                    "role"
                  ],
                  "name": "AssociatedWith"
                },
                "Searchable": {
                  "addToFilters": true,
                  "fieldName": "roles",
                  "fieldType": "URN",
                  "filterNameOverride": "Role",
                  "hasValuesFieldName": "hasRoles",
                  "queryByDefault": false
                },
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": "string",
                "name": "urn",
                "doc": "Urn of the External Role"
              }
            ],
            "doc": "Properties of an applied Role. For now, just an Urn"
          }
        }
      ],
      "name": "roles",
      "default": null,
      "doc": "List of Roles which needs to be associated"
    }
  ],
  "doc": "Aspect used for associating roles to a dataset or any asset"
}
```





#### dashboardUsageStatistics (Timeseries)
Experimental (Subject to breaking change) -- Stats corresponding to dashboard's usage.

If this aspect represents the latest snapshot of the statistics about a Dashboard, the eventGranularity field should be null. 
If this aspect represents a bucketed window of usage statistics (e.g. over a day), then the eventGranularity field should be set accordingly. 



#### Fields



| Field | Type | Required | Description | Annotations |
|-------|------|----------|-------------|-------------|
| timestampMillis | long | ✓ | The event timestamp field as epoch at UTC in milli seconds. |  |
| eventGranularity | TimeWindowSize |  | Granularity of the event if applicable |  |
| partitionSpec | PartitionSpec |  | The optional partition specification. |  |
| messageId | string |  | The optional messageId, if provided serves as a custom user-defined unique identifier for an aspe... |  |
| viewsCount | int |  | The total number of times dashboard has been viewed |  |
| executionsCount | int |  | The total number of dashboard executions (refreshes / syncs) |  |
| uniqueUserCount | int |  | Unique user count |  |
| userCounts | DashboardUserUsageCounts[] |  | Users within this bucket, with frequency counts |  |
| favoritesCount | int |  | The total number of times that the dashboard has been favorited |  |
| lastViewedAt | long |  | Last viewed at  This should not be set in cases where statistics are windowed. |  |



#### Raw Schema


```javascript
{
  "type": "record",
  "Aspect": {
    "name": "dashboardUsageStatistics",
    "type": "timeseries"
  },
  "name": "DashboardUsageStatistics",
  "namespace": "com.linkedin.dashboard",
  "fields": [
    {
      "type": "long",
      "name": "timestampMillis",
      "doc": "The event timestamp field as epoch at UTC in milli seconds."
    },
    {
      "type": [
        "null",
        {
          "type": "record",
          "name": "TimeWindowSize",
          "namespace": "com.linkedin.timeseries",
          "fields": [
            {
              "type": {
                "type": "enum",
                "name": "CalendarInterval",
                "namespace": "com.linkedin.timeseries",
                "symbols": [
                  "SECOND",
                  "MINUTE",
                  "HOUR",
                  "DAY",
                  "WEEK",
                  "MONTH",
                  "QUARTER",
                  "YEAR"
                ]
              },
              "name": "unit",
              "doc": "Interval unit such as minute/hour/day etc."
            },
            {
              "type": "int",
              "name": "multiple",
              "default": 1,
              "doc": "How many units. Defaults to 1."
            }
          ],
          "doc": "Defines the size of a time window."
        }
      ],
      "name": "eventGranularity",
      "default": null,
      "doc": "Granularity of the event if applicable"
    },
    {
      "type": [
        {
          "type": "record",
          "name": "PartitionSpec",
          "namespace": "com.linkedin.timeseries",
          "fields": [
            {
              "TimeseriesField": {},
              "type": "string",
              "name": "partition",
              "doc": "A unique id / value for the partition for which statistics were collected,\ngenerated by applying the key definition to a given row."
            },
            {
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "TimeWindow",
                  "namespace": "com.linkedin.timeseries",
                  "fields": [
                    {
                      "type": "long",
                      "name": "startTimeMillis",
                      "doc": "Start time as epoch at UTC."
                    },
                    {
                      "type": "com.linkedin.timeseries.TimeWindowSize",
                      "name": "length",
                      "doc": "The length of the window."
                    }
                  ]
                }
              ],
              "name": "timePartition",
              "default": null,
              "doc": "Time window of the partition, if we are able to extract it from the partition key."
            },
            {
              "deprecated": true,
              "type": {
                "type": "enum",
                "name": "PartitionType",
                "namespace": "com.linkedin.timeseries",
                "symbols": [
                  "FULL_TABLE",
                  "QUERY",
                  "PARTITION"
                ]
              },
              "name": "type",
              "default": "PARTITION",
              "doc": "Unused!"
            }
          ],
          "doc": "A reference to a specific partition in a dataset."
        },
        "null"
      ],
      "name": "partitionSpec",
      "default": {
        "partition": "FULL_TABLE_SNAPSHOT",
        "type": "FULL_TABLE",
        "timePartition": null
      },
      "doc": "The optional partition specification."
    },
    {
      "type": [
        "null",
        "string"
      ],
      "name": "messageId",
      "default": null,
      "doc": "The optional messageId, if provided serves as a custom user-defined unique identifier for an aspect value."
    },
    {
      "TimeseriesField": {},
      "type": [
        "null",
        "int"
      ],
      "name": "viewsCount",
      "default": null,
      "doc": "The total number of times dashboard has been viewed"
    },
    {
      "TimeseriesField": {},
      "type": [
        "null",
        "int"
      ],
      "name": "executionsCount",
      "default": null,
      "doc": "The total number of dashboard executions (refreshes / syncs) "
    },
    {
      "TimeseriesField": {},
      "type": [
        "null",
        "int"
      ],
      "name": "uniqueUserCount",
      "default": null,
      "doc": "Unique user count"
    },
    {
      "TimeseriesFieldCollection": {
        "key": "user"
      },
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "DashboardUserUsageCounts",
            "namespace": "com.linkedin.dashboard",
            "fields": [
              {
                "java": {
                  "class": "com.linkedin.common.urn.Urn"
                },
                "type": "string",
                "name": "user",
                "doc": "The unique id of the user."
              },
              {
                "TimeseriesField": {},
                "type": [
                  "null",
                  "int"
                ],
                "name": "viewsCount",
                "default": null,
                "doc": "The number of times the user has viewed the dashboard"
              },
              {
                "TimeseriesField": {},
                "type": [
                  "null",
                  "int"
                ],
                "name": "executionsCount",
                "default": null,
                "doc": "The number of times the user has executed (refreshed) the dashboard"
              },
              {
                "TimeseriesField": {},
                "type": [
                  "null",
                  "int"
                ],
                "name": "usageCount",
                "default": null,
                "doc": "Normalized numeric metric representing user's dashboard usage -- the number of times the user executed or viewed the dashboard. "
              },
              {
                "TimeseriesField": {},
                "type": [
                  "null",
                  "string"
                ],
                "name": "userEmail",
                "default": null,
                "doc": "If user_email is set, we attempt to resolve the user's urn upon ingest"
              }
            ],
            "doc": "Records a single user's usage counts for a given resource"
          }
        }
      ],
      "name": "userCounts",
      "default": null,
      "doc": "Users within this bucket, with frequency counts"
    },
    {
      "TimeseriesField": {},
      "type": [
        "null",
        "int"
      ],
      "name": "favoritesCount",
      "default": null,
      "doc": "The total number of times that the dashboard has been favorited "
    },
    {
      "TimeseriesField": {},
      "type": [
        "null",
        "long"
      ],
      "name": "lastViewedAt",
      "default": null,
      "doc": "Last viewed at\n\nThis should not be set in cases where statistics are windowed. "
    }
  ],
  "doc": "Experimental (Subject to breaking change) -- Stats corresponding to dashboard's usage.\n\nIf this aspect represents the latest snapshot of the statistics about a Dashboard, the eventGranularity field should be null. \nIf this aspect represents a bucketed window of usage statistics (e.g. over a day), then the eventGranularity field should be set accordingly. "
}
```





### Common Types

These types are used across multiple aspects in this entity.

#### AuditStamp

Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into a particular lifecycle stage, and who acted to move it into that specific lifecycle stage.

**Fields:**

- `time` (long): When did the resource/association/sub-resource move into the specific lifecyc...
- `actor` (string): The entity (e.g. a member URN) which will be credited for moving the resource...
- `impersonator` (string?): The entity (e.g. a service URN) which performs the change on behalf of the Ac...
- `message` (string?): Additional context around how DataHub was informed of the particular change. ...

#### ChangeAuditStamps

Data captured on a resource/association/sub-resource level giving insight into when that resource/association/sub-resource moved into various lifecycle stages, and who acted to move it into those lifecycle stages. The recommended best practice is to include this record in your record schema, and annotate its fields as @readOnly in your resource. See https://github.com/linkedin/rest.li/wiki/Validation-in-Rest.li#restli-validation-annotations

**Fields:**

- `created` (AuditStamp): An AuditStamp corresponding to the creation of this resource/association/sub-...
- `lastModified` (AuditStamp): An AuditStamp corresponding to the last modification of this resource/associa...
- `deleted` (AuditStamp?): An AuditStamp corresponding to the deletion of this resource/association/sub-...

#### Edge

A common structure to represent all edges to entities when used inside aspects as collections
This ensures that all edges have common structure around audit-stamps and will support PATCH, time-travel automatically.

**Fields:**

- `sourceUrn` (string?): Urn of the source of this relationship edge. If not specified, assumed to be ...
- `destinationUrn` (string): Urn of the destination of this relationship edge.
- `created` (AuditStamp?): Audit stamp containing who created this relationship edge and when
- `lastModified` (AuditStamp?): Audit stamp containing who last modified this relationship edge and when
- `properties` (map?): A generic properties bag that allows us to store specific information on this...

#### FormAssociation

Properties of an applied form.

**Fields:**

- `urn` (string): Urn of the applied form
- `incompletePrompts` (FormPromptAssociation[]): A list of prompts that are not yet complete for this form.
- `completedPrompts` (FormPromptAssociation[]): A list of prompts that have been completed for this form.

#### IncidentSummaryDetails

Summary statistics about incidents on an entity.

**Fields:**

- `urn` (string): The urn of the incident
- `type` (string): The type of an incident
- `createdAt` (long): The time at which the incident was raised in milliseconds since epoch.
- `resolvedAt` (long?): The time at which the incident was marked as resolved in milliseconds since e...
- `priority` (int?): The priority of the incident

#### TestResult

Information about a Test Result

**Fields:**

- `test` (string): The urn of the test
- `type` (TestResultType): The type of the result
- `testDefinitionMd5` (string?): The md5 of the test definition that was used to compute this result. See Test...
- `lastComputed` (AuditStamp?): The audit stamp of when the result was computed, including the actor who comp...


### Relationships

#### Self
These are the relationships to itself, stored in this entity's aspects
- DashboardContainsDashboard (via `dashboardInfo.dashboards`)
#### Outgoing
These are the relationships stored in this entity's aspects
- Contains

   - Chart via `dashboardInfo.charts`
   - Chart via `dashboardInfo.chartEdges`
- Consumes

   - Dataset via `dashboardInfo.datasets`
   - Dataset via `dashboardInfo.datasetEdges`
- OwnedBy

   - Corpuser via `ownership.owners.owner`
   - CorpGroup via `ownership.owners.owner`
- ownershipType

   - OwnershipType via `ownership.owners.typeUrn`
- TaggedWith

   - Tag via `globalTags.tags`
   - Tag via `inputFields.fields.schemaField.globalTags.tags`
- TermedWith

   - GlossaryTerm via `glossaryTerms.terms.urn`
   - GlossaryTerm via `inputFields.fields.schemaField.glossaryTerms.terms.urn`
- AssociatedWith

   - Domain via `domains.domains`
   - Application via `applications.applications`
   - Role via `access.roles.urn`
- IsPartOf

   - Container via `container.container`
- consumesField

   - SchemaField via `inputFields.fields.schemaFieldUrn`
- SchemaFieldTaggedWith

   - Tag via `inputFields.fields.schemaField.globalTags`
- SchemaFieldWithGlossaryTerm

   - GlossaryTerm via `inputFields.fields.schemaField.glossaryTerms`
- ResolvedIncidents

   - Incident via `incidentsSummary.resolvedIncidentDetails`
- ActiveIncidents

   - Incident via `incidentsSummary.activeIncidentDetails`
- IsFailing

   - Test via `testResults.failing`
- IsPassing

   - Test via `testResults.passing`
### [Global Metadata Model](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)
![Global Graph](https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png)


:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-models](https://github.com/datahub-project/datahub/tree/master/metadata-models) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
