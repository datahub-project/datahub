# Dashboard Entity

The Dashboard entity represents collections of visualizations and reports in BI tools (e.g., Looker, Tableau, PowerBI). This guide covers comprehensive dashboard operations in SDK V2.

## Creating a Dashboard

### Minimal Dashboard

Only tool and id are required:

```java
Dashboard dashboard = Dashboard.builder()
    .tool("looker")
    .id("my_sales_dashboard")
    .build();
```

### With Metadata

Add title and description at construction:

```java
Dashboard dashboard = Dashboard.builder()
    .tool("tableau")
    .id("executive_dashboard")
    .title("Executive KPI Dashboard")
    .description("Real-time executive dashboard showing key business metrics")
    .build();
```

### With Custom Properties

Include custom properties in builder:

```java
Map<String, String> props = new HashMap<>();
props.put("team", "business-intelligence");
props.put("refresh_schedule", "hourly");

Dashboard dashboard = Dashboard.builder()
    .tool("powerbi")
    .id("sales_dashboard")
    .title("Sales Performance")
    .customProperties(props)
    .build();
```

## URN Construction

Dashboard URNs follow the pattern:

```
urn:li:dashboard:({tool},{id})
```

**Automatic URN creation:**

```java
Dashboard dashboard = Dashboard.builder()
    .tool("looker")
    .id("regional_sales")
    .build();

DashboardUrn urn = dashboard.getDashboardUrn();
// urn:li:dashboard:(looker,regional_sales)
```

## Supported BI Tools

Common tool identifiers:

- `looker` - Looker
- `tableau` - Tableau
- `powerbi` - Power BI
- `superset` - Apache Superset
- `metabase` - Metabase
- `redash` - Redash
- `mode` - Mode Analytics
- `quicksight` - Amazon QuickSight
- `thoughtspot` - ThoughtSpot

## Tags

### Adding Tags

```java
// Simple tag name (auto-prefixed)
dashboard.addTag("executive");
// Creates: urn:li:tag:executive

// Full tag URN
dashboard.addTag("urn:li:tag:production");
```

### Removing Tags

```java
dashboard.removeTag("executive");
dashboard.removeTag("urn:li:tag:production");
```

### Tag Chaining

```java
dashboard.addTag("executive")
         .addTag("real-time")
         .addTag("kpi");
```

## Owners

### Adding Owners

```java
import com.linkedin.common.OwnershipType;

// Business owner
dashboard.addOwner(
    "urn:li:corpuser:john_doe",
    OwnershipType.BUSINESS_OWNER
);

// Technical owner
dashboard.addOwner(
    "urn:li:corpuser:bi_team",
    OwnershipType.TECHNICAL_OWNER
);

// Data steward
dashboard.addOwner(
    "urn:li:corpuser:governance",
    OwnershipType.DATA_STEWARD
);
```

### Removing Owners

```java
dashboard.removeOwner("urn:li:corpuser:john_doe");
```

### Owner Types

Available ownership types:

- `BUSINESS_OWNER` - Business stakeholder
- `TECHNICAL_OWNER` - Maintains the technical implementation
- `DATA_STEWARD` - Manages data quality and compliance
- `DATAOWNER` - Generic data owner
- `DEVELOPER` - Software developer
- `PRODUCER` - Dashboard producer/creator
- `CONSUMER` - Dashboard consumer
- `STAKEHOLDER` - Other stakeholder

## Glossary Terms

### Adding Terms

```java
dashboard.addTerm("urn:li:glossaryTerm:ExecutiveMetrics");
dashboard.addTerm("urn:li:glossaryTerm:BusinessIntelligence");
```

### Removing Terms

```java
dashboard.removeTerm("urn:li:glossaryTerm:ExecutiveMetrics");
```

### Term Chaining

```java
dashboard.addTerm("urn:li:glossaryTerm:KeyPerformanceIndicator")
         .addTerm("urn:li:glossaryTerm:SalesMetrics")
         .addTerm("urn:li:glossaryTerm:RealTimeData");
```

## Domain

### Setting Domain

```java
dashboard.setDomain("urn:li:domain:Sales");
```

### Removing Domain

```java
// Remove a specific domain
dashboard.removeDomain("urn:li:domain:Sales");

// Or clear all domains
dashboard.clearDomains();
```

## Custom Properties

### Adding Individual Properties

```java
dashboard.addCustomProperty("team", "sales-operations");
dashboard.addCustomProperty("refresh_schedule", "hourly");
dashboard.addCustomProperty("data_source", "snowflake");
```

### Setting All Properties

Replace all custom properties:

```java
Map<String, String> properties = new HashMap<>();
properties.put("team", "business-intelligence");
properties.put("refresh_schedule", "real-time");
properties.put("access_level", "executive");

dashboard.setCustomProperties(properties);
```

### Removing Properties

```java
dashboard.removeCustomProperty("refresh_schedule");
```

## Reading Dashboard Metadata

### Get Title

```java
String title = dashboard.getTitle();
```

### Get Description

```java
String description = dashboard.getDescription();
```

## Lineage Operations

Dashboard lineage represents the data sources (datasets) that feed into the dashboard. This creates upstream lineage relationships from the dashboard to its source datasets.

### Adding Input Datasets

Add datasets one at a time:

```java
// Using DatasetUrn
DatasetUrn dataset = DatasetUrn.createFromString(
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales.orders,PROD)"
);
dashboard.addInputDataset(dataset);

// Using string URN
dashboard.addInputDataset(
    "urn:li:dataset:(urn:li:dataPlatform:bigquery,marketing.campaigns,PROD)"
);
```

### Setting Input Datasets

Replace all input datasets at once:

```java
List<DatasetUrn> datasets = Arrays.asList(
    DatasetUrn.createFromString(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales.orders,PROD)"
    ),
    DatasetUrn.createFromString(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales.customers,PROD)"
    )
);

dashboard.setInputDatasets(datasets);
```

### Removing Input Datasets

```java
// Using DatasetUrn
DatasetUrn dataset = DatasetUrn.createFromString(
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales.orders,PROD)"
);
dashboard.removeInputDataset(dataset);

// Using string URN
dashboard.removeInputDataset(
    "urn:li:dataset:(urn:li:dataPlatform:bigquery,marketing.campaigns,PROD)"
);
```

### Getting Input Datasets

Retrieve all input datasets:

```java
List<DatasetUrn> inputDatasets = dashboard.getInputDatasets();
for (DatasetUrn dataset : inputDatasets) {
    System.out.println("Dataset: " + dataset);
}
```

### Lineage Chaining

```java
dashboard.addInputDataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,sales.orders,PROD)")
         .addInputDataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,sales.customers,PROD)")
         .addInputDataset("urn:li:dataset:(urn:li:dataPlatform:salesforce,Account,PROD)");
```

## Chart Relationships

Chart relationships represent the visualizations embedded in a dashboard. This creates "Contains" relationships between the dashboard and its charts.

### Adding Charts

Add charts one at a time:

```java
// Using ChartUrn
ChartUrn chart = new ChartUrn("tableau", "revenue_chart");
dashboard.addChart(chart);

// Using string URN
dashboard.addChart("urn:li:chart:(looker,sales_performance_chart)");
```

### Setting Charts

Replace all charts at once:

```java
List<ChartUrn> charts = Arrays.asList(
    new ChartUrn("tableau", "revenue_chart"),
    new ChartUrn("tableau", "customer_satisfaction_chart"),
    new ChartUrn("tableau", "regional_breakdown_chart")
);

dashboard.setCharts(charts);
```

### Removing Charts

```java
// Using ChartUrn
ChartUrn chart = new ChartUrn("tableau", "revenue_chart");
dashboard.removeChart(chart);

// Using string URN
dashboard.removeChart("urn:li:chart:(looker,sales_performance_chart)");
```

### Getting Charts

Retrieve all charts:

```java
List<ChartUrn> charts = dashboard.getCharts();
for (ChartUrn chart : charts) {
    System.out.println("Chart: " + chart);
}
```

### Chart Chaining

```java
dashboard.addChart(new ChartUrn("looker", "revenue_chart"))
         .addChart(new ChartUrn("looker", "customer_chart"))
         .addChart(new ChartUrn("looker", "product_chart"));
```

## Dashboard-Specific Properties

### Dashboard URL

Set a direct link to the dashboard in its native BI tool:

```java
// Set dashboard URL
dashboard.setDashboardUrl("https://tableau.company.com/views/sales-dashboard");

// Get dashboard URL
String url = dashboard.getDashboardUrl();
```

### Last Refreshed

Track when the dashboard data was last updated:

```java
// Set last refreshed timestamp (milliseconds since epoch)
long currentTime = System.currentTimeMillis();
dashboard.setLastRefreshed(currentTime);

// Get last refreshed timestamp
Long lastRefreshed = dashboard.getLastRefreshed();
if (lastRefreshed != null) {
    System.out.println("Last refreshed at: " + new Date(lastRefreshed));
}
```

### Combined Dashboard Properties

```java
dashboard.setDashboardUrl("https://looker.company.com/dashboards/executive")
         .setLastRefreshed(System.currentTimeMillis());
```

## Complete Example

```java
import datahub.client.v2.DataHubClientV2;
import datahub.client.v2.entity.Dashboard;
import com.linkedin.common.OwnershipType;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class DashboardExample {
    public static void main(String[] args) {
        // Create client
        DataHubClientV2 client = DataHubClientV2.builder()
            .server("http://localhost:8080")
            .build();

        try {
            // Build dashboard with all metadata
            Dashboard dashboard = Dashboard.builder()
                .tool("looker")
                .id("sales_performance_dashboard")
                .title("Sales Performance Dashboard")
                .description("Executive dashboard showing key sales metrics and regional performance")
                .build();

            // Add tags
            dashboard.addTag("executive")
                     .addTag("sales")
                     .addTag("production");

            // Add owners
            dashboard.addOwner("urn:li:corpuser:sales_team", OwnershipType.BUSINESS_OWNER)
                     .addOwner("urn:li:corpuser:bi_team", OwnershipType.TECHNICAL_OWNER);

            // Add glossary terms
            dashboard.addTerm("urn:li:glossaryTerm:SalesMetrics")
                     .addTerm("urn:li:glossaryTerm:ExecutiveDashboard");

            // Set domain
            dashboard.setDomain("urn:li:domain:Sales");

            // Add custom properties
            dashboard.addCustomProperty("team", "sales-operations")
                     .addCustomProperty("refresh_schedule", "hourly")
                     .addCustomProperty("data_source", "snowflake");

            // Upsert to DataHub
            client.entities().upsert(dashboard);

            System.out.println("Successfully created dashboard: " + dashboard.getUrn());

        } catch (IOException | ExecutionException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            try {
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
```

## Updating Existing Dashboards

### Load and Modify

```java
// Load existing dashboard
DashboardUrn urn = new DashboardUrn("looker", "my_dashboard");
Dashboard dashboard = client.entities().get(urn);

// Add new metadata (creates patches)
dashboard.addTag("new-tag")
         .addOwner("urn:li:corpuser:new_owner", OwnershipType.TECHNICAL_OWNER);

// Apply patches
client.entities().update(dashboard);
```

### Incremental Updates

```java
// Just add what you need
dashboard.addTag("real-time");
client.entities().update(dashboard);

// Later, add more
dashboard.addCustomProperty("updated_at", String.valueOf(System.currentTimeMillis()));
client.entities().update(dashboard);
```

## Builder Options Reference

| Method                  | Required | Description                                    |
| ----------------------- | -------- | ---------------------------------------------- |
| `tool(String)`          | ✅ Yes   | BI tool identifier (e.g., "looker", "tableau") |
| `id(String)`            | ✅ Yes   | Dashboard identifier within the tool           |
| `title(String)`         | No       | Dashboard title                                |
| `description(String)`   | No       | Dashboard description                          |
| `customProperties(Map)` | No       | Map of custom key-value properties             |

## Patch-Based Operations

Dashboard uses patch-based updates for metadata operations. All methods like `addTag()`, `addOwner()`, etc. create patches that are accumulated until `upsert()` or `update()` is called.

**Benefits:**

- **Efficient**: Multiple operations batched into fewer API calls
- **Atomic**: All changes succeed or fail together
- **Incremental**: Only specified fields are modified, others remain unchanged

**Example:**

```java
Dashboard dashboard = Dashboard.builder()
    .tool("tableau")
    .id("sales_dashboard")
    .build();

// These create patches (no API calls yet)
dashboard.addTag("production");
dashboard.addOwner("urn:li:corpuser:owner", OwnershipType.BUSINESS_OWNER);
dashboard.setDomain("urn:li:domain:Sales");

// Single API call emits all patches
client.entities().upsert(dashboard);
```

## Common Patterns

### Creating Multiple Dashboards

```java
List<String> dashboardIds = Arrays.asList("dashboard1", "dashboard2", "dashboard3");

for (String dashboardId : dashboardIds) {
    Dashboard dashboard = Dashboard.builder()
        .tool("looker")
        .id(dashboardId)
        .title("Dashboard " + dashboardId)
        .build();

    dashboard.addTag("auto-generated")
             .addCustomProperty("created_by", "sync_job");

    client.entities().upsert(dashboard);
}
```

### Batch Metadata Addition

```java
Dashboard dashboard = Dashboard.builder()
    .tool("tableau")
    .id("executive_dashboard")
    .build();

List<String> tags = Arrays.asList("executive", "kpi", "real-time", "production");
tags.forEach(dashboard::addTag);

client.entities().upsert(dashboard);  // Emits all tags in one call
```

### Conditional Metadata

```java
if (isExecutiveDashboard(dashboard)) {
    dashboard.addTag("executive")
             .addTerm("urn:li:glossaryTerm:ExecutiveMetrics");
}

if (requiresGovernance(dashboard)) {
    dashboard.addOwner("urn:li:corpuser:governance_team", OwnershipType.DATA_STEWARD);
}
```

### Dashboard with Full Lineage Context

```java
// Create dashboard with rich metadata
Dashboard dashboard = Dashboard.builder()
    .tool("looker")
    .id("customer_360_dashboard")
    .title("Customer 360 Dashboard")
    .description("Comprehensive customer analytics dashboard")
    .build();

// Add business context
dashboard.addTerm("urn:li:glossaryTerm:CustomerAnalytics")
         .addTerm("urn:li:glossaryTerm:BusinessIntelligence")
         .setDomain("urn:li:domain:Customer");

// Add input datasets for lineage
dashboard.addInputDataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,customer.profile,PROD)")
         .addInputDataset("urn:li:dataset:(urn:li:dataPlatform:salesforce,Account,PROD)")
         .addInputDataset("urn:li:dataset:(urn:li:dataPlatform:zendesk,Tickets,PROD)");

// Add embedded charts
dashboard.addChart(new ChartUrn("looker", "customer_segmentation_chart"))
         .addChart(new ChartUrn("looker", "lifetime_value_chart"))
         .addChart(new ChartUrn("looker", "support_tickets_chart"));

// Add operational metadata
dashboard.addCustomProperty("data_sources", "snowflake,salesforce,zendesk")
         .addCustomProperty("refresh_schedule", "every_15_minutes")
         .addCustomProperty("sla_tier", "tier1")
         .addCustomProperty("business_criticality", "high");

// Set dashboard properties
dashboard.setDashboardUrl("https://looker.company.com/dashboards/customer-360")
         .setLastRefreshed(System.currentTimeMillis());

// Add ownership and governance
dashboard.addOwner("urn:li:corpuser:product_team", OwnershipType.BUSINESS_OWNER)
         .addOwner("urn:li:corpuser:bi_team", OwnershipType.TECHNICAL_OWNER)
         .addTag("production")
         .addTag("customer-facing");

client.entities().upsert(dashboard);
```

## Comparison with Chart Entity

Dashboard and Chart are similar but serve different purposes:

| Feature          | Dashboard                     | Chart                     |
| ---------------- | ----------------------------- | ------------------------- |
| Purpose          | Collection of visualizations  | Single visualization      |
| URN Pattern      | `(tool,id)`                   | `(tool,id)`               |
| Patch Operations | ✅ Full support               | ✅ Full support           |
| Common Use Cases | Executive dashboards, reports | Individual graphs, charts |

## Next Steps

- **[Chart Entity Guide](./chart-entity.md)** - Working with chart entities
- **[Dataset Entity Guide](./dataset-entity.md)** - Working with dataset entities
- **[Patch Operations](./patch-operations.md)** - Deep dive into patches
- **[Migration Guide](./migration-from-v1.md)** - Upgrading from V1

## Examples

### Basic Dashboard Creation

```java
{{ inline /metadata-integration/java/examples/src/main/java/io/datahubproject/examples/v2/DashboardCreateExample.java show_path_as_comment }}
```

### Comprehensive Dashboard with Metadata

```java
{{ inline /metadata-integration/java/examples/src/main/java/io/datahubproject/examples/v2/DashboardFullExample.java show_path_as_comment }}
```

### Dashboard with Lineage and Relationships

```java
{{ inline /metadata-integration/java/examples/src/main/java/io/datahubproject/examples/v2/DashboardLineageExample.java show_path_as_comment }}
```
