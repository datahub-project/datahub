
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
# Inlined from /metadata-integration/java/examples/src/main/java/io/datahubproject/examples/v2/DashboardCreateExample.java
package io.datahubproject.examples.v2;

import com.linkedin.common.OwnershipType;
import datahub.client.v2.DataHubClientV2;
import datahub.client.v2.entity.Dashboard;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Example demonstrating how to create a Dashboard using Java SDK V2.
 *
 * <p>This example shows:
 *
 * <ul>
 *   <li>Creating a DataHubClientV2
 *   <li>Building a Dashboard with fluent builder
 *   <li>Adding tags, owners, and custom properties
 *   <li>Upserting to DataHub
 * </ul>
 */
public class DashboardCreateExample {

  public static void main(String[] args)
      throws IOException, ExecutionException, InterruptedException {
    // Create client (use environment variables or pass explicit values)
    DataHubClientV2 client =
        DataHubClientV2.builder()
            .server(System.getenv().getOrDefault("DATAHUB_SERVER", "http://localhost:8080"))
            .token(System.getenv("DATAHUB_TOKEN")) // Optional
            .build();

    try {
      // Test connection
      if (!client.testConnection()) {
        System.err.println("Failed to connect to DataHub server");
        return;
      }
      System.out.println("✓ Connected to DataHub");

      // Build dashboard with metadata
      Dashboard dashboard =
          Dashboard.builder()
              .tool("looker")
              .id("sales_performance_dashboard")
              .title("Sales Performance Dashboard")
              .description(
                  "Executive dashboard showing key sales metrics including revenue, growth, and regional performance")
              .build();

      System.out.println("✓ Built dashboard with URN: " + dashboard.getUrn());

      // Add tags
      dashboard.addTag("executive").addTag("sales").addTag("production");

      System.out.println("✓ Added 3 tags");

      // Add owners
      dashboard
          .addOwner("urn:li:corpuser:datahub", OwnershipType.BUSINESS_OWNER)
          .addOwner("urn:li:corpuser:data_team", OwnershipType.TECHNICAL_OWNER);

      System.out.println("✓ Added 2 owners");

      // Add custom properties
      dashboard
          .addCustomProperty("team", "sales-operations")
          .addCustomProperty("refresh_schedule", "hourly")
          .addCustomProperty("data_source", "snowflake")
          .addCustomProperty("dashboard_type", "executive");

      System.out.println("✓ Added 4 custom properties");

      // Upsert to DataHub
      client.entities().upsert(dashboard);

      System.out.println("✓ Successfully created dashboard in DataHub!");
      System.out.println("\n  URN: " + dashboard.getUrn());
      System.out.println(
          "  View in DataHub: "
              + client.getConfig().getServer()
              + "/dashboard/"
              + dashboard.getUrn());

    } finally {
      client.close();
    }
  }
}

```

### Comprehensive Dashboard with Metadata

```java
# Inlined from /metadata-integration/java/examples/src/main/java/io/datahubproject/examples/v2/DashboardFullExample.java
package io.datahubproject.examples.v2;

import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.ChartUrn;
import com.linkedin.common.urn.DatasetUrn;
import datahub.client.v2.DataHubClientV2;
import datahub.client.v2.entity.Dashboard;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Comprehensive example demonstrating all Dashboard metadata operations using Java SDK V2.
 *
 * <p>This example shows:
 *
 * <ul>
 *   <li>Creating a dashboard with complete metadata
 *   <li>Adding tags, owners, glossary terms
 *   <li>Setting domain and custom properties
 *   <li>Adding input dataset lineage
 *   <li>Adding chart relationships
 *   <li>Setting dashboard-specific properties (URL, last refreshed)
 *   <li>Combining all operations in single entity
 * </ul>
 */
public class DashboardFullExample {

  public static void main(String[] args)
      throws IOException, ExecutionException, InterruptedException, URISyntaxException {
    // Create client
    DataHubClientV2 client =
        DataHubClientV2.builder()
            .server(System.getenv().getOrDefault("DATAHUB_SERVER", "http://localhost:8080"))
            .token(System.getenv("DATAHUB_TOKEN"))
            .build();

    try {
      // Test connection
      if (!client.testConnection()) {
        System.err.println("Failed to connect to DataHub server");
        return;
      }
      System.out.println("✓ Connected to DataHub");

      // Build comprehensive dashboard with all metadata types
      Dashboard dashboard =
          Dashboard.builder()
              .tool("tableau")
              .id("executive_kpi_dashboard")
              .title("Executive KPI Dashboard")
              .description(
                  "Comprehensive executive dashboard showing key performance indicators across all business units. "
                      + "Includes financial metrics, operational KPIs, customer satisfaction scores, and employee engagement data. "
                      + "Updated hourly with real-time data from multiple source systems.")
              .build();

      System.out.println("✓ Built dashboard with URN: " + dashboard.getUrn());

      // Add multiple tags for categorization
      dashboard
          .addTag("executive")
          .addTag("kpi")
          .addTag("real-time")
          .addTag("cross-functional")
          .addTag("production");

      System.out.println("✓ Added 5 tags");

      // Add multiple owners with different roles
      dashboard
          .addOwner("urn:li:corpuser:ceo", OwnershipType.BUSINESS_OWNER)
          .addOwner("urn:li:corpuser:bi_team", OwnershipType.TECHNICAL_OWNER)
          .addOwner("urn:li:corpuser:data_governance", OwnershipType.DATA_STEWARD);

      System.out.println("✓ Added 3 owners");

      // Add glossary terms for business context
      dashboard
          .addTerm("urn:li:glossaryTerm:ExecutiveMetrics")
          .addTerm("urn:li:glossaryTerm:KeyPerformanceIndicator")
          .addTerm("urn:li:glossaryTerm:BusinessIntelligence");

      System.out.println("✓ Added 3 glossary terms");

      // Set domain for organizational structure
      dashboard.setDomain("urn:li:domain:Executive");

      System.out.println("✓ Set domain");

      // Add comprehensive custom properties
      dashboard
          .addCustomProperty("team", "business-intelligence")
          .addCustomProperty("refresh_schedule", "hourly")
          .addCustomProperty("data_sources", "snowflake,salesforce,workday")
          .addCustomProperty("dashboard_type", "executive_summary")
          .addCustomProperty("sla_tier", "tier1")
          .addCustomProperty("business_criticality", "critical")
          .addCustomProperty("access_level", "executive-only")
          .addCustomProperty("compliance_reviewed", "2024-01-15")
          .addCustomProperty("primary_contact", "bi-team@company.com")
          .addCustomProperty("documentation_url", "https://wiki.company.com/dashboards/exec-kpi");

      System.out.println("✓ Added 10 custom properties");

      // Add input datasets (lineage from data sources)
      List<DatasetUrn> inputDatasets =
          Arrays.asList(
              DatasetUrn.createFromString(
                  "urn:li:dataset:(urn:li:dataPlatform:snowflake,financial.metrics,PROD)"),
              DatasetUrn.createFromString(
                  "urn:li:dataset:(urn:li:dataPlatform:salesforce,Account,PROD)"),
              DatasetUrn.createFromString(
                  "urn:li:dataset:(urn:li:dataPlatform:workday,EmployeeData,PROD)"));

      for (DatasetUrn datasetUrn : inputDatasets) {
        dashboard.addInputDataset(datasetUrn);
      }

      System.out.println("✓ Added 3 input datasets (lineage)");

      // Add charts contained in this dashboard
      List<ChartUrn> charts =
          Arrays.asList(
              new ChartUrn("tableau", "revenue_chart"),
              new ChartUrn("tableau", "customer_satisfaction_chart"),
              new ChartUrn("tableau", "employee_engagement_chart"),
              new ChartUrn("tableau", "operational_metrics_chart"));

      for (ChartUrn chartUrn : charts) {
        dashboard.addChart(chartUrn);
      }

      System.out.println("✓ Added 4 charts (relationships)");

      // Set dashboard-specific properties
      dashboard.setDashboardUrl("https://tableau.company.com/views/executive_kpi_dashboard");
      dashboard.setLastRefreshed(System.currentTimeMillis());

      System.out.println("✓ Set dashboard URL and last refreshed timestamp");

      // Count accumulated patches
      System.out.println("\nAccumulated " + dashboard.getPendingPatches().size() + " patches");

      // Upsert to DataHub - all metadata in single operation
      client.entities().upsert(dashboard);

      System.out.println("\n✓ Successfully created comprehensive dashboard in DataHub!");
      System.out.println("\nSummary:");
      System.out.println("  URN: " + dashboard.getUrn());
      System.out.println("  Tool: tableau");
      System.out.println("  Tags: 5");
      System.out.println("  Owners: 3");
      System.out.println("  Glossary Terms: 3");
      System.out.println("  Domain: Executive");
      System.out.println("  Custom Properties: 10");
      System.out.println("  Input Datasets: 3");
      System.out.println("  Charts: 4");
      System.out.println("  Dashboard URL: Set");
      System.out.println("  Last Refreshed: " + System.currentTimeMillis());
      System.out.println(
          "\n  View in DataHub: "
              + client.getConfig().getServer()
              + "/dashboard/"
              + dashboard.getUrn());

    } finally {
      client.close();
    }
  }
}

```

### Dashboard with Lineage and Relationships

```java
# Inlined from /metadata-integration/java/examples/src/main/java/io/datahubproject/examples/v2/DashboardLineageExample.java
package io.datahubproject.examples.v2;

import com.linkedin.common.urn.ChartUrn;
import com.linkedin.common.urn.DatasetUrn;
import datahub.client.v2.DataHubClientV2;
import datahub.client.v2.entity.Dashboard;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Example demonstrating Dashboard lineage and relationship operations using Java SDK V2.
 *
 * <p>This example focuses on:
 *
 * <ul>
 *   <li>Creating dashboards with input dataset lineage (what data sources feed the dashboard)
 *   <li>Managing chart relationships (what visualizations are embedded in the dashboard)
 *   <li>Adding and removing datasets and charts incrementally
 *   <li>Retrieving lineage and relationship information
 *   <li>Setting dashboard-specific properties (URL, last refreshed)
 * </ul>
 *
 * <p>Lineage enables you to track data flow from source datasets through to dashboard
 * visualizations, helping users understand data provenance and impact analysis.
 */
public class DashboardLineageExample {

  public static void main(String[] args)
      throws IOException, ExecutionException, InterruptedException, URISyntaxException {
    // Create client
    DataHubClientV2 client =
        DataHubClientV2.builder()
            .server(System.getenv().getOrDefault("DATAHUB_SERVER", "http://localhost:8080"))
            .token(System.getenv("DATAHUB_TOKEN"))
            .build();

    try {
      // Test connection
      if (!client.testConnection()) {
        System.err.println("Failed to connect to DataHub server");
        return;
      }
      System.out.println("✓ Connected to DataHub\n");

      // ==================== Example 1: Creating Dashboard with Lineage ====================
      System.out.println("Example 1: Creating Dashboard with Input Datasets and Charts");
      System.out.println("=".repeat(70));

      Dashboard salesDashboard =
          Dashboard.builder()
              .tool("looker")
              .id("sales_performance_dashboard")
              .title("Sales Performance Dashboard")
              .description("Real-time sales metrics and regional performance analysis")
              .build();

      // Define input datasets (what data feeds this dashboard)
      List<DatasetUrn> salesDatasets =
          Arrays.asList(
              DatasetUrn.createFromString(
                  "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales.orders,PROD)"),
              DatasetUrn.createFromString(
                  "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales.customers,PROD)"),
              DatasetUrn.createFromString(
                  "urn:li:dataset:(urn:li:dataPlatform:salesforce,Opportunity,PROD)"));

      // Add input datasets to establish lineage
      for (DatasetUrn dataset : salesDatasets) {
        salesDashboard.addInputDataset(dataset);
      }

      System.out.println("✓ Added " + salesDatasets.size() + " input datasets");

      // Define charts embedded in this dashboard
      List<ChartUrn> salesCharts =
          Arrays.asList(
              new ChartUrn("looker", "sales_revenue_chart"),
              new ChartUrn("looker", "regional_breakdown_chart"),
              new ChartUrn("looker", "top_customers_chart"));

      // Add charts to establish relationships
      for (ChartUrn chart : salesCharts) {
        salesDashboard.addChart(chart);
      }

      System.out.println("✓ Added " + salesCharts.size() + " charts");

      // Set dashboard URL for easy access
      salesDashboard.setDashboardUrl("https://looker.company.com/dashboards/sales-performance");
      salesDashboard.setLastRefreshed(System.currentTimeMillis());

      System.out.println("✓ Set dashboard URL and refresh timestamp");

      // Upsert to DataHub
      client.entities().upsert(salesDashboard);
      System.out.println("✓ Created dashboard: " + salesDashboard.getUrn());
      System.out.println();

      // ==================== Example 2: Adding More Datasets and Charts ====================
      System.out.println("Example 2: Adding Additional Datasets and Charts");
      System.out.println("=".repeat(70));

      // Add another dataset (incremental update)
      DatasetUrn productDataset =
          DatasetUrn.createFromString(
              "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales.products,PROD)");
      salesDashboard.addInputDataset(productDataset);

      System.out.println("✓ Added additional dataset: sales.products");

      // Add another chart
      ChartUrn productPerformanceChart = new ChartUrn("looker", "product_performance_chart");
      salesDashboard.addChart(productPerformanceChart);

      System.out.println("✓ Added additional chart: product_performance_chart");

      // Update in DataHub
      client.entities().upsert(salesDashboard);
      System.out.println("✓ Updated dashboard with new lineage");
      System.out.println();

      // ==================== Example 3: Removing Datasets and Charts ====================
      System.out.println("Example 3: Removing Datasets and Charts");
      System.out.println("=".repeat(70));

      // Remove a dataset (e.g., if it's no longer used)
      DatasetUrn opportunityDataset =
          DatasetUrn.createFromString(
              "urn:li:dataset:(urn:li:dataPlatform:salesforce,Opportunity,PROD)");
      salesDashboard.removeInputDataset(opportunityDataset);

      System.out.println("✓ Removed dataset: Opportunity");

      // Remove a chart
      ChartUrn topCustomersChart = new ChartUrn("looker", "top_customers_chart");
      salesDashboard.removeChart(topCustomersChart);

      System.out.println("✓ Removed chart: top_customers_chart");

      // Update in DataHub
      client.entities().upsert(salesDashboard);
      System.out.println("✓ Updated dashboard after removals");
      System.out.println();

      // ==================== Example 4: Setting Complete Lineage at Once ====================
      System.out.println("Example 4: Setting Complete Lineage (Replace All)");
      System.out.println("=".repeat(70));

      Dashboard marketingDashboard =
          Dashboard.builder()
              .tool("tableau")
              .id("marketing_campaign_dashboard")
              .title("Marketing Campaign Dashboard")
              .description("Campaign performance and ROI analysis")
              .build();

      // Set all input datasets at once (replaces any existing)
      List<DatasetUrn> marketingDatasets =
          Arrays.asList(
              DatasetUrn.createFromString(
                  "urn:li:dataset:(urn:li:dataPlatform:bigquery,marketing.campaigns,PROD)"),
              DatasetUrn.createFromString(
                  "urn:li:dataset:(urn:li:dataPlatform:bigquery,marketing.leads,PROD)"),
              DatasetUrn.createFromString(
                  "urn:li:dataset:(urn:li:dataPlatform:bigquery,marketing.conversions,PROD)"));

      marketingDashboard.setInputDatasets(marketingDatasets);

      System.out.println("✓ Set " + marketingDatasets.size() + " input datasets at once");

      // Set all charts at once
      List<ChartUrn> marketingCharts =
          Arrays.asList(
              new ChartUrn("tableau", "campaign_roi_chart"),
              new ChartUrn("tableau", "lead_funnel_chart"),
              new ChartUrn("tableau", "conversion_rate_chart"));

      marketingDashboard.setCharts(marketingCharts);

      System.out.println("✓ Set " + marketingCharts.size() + " charts at once");

      // Set dashboard properties
      marketingDashboard.setDashboardUrl("https://tableau.company.com/views/marketing-campaigns");
      marketingDashboard.setLastRefreshed(System.currentTimeMillis() - 3600000); // 1 hour ago

      // Upsert to DataHub
      client.entities().upsert(marketingDashboard);
      System.out.println("✓ Created dashboard: " + marketingDashboard.getUrn());
      System.out.println();

      // ==================== Example 5: Retrieving Lineage Information ====================
      System.out.println("Example 5: Retrieving Lineage and Relationships");
      System.out.println("=".repeat(70));

      // Load dashboard from DataHub
      Dashboard loadedDashboard =
          client.entities().get(salesDashboard.getUrn().toString(), Dashboard.class);

      if (loadedDashboard != null) {
        // Get input datasets
        List<DatasetUrn> inputDatasets = loadedDashboard.getInputDatasets();
        System.out.println("Input Datasets (" + inputDatasets.size() + "):");
        for (DatasetUrn dataset : inputDatasets) {
          System.out.println("  - " + dataset);
        }

        // Get charts
        List<ChartUrn> charts = loadedDashboard.getCharts();
        System.out.println("\nCharts (" + charts.size() + "):");
        for (ChartUrn chart : charts) {
          System.out.println("  - " + chart);
        }

        // Get dashboard properties
        String dashboardUrl = loadedDashboard.getDashboardUrl();
        Long lastRefreshed = loadedDashboard.getLastRefreshed();

        System.out.println("\nDashboard Properties:");
        System.out.println("  URL: " + dashboardUrl);
        System.out.println("  Last Refreshed: " + lastRefreshed);
        System.out.println("  Title: " + loadedDashboard.getTitle());
        System.out.println("  Description: " + loadedDashboard.getDescription());
      }

      System.out.println();

      // ==================== Summary ====================
      System.out.println("Summary");
      System.out.println("=".repeat(70));
      System.out.println("✓ Created 2 dashboards with complete lineage");
      System.out.println("✓ Demonstrated incremental additions and removals");
      System.out.println("✓ Showed batch setting of datasets and charts");
      System.out.println("✓ Retrieved and displayed lineage information");
      System.out.println();
      System.out.println("Key Takeaways:");
      System.out.println("  • Input datasets create upstream lineage to data sources");
      System.out.println("  • Charts represent embedded visualizations");
      System.out.println("  • Use add/remove for incremental changes");
      System.out.println("  • Use set methods to replace all at once");
      System.out.println("  • Lineage enables data provenance and impact analysis");

    } finally {
      client.close();
    }
  }
}

```



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-integration](https://github.com/datahub-project/datahub/tree/master/metadata-integration) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
