# Chart Entity

The Chart entity represents visualizations and reports in BI tools (e.g., Looker, Tableau, Superset). This guide covers chart operations in SDK V2.

## Creating a Chart

### Minimal Chart

Only tool and id are required:

```java
Chart chart = Chart.builder()
    .tool("looker")
    .id("my_sales_chart")
    .build();
```

### With Metadata

Add title, description, and custom properties:

```java
Chart chart = Chart.builder()
    .tool("tableau")
    .id("sales_dashboard_chart_1")
    .title("Sales Performance by Region")
    .description("Monthly sales broken down by geographic region")
    .build();
```

### With Custom Properties

```java
Map<String, String> properties = new HashMap<>();
properties.put("dashboard", "executive_dashboard");
properties.put("refresh_schedule", "hourly");

Chart chart = Chart.builder()
    .tool("looker")
    .id("revenue_chart")
    .title("Revenue Trends")
    .customProperties(properties)
    .build();
```

## URN Construction

Chart URNs follow the pattern:

```
urn:li:chart:({tool},{id})
```

**Example:**

```java
Chart chart = Chart.builder()
    .tool("looker")
    .id("my_chart")
    .build();

ChartUrn urn = chart.getChartUrn();
// urn:li:chart:(looker,my_chart)
```

## Supported BI Tools

Common tool identifiers:

- `looker` - Looker
- `tableau` - Tableau
- `superset` - Apache Superset
- `powerbi` - Power BI
- `metabase` - Metabase
- `redash` - Redash
- `mode` - Mode Analytics

## Chart Operations

### Adding Tags

Add tags to categorize and classify your charts:

```java
// Simple tag (automatically adds "urn:li:tag:" prefix)
chart.addTag("pii");
chart.addTag("financial");

// Or use full URN
chart.addTag("urn:li:tag:production");
```

### Managing Owners

Add owners with different ownership types:

```java
import com.linkedin.common.OwnershipType;

// Add technical owner
chart.addOwner("urn:li:corpuser:data_team", OwnershipType.TECHNICAL_OWNER);

// Add business owner
chart.addOwner("urn:li:corpuser:sales_team", OwnershipType.BUSINESS_OWNER);

// Add data steward
chart.addOwner("urn:li:corpuser:compliance_team", OwnershipType.DATA_STEWARD);

// Remove an owner
chart.removeOwner("urn:li:corpuser:old_owner");
```

### Adding Glossary Terms

Link charts to business glossary terms:

```java
chart.addTerm("urn:li:glossaryTerm:SalesMetrics");
chart.addTerm("urn:li:glossaryTerm:QuarterlyReporting");

// Remove a term
chart.removeTerm("urn:li:glossaryTerm:OldTerm");
```

### Setting Domain

Organize charts into domains:

```java
// Set domain
chart.setDomain("urn:li:domain:Sales");

// Remove domain
chart.setDomain(null);
// or
chart.removeDomain();
```

### Setting Description and Title

Update chart description and title using patch-based updates:

```java
chart.setDescription("Updated chart description");
chart.setTitle("New Chart Title");
```

### Managing Custom Properties

Add, update, or remove custom properties:

```java
// Add individual properties
chart.addCustomProperty("refresh_schedule", "hourly");
chart.addCustomProperty("chart_type", "bar");

// Set all properties at once (replaces existing)
Map<String, String> props = new HashMap<>();
props.put("dashboard_url", "https://dashboard.example.com");
props.put("author", "data_team");
chart.setCustomProperties(props);

// Remove a property
chart.removeCustomProperty("old_property");
```

## Lineage Operations

Chart lineage defines the data flow relationships between charts and the datasets they consume. This is essential for impact analysis, data governance, and understanding data dependencies.

### Setting Input Datasets

Define which datasets a chart consumes:

```java
import com.linkedin.common.urn.DatasetUrn;
import java.util.Arrays;

// Create dataset URNs
DatasetUrn salesDataset = DatasetUrn.createFromString(
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales.transactions,PROD)");
DatasetUrn customerDataset = DatasetUrn.createFromString(
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales.customers,PROD)");

// Set all input datasets at once (replaces existing)
chart.setInputDatasets(Arrays.asList(salesDataset, customerDataset));
```

### Adding Individual Input Datasets

Add input datasets incrementally:

```java
// Add datasets one at a time
chart.addInputDataset(salesDataset);
chart.addInputDataset(customerDataset);

// This pattern is useful when:
// - Building lineage incrementally
// - Discovering datasets during chart analysis
// - Adding new data sources to existing chart
```

### Removing Input Datasets

Remove datasets that are no longer consumed:

```java
DatasetUrn legacyDataset = DatasetUrn.createFromString(
    "urn:li:dataset:(urn:li:dataPlatform:postgres,legacy.old_table,PROD)");

// Remove specific dataset from lineage
chart.removeInputDataset(legacyDataset);
```

### Retrieving Input Datasets

Get the list of datasets a chart consumes:

```java
// Load chart from DataHub
ChartUrn chartUrn = new ChartUrn("looker", "my_chart");
Chart chart = client.entities().get(chartUrn);

// Get all input datasets
List<DatasetUrn> inputDatasets = chart.getInputDatasets();
System.out.println("Chart consumes " + inputDatasets.size() + " datasets:");
for (DatasetUrn dataset : inputDatasets) {
    System.out.println("  - " + dataset);
}
```

### Complete Lineage Example

```java
// Create chart with comprehensive lineage
Chart salesChart = Chart.builder()
    .tool("tableau")
    .id("sales_dashboard_chart")
    .title("Sales Performance")
    .build();

// Define input datasets
DatasetUrn transactions = DatasetUrn.createFromString(
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales.transactions,PROD)");
DatasetUrn customers = DatasetUrn.createFromString(
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales.customers,PROD)");
DatasetUrn products = DatasetUrn.createFromString(
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales.products,PROD)");

// Set lineage
salesChart.setInputDatasets(Arrays.asList(transactions, customers, products));

// Add metadata
salesChart.addTag("sales")
         .addOwner("urn:li:corpuser:data_team", OwnershipType.TECHNICAL_OWNER)
         .setDomain("urn:li:domain:Sales");

// Save to DataHub
client.entities().upsert(salesChart);
```

## Chart-Specific Properties

Charts have several specialized properties beyond basic metadata:

### Chart Type

Set the visualization type:

```java
// Available types: BAR, LINE, PIE, TABLE, TEXT, BOXPLOT, AREA, SCATTER
chart.setChartType("BAR");

// Get chart type
String chartType = chart.getChartType();
```

### Access Level

Control chart visibility:

```java
// Available levels: PUBLIC, PRIVATE
chart.setAccess("PUBLIC");

// Get access level
String access = chart.getAccess();
```

### External and Chart URLs

Set URLs for accessing the chart:

```java
// External URL - link to view chart in source BI tool
chart.setExternalUrl("https://looker.company.com/dashboards/123");

// Chart URL - direct URL to chart (may be different from external URL)
chart.setChartUrl("https://looker.company.com/embed/charts/456");

// Get URLs
String externalUrl = chart.getExternalUrl();
String chartUrl = chart.getChartUrl();
```

### Last Refreshed Timestamp

Track when chart data was last updated:

```java
// Set timestamp (milliseconds since epoch)
long currentTime = System.currentTimeMillis();
chart.setLastRefreshed(currentTime);

// Or use a specific time
long specificTime = Instant.parse("2025-10-29T10:00:00Z").toEpochMilli();
chart.setLastRefreshed(specificTime);

// Get last refreshed time
Long lastRefreshed = chart.getLastRefreshed();
if (lastRefreshed != null) {
    Instant refreshTime = Instant.ofEpochMilli(lastRefreshed);
    System.out.println("Last refreshed: " + refreshTime);
}
```

### Complete Properties Example

```java
import java.time.Instant;

Chart chart = Chart.builder()
    .tool("looker")
    .id("sales_performance")
    .title("Sales Performance Dashboard")
    .build();

// Set all chart-specific properties
chart.setChartType("BAR")
     .setAccess("PUBLIC")
     .setExternalUrl("https://looker.company.com/dashboards/sales")
     .setChartUrl("https://looker.company.com/embed/charts/sales_performance")
     .setLastRefreshed(System.currentTimeMillis());

// Set lineage
DatasetUrn salesDataset = DatasetUrn.createFromString(
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales.transactions,PROD)");
chart.addInputDataset(salesDataset);

// Add metadata
chart.addTag("sales")
     .setDomain("urn:li:domain:Sales");

client.entities().upsert(chart);
```

## Complete Example

Here's a comprehensive example showing all chart operations:

```java
import com.linkedin.common.OwnershipType;
import datahub.client.v2.DataHubClientV2;
import datahub.client.v2.entity.Chart;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class ChartExample {
    public static void main(String[] args)
        throws IOException, ExecutionException, InterruptedException {
        // Create client
        DataHubClientV2 client = DataHubClientV2.builder()
            .server("http://localhost:8080")
            .build();

        try {
            // Create chart with basic metadata
            Chart chart = Chart.builder()
                .tool("looker")
                .id("regional_sales_chart")
                .title("Regional Sales Performance")
                .description("Quarterly sales broken down by region and product category")
                .build();

            // Add tags for categorization
            chart.addTag("sales")
                 .addTag("executive")
                 .addTag("quarterly-review");

            // Add owners
            chart.addOwner("urn:li:corpuser:sales_team", OwnershipType.TECHNICAL_OWNER)
                 .addOwner("urn:li:corpuser:bi_team", OwnershipType.DATA_STEWARD);

            // Add glossary terms
            chart.addTerm("urn:li:glossaryTerm:SalesMetrics")
                 .addTerm("urn:li:glossaryTerm:QuarterlyReporting");

            // Set domain
            chart.setDomain("urn:li:domain:Sales");

            // Add custom properties
            chart.addCustomProperty("dashboard", "executive_overview")
                 .addCustomProperty("chart_type", "bar")
                 .addCustomProperty("data_source", "snowflake")
                 .addCustomProperty("refresh_schedule", "hourly");

            // Upsert to DataHub (emits all accumulated patches)
            client.entities().upsert(chart);

            System.out.println("Successfully created chart: " + chart.getUrn());
            System.out.println("Total patches: " + chart.getPendingPatches().size());

        } finally {
            client.close();
        }
    }
}
```

## Builder Options Reference

| Method                  | Required | Description                                    |
| ----------------------- | -------- | ---------------------------------------------- |
| `tool(String)`          | ✅ Yes   | BI tool identifier (e.g., "looker", "tableau") |
| `id(String)`            | ✅ Yes   | Chart identifier within the tool               |
| `title(String)`         | No       | Chart title                                    |
| `description(String)`   | No       | Chart description                              |
| `customProperties(Map)` | No       | Map of custom key-value properties             |

## Patch-Based Operations

Chart entities now support patch-based operations similar to Dataset. All mutations (addTag, addOwner, etc.) create patch MCPs that accumulate until save(). This enables:

- **Efficient batching**: Multiple operations in a single network call
- **Incremental updates**: Only modified fields are sent to the server
- **Fluent chaining**: Build complex metadata in a readable way

Available patch operations:

| Operation                           | Description                        |
| ----------------------------------- | ---------------------------------- |
| `addTag(String)`                    | Add a tag to the chart             |
| `removeTag(String)`                 | Remove a tag from the chart        |
| `addOwner(String, OwnershipType)`   | Add an owner with ownership type   |
| `removeOwner(String)`               | Remove an owner from the chart     |
| `addTerm(String)`                   | Add a glossary term to the chart   |
| `removeTerm(String)`                | Remove a glossary term             |
| `setDomain(String)`                 | Set the domain for the chart       |
| `removeDomain()`                    | Remove the domain from the chart   |
| `setDescription(String)`            | Update chart description           |
| `setTitle(String)`                  | Update chart title                 |
| `addCustomProperty(String, String)` | Add or update a custom property    |
| `removeCustomProperty(String)`      | Remove a custom property           |
| `setCustomProperties(Map)`          | Replace all custom properties      |
| `setInputDatasets(List)`            | Set input datasets (lineage)       |
| `addInputDataset(DatasetUrn)`       | Add an input dataset (lineage)     |
| `removeInputDataset(DatasetUrn)`    | Remove an input dataset (lineage)  |
| `setExternalUrl(String)`            | Set external URL for the chart     |
| `setChartUrl(String)`               | Set chart URL                      |
| `setLastRefreshed(long)`            | Set last refreshed timestamp       |
| `setChartType(String)`              | Set chart type (BAR, LINE, etc.)   |
| `setAccess(String)`                 | Set access level (PUBLIC, PRIVATE) |

See [Patch Operations Guide](./patch-operations.md) for more details on how patch-based updates work.

## Common Patterns

### Creating Multiple Charts

```java
List<String> chartIds = Arrays.asList("chart1", "chart2", "chart3");

for (String chartId : chartIds) {
    Chart chart = Chart.builder()
        .tool("looker")
        .id(chartId)
        .title("Chart " + chartId)
        .build();

    client.entities().upsert(chart);
}
```

### Linking Charts to Dashboards

Use custom properties to track relationships:

```java
Chart chart = Chart.builder()
    .tool("tableau")
    .id("sales_chart")
    .build();

Map<String, String> props = new HashMap<>();
props.put("dashboard_id", "executive_dashboard");
props.put("position", "top_left");

chart.setCustomProperties(props);
client.entities().upsert(chart);
```

## Updating Charts

### Load and Modify

```java
// Load existing chart
ChartUrn urn = new ChartUrn("looker", "my_chart");
Chart chart = client.entities().get(urn);

// Modify
chart.setDescription("Updated description");

// Save changes
client.entities().update(chart);
```

## Next Steps

- **[Dataset Entity Guide](./dataset-entity.md)** - Comprehensive dataset operations
- **[Entities Overview](./entities-overview.md)** - Common patterns across entities
- **[Patch Operations](./patch-operations.md)** - Understanding incremental updates

## Examples

### Basic Chart Creation

```java
# Inlined from /metadata-integration/java/examples/src/main/java/io/datahubproject/examples/v2/ChartCreateExample.java
package io.datahubproject.examples.v2;

import datahub.client.v2.DataHubClientV2;
import datahub.client.v2.entity.Chart;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Example demonstrating how to create a Chart using Java SDK V2.
 *
 * <p>This example shows:
 *
 * <ul>
 *   <li>Creating a DataHubClientV2
 *   <li>Building a Chart with fluent builder
 *   <li>Setting chart properties
 *   <li>Upserting to DataHub
 * </ul>
 *
 * <p>Note: Chart currently has basic functionality. For advanced operations like tags and owners,
 * use patch-based patterns similar to Dataset.
 */
public class ChartCreateExample {

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

      // Prepare custom properties
      Map<String, String> customProperties = new HashMap<>();
      customProperties.put("dashboard", "executive_summary");
      customProperties.put("refresh_rate", "daily");
      customProperties.put("data_source", "snowflake.analytics.revenue_summary");
      customProperties.put("chart_type", "stacked_bar");

      // Build chart with metadata
      Chart chart =
          Chart.builder()
              .tool("looker")
              .id("customer_revenue_by_region")
              .title("Revenue by Region")
              .description(
                  "Monthly revenue breakdown by geographic region with year-over-year comparison")
              .customProperties(customProperties)
              .build();

      System.out.println("✓ Built chart with URN: " + chart.getUrn());

      // Upsert to DataHub
      client.entities().upsert(chart);

      System.out.println("✓ Successfully created chart in DataHub!");
      System.out.println("\n  URN: " + chart.getUrn());
      System.out.println(
          "  View in DataHub: " + client.getConfig().getServer() + "/chart/" + chart.getUrn());

    } finally {
      client.close();
    }
  }
}

```

### Comprehensive Chart with Metadata and Lineage

```java
# Inlined from /metadata-integration/java/examples/src/main/java/io/datahubproject/examples/v2/ChartFullExample.java
package io.datahubproject.examples.v2;

import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.DatasetUrn;
import datahub.client.v2.DataHubClientV2;
import datahub.client.v2.entity.Chart;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

/**
 * Comprehensive example demonstrating all Chart metadata operations using Java SDK V2.
 *
 * <p>This example shows:
 *
 * <ul>
 *   <li>Creating a chart with complete metadata
 *   <li>Adding tags, owners, glossary terms
 *   <li>Setting domain and custom properties
 *   <li>Setting up lineage with input datasets
 *   <li>Setting chart-specific properties (type, access, URLs, refresh time)
 *   <li>Combining all operations in single entity
 * </ul>
 */
public class ChartFullExample {

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

      // Build comprehensive chart with all metadata types
      Chart chart =
          Chart.builder()
              .tool("looker")
              .id("executive_dashboard_sales_chart")
              .title("Executive Sales Performance Dashboard")
              .description(
                  "Comprehensive sales performance visualization showing quarterly revenue trends, "
                      + "regional breakdowns, and product category performance. "
                      + "This chart is the primary view for executive leadership to track business metrics.")
              .build();

      System.out.println("✓ Built chart with URN: " + chart.getUrn());

      // Add multiple tags for categorization
      chart
          .addTag("executive")
          .addTag("sales")
          .addTag("financial")
          .addTag("quarterly-review")
          .addTag("mission-critical");

      System.out.println("✓ Added 5 tags");

      // Add multiple owners with different roles
      chart
          .addOwner("urn:li:corpuser:sales_team", OwnershipType.TECHNICAL_OWNER)
          .addOwner("urn:li:corpuser:bi_team", OwnershipType.DATA_STEWARD)
          .addOwner("urn:li:corpuser:executive_team", OwnershipType.BUSINESS_OWNER);

      System.out.println("✓ Added 3 owners");

      // Add glossary terms for business context
      chart
          .addTerm("urn:li:glossaryTerm:SalesMetrics")
          .addTerm("urn:li:glossaryTerm:QuarterlyReporting")
          .addTerm("urn:li:glossaryTerm:ExecutiveDashboard");

      System.out.println("✓ Added 3 glossary terms");

      // Set domain for organizational structure
      chart.setDomain("urn:li:domain:Sales");

      System.out.println("✓ Set domain");

      // Add comprehensive custom properties
      chart
          .addCustomProperty("visualization_tool", "looker")
          .addCustomProperty("dashboard_id", "executive_overview")
          .addCustomProperty("refresh_schedule", "hourly")
          .addCustomProperty("data_source", "snowflake")
          .addCustomProperty("sla_tier", "tier1")
          .addCustomProperty("chart_position", "top_center")
          .addCustomProperty("business_criticality", "mission-critical");

      System.out.println("✓ Added 7 custom properties");

      // Set chart-specific properties
      chart
          .setChartType("BAR") // Chart type: BAR, LINE, PIE, TABLE, TEXT, BOXPLOT
          .setAccess("PUBLIC") // Access level: PUBLIC or PRIVATE
          .setExternalUrl("https://looker.company.com/charts/executive_dashboard_sales_chart")
          .setChartUrl("https://looker.company.com/embed/charts/executive_dashboard_sales_chart")
          .setLastRefreshed(System.currentTimeMillis()); // When chart data was last refreshed

      System.out.println("✓ Set chart-specific properties (type, access, URLs, refresh time)");

      // Set up lineage: Define which datasets this chart consumes
      // This chart visualizes data from sales transactions and customer data
      DatasetUrn salesDataset =
          DatasetUrn.createFromString(
              "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales.transactions,PROD)");
      DatasetUrn customerDataset =
          DatasetUrn.createFromString(
              "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales.customers,PROD)");
      DatasetUrn regionDataset =
          DatasetUrn.createFromString(
              "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales.regions,PROD)");

      chart.setInputDatasets(Arrays.asList(salesDataset, customerDataset, regionDataset));

      System.out.println("✓ Set input datasets (lineage): 3 datasets");

      // Count accumulated patches
      System.out.println("\nAccumulated " + chart.getPendingPatches().size() + " patches");

      // Upsert to DataHub - all metadata in single operation
      client.entities().upsert(chart);

      System.out.println("\n✓ Successfully created comprehensive chart in DataHub!");
      System.out.println("\nSummary:");
      System.out.println("  URN: " + chart.getUrn());
      System.out.println("  Tool: looker");
      System.out.println("  Tags: 5");
      System.out.println("  Owners: 3");
      System.out.println("  Glossary Terms: 3");
      System.out.println("  Domain: Sales");
      System.out.println("  Custom Properties: 7");
      System.out.println("  Chart Type: BAR");
      System.out.println("  Access Level: PUBLIC");
      System.out.println("  Input Datasets: 3 (lineage relationships)");
      System.out.println(
          "\n  View in DataHub: " + client.getConfig().getServer() + "/chart/" + chart.getUrn());

    } finally {
      client.close();
    }
  }
}

```

### Chart Lineage Operations

```java
# Inlined from /metadata-integration/java/examples/src/main/java/io/datahubproject/examples/v2/ChartLineageExample.java
package io.datahubproject.examples.v2;

import com.linkedin.common.urn.DatasetUrn;
import datahub.client.v2.DataHubClientV2;
import datahub.client.v2.entity.Chart;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Example demonstrating chart lineage operations using Java SDK V2.
 *
 * <p>This example shows:
 *
 * <ul>
 *   <li>Creating a chart with input datasets (lineage)
 *   <li>Adding individual input datasets
 *   <li>Removing input datasets
 *   <li>Retrieving input datasets
 *   <li>Best practices for managing chart lineage
 * </ul>
 */
public class ChartLineageExample {

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

      // ==================== Example 1: Create chart with input datasets ====================
      System.out.println("Example 1: Creating chart with multiple input datasets");
      System.out.println("--------------------------------------------------------");

      Chart salesChart =
          Chart.builder()
              .tool("tableau")
              .id("sales_performance_chart")
              .title("Sales Performance Dashboard")
              .description("Aggregates sales data from multiple sources")
              .build();

      // Define the datasets this chart consumes
      // In a real scenario, these datasets would already exist in DataHub
      DatasetUrn transactionsDataset =
          DatasetUrn.createFromString(
              "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.transactions,PROD)");
      DatasetUrn customersDataset =
          DatasetUrn.createFromString(
              "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.customers,PROD)");
      DatasetUrn productsDataset =
          DatasetUrn.createFromString(
              "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales_db.products,PROD)");

      // Set all input datasets at once
      salesChart.setInputDatasets(
          Arrays.asList(transactionsDataset, customersDataset, productsDataset));

      System.out.println("Created chart: " + salesChart.getUrn());
      System.out.println("Input datasets:");
      System.out.println("  - " + transactionsDataset);
      System.out.println("  - " + customersDataset);
      System.out.println("  - " + productsDataset);

      // Upsert chart to DataHub
      client.entities().upsert(salesChart);
      System.out.println("✓ Chart with lineage created in DataHub\n");

      // ==================== Example 2: Add input datasets incrementally ====================
      System.out.println("Example 2: Adding input datasets incrementally");
      System.out.println("------------------------------------------------");

      Chart revenueChart =
          Chart.builder()
              .tool("looker")
              .id("revenue_trends_chart")
              .title("Revenue Trends Over Time")
              .description("Shows revenue trends from various data sources")
              .build();

      // Add input datasets one by one
      DatasetUrn revenueDataset =
          DatasetUrn.createFromString(
              "urn:li:dataset:(urn:li:dataPlatform:bigquery,finance.revenue,PROD)");
      revenueChart.addInputDataset(revenueDataset);
      System.out.println("Added input dataset: " + revenueDataset);

      DatasetUrn forecastDataset =
          DatasetUrn.createFromString(
              "urn:li:dataset:(urn:li:dataPlatform:bigquery,finance.forecast,PROD)");
      revenueChart.addInputDataset(forecastDataset);
      System.out.println("Added input dataset: " + forecastDataset);

      // This pattern is useful when building lineage incrementally
      // or when input datasets are discovered during chart analysis
      DatasetUrn budgetDataset =
          DatasetUrn.createFromString(
              "urn:li:dataset:(urn:li:dataPlatform:bigquery,finance.budget,PROD)");
      revenueChart.addInputDataset(budgetDataset);
      System.out.println("Added input dataset: " + budgetDataset);

      client.entities().upsert(revenueChart);
      System.out.println("✓ Chart with incremental lineage created in DataHub\n");

      // ==================== Example 3: Remove input datasets ====================
      System.out.println("Example 3: Removing input datasets");
      System.out.println("-----------------------------------");

      Chart analyticsChart =
          Chart.builder()
              .tool("superset")
              .id("customer_analytics_chart")
              .title("Customer Analytics")
              .description("Customer behavior analysis")
              .build();

      // Set initial input datasets
      DatasetUrn customerBehaviorDataset =
          DatasetUrn.createFromString(
              "urn:li:dataset:(urn:li:dataPlatform:postgres,analytics.customer_behavior,PROD)");
      DatasetUrn clickstreamDataset =
          DatasetUrn.createFromString(
              "urn:li:dataset:(urn:li:dataPlatform:kafka,analytics.clickstream,PROD)");
      DatasetUrn legacyDataset =
          DatasetUrn.createFromString(
              "urn:li:dataset:(urn:li:dataPlatform:postgres,analytics.legacy_data,PROD)");

      analyticsChart.setInputDatasets(
          Arrays.asList(customerBehaviorDataset, clickstreamDataset, legacyDataset));
      System.out.println("Initial input datasets: 3");

      // Remove legacy dataset (no longer used)
      analyticsChart.removeInputDataset(legacyDataset);
      System.out.println("Removed legacy dataset: " + legacyDataset);

      client.entities().upsert(analyticsChart);
      System.out.println("✓ Chart lineage updated in DataHub\n");

      // ==================== Example 4: Retrieve input datasets ====================
      System.out.println("Example 4: Retrieving input datasets from existing chart");
      System.out.println("---------------------------------------------------------");

      // Load an existing chart and inspect its lineage
      // Note: In a real scenario, you would load from DataHub:
      // Chart existingChart = client.entities().get(salesChart.getChartUrn());

      // For this example, we'll use the chart we just created
      Chart existingChart =
          Chart.builder()
              .tool("tableau")
              .id("sales_performance_chart")
              .title("Sales Performance Dashboard")
              .build();

      // In production, after loading from DataHub, you can retrieve input datasets
      List<DatasetUrn> inputDatasets = existingChart.getInputDatasets();
      System.out.println("Chart: " + existingChart.getUrn());
      System.out.println("Input datasets: " + inputDatasets.size());
      for (DatasetUrn dataset : inputDatasets) {
        System.out.println("  - " + dataset);
      }

      // ==================== Example 5: Complex lineage scenario ====================
      System.out.println("\nExample 5: Complex lineage with multiple chart types");
      System.out.println("-----------------------------------------------------");

      // Create a comprehensive dashboard with multiple charts
      Chart[] dashboardCharts =
          new Chart[] {
            Chart.builder()
                .tool("looker")
                .id("dashboard_sales_by_region")
                .title("Sales by Region")
                .build(),
            Chart.builder()
                .tool("looker")
                .id("dashboard_top_products")
                .title("Top Products")
                .build(),
            Chart.builder()
                .tool("looker")
                .id("dashboard_customer_segments")
                .title("Customer Segments")
                .build()
          };

      // Define shared and specific datasets
      DatasetUrn sharedSalesDataset =
          DatasetUrn.createFromString(
              "urn:li:dataset:(urn:li:dataPlatform:snowflake,warehouse.sales_fact,PROD)");
      DatasetUrn regionDimensionDataset =
          DatasetUrn.createFromString(
              "urn:li:dataset:(urn:li:dataPlatform:snowflake,warehouse.region_dim,PROD)");
      DatasetUrn productDimensionDataset =
          DatasetUrn.createFromString(
              "urn:li:dataset:(urn:li:dataPlatform:snowflake,warehouse.product_dim,PROD)");
      DatasetUrn customerDimensionDataset =
          DatasetUrn.createFromString(
              "urn:li:dataset:(urn:li:dataPlatform:snowflake,warehouse.customer_dim,PROD)");

      // Sales by Region chart consumes sales and region data
      dashboardCharts[0].setInputDatasets(
          Arrays.asList(sharedSalesDataset, regionDimensionDataset));

      // Top Products chart consumes sales and product data
      dashboardCharts[1].setInputDatasets(
          Arrays.asList(sharedSalesDataset, productDimensionDataset));

      // Customer Segments chart consumes sales and customer data
      dashboardCharts[2].setInputDatasets(
          Arrays.asList(sharedSalesDataset, customerDimensionDataset));

      // Upsert all charts
      for (Chart chart : dashboardCharts) {
        client.entities().upsert(chart);
        System.out.println("Created chart: " + chart.getUrn());
      }

      System.out.println(
          "\n✓ Successfully created "
              + dashboardCharts.length
              + " charts with complex lineage relationships");

      // ==================== Summary ====================
      System.out.println("\n" + "=".repeat(60));
      System.out.println("SUMMARY: Chart Lineage Operations");
      System.out.println("=".repeat(60));
      System.out.println("\nKey Takeaways:");
      System.out.println("1. Use setInputDatasets() to define all lineage relationships at once");
      System.out.println("2. Use addInputDataset() to add lineage incrementally");
      System.out.println("3. Use removeInputDataset() to remove outdated lineage");
      System.out.println("4. Use getInputDatasets() to inspect existing lineage");
      System.out.println(
          "5. Lineage helps users understand data flow and impact analysis in DataHub");
      System.out.println("\nBest Practices:");
      System.out.println("- Always establish lineage when creating charts");
      System.out.println("- Keep lineage up-to-date when chart queries change");
      System.out.println("- Use lineage for impact analysis and data governance");
      System.out.println("- Document shared datasets used across multiple charts");

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
