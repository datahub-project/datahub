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
{{ inline /metadata-integration/java/examples/src/main/java/io/datahubproject/examples/v2/ChartCreateExample.java show_path_as_comment }}
```

### Comprehensive Chart with Metadata and Lineage

```java
{{ inline /metadata-integration/java/examples/src/main/java/io/datahubproject/examples/v2/ChartFullExample.java show_path_as_comment }}
```

### Chart Lineage Operations

```java
{{ inline /metadata-integration/java/examples/src/main/java/io/datahubproject/examples/v2/ChartLineageExample.java show_path_as_comment }}
```
