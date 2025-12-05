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
