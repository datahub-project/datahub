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
