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
