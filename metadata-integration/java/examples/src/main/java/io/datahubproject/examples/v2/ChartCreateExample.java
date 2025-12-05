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
