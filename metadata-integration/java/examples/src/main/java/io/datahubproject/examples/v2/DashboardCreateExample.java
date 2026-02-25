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
