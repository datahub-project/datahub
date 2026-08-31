package io.datahubproject.examples.v2;

import com.linkedin.common.OwnershipType;
import datahub.client.v2.DataHubClientV2;
import datahub.client.v2.entity.DataJob;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Example demonstrating how to create a DataJob using Java SDK V2.
 *
 * <p>This example shows:
 *
 * <ul>
 *   <li>Creating a DataHubClientV2
 *   <li>Building a DataJob with fluent builder
 *   <li>Adding tags, owners, and custom properties
 *   <li>Upserting to DataHub
 * </ul>
 */
public class DataJobCreateExample {

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

      // Build data job with metadata
      DataJob dataJob =
          DataJob.builder()
              .orchestrator("airflow")
              .flowId("user_analytics_dag")
              .cluster("prod")
              .jobId("process_user_events")
              .description("Processes user interaction events and loads into data warehouse")
              .name("Process User Events")
              .type("BATCH")
              .build();

      System.out.println("✓ Built data job with URN: " + dataJob.getUrn());

      // Add tags
      dataJob.addTag("critical").addTag("etl").addTag("user-data");

      System.out.println("✓ Added 3 tags");

      // Add owners
      dataJob
          .addOwner("urn:li:corpuser:datahub", OwnershipType.TECHNICAL_OWNER)
          .addOwner("urn:li:corpuser:data_team", OwnershipType.DATA_STEWARD);

      System.out.println("✓ Added 2 owners");

      // Add custom properties
      dataJob
          .addCustomProperty("team", "data-engineering")
          .addCustomProperty("schedule", "0 2 * * *")
          .addCustomProperty("retries", "3")
          .addCustomProperty("timeout", "3600");

      System.out.println("✓ Added 4 custom properties");

      // Upsert to DataHub
      client.entities().upsert(dataJob);

      System.out.println("✓ Successfully created data job in DataHub!");
      System.out.println("\n  URN: " + dataJob.getUrn());
      System.out.println(
          "  View in DataHub: " + client.getConfig().getServer() + "/dataJob/" + dataJob.getUrn());

    } finally {
      client.close();
    }
  }
}
