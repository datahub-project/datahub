package io.datahubproject.examples.v2;

import com.linkedin.common.OwnershipType;
import datahub.client.v2.DataHubClientV2;
import datahub.client.v2.entity.Dataset;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Example demonstrating how to create a Dataset using Java SDK V2.
 *
 * <p>This example shows:
 *
 * <ul>
 *   <li>Creating a DataHubClientV2
 *   <li>Building a Dataset with fluent builder
 *   <li>Adding tags, owners, and custom properties
 *   <li>Upserting to DataHub
 * </ul>
 */
public class DatasetCreateExample {

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

      // Build dataset with metadata
      Dataset dataset =
          Dataset.builder()
              .platform("snowflake")
              .name("analytics.public.user_events")
              .env("PROD")
              .description("User interaction events from web and mobile applications")
              .displayName("User Events")
              .build();

      System.out.println("✓ Built dataset with URN: " + dataset.getUrn());

      // Add tags
      dataset.addTag("pii").addTag("analytics").addTag("gdpr");

      System.out.println("✓ Added 3 tags");

      // Add owners
      dataset
          .addOwner("urn:li:corpuser:datahub", OwnershipType.TECHNICAL_OWNER)
          .addOwner("urn:li:corpuser:data_team", OwnershipType.DATA_STEWARD);

      System.out.println("✓ Added 2 owners");

      // Add custom properties
      dataset
          .addCustomProperty("team", "data-engineering")
          .addCustomProperty("retention_days", "365")
          .addCustomProperty("refresh_schedule", "daily")
          .addCustomProperty("source_system", "web_analytics");

      System.out.println("✓ Added 4 custom properties");

      // Upsert to DataHub
      client.entities().upsert(dataset);

      System.out.println("✓ Successfully created dataset in DataHub!");
      System.out.println("\n  URN: " + dataset.getUrn());
      System.out.println(
          "  View in DataHub: " + client.getConfig().getServer() + "/dataset/" + dataset.getUrn());

    } finally {
      client.close();
    }
  }
}
