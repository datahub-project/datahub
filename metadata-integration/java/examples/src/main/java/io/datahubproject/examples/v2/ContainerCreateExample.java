package io.datahubproject.examples.v2;

import com.linkedin.common.OwnershipType;
import datahub.client.v2.DataHubClientV2;
import datahub.client.v2.entity.Container;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Example demonstrating how to create a Container using Java SDK V2.
 *
 * <p>This example shows:
 *
 * <ul>
 *   <li>Creating a DataHubClientV2
 *   <li>Building a database Container with fluent builder
 *   <li>Adding tags, owners, and domain
 *   <li>Upserting to DataHub
 * </ul>
 */
public class ContainerCreateExample {

  public static void main(String[] args)
      throws IOException, ExecutionException, InterruptedException {
    // Create client (use environment variables or pass explicit values)
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

      // Build database container with metadata
      Container database =
          Container.builder()
              .platform("snowflake")
              .database("analytics_db")
              .env("PROD")
              .displayName("Analytics Database")
              .description("Production database for analytics and reporting")
              .qualifiedName("prod.snowflake.analytics_db")
              .build();

      System.out.println("✓ Built database container with URN: " + database.getUrn());

      // Add tags
      database.addTag("production").addTag("analytics").addTag("critical");

      System.out.println("✓ Added 3 tags");

      // Add owners
      database
          .addOwner("urn:li:corpuser:datahub", OwnershipType.TECHNICAL_OWNER)
          .addOwner("urn:li:corpuser:data_team", OwnershipType.DATA_STEWARD);

      System.out.println("✓ Added 2 owners");

      // Set domain
      database.setDomain("urn:li:domain:Analytics");

      System.out.println("✓ Set domain");

      // Upsert to DataHub
      client.entities().upsert(database);

      System.out.println("✓ Successfully created database container in DataHub!");
      System.out.println("\n  URN: " + database.getUrn());
      System.out.println(
          "  View in DataHub: "
              + client.getConfig().getServer()
              + "/container/"
              + database.getUrn());

    } finally {
      client.close();
    }
  }
}
