package io.datahubproject.examples.v2;

import com.linkedin.common.OwnershipType;
import datahub.client.v2.DataHubClientV2;
import datahub.client.v2.entity.Dataset;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Example demonstrating patch-based updates using Java SDK V2.
 *
 * <p>This example shows:
 *
 * <ul>
 *   <li>Adding tags, owners, and properties to existing dataset
 *   <li>Patch accumulation pattern
 *   <li>Efficient incremental updates
 * </ul>
 */
public class DatasetPatchExample {

  public static void main(String[] args)
      throws IOException, ExecutionException, InterruptedException {
    // Create client
    DataHubClientV2 client =
        DataHubClientV2.builder()
            .server(System.getenv().getOrDefault("DATAHUB_SERVER", "http://localhost:8080"))
            .token(System.getenv("DATAHUB_TOKEN"))
            .build();

    try {
      // Create a simple dataset
      Dataset dataset =
          Dataset.builder()
              .platform("snowflake")
              .name("analytics.public.user_events")
              .env("PROD")
              .build();

      System.out.println("Dataset URN: " + dataset.getUrn());

      // Add metadata using patches (accumulate without emitting)
      dataset
          .addTag("pii")
          .addTag("analytics")
          .addOwner("urn:li:corpuser:datahub", OwnershipType.TECHNICAL_OWNER)
          .addCustomProperty("team", "data-engineering");

      System.out.println("Accumulated " + dataset.getPendingPatches().size() + " patches");

      // Emit all patches in single upsert
      client.entities().upsert(dataset);

      System.out.println("✓ Successfully applied all patches!");

      // Later, add more patches
      dataset
          .addTag("gdpr")
          .addCustomProperty("updated_at", String.valueOf(System.currentTimeMillis()));

      System.out.println("Accumulated " + dataset.getPendingPatches().size() + " more patches");

      // Apply new patches
      client.entities().upsert(dataset);

      System.out.println("✓ Successfully applied additional patches!");

    } finally {
      client.close();
    }
  }
}
