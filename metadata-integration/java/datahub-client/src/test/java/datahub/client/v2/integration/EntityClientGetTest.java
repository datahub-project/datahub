package datahub.client.v2.integration;

import static org.junit.Assert.*;

import datahub.client.v2.entity.Dataset;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.junit.Test;

/**
 * Focused integration tests for EntityClient.get() method.
 *
 * <p>These tests specifically test the get() functionality in isolation.
 */
public class EntityClientGetTest extends BaseIntegrationTest {

  @Test
  public void testGetDatasetAfterWrite()
      throws IOException, ExecutionException, InterruptedException {
    // Step 1: Create and write a simple dataset
    Dataset dataset =
        Dataset.builder()
            .platform("java_sdk_v2_test")
            .name("test_get_method")
            .env("DEV")
            .description("Testing the get method")
            .displayName("Get Test Dataset")
            .build();

    System.out.println("=== STEP 1: Writing dataset ===");
    client.entities().upsert(dataset);
    System.out.println("Successfully wrote dataset: " + dataset.getUrn());

    // Step 2: Try to read it back using get()
    System.out.println("\n=== STEP 2: Reading dataset back ===");
    String urn = dataset.getUrn().toString();
    System.out.println("Reading URN: " + urn);

    try {
      Dataset retrieved = client.entities().get(urn, Dataset.class);
      System.out.println("Successfully retrieved dataset: " + retrieved);

      // Step 3: Verify basic properties
      assertNotNull("Retrieved dataset should not be null", retrieved);
      assertEquals("URN should match", urn, retrieved.getUrn().toString());

      // Step 4: Try to access description
      System.out.println("\n=== STEP 3: Checking description ===");
      String description = retrieved.getDescription();
      System.out.println("Retrieved description: " + description);

      assertNotNull("Description should not be null", description);
      assertEquals("Description should match", "Testing the get method", description);

    } catch (Exception e) {
      System.err.println("ERROR: Failed to get dataset");
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  public void testGetWithCustomAspects()
      throws IOException, ExecutionException, InterruptedException {
    // Create a dataset
    Dataset dataset =
        Dataset.builder()
            .platform("java_sdk_v2_test")
            .name("test_get_custom_aspects")
            .env("DEV")
            .description("Testing custom aspects")
            .build();

    System.out.println("=== Writing dataset ===");
    client.entities().upsert(dataset);

    // Try to get with specific aspects
    System.out.println("\n=== Reading with custom aspects ===");
    try {
      Dataset retrieved =
          client
              .entities()
              .get(
                  dataset.getUrn().toString(),
                  Dataset.class,
                  java.util.List.of(com.linkedin.dataset.DatasetProperties.class));

      assertNotNull("Retrieved dataset should not be null", retrieved);
      System.out.println("Successfully retrieved with custom aspects");
    } catch (Exception e) {
      System.err.println("ERROR: Failed to get dataset with custom aspects");
      e.printStackTrace();
      throw e;
    }
  }
}
