package datahub.client.v2.integration;

import static org.junit.Assert.*;

import datahub.client.v2.DataHubClientV2;
import datahub.client.v2.config.DataHubClientConfigV2;
import datahub.client.v2.config.EmitMode;
import datahub.client.v2.entity.Dataset;
import org.junit.Test;

/**
 * Integration tests for EmitMode functionality.
 *
 * <p>Tests the default SYNC_PRIMARY mode to ensure proper consistency guarantees: - Primary storage
 * (SQL) updates are synchronous - Search index (Elasticsearch) updates are asynchronous - Race
 * conditions are handled correctly
 */
public class EmitModeIntegrationTest extends BaseIntegrationTest {

  /**
   * Test SYNC_PRIMARY mode (default) with rapid write-update-read sequence.
   *
   * <p>This test verifies that: 1. Initial write is immediately visible in primary storage 2.
   * Subsequent update is immediately visible in primary storage 3. Read reflects the latest update
   * (not stale data)
   *
   * <p>SYNC_PRIMARY mode guarantees that reads from primary storage are consistent, even though
   * search index updates are asynchronous.
   */
  @Test
  public void testSyncPrimaryMode_WriteUpdateRead() throws Exception {
    // Create a separate client with SYNC_PRIMARY mode (default) for this test
    DataHubClientConfigV2 config =
        DataHubClientConfigV2.builder()
            .server(TEST_SERVER)
            .token(client.getConfig().getToken())
            .emitMode(EmitMode.SYNC_PRIMARY) // Explicit for clarity
            .build();

    DataHubClientV2 testClient = DataHubClientV2.builder().config(config).build();

    String datasetName = "test_sync_primary_" + System.currentTimeMillis();

    try {
      // Step 1: Write initial dataset with description
      Dataset dataset =
          Dataset.builder()
              .platform("java_sdk_v2_test")
              .name(datasetName)
              .env("DEV")
              .description("Initial description")
              .build();

      testClient.entities().upsert(dataset);

      // Step 2: Immediately update the dataset with new description
      Dataset updatedDataset =
          Dataset.builder()
              .platform("java_sdk_v2_test")
              .name(datasetName)
              .env("DEV")
              .description("Updated description")
              .build();

      testClient.entities().upsert(updatedDataset);

      // Step 3: Immediately read back the dataset
      String datasetUrn = dataset.getUrn().toString();
      Dataset readDataset = testClient.entities().get(datasetUrn, Dataset.class);

      // Verify the read reflects the latest update
      assertNotNull("Dataset should be found", readDataset);
      assertEquals(
          "Description should reflect the latest update",
          "Updated description",
          readDataset.getDescription());

    } finally {
      testClient.close();
    }
  }

  /**
   * Test that SYNC_PRIMARY is the default mode when not explicitly specified.
   *
   * <p>Verifies that clients created without specifying emitMode use SYNC_PRIMARY by default.
   */
  @Test
  public void testDefaultEmitMode() {
    // Verify the default mode is SYNC_PRIMARY
    assertEquals(
        "Default emit mode should be SYNC_PRIMARY",
        EmitMode.SYNC_PRIMARY,
        client.getConfig().getEmitMode());
  }
}
