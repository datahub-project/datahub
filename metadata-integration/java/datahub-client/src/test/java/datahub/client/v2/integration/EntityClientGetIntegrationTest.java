package datahub.client.v2.integration;

import static org.junit.Assert.*;

import com.linkedin.dataset.DatasetProperties;
import datahub.client.v2.entity.Dataset;
import java.util.List;
import org.junit.Test;

/**
 * Focused integration tests for EntityClient.get() method.
 *
 * <p>These tests specifically test the get() functionality in isolation.
 */
public class EntityClientGetIntegrationTest extends BaseIntegrationTest {

  @Test
  public void testGetDatasetAfterWrite() throws Exception {
    Dataset dataset =
        Dataset.builder()
            .platform("java_sdk_v2_test")
            .name("test_get_method_" + System.currentTimeMillis())
            .env("DEV")
            .description("Testing the get method")
            .displayName("Get Test Dataset")
            .build();
    client.entities().upsert(dataset);

    String urn = dataset.getUrn().toString();
    Dataset retrieved = client.entities().get(urn, Dataset.class);

    assertNotNull("Retrieved dataset should not be null", retrieved);
    assertEquals("URN should match", urn, retrieved.getUrn().toString());
    assertEquals("Description should match", "Testing the get method", retrieved.getDescription());
  }

  @Test
  public void testGetWithCustomAspects() throws Exception {
    Dataset dataset =
        Dataset.builder()
            .platform("java_sdk_v2_test")
            .name("test_get_custom_aspects_" + System.currentTimeMillis())
            .env("DEV")
            .description("Testing custom aspects")
            .build();
    client.entities().upsert(dataset);

    Dataset retrieved =
        client
            .entities()
            .get(dataset.getUrn().toString(), Dataset.class, List.of(DatasetProperties.class));

    assertNotNull("Retrieved dataset should not be null", retrieved);
    DatasetProperties props = retrieved.getAspectLazy(DatasetProperties.class);
    assertNotNull("DatasetProperties should be fetched", props);
    assertEquals("Testing custom aspects", props.getDescription());
  }
}
