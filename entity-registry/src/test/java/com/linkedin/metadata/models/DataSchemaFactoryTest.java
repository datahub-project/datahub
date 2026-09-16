package com.linkedin.metadata.models;

import static org.testng.Assert.*;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.metadata.models.registry.TestConstants;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import org.testng.annotations.Test;

public class DataSchemaFactoryTest {

  /**
   * Integration Test: Optimized vs Sequential Loading Produces Identical Schemas
   *
   * <p>Loads DataSchemaFactory with both optimized and sequential modes and verifies that all 4
   * internal maps contain identical data: - entitySchemas - aspectSchemas - eventSchemas -
   * aspectClasses
   */
  @Test
  public void testOptimizedLoadingProducesIdenticalSchemas() {
    DataSchemaFactory sequential = new DataSchemaFactory(false);
    DataSchemaFactory optimized = new DataSchemaFactory(true);

    validateMaps(
        optimized.getEntitySchemaMap(),
        sequential.getEntitySchemaMap(),
        "entitySchemas should be identical");
    validateMaps(
        optimized.getAspectSchemaMap(),
        sequential.getAspectSchemaMap(),
        "aspectSchemas should be identical");
    validateMaps(
        optimized.getEventSchemaMap(),
        sequential.getEventSchemaMap(),
        "eventSchemas should be identical");
    validateMaps(
        optimized.getAspectClassMap(),
        sequential.getAspectClassMap(),
        "aspectClasses should be identical");
  }

  private void validateMaps(Map<?, ?> expected, Map<?, ?> actual, String message) {
    assertEquals(actual.size(), expected.size(), message);
    for (Map.Entry<?, ?> entry : expected.entrySet()) {
      assertTrue(actual.containsKey(entry.getKey()), message + ": missing key " + entry.getKey());
    }
  }

  @Test
  public void testCustomClassLoading() throws Exception {
    DataSchemaFactory dsf =
        DataSchemaFactory.withCustomClasspath(
            Paths.get(
                TestConstants.BASE_DIRECTORY
                    + "/"
                    + TestConstants.TEST_REGISTRY
                    + "/"
                    + TestConstants.TEST_VERSION.toString()));
    // Assert that normally found aspects from the core model are missing
    Optional<DataSchema> dataSchema = dsf.getAspectSchema("datasetProfile");
    assertFalse(dataSchema.isPresent(), "datasetProfile");
    // Assert that we're able to find the aspect schema from the test model
    dataSchema = dsf.getAspectSchema(TestConstants.TEST_ASPECT_NAME);
    assertTrue(dataSchema.isPresent(), TestConstants.TEST_ASPECT_NAME);
  }
}
