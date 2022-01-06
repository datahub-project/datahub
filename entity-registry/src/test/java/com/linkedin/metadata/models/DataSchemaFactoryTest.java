package com.linkedin.metadata.models;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.metadata.models.registry.TestConstants;
import java.nio.file.Paths;
import java.util.Optional;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class DataSchemaFactoryTest {
  @Test
  public void testCustomClassLoading() throws Exception {
    DataSchemaFactory dsf = DataSchemaFactory.withCustomClasspath(Paths.get(
        TestConstants.BASE_DIRECTORY + "/" + TestConstants.TEST_REGISTRY + "/"
            + TestConstants.TEST_VERSION.toString()));
    // Assert that normally found aspects from the core model are missing
    Optional<DataSchema> dataSchema = dsf.getAspectSchema("datasetProfile");
    assertFalse(dataSchema.isPresent(), "datasetProfile");
    // Assert that we're able to find the aspect schema from the test model
    dataSchema = dsf.getAspectSchema(TestConstants.TEST_ASPECT_NAME);
    assertTrue(dataSchema.isPresent(), TestConstants.TEST_ASPECT_NAME);
  }
}
