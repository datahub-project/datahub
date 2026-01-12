package datahub.client.v2.integration;

import static org.junit.Assert.*;

import com.linkedin.common.OwnershipType;
import datahub.client.v2.entity.MLModel;
import org.junit.Test;

/**
 * Integration tests for MLModel entity with actual DataHub server.
 *
 * <p>These tests require a running DataHub instance. Set DATAHUB_SERVER environment variable.
 *
 * <p>To run: export DATAHUB_SERVER=http://localhost:8080 ./gradlew
 * :metadata-integration:java:datahub-client:test --tests "*Integration*"
 */
public class MLModelIntegrationTest extends BaseIntegrationTest {

  @Test
  public void testMLModelCreateMinimal() throws Exception {
    MLModel model =
        MLModel.builder()
            .platform("tensorflow")
            .name("test_model_minimal_" + System.currentTimeMillis())
            .build();

    client.entities().upsert(model);

    assertNotNull(model.getUrn());
  }

  @Test
  public void testMLModelCreateWithMetadata() throws Exception {
    MLModel model =
        MLModel.builder()
            .platform("tensorflow")
            .name("test_model_with_metadata_" + System.currentTimeMillis())
            .displayName("Test ML Model")
            .description("This is a test ML model created by Java SDK V2")
            .build();

    client.entities().upsert(model);

    assertNotNull(model.getUrn());
  }

  @Test
  public void testMLModelWithTags() throws Exception {
    MLModel model =
        MLModel.builder()
            .platform("tensorflow")
            .name("test_model_with_tags_" + System.currentTimeMillis())
            .displayName("Model with tags")
            .build();

    model.addTag("test-tag-1");
    model.addTag("test-tag-2");
    model.addTag("production");

    client.entities().upsert(model);

    assertNotNull(model.getUrn());
  }

  @Test
  public void testMLModelWithOwners() throws Exception {
    MLModel model =
        MLModel.builder()
            .platform("tensorflow")
            .name("test_model_with_owners_" + System.currentTimeMillis())
            .displayName("Model with owners")
            .build();

    model.addOwner("urn:li:corpuser:datahub", OwnershipType.TECHNICAL_OWNER);
    model.addOwner("urn:li:corpuser:admin", OwnershipType.BUSINESS_OWNER);

    client.entities().upsert(model);

    assertNotNull(model.getUrn());
  }

  @Test
  public void testMLModelWithDomain() throws Exception {
    MLModel model =
        MLModel.builder()
            .platform("tensorflow")
            .name("test_model_with_domain_" + System.currentTimeMillis())
            .displayName("Model with domain")
            .build();

    model.setDomain("urn:li:domain:Engineering");

    client.entities().upsert(model);

    assertNotNull(model.getUrn());
  }

  @Test
  public void testMLModelWithCustomProperties() throws Exception {
    MLModel model =
        MLModel.builder()
            .platform("tensorflow")
            .name("test_model_with_custom_props_" + System.currentTimeMillis())
            .displayName("Model with custom properties")
            .build();

    model.addCustomProperty("framework", "tensorflow");
    model.addCustomProperty("version", "2.10");
    model.addCustomProperty("created_by", "java_sdk_v2");

    client.entities().upsert(model);

    assertNotNull(model.getUrn());
  }

  @Test
  public void testMLModelWithMetrics() throws Exception {
    MLModel model =
        MLModel.builder()
            .platform("tensorflow")
            .name("test_model_with_metrics_" + System.currentTimeMillis())
            .displayName("Model with metrics")
            .build();

    model.addTrainingMetric("accuracy", "0.94");
    model.addTrainingMetric("precision", "0.91");
    model.addTrainingMetric("recall", "0.89");

    client.entities().upsert(model);

    assertNotNull(model.getUrn());
  }

  @Test
  public void testMLModelWithHyperParams() throws Exception {
    MLModel model =
        MLModel.builder()
            .platform("tensorflow")
            .name("test_model_with_hyperparams_" + System.currentTimeMillis())
            .displayName("Model with hyperparameters")
            .build();

    model.addHyperParam("learning_rate", "0.001");
    model.addHyperParam("batch_size", "32");
    model.addHyperParam("epochs", "100");

    client.entities().upsert(model);

    assertNotNull(model.getUrn());
  }

  @Test
  public void testMLModelWithProperties() throws Exception {
    MLModel model =
        MLModel.builder()
            .platform("tensorflow")
            .name("test_model_with_properties_" + System.currentTimeMillis())
            .displayName("Model with properties")
            .build();

    model.setExternalUrl("https://model-registry.example.com/models/test-model");

    client.entities().upsert(model);

    assertNotNull(model.getUrn());
  }

  @Test
  public void testMLModelFullMetadata() throws Exception {
    String testRunValue = String.valueOf(System.currentTimeMillis());
    MLModel model =
        MLModel.builder()
            .platform("tensorflow")
            .name("test_model_full_metadata_" + System.currentTimeMillis())
            .displayName("Complete ML Model")
            .build();

    // Add all types of metadata
    model.setDescription("Complete model with all metadata from Java SDK V2");

    model.addTag("java-sdk-v2");
    model.addTag("integration-test");
    model.addTag("ml-model");

    model.addOwner("urn:li:corpuser:datahub", OwnershipType.TECHNICAL_OWNER);
    model.setDomain("urn:li:domain:MachineLearning");

    model.addCustomProperty("created_by", "java_sdk_v2");
    model.addCustomProperty("test_run", testRunValue);

    model.addTrainingMetric("accuracy", "0.96");
    model.addTrainingMetric("f1_score", "0.94");

    model.addHyperParam("learning_rate", "0.001");
    model.addHyperParam("batch_size", "64");

    model.setExternalUrl("https://model-registry.example.com/models/complete-model");

    client.entities().upsert(model);

    assertNotNull(model.getUrn());

    // Validate all metadata was written correctly
    MLModel fetched = client.entities().get(model.getUrn().toString(), MLModel.class);
    assertNotNull(fetched);

    // Validate description directly from aspect
    com.linkedin.ml.metadata.MLModelProperties props =
        fetched.getAspectLazy(com.linkedin.ml.metadata.MLModelProperties.class);
    assertNotNull("MLModelProperties aspect should exist", props);
    assertNotNull("Description should exist", props.getDescription());
    assertEquals("Complete model with all metadata from Java SDK V2", props.getDescription());

    // Validate tags
    validateEntityHasTags(
        model.getUrn().toString(),
        MLModel.class,
        java.util.Arrays.asList("java-sdk-v2", "integration-test", "ml-model"));

    // Validate owners
    validateEntityHasOwners(
        model.getUrn().toString(),
        MLModel.class,
        java.util.Arrays.asList("urn:li:corpuser:datahub"));

    // Validate custom properties
    java.util.Map<String, String> expectedCustomProps = new java.util.HashMap<>();
    expectedCustomProps.put("created_by", "java_sdk_v2");
    expectedCustomProps.put("test_run", testRunValue);
    validateEntityCustomProperties(model.getUrn().toString(), MLModel.class, expectedCustomProps);

    // Verify training metrics exist (reuse props from above)
    assertNotNull("Training metrics should exist", props.getTrainingMetrics());
    assertEquals("Should have 2 training metrics", 2, props.getTrainingMetrics().size());

    // Verify hyperparams exist
    assertNotNull("Hyperparams should exist", props.getHyperParams());
    assertEquals("Should have 2 hyperparams", 2, props.getHyperParams().size());

    // Verify external URL
    assertNotNull("External URL should exist", props.getExternalUrl());
    assertEquals(
        "https://model-registry.example.com/models/complete-model",
        props.getExternalUrl().toString());
  }

  @Test
  public void testMultipleMLModelCreation() throws Exception {
    // Create multiple models in sequence
    for (int i = 0; i < 5; i++) {
      MLModel model =
          MLModel.builder()
              .platform("tensorflow")
              .name("test_model_multi_" + i + "_" + System.currentTimeMillis())
              .displayName("Model number " + i)
              .build();

      model.addTag("batch-test");
      model.addCustomProperty("index", String.valueOf(i));

      client.entities().upsert(model);
    }

    // If we get here, all models were created successfully
    assertTrue(true);
  }

  @Test
  public void testMLModelMultipleOperationsSameAspect() throws Exception {
    // Test that multiple operations on the same aspect (mlModelProperties) don't drop updates
    // when performed on the same entity instance before upsert
    MLModel model =
        MLModel.builder()
            .platform("tensorflow")
            .name("test_model_same_aspect_" + System.currentTimeMillis())
            .displayName("Model with multiple aspect operations")
            .build();

    // All these operations modify mlModelProperties aspect
    model.addCustomProperty("prop1", "value1");
    model.addCustomProperty("prop2", "value2");
    model.setDescription("Test description");
    model.addCustomProperty("prop3", "value3");
    model.setExternalUrl("https://example.com/model");
    model.addTrainingMetric("accuracy", "0.95");
    model.addCustomProperty("prop4", "value4");
    model.addHyperParam("learning_rate", "0.001");

    client.entities().upsert(model);

    assertNotNull(model.getUrn());

    // Verify all operations were preserved by fetching entity from backend
    MLModel fetched = client.entities().get(model.getUrn().toString(), MLModel.class);
    assertNotNull(fetched);

    com.linkedin.ml.metadata.MLModelProperties props =
        fetched.getAspectLazy(com.linkedin.ml.metadata.MLModelProperties.class);
    assertNotNull(props);

    // Verify all 4 custom properties are present
    assertTrue(props.hasCustomProperties());
    assertEquals("value1", props.getCustomProperties().get("prop1"));
    assertEquals("value2", props.getCustomProperties().get("prop2"));
    assertEquals("value3", props.getCustomProperties().get("prop3"));
    assertEquals("value4", props.getCustomProperties().get("prop4"));

    // Verify description, externalUrl, training metric, and hyperParam
    assertEquals("Test description", props.getDescription());
    assertEquals("https://example.com/model", props.getExternalUrl().toString());
    assertNotNull(props.getTrainingMetrics());
    assertEquals(1, props.getTrainingMetrics().size());
    assertEquals("accuracy", props.getTrainingMetrics().get(0).getName());
    assertEquals("0.95", props.getTrainingMetrics().get(0).getValue());
    assertNotNull(props.getHyperParams());
    assertEquals(1, props.getHyperParams().size());
    assertEquals("learning_rate", props.getHyperParams().get(0).getName());
    assertEquals("0.001", props.getHyperParams().get(0).getValue());
  }

  @Test
  public void testMLModelUpdateExistingData() throws Exception {
    // Test that updating an existing entity preserves existing data
    // This is the critical test for dropped updates
    String modelName = "test_model_update_existing_" + System.currentTimeMillis();

    // Step 1: Create initial model with some custom properties
    MLModel model1 =
        MLModel.builder()
            .platform("tensorflow")
            .name(modelName)
            .displayName("Initial model")
            .description("Initial description")
            .build();

    model1.addCustomProperty("initial_prop1", "initial_value1");
    model1.addCustomProperty("initial_prop2", "initial_value2");
    model1.addTrainingMetric("initial_metric", "0.90");

    client.entities().upsert(model1);
    assertNotNull(model1.getUrn());

    // Step 2: Create NEW entity instance with same URN and add more properties
    // This simulates a separate process/session updating the same entity
    // Use ONLY patches for updates (no builder properties that would replace data)
    MLModel model2 = MLModel.builder().platform("tensorflow").name(modelName).build();

    model2.addCustomProperty("new_prop1", "new_value1");
    model2.addCustomProperty("new_prop2", "new_value2");
    model2.addTrainingMetric("new_metric", "0.95");

    client.entities().upsert(model2);

    // Step 3: Verify behavior by fetching the entity
    // Expected: All properties from BOTH model1 and model2 should be present
    // If direct aspect modification causes dropped updates, only model2 properties will be present
    MLModel fetched = client.entities().get(model2.getUrn().toString(), MLModel.class);
    assertNotNull(fetched);

    com.linkedin.ml.metadata.MLModelProperties props =
        fetched.getAspectLazy(com.linkedin.ml.metadata.MLModelProperties.class);
    assertNotNull(props);

    // Verify initial properties from model1 are still present
    assertTrue(
        "initial_prop1 should be present (no dropped updates)",
        props.hasCustomProperties() && props.getCustomProperties().containsKey("initial_prop1"));
    assertEquals("initial_value1", props.getCustomProperties().get("initial_prop1"));
    assertEquals("initial_value2", props.getCustomProperties().get("initial_prop2"));

    // Verify new properties from model2 are present
    assertTrue(props.getCustomProperties().containsKey("new_prop1"));
    assertEquals("new_value1", props.getCustomProperties().get("new_prop1"));
    assertEquals("new_value2", props.getCustomProperties().get("new_prop2"));

    // Verify both training metrics are present
    assertNotNull(props.getTrainingMetrics());
    assertEquals(
        "Both training metrics should be present (no dropped updates)",
        2,
        props.getTrainingMetrics().size());
    boolean hasInitialMetric = false;
    boolean hasNewMetric = false;
    for (com.linkedin.ml.metadata.MLMetric metric : props.getTrainingMetrics()) {
      if ("initial_metric".equals(metric.getName())) {
        hasInitialMetric = true;
        assertEquals("0.90", metric.getValue());
      }
      if ("new_metric".equals(metric.getName())) {
        hasNewMetric = true;
        assertEquals("0.95", metric.getValue());
      }
    }
    assertTrue("initial_metric should be present", hasInitialMetric);
    assertTrue("new_metric should be present", hasNewMetric);
  }

  @Test
  public void testMLModelSequentialUpdates() throws Exception {
    // Test a sequence of updates to verify no data loss
    String modelName = "test_model_sequential_" + System.currentTimeMillis();

    // Update 1: Create with initial custom property
    MLModel model1 = MLModel.builder().platform("tensorflow").name(modelName).build();
    model1.addCustomProperty("update1_prop", "value1");
    client.entities().upsert(model1);

    // Update 2: Add another custom property (new instance)
    MLModel model2 = MLModel.builder().platform("tensorflow").name(modelName).build();
    model2.addCustomProperty("update2_prop", "value2");
    client.entities().upsert(model2);

    // Update 3: Add third custom property (new instance)
    MLModel model3 = MLModel.builder().platform("tensorflow").name(modelName).build();
    model3.addCustomProperty("update3_prop", "value3");
    client.entities().upsert(model3);

    // Verify all three updates are preserved (no dropped updates)
    MLModel fetched = client.entities().get(model3.getUrn().toString(), MLModel.class);
    assertNotNull(fetched);

    com.linkedin.ml.metadata.MLModelProperties props =
        fetched.getAspectLazy(com.linkedin.ml.metadata.MLModelProperties.class);
    assertNotNull(props);

    // Expected: All three properties should be present
    // If direct aspect modification causes dropped updates, only update3_prop will be present
    assertTrue("update1_prop should be present", props.hasCustomProperties());
    assertTrue(
        "update1_prop should be present (no dropped updates)",
        props.getCustomProperties().containsKey("update1_prop"));
    assertEquals("value1", props.getCustomProperties().get("update1_prop"));

    assertTrue(
        "update2_prop should be present (no dropped updates)",
        props.getCustomProperties().containsKey("update2_prop"));
    assertEquals("value2", props.getCustomProperties().get("update2_prop"));

    assertTrue(
        "update3_prop should be present (no dropped updates)",
        props.getCustomProperties().containsKey("update3_prop"));
    assertEquals("value3", props.getCustomProperties().get("update3_prop"));
  }
}
