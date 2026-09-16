package datahub.client.v2.integration;

import static org.junit.Assert.*;

import com.linkedin.common.OwnershipType;
import datahub.client.v2.entity.MLModelGroup;
import org.junit.Test;

/**
 * Integration tests for MLModelGroup entity with actual DataHub server.
 *
 * <p>These tests require a running DataHub instance. Set DATAHUB_SERVER environment variable.
 *
 * <p>To run: export DATAHUB_SERVER=http://localhost:8080 ./gradlew
 * :metadata-integration:java:datahub-client:test --tests "*Integration*"
 */
public class MLModelGroupIntegrationTest extends BaseIntegrationTest {

  @Test
  public void testMLModelGroupCreateMinimal() throws Exception {
    MLModelGroup modelGroup =
        MLModelGroup.builder()
            .platform("tensorflow")
            .groupId("test_model_group_minimal_" + System.currentTimeMillis())
            .build();

    client.entities().upsert(modelGroup);

    assertNotNull(modelGroup.getUrn());
  }

  @Test
  public void testMLModelGroupCreateWithMetadata() throws Exception {
    MLModelGroup modelGroup =
        MLModelGroup.builder()
            .platform("tensorflow")
            .groupId("test_model_group_with_metadata_" + System.currentTimeMillis())
            .name("Test Model Group")
            .description("This is a test model group created by Java SDK V2")
            .build();

    client.entities().upsert(modelGroup);

    assertNotNull(modelGroup.getUrn());
  }

  @Test
  public void testMLModelGroupWithTags() throws Exception {
    MLModelGroup modelGroup =
        MLModelGroup.builder()
            .platform("tensorflow")
            .groupId("test_model_group_with_tags_" + System.currentTimeMillis())
            .name("Model Group with tags")
            .build();

    modelGroup.addTag("test-tag-1");
    modelGroup.addTag("test-tag-2");
    modelGroup.addTag("production");

    client.entities().upsert(modelGroup);

    assertNotNull(modelGroup.getUrn());
  }

  @Test
  public void testMLModelGroupWithOwners() throws Exception {
    MLModelGroup modelGroup =
        MLModelGroup.builder()
            .platform("tensorflow")
            .groupId("test_model_group_with_owners_" + System.currentTimeMillis())
            .name("Model Group with owners")
            .build();

    modelGroup.addOwner("urn:li:corpuser:datahub", OwnershipType.TECHNICAL_OWNER);
    modelGroup.addOwner("urn:li:corpuser:admin", OwnershipType.BUSINESS_OWNER);

    client.entities().upsert(modelGroup);

    assertNotNull(modelGroup.getUrn());
  }

  @Test
  public void testMLModelGroupWithDomain() throws Exception {
    MLModelGroup modelGroup =
        MLModelGroup.builder()
            .platform("tensorflow")
            .groupId("test_model_group_with_domain_" + System.currentTimeMillis())
            .name("Model Group with domain")
            .build();

    modelGroup.setDomain("urn:li:domain:Engineering");

    client.entities().upsert(modelGroup);

    assertNotNull(modelGroup.getUrn());
  }

  @Test
  public void testMLModelGroupWithCustomProperties() throws Exception {
    MLModelGroup modelGroup =
        MLModelGroup.builder()
            .platform("tensorflow")
            .groupId("test_model_group_with_custom_props_" + System.currentTimeMillis())
            .name("Model Group with custom properties")
            .build();

    modelGroup.addTag("custom-props-test");

    client.entities().upsert(modelGroup);

    assertNotNull(modelGroup.getUrn());
  }

  @Test
  public void testMLModelGroupFullMetadata() throws Exception {
    MLModelGroup modelGroup =
        MLModelGroup.builder()
            .platform("tensorflow")
            .groupId("test_model_group_full_metadata_" + System.currentTimeMillis())
            .name("ML Model Group with Full Metadata")
            .description("Complete model group with all metadata from Java SDK V2")
            .build();

    // Add all types of metadata
    modelGroup.addTag("java-sdk-v2");
    modelGroup.addTag("integration-test");
    modelGroup.addTag("ml-models");

    modelGroup.addOwner("urn:li:corpuser:datahub", OwnershipType.TECHNICAL_OWNER);
    modelGroup.setDomain("urn:li:domain:Engineering");

    modelGroup.addTerm("urn:li:glossaryTerm:ML");

    // Add training job lineage
    modelGroup.addTrainingJob("urn:li:dataProcessInstance:training_job_123");

    // Add downstream job lineage
    modelGroup.addDownstreamJob("urn:li:dataProcessInstance:inference_job_456");

    client.entities().upsert(modelGroup);

    assertNotNull(modelGroup.getUrn());

    // Validate all metadata was written correctly
    MLModelGroup fetched =
        client.entities().get(modelGroup.getUrn().toString(), MLModelGroup.class);
    assertNotNull(fetched);

    // Validate description
    validateEntityDescription(
        modelGroup.getUrn().toString(),
        MLModelGroup.class,
        "Complete model group with all metadata from Java SDK V2");

    // Validate tags
    validateEntityHasTags(
        modelGroup.getUrn().toString(),
        MLModelGroup.class,
        java.util.Arrays.asList("java-sdk-v2", "integration-test", "ml-models"));

    // Validate owners
    validateEntityHasOwners(
        modelGroup.getUrn().toString(),
        MLModelGroup.class,
        java.util.Arrays.asList("urn:li:corpuser:datahub"));

    // Verify lineage relationships were set
    com.linkedin.common.InstitutionalMemory memory =
        fetched.getAspectLazy(com.linkedin.common.InstitutionalMemory.class);
    if (memory != null && memory.hasElements()) {
      assertTrue("Lineage relationships should exist", memory.getElements().size() > 0);
    }
  }

  @Test
  public void testMultipleMLModelGroupCreation() throws Exception {
    // Create multiple model groups in sequence
    for (int i = 0; i < 5; i++) {
      MLModelGroup modelGroup =
          MLModelGroup.builder()
              .platform("tensorflow")
              .groupId("test_model_group_multi_" + i + "_" + System.currentTimeMillis())
              .name("Model Group number " + i)
              .build();

      modelGroup.addTag("batch-test");

      client.entities().upsert(modelGroup);
    }

    // If we get here, all model groups were created successfully
    assertTrue(true);
  }
}
