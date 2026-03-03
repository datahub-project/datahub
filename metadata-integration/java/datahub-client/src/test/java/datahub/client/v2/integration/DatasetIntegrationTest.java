package datahub.client.v2.integration;

import static org.junit.Assert.*;

import com.linkedin.common.OwnershipType;
import datahub.client.v2.entity.Dataset;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.junit.Test;

/**
 * Integration tests for Dataset entity with actual DataHub server.
 *
 * <p>These tests require a running DataHub instance. Set DATAHUB_SERVER environment variable.
 *
 * <p>To run: export DATAHUB_SERVER=http://localhost:8080 ./gradlew
 * :metadata-integration:java:datahub-client:test --tests "*Integration*"
 */
public class DatasetIntegrationTest extends BaseIntegrationTest {

  @Test
  public void testDatasetCreateMinimal()
      throws IOException, ExecutionException, InterruptedException {
    Dataset dataset =
        Dataset.builder()
            .platform("java_sdk_v2_test")
            .name("test_dataset_minimal")
            .env("DEV")
            .build();

    client.entities().upsert(dataset);

    // If we get here without exception, test passed
    assertNotNull(dataset.getUrn());
  }

  @Test
  public void testDatasetCreateWithDescription() throws Exception {
    String uniqueName = "test_dataset_with_description_" + System.currentTimeMillis();
    Dataset dataset =
        Dataset.builder()
            .platform("java_sdk_v2_test")
            .name(uniqueName)
            .env("DEV")
            .description("This is a test dataset created by Java SDK V2")
            .displayName("Test Dataset")
            .build();

    client.entities().upsert(dataset);

    assertNotNull(dataset.getUrn());

    // Validate the data was written correctly by reading it back
    validateEntityDescription(
        dataset.getUrn().toString(),
        Dataset.class,
        "This is a test dataset created by Java SDK V2");
  }

  @Test
  public void testDatasetWithTags() throws Exception {
    String uniqueName = "test_dataset_with_tags_" + System.currentTimeMillis();
    Dataset dataset =
        Dataset.builder()
            .platform("java_sdk_v2_test")
            .name(uniqueName)
            .env("DEV")
            .description("Dataset with tags")
            .build();

    dataset.addTag("test-tag-1");
    dataset.addTag("test-tag-2");
    dataset.addTag("pii");

    client.entities().upsert(dataset);

    assertNotNull(dataset.getUrn());

    // Validate tags were written correctly
    validateEntityHasTags(
        dataset.getUrn().toString(),
        Dataset.class,
        Arrays.asList("test-tag-1", "test-tag-2", "pii"));
  }

  @Test
  public void testDatasetWithOwners() throws Exception {
    String uniqueName = "test_dataset_with_owners_" + System.currentTimeMillis();
    Dataset dataset =
        Dataset.builder()
            .platform("java_sdk_v2_test")
            .name(uniqueName)
            .env("DEV")
            .description("Dataset with owners")
            .build();

    dataset.addOwner("urn:li:corpuser:datahub", OwnershipType.TECHNICAL_OWNER);
    dataset.addOwner("urn:li:corpuser:admin", OwnershipType.TECHNICAL_OWNER);

    client.entities().upsert(dataset);

    assertNotNull(dataset.getUrn());

    // Validate owners were written correctly
    validateEntityHasOwners(
        dataset.getUrn().toString(),
        Dataset.class,
        Arrays.asList("urn:li:corpuser:datahub", "urn:li:corpuser:admin"));
  }

  @Test
  public void testDatasetWithGlossaryTerms()
      throws IOException, ExecutionException, InterruptedException {
    String uniqueName = "test_dataset_with_terms_" + System.currentTimeMillis();
    Dataset dataset =
        Dataset.builder()
            .platform("java_sdk_v2_test")
            .name(uniqueName)
            .env("DEV")
            .description("Dataset with glossary terms")
            .build();

    // Note: These terms might not exist in your DataHub instance
    // In production, you'd create the terms first or use existing ones
    dataset.addTerm("urn:li:glossaryTerm:Classification.HighlyConfidential");

    client.entities().upsert(dataset);

    assertNotNull(dataset.getUrn());
  }

  @Test
  public void testDatasetWithDomain() throws IOException, ExecutionException, InterruptedException {
    String uniqueName = "test_dataset_with_domain_" + System.currentTimeMillis();
    Dataset dataset =
        Dataset.builder()
            .platform("java_sdk_v2_test")
            .name(uniqueName)
            .env("DEV")
            .description("Dataset with domain")
            .build();

    // Note: This domain might not exist in your DataHub instance
    // Create it first or use an existing one
    dataset.setDomain("urn:li:domain:Engineering");

    client.entities().upsert(dataset);

    assertNotNull(dataset.getUrn());
  }

  @Test
  public void testDatasetWithSubTypes() throws Exception {
    String uniqueName = "test_dataset_with_subtypes_" + System.currentTimeMillis();
    Dataset dataset =
        Dataset.builder()
            .platform("java_sdk_v2_test")
            .name(uniqueName)
            .env("DEV")
            .description("Dataset with subtypes")
            .build();

    dataset.setSubTypes("view", "materialized");

    client.entities().upsert(dataset);

    assertNotNull(dataset.getUrn());

    // Validate subtypes were written correctly
    validateEntityHasSubTypes(
        dataset.getUrn().toString(), Dataset.class, Arrays.asList("view", "materialized"));
  }

  @Test
  public void testDatasetWithStructuredProperties() throws Exception {
    String uniqueName = "test_dataset_with_structured_properties_" + System.currentTimeMillis();
    Dataset dataset =
        Dataset.builder()
            .platform("java_sdk_v2_test")
            .name(uniqueName)
            .env("DEV")
            .description("Dataset with structured properties")
            .build();

    // Set single value properties (type automatically detected)
    dataset.setStructuredProperty("io.acryl.dataManagement.replicationSLA", "24h");
    dataset.setStructuredProperty("io.acryl.dataQuality.qualityScore", 95.5);

    // Set multiple value properties
    dataset.setStructuredProperty(
        "io.acryl.dataManagement.certifications", Arrays.asList("SOC2", "HIPAA", "GDPR"));
    dataset.setStructuredProperty("io.acryl.privacy.retentionDays", 90, 180, 365);

    client.entities().upsert(dataset);

    assertNotNull(dataset.getUrn());

    // Validate structured properties were written correctly
    Dataset retrieved = client.entities().get(dataset.getUrn().toString(), Dataset.class);
    assertNotNull(retrieved);

    com.linkedin.structured.StructuredProperties structuredProps =
        retrieved.getAspectLazy(com.linkedin.structured.StructuredProperties.class);
    assertNotNull(structuredProps);
    assertNotNull(structuredProps.getProperties());

    // We should have 4 structured properties
    assertEquals(4, structuredProps.getProperties().size());
  }

  @Test
  public void testDatasetWithCustomProperties() throws Exception {
    String uniqueName = "test_dataset_with_custom_properties_" + System.currentTimeMillis();
    Dataset dataset =
        Dataset.builder()
            .platform("java_sdk_v2_test")
            .name(uniqueName)
            .env("DEV")
            .description("Dataset with custom properties")
            .build();

    dataset.addCustomProperty("team", "data-engineering");
    dataset.addCustomProperty("cost_center", "12345");
    dataset.addCustomProperty("retention_period", "90_days");

    client.entities().upsert(dataset);

    assertNotNull(dataset.getUrn());

    // Validate custom properties were written correctly
    Map<String, String> expectedCustomProps = new HashMap<>();
    expectedCustomProps.put("team", "data-engineering");
    expectedCustomProps.put("cost_center", "12345");
    expectedCustomProps.put("retention_period", "90_days");
    validateEntityCustomProperties(dataset.getUrn().toString(), Dataset.class, expectedCustomProps);
  }

  @Test
  public void testDatasetFullMetadata() throws Exception {
    String uniqueName = "test_dataset_full_metadata_" + System.currentTimeMillis();
    Dataset dataset =
        Dataset.builder()
            .platform("java_sdk_v2_test")
            .name(uniqueName)
            .env("PROD")
            .description("Dataset with full metadata from Java SDK V2")
            .displayName("Full Metadata Test Dataset")
            .build();

    // Add all types of metadata
    dataset.addTag("java-sdk-v2");
    dataset.addTag("integration-test");
    dataset.addTag("pii");

    dataset.addOwner("urn:li:corpuser:datahub", OwnershipType.TECHNICAL_OWNER);
    dataset.addOwner("urn:li:corpuser:admin", OwnershipType.TECHNICAL_OWNER);

    String testRunValue = String.valueOf(System.currentTimeMillis());
    dataset.addCustomProperty("created_by", "java_sdk_v2");
    dataset.addCustomProperty("test_run", testRunValue);

    client.entities().upsert(dataset);

    assertNotNull(dataset.getUrn());

    // Validate all metadata was written correctly
    validateEntityDescription(
        dataset.getUrn().toString(), Dataset.class, "Dataset with full metadata from Java SDK V2");

    validateEntityHasTags(
        dataset.getUrn().toString(),
        Dataset.class,
        Arrays.asList("java-sdk-v2", "integration-test", "pii"));

    validateEntityHasOwners(
        dataset.getUrn().toString(),
        Dataset.class,
        Arrays.asList("urn:li:corpuser:datahub", "urn:li:corpuser:admin"));

    Map<String, String> expectedCustomProps = new HashMap<>();
    expectedCustomProps.put("created_by", "java_sdk_v2");
    expectedCustomProps.put("test_run", testRunValue);
    validateEntityCustomProperties(dataset.getUrn().toString(), Dataset.class, expectedCustomProps);
  }

  @Test
  public void testDatasetUpdate() throws Exception {
    String uniqueName = "test_dataset_update_" + System.currentTimeMillis();
    String updatedAtValue = String.valueOf(System.currentTimeMillis());
    // Create initial dataset
    Dataset dataset =
        Dataset.builder()
            .platform("java_sdk_v2_test")
            .name(uniqueName)
            .env("DEV")
            .description("Initial description")
            .build();

    client.entities().upsert(dataset);

    // Update with new metadata
    dataset.setDescription("Updated description");
    dataset.addTag("updated");
    dataset.addCustomProperty("updated_at", updatedAtValue);

    client.entities().upsert(dataset);

    assertNotNull(dataset.getUrn());

    // Validate the updates were written correctly
    Dataset fetched = client.entities().get(dataset.getUrn().toString(), Dataset.class);
    assertNotNull(fetched);

    // Verify description was changed
    validateEntityDescription(dataset.getUrn().toString(), Dataset.class, "Updated description");

    // Verify new tag was added
    validateEntityHasTags(dataset.getUrn().toString(), Dataset.class, Arrays.asList("updated"));

    // Verify new custom property was added
    Map<String, String> expectedCustomProps = new HashMap<>();
    expectedCustomProps.put("updated_at", updatedAtValue);
    validateEntityCustomProperties(dataset.getUrn().toString(), Dataset.class, expectedCustomProps);
  }

  @Test
  public void testMultipleDatasetCreation()
      throws IOException, ExecutionException, InterruptedException {
    String uniqueName = "test_dataset_multi_" + System.currentTimeMillis();
    // Create multiple datasets in sequence
    for (int i = 0; i < 5; i++) {
      Dataset dataset =
          Dataset.builder()
              .platform("java_sdk_v2_test")
              .name(uniqueName + "_" + i)
              .env("DEV")
              .description("Dataset number " + i)
              .build();

      dataset.addTag("batch-test");
      dataset.addCustomProperty("index", String.valueOf(i));

      client.entities().upsert(dataset);
    }

    // If we get here, all datasets were created successfully
    assertTrue(true);
  }

  // ========== Integration Tests for SET Operations ==========

  @Test
  public void testDatasetSetTags() throws Exception {
    // Create dataset with unique name to ensure fresh data (no caching)
    String uniqueName = "test_dataset_set_tags_" + System.currentTimeMillis();
    Dataset dataset =
        Dataset.builder()
            .platform("java_sdk_v2_test")
            .name(uniqueName)
            .env("DEV")
            .description("Testing setTags() replacement")
            .build();

    dataset.addTag("initial-tag-1");
    dataset.addTag("initial-tag-2");
    client.entities().upsert(dataset);

    // Verify initial tags
    validateEntityHasTags(
        dataset.getUrn().toString(),
        Dataset.class,
        Arrays.asList("initial-tag-1", "initial-tag-2"));

    // Now use setTags() to REPLACE all tags
    Dataset fetchedDataset = client.entities().get(dataset.getUrn().toString(), Dataset.class);
    fetchedDataset.setTags(Arrays.asList("replaced-tag-1", "replaced-tag-2", "replaced-tag-3"));
    client.entities().upsert(fetchedDataset);

    // Verify tags were REPLACED (old tags should be gone)
    validateEntityHasTags(
        dataset.getUrn().toString(),
        Dataset.class,
        Arrays.asList("replaced-tag-1", "replaced-tag-2", "replaced-tag-3"));

    // Verify old tags are NOT present
    Dataset finalDataset = client.entities().get(dataset.getUrn().toString(), Dataset.class);
    List<com.linkedin.common.TagAssociation> tags = finalDataset.getTags();
    List<String> actualTags = new ArrayList<>();
    for (com.linkedin.common.TagAssociation tagAssoc : tags) {
      String tagUrn = tagAssoc.getTag().toString();
      String tagName = tagUrn.substring(tagUrn.lastIndexOf(':') + 1);
      actualTags.add(tagName);
    }

    assertFalse(
        "Old tag 'initial-tag-1' should not be present after setTags()",
        actualTags.contains("initial-tag-1"));
    assertFalse(
        "Old tag 'initial-tag-2' should not be present after setTags()",
        actualTags.contains("initial-tag-2"));
  }

  @Test
  public void testDatasetSetOwners() throws Exception {
    String uniqueName = "test_dataset_set_owners_" + System.currentTimeMillis();
    // Create dataset with initial owners using addOwner()
    Dataset dataset =
        Dataset.builder()
            .platform("java_sdk_v2_test")
            .name(uniqueName)
            .env("DEV")
            .description("Testing setOwners() replacement")
            .build();

    dataset.addOwner("urn:li:corpuser:initial_owner1", OwnershipType.TECHNICAL_OWNER);
    dataset.addOwner("urn:li:corpuser:initial_owner2", OwnershipType.TECHNICAL_OWNER);
    client.entities().upsert(dataset);

    // Verify initial owners
    validateEntityHasOwners(
        dataset.getUrn().toString(),
        Dataset.class,
        Arrays.asList("urn:li:corpuser:initial_owner1", "urn:li:corpuser:initial_owner2"));

    // Now use setOwners() to REPLACE all owners
    Dataset fetchedDataset = client.entities().get(dataset.getUrn().toString(), Dataset.class);

    // Create Owner objects for setOwners()
    com.linkedin.common.Owner owner1 = new com.linkedin.common.Owner();
    owner1.setOwner(com.linkedin.common.urn.UrnUtils.getUrn("urn:li:corpuser:new_owner1"));
    owner1.setType(OwnershipType.BUSINESS_OWNER);

    com.linkedin.common.Owner owner2 = new com.linkedin.common.Owner();
    owner2.setOwner(com.linkedin.common.urn.UrnUtils.getUrn("urn:li:corpuser:new_owner2"));
    owner2.setType(OwnershipType.BUSINESS_OWNER);

    fetchedDataset.setOwners(Arrays.asList(owner1, owner2));
    client.entities().upsert(fetchedDataset);

    // Verify owners were REPLACED (old owners should be gone)
    validateEntityHasOwners(
        dataset.getUrn().toString(),
        Dataset.class,
        Arrays.asList("urn:li:corpuser:new_owner1", "urn:li:corpuser:new_owner2"));

    // Verify old owners are NOT present
    Dataset finalDataset = client.entities().get(dataset.getUrn().toString(), Dataset.class);
    List<com.linkedin.common.Owner> owners = finalDataset.getOwners();
    List<String> actualOwnerUrns = new ArrayList<>();
    for (com.linkedin.common.Owner owner : owners) {
      actualOwnerUrns.add(owner.getOwner().toString());
    }

    assertFalse(
        "Old owner 'initial_owner1' should not be present after setOwners()",
        actualOwnerUrns.contains("urn:li:corpuser:initial_owner1"));
    assertFalse(
        "Old owner 'initial_owner2' should not be present after setOwners()",
        actualOwnerUrns.contains("urn:li:corpuser:initial_owner2"));
  }

  @Test
  public void testDatasetSetTerms() throws Exception {
    // Create dataset with initial terms using addTerm()
    String uniqueName = "test_dataset_set_terms_" + System.currentTimeMillis();
    Dataset dataset =
        Dataset.builder()
            .platform("java_sdk_v2_test")
            .name(uniqueName)
            .env("DEV")
            .description("Testing setTerms() replacement")
            .build();

    dataset.addTerm("urn:li:glossaryTerm:InitialTerm1");
    dataset.addTerm("urn:li:glossaryTerm:InitialTerm2");
    client.entities().upsert(dataset);

    // Now use setTerms() to REPLACE all terms
    Dataset fetchedDataset = client.entities().get(dataset.getUrn().toString(), Dataset.class);
    fetchedDataset.setTerms(
        Arrays.asList(
            "urn:li:glossaryTerm:NewTerm1",
            "urn:li:glossaryTerm:NewTerm2",
            "urn:li:glossaryTerm:NewTerm3"));
    client.entities().upsert(fetchedDataset);

    // Verify terms were REPLACED
    Dataset finalDataset = client.entities().get(dataset.getUrn().toString(), Dataset.class);
    List<com.linkedin.common.GlossaryTermAssociation> terms = finalDataset.getTerms();

    assertNotNull("Terms should be present", terms);
    assertFalse("Terms should not be empty", terms.isEmpty());

    List<String> actualTermUrns = new ArrayList<>();
    for (com.linkedin.common.GlossaryTermAssociation termAssoc : terms) {
      actualTermUrns.add(termAssoc.getUrn().toString());
    }

    // Debug: print actual terms
    System.out.println("Actual terms found: " + actualTermUrns);

    // Verify new terms are present
    assertTrue(
        "New term should be present. Actual terms: " + actualTermUrns,
        actualTermUrns.contains("urn:li:glossaryTerm:NewTerm1"));
    assertTrue(
        "New term should be present", actualTermUrns.contains("urn:li:glossaryTerm:NewTerm2"));
    assertTrue(
        "New term should be present", actualTermUrns.contains("urn:li:glossaryTerm:NewTerm3"));

    // Verify old terms are NOT present
    assertFalse(
        "Old term should not be present after setTerms()",
        actualTermUrns.contains("urn:li:glossaryTerm:InitialTerm1"));
    assertFalse(
        "Old term should not be present after setTerms()",
        actualTermUrns.contains("urn:li:glossaryTerm:InitialTerm2"));
  }

  @Test
  public void testDatasetSetTagsMultipleCalls() throws Exception {
    // Test that multiple setTags() calls - last one wins
    String uniqueName = "test_dataset_set_tags_multiple_" + System.currentTimeMillis();
    Dataset dataset =
        Dataset.builder()
            .platform("java_sdk_v2_test")
            .name(uniqueName)
            .env("DEV")
            .description("Testing multiple setTags() calls")
            .build();

    // First setTags()
    dataset.setTags(Arrays.asList("first-call-tag1", "first-call-tag2"));
    // Second setTags() - should replace the first
    dataset.setTags(Arrays.asList("second-call-tag"));
    // Third setTags() - should replace the second
    dataset.setTags(Arrays.asList("final-tag-1", "final-tag-2", "final-tag-3"));

    client.entities().upsert(dataset);

    // Verify only the LAST setTags() call's tags are present
    validateEntityHasTags(
        dataset.getUrn().toString(),
        Dataset.class,
        Arrays.asList("final-tag-1", "final-tag-2", "final-tag-3"));

    // Verify tags from earlier calls are NOT present
    Dataset finalDataset = client.entities().get(dataset.getUrn().toString(), Dataset.class);
    List<com.linkedin.common.TagAssociation> tags = finalDataset.getTags();
    List<String> actualTags = new ArrayList<>();
    for (com.linkedin.common.TagAssociation tagAssoc : tags) {
      String tagUrn = tagAssoc.getTag().toString();
      String tagName = tagUrn.substring(tagUrn.lastIndexOf(':') + 1);
      actualTags.add(tagName);
    }

    assertEquals("Should have exactly 3 tags", 3, actualTags.size());
    assertFalse("First call tags should not be present", actualTags.contains("first-call-tag1"));
    assertFalse("First call tags should not be present", actualTags.contains("first-call-tag2"));
    assertFalse("Second call tags should not be present", actualTags.contains("second-call-tag"));
  }

  @Test
  public void testDatasetMixedAddAndSetTags() throws Exception {
    // Test that setTags() clears any pending addTag() patches
    String uniqueName = "test_dataset_mixed_add_set_" + System.currentTimeMillis();
    Dataset dataset =
        Dataset.builder()
            .platform("java_sdk_v2_test")
            .name(uniqueName)
            .env("DEV")
            .description("Testing mixed addTag() and setTags()")
            .build();

    // First add some tags using addTag() (creates patches)
    dataset.addTag("patch-tag-1");
    dataset.addTag("patch-tag-2");

    // Then call setTags() - should clear the patches and use full replacement
    dataset.setTags(Arrays.asList("set-tag-1", "set-tag-2", "set-tag-3"));

    client.entities().upsert(dataset);

    // Verify only setTags() tags are present (patches should have been cleared)
    validateEntityHasTags(
        dataset.getUrn().toString(),
        Dataset.class,
        Arrays.asList("set-tag-1", "set-tag-2", "set-tag-3"));

    // Verify patch tags are NOT present
    Dataset finalDataset = client.entities().get(dataset.getUrn().toString(), Dataset.class);
    List<com.linkedin.common.TagAssociation> tags = finalDataset.getTags();
    List<String> actualTags = new ArrayList<>();
    for (com.linkedin.common.TagAssociation tagAssoc : tags) {
      String tagUrn = tagAssoc.getTag().toString();
      String tagName = tagUrn.substring(tagUrn.lastIndexOf(':') + 1);
      actualTags.add(tagName);
    }

    assertEquals("Should have exactly 3 tags", 3, actualTags.size());
    assertFalse("Patch tags should not be present", actualTags.contains("patch-tag-1"));
    assertFalse("Patch tags should not be present", actualTags.contains("patch-tag-2"));
  }

  @Test
  public void testDatasetSetTagsEmpty() throws Exception {
    // Test that setTags([]) clears all tags (use unique name to avoid data from previous runs)
    String uniqueName = "test_dataset_set_tags_empty_" + System.currentTimeMillis();
    Dataset dataset =
        Dataset.builder()
            .platform("java_sdk_v2_test")
            .name(uniqueName)
            .env("DEV")
            .description("Testing setTags() with empty list")
            .build();

    // First create with some tags
    dataset.addTag("tag-to-remove-1");
    dataset.addTag("tag-to-remove-2");
    client.entities().upsert(dataset);

    // Verify tags exist
    validateEntityHasTags(
        dataset.getUrn().toString(),
        Dataset.class,
        Arrays.asList("tag-to-remove-1", "tag-to-remove-2"));

    // Now set empty tags list - should clear all tags
    Dataset fetchedDataset = client.entities().get(dataset.getUrn().toString(), Dataset.class);
    fetchedDataset.setTags(Arrays.asList()); // Empty list
    client.entities().upsert(fetchedDataset);

    // Verify all tags were removed
    Dataset finalDataset = client.entities().get(dataset.getUrn().toString(), Dataset.class);
    List<com.linkedin.common.TagAssociation> tags = finalDataset.getTags();

    assertNotNull("Tags should exist", tags);
    assertEquals("Should have zero tags", 0, tags.size());
  }

  // ========== Description Operations Tests ==========

  @Test
  public void testDatasetSetSystemDescription() throws Exception {
    String uniqueName = "test_dataset_system_description_" + System.currentTimeMillis();
    Dataset dataset =
        Dataset.builder().platform("java_sdk_v2_test").name(uniqueName).env("DEV").build();

    dataset.setSystemDescription("System-generated description from ingestion");
    client.entities().upsert(dataset);

    // Verify system description was written
    Dataset fetched = client.entities().get(dataset.getUrn().toString(), Dataset.class);
    com.linkedin.dataset.DatasetProperties props =
        fetched.getAspectLazy(com.linkedin.dataset.DatasetProperties.class);
    assertNotNull("DatasetProperties aspect should exist", props);
    assertEquals(
        "System description should match",
        "System-generated description from ingestion",
        props.getDescription());
  }

  @Test
  public void testDatasetSetEditableDescription() throws Exception {
    String uniqueName = "test_dataset_editable_description_" + System.currentTimeMillis();
    Dataset dataset =
        Dataset.builder().platform("java_sdk_v2_test").name(uniqueName).env("DEV").build();

    dataset.setEditableDescription("User-provided description override");
    client.entities().upsert(dataset);

    // Verify editable description was written
    Dataset fetched = client.entities().get(dataset.getUrn().toString(), Dataset.class);
    com.linkedin.dataset.EditableDatasetProperties editableProps =
        fetched.getAspectLazy(com.linkedin.dataset.EditableDatasetProperties.class);
    assertNotNull("EditableDatasetProperties aspect should exist", editableProps);
    assertEquals(
        "Editable description should match",
        "User-provided description override",
        editableProps.getDescription());
  }

  @Test
  public void testDatasetGetDescriptionPrefersEditable() throws Exception {
    String uniqueName = "test_dataset_description_preference_" + System.currentTimeMillis();
    Dataset dataset =
        Dataset.builder().platform("java_sdk_v2_test").name(uniqueName).env("DEV").build();

    // Set both system and editable descriptions
    dataset.setSystemDescription("System description");
    dataset.setEditableDescription("Editable description");
    client.entities().upsert(dataset);

    // Fetch and verify getDescription() prefers editable
    Dataset fetched = client.entities().get(dataset.getUrn().toString(), Dataset.class);
    String description = fetched.getDescription();
    assertEquals(
        "getDescription() should prefer editable over system", "Editable description", description);
  }

  // ========== Display Name Operations Tests ==========

  @Test
  public void testDatasetSetSystemDisplayName() throws Exception {
    String uniqueName = "test_dataset_system_display_name_" + System.currentTimeMillis();
    Dataset dataset =
        Dataset.builder().platform("java_sdk_v2_test").name(uniqueName).env("DEV").build();

    dataset.setSystemDisplayName("system_table_name");
    client.entities().upsert(dataset);

    // Verify system display name was written
    Dataset fetched = client.entities().get(dataset.getUrn().toString(), Dataset.class);
    com.linkedin.dataset.DatasetProperties props =
        fetched.getAspectLazy(com.linkedin.dataset.DatasetProperties.class);
    assertNotNull("DatasetProperties aspect should exist", props);
    assertEquals("System display name should match", "system_table_name", props.getName());
  }

  @Test
  public void testDatasetSetEditableDisplayName() throws Exception {
    String uniqueName = "test_dataset_editable_display_name_" + System.currentTimeMillis();
    Dataset dataset =
        Dataset.builder().platform("java_sdk_v2_test").name(uniqueName).env("DEV").build();

    dataset.setEditableDisplayName("User Friendly Table Name");
    client.entities().upsert(dataset);

    // Verify editable display name was written
    Dataset fetched = client.entities().get(dataset.getUrn().toString(), Dataset.class);
    com.linkedin.dataset.EditableDatasetProperties editableProps =
        fetched.getAspectLazy(com.linkedin.dataset.EditableDatasetProperties.class);
    assertNotNull("EditableDatasetProperties aspect should exist", editableProps);
    assertEquals(
        "Editable display name should match", "User Friendly Table Name", editableProps.getName());
  }

  @Test
  public void testDatasetGetDisplayNamePrefersEditable() throws Exception {
    String uniqueName = "test_dataset_display_name_preference_" + System.currentTimeMillis();
    Dataset dataset =
        Dataset.builder().platform("java_sdk_v2_test").name(uniqueName).env("DEV").build();

    // Set both system and editable display names
    dataset.setSystemDisplayName("system_name");
    dataset.setEditableDisplayName("User Friendly Name");
    client.entities().upsert(dataset);

    // Fetch and verify getDisplayName() prefers editable
    Dataset fetched = client.entities().get(dataset.getUrn().toString(), Dataset.class);
    String displayName = fetched.getDisplayName();
    assertEquals(
        "getDisplayName() should prefer editable over system", "User Friendly Name", displayName);
  }

  // ========== Schema Operations Tests ==========

  @Test
  public void testDatasetSetSchema() throws Exception {
    String uniqueName = "test_dataset_set_schema_" + System.currentTimeMillis();
    Dataset dataset =
        Dataset.builder().platform("java_sdk_v2_test").name(uniqueName).env("DEV").build();

    // Create schema metadata
    com.linkedin.schema.SchemaMetadata schema = new com.linkedin.schema.SchemaMetadata();
    schema.setSchemaName("MySchema");
    schema.setPlatform(dataset.getDatasetUrn().getPlatformEntity());
    schema.setVersion(0L);
    schema.setHash("abc123");
    schema.setPlatformSchema(
        com.linkedin.schema.SchemaMetadata.PlatformSchema.create(
            new com.linkedin.schema.MySqlDDL().setTableSchema("CREATE TABLE ...")));
    schema.setFields(new com.linkedin.schema.SchemaFieldArray());

    dataset.setSchema(schema);
    client.entities().upsert(dataset);

    // Verify schema was written
    Dataset fetched = client.entities().get(dataset.getUrn().toString(), Dataset.class);
    com.linkedin.schema.SchemaMetadata fetchedSchema = fetched.getSchema();
    assertNotNull("SchemaMetadata aspect should exist", fetchedSchema);
    assertEquals("Schema name should match", "MySchema", fetchedSchema.getSchemaName());
    assertEquals("Schema hash should match", "abc123", fetchedSchema.getHash());
  }

  @Test
  public void testDatasetSetSchemaFields() throws Exception {
    String uniqueName = "test_dataset_set_schema_fields_" + System.currentTimeMillis();
    Dataset dataset =
        Dataset.builder().platform("java_sdk_v2_test").name(uniqueName).env("DEV").build();

    // Create schema fields
    List<com.linkedin.schema.SchemaField> fields = new ArrayList<>();

    // String field
    com.linkedin.schema.SchemaField userIdField = new com.linkedin.schema.SchemaField();
    userIdField.setFieldPath("user_id");
    userIdField.setNativeDataType("VARCHAR(255)");
    userIdField.setType(
        new com.linkedin.schema.SchemaFieldDataType()
            .setType(
                com.linkedin.schema.SchemaFieldDataType.Type.create(
                    new com.linkedin.schema.StringType())));
    fields.add(userIdField);

    // Number field
    com.linkedin.schema.SchemaField amountField = new com.linkedin.schema.SchemaField();
    amountField.setFieldPath("amount");
    amountField.setNativeDataType("DECIMAL(10,2)");
    amountField.setType(
        new com.linkedin.schema.SchemaFieldDataType()
            .setType(
                com.linkedin.schema.SchemaFieldDataType.Type.create(
                    new com.linkedin.schema.NumberType())));
    fields.add(amountField);

    dataset.setSchemaFields(fields);
    client.entities().upsert(dataset);

    // Verify schema fields were written
    Dataset fetched = client.entities().get(dataset.getUrn().toString(), Dataset.class);
    com.linkedin.schema.SchemaMetadata fetchedSchema = fetched.getSchema();
    assertNotNull("SchemaMetadata aspect should exist", fetchedSchema);
    assertNotNull("Schema fields should exist", fetchedSchema.getFields());
    assertEquals("Should have 2 fields", 2, fetchedSchema.getFields().size());
    assertEquals(
        "First field should be user_id",
        "user_id",
        fetchedSchema.getFields().get(0).getFieldPath());
    assertEquals(
        "Second field should be amount", "amount", fetchedSchema.getFields().get(1).getFieldPath());
  }

  // ========== Remove Operations Tests ==========

  @Test
  public void testDatasetRemoveTag() throws Exception {
    Dataset dataset =
        Dataset.builder()
            .platform("java_sdk_v2_test")
            .name("test_dataset_remove_tag")
            .env("DEV")
            .build();

    // First add tags
    dataset.addTag("tag1").addTag("tag2").addTag("tag3");
    client.entities().upsert(dataset);

    // Verify tags exist
    validateEntityHasTags(
        dataset.getUrn().toString(), Dataset.class, Arrays.asList("tag1", "tag2", "tag3"));

    // Now remove one tag
    Dataset fetched = client.entities().get(dataset.getUrn().toString(), Dataset.class);
    fetched.removeTag("tag2");
    client.entities().upsert(fetched);

    // Verify tag was removed
    validateEntityHasTags(
        dataset.getUrn().toString(), Dataset.class, Arrays.asList("tag1", "tag3"));
  }

  @Test
  public void testDatasetRemoveOwner() throws Exception {
    String uniqueName = "test_dataset_remove_owner_" + System.currentTimeMillis();
    Dataset dataset =
        Dataset.builder().platform("java_sdk_v2_test").name(uniqueName).env("DEV").build();

    // First add owners
    dataset
        .addOwner("urn:li:corpuser:user1", OwnershipType.TECHNICAL_OWNER)
        .addOwner("urn:li:corpuser:user2", OwnershipType.TECHNICAL_OWNER);
    client.entities().upsert(dataset);

    // Verify owners exist
    Dataset fetched1 = client.entities().get(dataset.getUrn().toString(), Dataset.class);
    List<com.linkedin.common.Owner> owners1 = fetched1.getOwners();
    assertNotNull("Owners should exist", owners1);
    assertEquals("Should have 2 owners", 2, owners1.size());

    // Now remove one owner
    Dataset fetched2 = client.entities().get(dataset.getUrn().toString(), Dataset.class);
    fetched2.removeOwner("urn:li:corpuser:user1");
    client.entities().upsert(fetched2);

    // Verify owner was removed
    Dataset fetched3 = client.entities().get(dataset.getUrn().toString(), Dataset.class);
    List<com.linkedin.common.Owner> owners2 = fetched3.getOwners();
    assertNotNull("Owners should exist", owners2);
    assertEquals("Should have 1 owner remaining", 1, owners2.size());
    assertEquals(
        "Remaining owner should be user2",
        "urn:li:corpuser:user2",
        owners2.get(0).getOwner().toString());
  }

  @Test
  public void testDatasetRemoveTerm() throws Exception {
    String uniqueName = "test_dataset_remove_term_" + System.currentTimeMillis();
    Dataset dataset =
        Dataset.builder().platform("java_sdk_v2_test").name(uniqueName).env("DEV").build();

    // Add terms using addTerm() (PATCH-based)
    dataset.addTerm("urn:li:glossaryTerm:Term1");
    dataset.addTerm("urn:li:glossaryTerm:Term2");
    dataset.addTerm("urn:li:glossaryTerm:Term3");
    client.entities().upsert(dataset);

    // Verify terms exist
    Dataset fetched1 = client.entities().get(dataset.getUrn().toString(), Dataset.class);
    List<com.linkedin.common.GlossaryTermAssociation> terms1 = fetched1.getTerms();
    assertNotNull("Terms should exist", terms1);
    assertEquals("Should have 3 terms", 3, terms1.size());

    // Now remove one term using removeTerm()
    Dataset fetched2 = client.entities().get(dataset.getUrn().toString(), Dataset.class);
    fetched2.removeTerm("urn:li:glossaryTerm:Term2");
    client.entities().upsert(fetched2);

    // Verify term was removed
    Dataset fetched3 = client.entities().get(dataset.getUrn().toString(), Dataset.class);
    List<com.linkedin.common.GlossaryTermAssociation> terms2 = fetched3.getTerms();
    assertNotNull("Terms should exist", terms2);
    assertEquals("Should have 2 terms remaining", 2, terms2.size());
  }

  @Test
  public void testDatasetRemoveDomain() throws Exception {
    String uniqueName = "test_dataset_remove_domain_" + System.currentTimeMillis();
    Dataset dataset =
        Dataset.builder().platform("java_sdk_v2_test").name(uniqueName).env("DEV").build();

    // First set domain
    dataset.setDomain("urn:li:domain:Engineering");
    client.entities().upsert(dataset);

    // Verify domain exists
    Dataset fetched1 = client.entities().get(dataset.getUrn().toString(), Dataset.class);
    List<com.linkedin.common.urn.Urn> domains1 = fetched1.getDomains();
    assertNotNull("Domains should exist", domains1);
    assertEquals("Should have 1 domain", 1, domains1.size());

    // Now remove domain
    Dataset fetched2 = client.entities().get(dataset.getUrn().toString(), Dataset.class);
    Dataset mutable2 = fetched2.mutable();
    mutable2.removeDomain("urn:li:domain:Engineering");
    client.entities().upsert(mutable2);

    // Verify domain was removed
    Dataset fetched3 = client.entities().get(dataset.getUrn().toString(), Dataset.class);
    List<com.linkedin.common.urn.Urn> domains2 = fetched3.getDomains();
    assertNotNull("Domains should exist", domains2);
    assertEquals("Should have 0 domains", 0, domains2.size());
  }

  @Test
  public void testDatasetClearDomains() throws Exception {
    Dataset dataset =
        Dataset.builder()
            .platform("java_sdk_v2_test")
            .name("test_dataset_clear_domains")
            .env("DEV")
            .build();

    // First set domain
    dataset.setDomain("urn:li:domain:Marketing");
    client.entities().upsert(dataset);

    // Verify domain exists
    Dataset fetched1 = client.entities().get(dataset.getUrn().toString(), Dataset.class);
    List<com.linkedin.common.urn.Urn> domains1 = fetched1.getDomains();
    assertNotNull("Domains should exist", domains1);
    assertEquals("Should have 1 domain", 1, domains1.size());

    // Now clear all domains
    Dataset fetched2 = client.entities().get(dataset.getUrn().toString(), Dataset.class);
    Dataset mutable2 = fetched2.mutable();
    mutable2.clearDomains();
    client.entities().upsert(mutable2);

    // Verify all domains were removed
    Dataset fetched3 = client.entities().get(dataset.getUrn().toString(), Dataset.class);
    List<com.linkedin.common.urn.Urn> domains2 = fetched3.getDomains();
    assertNotNull("Domains should exist", domains2);
    assertEquals("Should have 0 domains", 0, domains2.size());
  }

  @Test
  public void testDatasetRemoveCustomProperty() throws Exception {
    String uniqueName = "test_dataset_remove_custom_property_" + System.currentTimeMillis();
    Dataset dataset =
        Dataset.builder().platform("java_sdk_v2_test").name(uniqueName).env("DEV").build();

    // First add custom properties
    dataset
        .addCustomProperty("prop1", "value1")
        .addCustomProperty("prop2", "value2")
        .addCustomProperty("prop3", "value3");
    client.entities().upsert(dataset);

    // Verify properties exist
    Dataset fetched1 = client.entities().get(dataset.getUrn().toString(), Dataset.class);
    com.linkedin.dataset.DatasetProperties props1 =
        fetched1.getAspectLazy(com.linkedin.dataset.DatasetProperties.class);
    assertNotNull("DatasetProperties aspect should exist", props1);
    assertNotNull("Custom properties should exist", props1.getCustomProperties());
    assertEquals("Should have 3 properties", 3, props1.getCustomProperties().size());

    // Now remove one property
    Dataset fetched2 = client.entities().get(dataset.getUrn().toString(), Dataset.class);
    Dataset mutable = fetched2.mutable(); // Get mutable copy
    mutable.removeCustomProperty("prop2");
    client.entities().upsert(mutable);

    // Verify property was removed
    Dataset fetched3 = client.entities().get(dataset.getUrn().toString(), Dataset.class);
    com.linkedin.dataset.DatasetProperties props2 =
        fetched3.getAspectLazy(com.linkedin.dataset.DatasetProperties.class);
    assertNotNull("DatasetProperties aspect should exist", props2);
    assertNotNull("Custom properties should exist", props2.getCustomProperties());
    assertEquals("Should have 2 properties remaining", 2, props2.getCustomProperties().size());
    assertFalse("prop2 should be removed", props2.getCustomProperties().containsKey("prop2"));
  }

  // ========== Builder Options Tests ==========

  @Test
  public void testDatasetWithPlatformInstance() throws Exception {
    Dataset dataset =
        Dataset.builder()
            .platform("kafka")
            .name("user-events-topic")
            .platformInstance("kafka-prod-cluster")
            .env("PROD")
            .build();

    client.entities().upsert(dataset);

    // Verify dataset was created with platform instance
    Dataset fetched = client.entities().get(dataset.getUrn().toString(), Dataset.class);
    com.linkedin.common.DataPlatformInstance platformInstance =
        fetched.getAspectLazy(com.linkedin.common.DataPlatformInstance.class);
    assertNotNull("DataPlatformInstance aspect should exist", platformInstance);
    assertTrue(
        "Platform instance should match",
        platformInstance.getInstance().toString().contains("kafka-prod-cluster"));
  }

  @Test
  public void testDatasetWithCustomPropertiesInBuilder() throws Exception {
    Map<String, String> customProps = new HashMap<>();
    customProps.put("team", "data-engineering");
    customProps.put("retention", "90_days");
    customProps.put("classification", "internal");

    Dataset dataset =
        Dataset.builder()
            .platform("java_sdk_v2_test")
            .name("test_dataset_builder_custom_props")
            .env("DEV")
            .customProperties(customProps)
            .build();

    client.entities().upsert(dataset);

    // Verify custom properties were written
    Dataset fetched = client.entities().get(dataset.getUrn().toString(), Dataset.class);
    com.linkedin.dataset.DatasetProperties props =
        fetched.getAspectLazy(com.linkedin.dataset.DatasetProperties.class);
    assertNotNull("DatasetProperties aspect should exist", props);
    assertNotNull("Custom properties should exist", props.getCustomProperties());
    assertEquals("Should have 3 properties", 3, props.getCustomProperties().size());
    assertEquals(
        "team property should match", "data-engineering", props.getCustomProperties().get("team"));
    assertEquals(
        "retention property should match", "90_days", props.getCustomProperties().get("retention"));
    assertEquals(
        "classification property should match",
        "internal",
        props.getCustomProperties().get("classification"));
  }
}
