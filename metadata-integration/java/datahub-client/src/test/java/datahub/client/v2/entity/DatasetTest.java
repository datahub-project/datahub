package datahub.client.v2.entity;

import static org.junit.Assert.*;

import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.NumberType;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.schema.StringType;
import datahub.event.MetadataChangeProposalWrapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/** Tests for Dataset entity builder and patch-based operations. */
public class DatasetTest {

  @Test
  public void testDatasetBuilderMinimal() {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    assertNotNull(dataset);
    assertNotNull(dataset.getUrn());
    assertNotNull(dataset.getDatasetUrn());
    assertEquals(dataset.getEntityType(), "dataset");
    assertTrue(dataset.isNewEntity());
  }

  @Test
  public void testDatasetBuilderWithDescription() {
    Dataset dataset =
        Dataset.builder()
            .platform("snowflake")
            .name("my_database.my_schema.my_table")
            .description("Test dataset description")
            .build();

    assertNotNull(dataset);
    List<MetadataChangeProposalWrapper> mcps = dataset.toMCPs();
    assertFalse(mcps.isEmpty());

    // Should have DatasetProperties aspect cached from builder
    boolean hasProperties =
        mcps.stream()
            .anyMatch(
                mcp -> mcp.getAspect().getClass().getSimpleName().equals("DatasetProperties"));
    assertTrue("Should have DatasetProperties aspect", hasProperties);
  }

  @Test
  public void testDatasetBuilderWithAllOptions() {
    Map<String, String> customProps = new HashMap<>();
    customProps.put("key1", "value1");
    customProps.put("key2", "value2");

    Dataset dataset =
        Dataset.builder()
            .platform("bigquery")
            .name("project.dataset.table")
            .env("DEV")
            .platformInstance("my-instance")
            .description("Full dataset test")
            .displayName("My Display Name")
            .customProperties(customProps)
            .build();

    assertNotNull(dataset);
    assertTrue(dataset.getUrn().toString().contains("bigquery"));
    assertTrue(dataset.getUrn().toString().contains("DEV"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDatasetBuilderMissingPlatform() {
    Dataset.builder().name("my_table").build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDatasetBuilderMissingName() {
    Dataset.builder().platform("postgres").build();
  }

  @Test
  public void testDatasetAddOwner() {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    dataset.addOwner("urn:li:corpuser:johndoe", OwnershipType.TECHNICAL_OWNER);

    // Verify patch MCP was created
    List<MetadataChangeProposal> patches = dataset.getPendingPatches();
    assertFalse("Should have pending patch", patches.isEmpty());
    assertEquals("ownership", patches.get(0).getAspectName());
  }

  @Test
  public void testDatasetAddMultipleOwners() {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    dataset.addOwner("urn:li:corpuser:johndoe", OwnershipType.TECHNICAL_OWNER);
    dataset.addOwner("urn:li:corpuser:janedoe", OwnershipType.TECHNICAL_OWNER);

    // Should have 2 patch MCPs
    List<MetadataChangeProposal> patches = dataset.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  @Test
  public void testDatasetRemoveOwner() {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    dataset.addOwner("urn:li:corpuser:johndoe", OwnershipType.TECHNICAL_OWNER);
    dataset.removeOwner("urn:li:corpuser:johndoe");

    // Should have 2 patches (add and remove)
    List<MetadataChangeProposal> patches = dataset.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  @Test
  public void testDatasetAddTag() {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    dataset.addTag("pii");
    dataset.addTag("urn:li:tag:sensitive");

    // Verify patch MCPs were created
    List<MetadataChangeProposal> patches = dataset.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    assertEquals("globalTags", patches.get(0).getAspectName());
  }

  @Test
  public void testDatasetRemoveTag() {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    dataset.addTag("tag1");
    dataset.removeTag("tag1");

    List<MetadataChangeProposal> patches = dataset.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  @Test
  public void testDatasetAddTerm() {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    dataset.addTerm("urn:li:glossaryTerm:CustomerData");
    dataset.addTerm("urn:li:glossaryTerm:PII");

    // Verify patch MCPs were created
    List<MetadataChangeProposal> patches = dataset.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    assertEquals("glossaryTerms", patches.get(0).getAspectName());
  }

  @Test
  public void testDatasetRemoveTerm() {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    dataset.addTerm("urn:li:glossaryTerm:term1");
    dataset.removeTerm("urn:li:glossaryTerm:term1");

    List<MetadataChangeProposal> patches = dataset.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  @Test
  public void testDatasetSetDomain() {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    dataset.setDomain("urn:li:domain:Marketing");

    // Verify domain aspect was cached (domains use aspect cache, not patches)
    List<MetadataChangeProposalWrapper> mcps = dataset.toMCPs();
    boolean hasDomainAspect = mcps.stream().anyMatch(mcp -> "domains".equals(mcp.getAspectName()));
    assertTrue("Should have domain aspect in cache", hasDomainAspect);
  }

  @Test
  public void testDatasetRemoveDomain() throws Exception {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    Urn marketingDomain = Urn.createFromString("urn:li:domain:Marketing");
    dataset.setDomain(marketingDomain.toString());
    dataset.removeDomain(marketingDomain);

    // Verify domain aspect was cached (domains use aspect cache, not patches)
    List<MetadataChangeProposalWrapper> mcps = dataset.toMCPs();
    boolean hasDomainAspect = mcps.stream().anyMatch(mcp -> "domains".equals(mcp.getAspectName()));
    assertTrue("Should have domain aspect in cache", hasDomainAspect);
  }

  @Test
  public void testDatasetSetCustomProperty() {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    dataset.addCustomProperty("env", "production");
    dataset.addCustomProperty("team", "data-engineering");

    // Verify patch MCPs were created (accumulated into single patch)
    List<MetadataChangeProposal> patches = dataset.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    assertEquals("datasetProperties", patches.get(0).getAspectName());
  }

  @Test
  public void testDatasetRemoveCustomProperty() {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    dataset.addCustomProperty("env", "production");
    dataset.removeCustomProperty("env");

    List<MetadataChangeProposal> patches = dataset.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    assertEquals("datasetProperties", patches.get(0).getAspectName());
  }

  @Test
  public void testDatasetSetSystemDescription() {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    dataset.setSystemDescription("System description");

    List<MetadataChangeProposal> patches = dataset.getPendingPatches();
    assertFalse("Should have system description patch", patches.isEmpty());
    assertEquals("datasetProperties", patches.get(0).getAspectName());
  }

  @Test
  public void testDatasetSetEditableDescription() {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    dataset.setEditableDescription("Editable description");

    List<MetadataChangeProposal> patches = dataset.getPendingPatches();
    assertFalse("Should have editable description patch", patches.isEmpty());
    assertEquals("editableDatasetProperties", patches.get(0).getAspectName());
  }

  @Test
  public void testDatasetFluentAPI() {
    Dataset dataset =
        Dataset.builder()
            .platform("snowflake")
            .name("sales_data")
            .description("Sales transactions")
            .build();

    // Test fluent method chaining
    dataset
        .addTag("pii")
        .addTag("sensitive")
        .addOwner("urn:li:corpuser:owner1", OwnershipType.TECHNICAL_OWNER)
        .addTerm("urn:li:glossaryTerm:Sales")
        .setDomain("urn:li:domain:Finance")
        .setSystemDisplayName("Sales Data")
        .addCustomProperty("retention", "7years");

    // Verify patches were accumulated
    List<MetadataChangeProposal> patches = dataset.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  @Test
  public void testDatasetToMCPs() {
    Dataset dataset =
        Dataset.builder()
            .platform("postgres")
            .name("my_table")
            .description("Test description")
            .build();

    // toMCPs returns cached aspects (from builder), not patches
    List<MetadataChangeProposalWrapper> mcps = dataset.toMCPs();

    assertNotNull(mcps);
    assertFalse(mcps.isEmpty());

    // Verify all MCPs have correct entity type and URN
    for (MetadataChangeProposalWrapper mcp : mcps) {
      assertEquals(mcp.getEntityType(), "dataset");
      assertNotNull(mcp.getEntityUrn());
      assertNotNull(mcp.getAspect());
    }
  }

  @Test
  public void testDatasetClearPendingPatches() {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    // Add some patches
    dataset.addTag("tag1");
    dataset.addOwner("urn:li:corpuser:owner1", OwnershipType.TECHNICAL_OWNER);

    assertTrue("Should have pending patches", dataset.hasPendingPatches());

    // Clear patches
    dataset.clearPendingPatches();

    assertFalse("Should not have pending patches", dataset.hasPendingPatches());
  }

  @Test
  public void testDatasetEqualsAndHashCode() {
    Dataset dataset1 = Dataset.builder().platform("postgres").name("my_table").build();

    Dataset dataset2 = Dataset.builder().platform("postgres").name("my_table").build();

    // Should be equal based on URN
    assertEquals(dataset1, dataset2);
    assertEquals(dataset1.hashCode(), dataset2.hashCode());
  }

  @Test
  public void testDatasetToString() {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    String str = dataset.toString();
    assertNotNull(str);
    assertTrue(str.contains("Dataset"));
    assertTrue(str.contains("urn"));
  }

  @Test
  public void testDatasetPatchVsFullAspect() {
    Dataset dataset1 =
        Dataset.builder()
            .platform("postgres")
            .name("table1")
            .description("Description from builder")
            .build();

    // Builder-provided description goes into cached aspects
    List<MetadataChangeProposalWrapper> mcps1 = dataset1.toMCPs();
    assertFalse("Should have cached aspects", mcps1.isEmpty());

    Dataset dataset2 = Dataset.builder().platform("postgres").name("table2").build();

    // Setter creates a patch
    dataset2.setSystemDescription("Description from setter");
    List<MetadataChangeProposal> patches2 = dataset2.getPendingPatches();
    assertFalse("Should have pending patches", patches2.isEmpty());
  }

  @Test
  public void testDatasetModeAwareBehavior() {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    // Default mode is SDK (null until bound to client)
    assertTrue("Default should be SDK mode", dataset.isSdkMode());

    // Set to ingestion mode
    dataset.setMode(datahub.client.v2.config.DataHubClientConfigV2.OperationMode.INGESTION);
    assertTrue("Should be in ingestion mode", dataset.isIngestionMode());

    // Set description creates system patch in ingestion mode
    dataset.setDescription("Description");
    List<MetadataChangeProposal> patches = dataset.getPendingPatches();
    assertEquals("datasetProperties", patches.get(0).getAspectName());
  }

  @Test
  public void testDatasetSetCustomPropertiesReplaceAll() {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    Map<String, String> properties = new HashMap<>();
    properties.put("team", "data-engineering");
    properties.put("retention", "90_days");
    properties.put("classification", "internal");

    dataset.setCustomProperties(properties);

    List<MetadataChangeProposal> patches = dataset.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    assertEquals("datasetProperties", patches.get(0).getAspectName());
  }

  @Test
  public void testDatasetSetSystemDisplayName() {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    dataset.setSystemDisplayName("System Display Name");

    List<MetadataChangeProposal> patches = dataset.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    assertEquals("datasetProperties", patches.get(0).getAspectName());
  }

  @Test
  public void testDatasetSetEditableDisplayName() {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    dataset.setEditableDisplayName("Editable Display Name");

    List<MetadataChangeProposal> patches = dataset.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    assertEquals("editableDatasetProperties", patches.get(0).getAspectName());
  }

  @Test
  public void testDatasetRemoveDomainWithParameter() {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    dataset.setDomain("urn:li:domain:Marketing");
    dataset.removeDomain("urn:li:domain:Marketing");

    // Verify domain aspect was cached (domains use aspect cache, not patches)
    List<MetadataChangeProposalWrapper> mcps = dataset.toMCPs();
    boolean hasDomainAspect = mcps.stream().anyMatch(mcp -> "domains".equals(mcp.getAspectName()));
    assertTrue("Should have domain aspect in cache", hasDomainAspect);
  }

  @Test
  public void testDatasetClearDomains() {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    dataset.setDomain("urn:li:domain:Marketing");
    dataset.clearDomains();

    // Verify domain aspect was cached (domains use aspect cache, not patches)
    List<MetadataChangeProposalWrapper> mcps = dataset.toMCPs();
    boolean hasDomainAspect = mcps.stream().anyMatch(mcp -> "domains".equals(mcp.getAspectName()));
    assertTrue("Should have domain aspect in cache", hasDomainAspect);
  }

  @Test
  public void testDatasetSetSchema() {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    SchemaMetadata schema = new SchemaMetadata();
    schema.setSchemaName("my_table");
    schema.setVersion(0L);
    schema.setHash("");
    schema.setPlatform(dataset.getDatasetUrn().getPlatformEntity());
    schema.setFields(new com.linkedin.schema.SchemaFieldArray());

    dataset.setSchema(schema);

    SchemaMetadata retrievedSchema = dataset.getSchema();
    assertNotNull("Schema should be set", retrievedSchema);
    assertEquals("my_table", retrievedSchema.getSchemaName());
  }

  @Test
  public void testDatasetSetSchemaFields() {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    List<SchemaField> fields = new ArrayList<>();

    // String field
    SchemaField userIdField = new SchemaField();
    userIdField.setFieldPath("user_id");
    userIdField.setNativeDataType("VARCHAR(255)");
    userIdField.setType(
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())));
    fields.add(userIdField);

    // Numeric field
    SchemaField amountField = new SchemaField();
    amountField.setFieldPath("amount");
    amountField.setNativeDataType("DECIMAL(10,2)");
    amountField.setType(
        new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType())));
    fields.add(amountField);

    dataset.setSchemaFields(fields);

    SchemaMetadata schema = dataset.getSchema();
    assertNotNull("Schema should be set", schema);
    assertNotNull("Schema fields should be set", schema.getFields());
    assertEquals("Should have 2 fields", 2, schema.getFields().size());
    assertEquals("user_id", schema.getFields().get(0).getFieldPath());
    assertEquals("amount", schema.getFields().get(1).getFieldPath());
  }

  @Test
  public void testDatasetGetDescription() {
    Dataset dataset =
        Dataset.builder()
            .platform("postgres")
            .name("my_table")
            .description("Test description")
            .build();

    String description = dataset.getDescription();
    assertNotNull("Description should not be null", description);
    assertEquals("Test description", description);
  }

  @Test
  public void testDatasetGetDescriptionPrefersEditable() {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    // Set both system and editable descriptions
    dataset.setSystemDescription("System description");
    dataset.setEditableDescription("Editable description");

    // Note: Since getDescription() prefers editable, but we're only creating patches here,
    // we can't fully test the preference without loading aspects from server.
    // This test validates the methods exist and work without errors.
    String description = dataset.getDescription();
    // Will be null because patches don't populate the aspect cache
  }

  @Test
  public void testDatasetGetDisplayName() {
    Dataset dataset =
        Dataset.builder()
            .platform("postgres")
            .name("my_table")
            .displayName("My Display Name")
            .build();

    String displayName = dataset.getDisplayName();
    assertNotNull("Display name should not be null", displayName);
    assertEquals("My Display Name", displayName);
  }

  @Test
  public void testDatasetGetDisplayNamePrefersEditable() {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    // Set both system and editable display names
    dataset.setSystemDisplayName("System Name");
    dataset.setEditableDisplayName("Editable Name");

    // Note: Since getDisplayName() prefers editable, but we're only creating patches here,
    // we can't fully test the preference without loading aspects from server.
    // This test validates the methods exist and work without errors.
    String displayName = dataset.getDisplayName();
    // Will be null because patches don't populate the aspect cache
  }

  @Test
  public void testDatasetSetDisplayNameModeAware() {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    // In SDK mode (default), setDisplayName should create editable patch
    dataset.setDisplayName("SDK Mode Name");
    List<MetadataChangeProposal> patches = dataset.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    assertEquals("editableDatasetProperties", patches.get(0).getAspectName());

    dataset.clearPendingPatches();

    // In INGESTION mode, setDisplayName should create system patch
    dataset.setMode(datahub.client.v2.config.DataHubClientConfigV2.OperationMode.INGESTION);
    dataset.setDisplayName("Ingestion Mode Name");
    patches = dataset.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    assertEquals("datasetProperties", patches.get(0).getAspectName());
  }

  // ========== Add/Remove Reversal Pattern Tests ==========

  @Test
  public void testAddTagThenRemoveSameTag() throws Exception {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    dataset.addTag("test-tag");
    dataset.removeTag("test-tag");

    List<MetadataChangeProposal> patches = dataset.getPendingPatches();
    assertEquals("Should have exactly one globalTags patch", 1, patches.size());
    assertEquals("globalTags", patches.get(0).getAspectName());

    // Parse the patch to verify operations
    String patchJson =
        patches.get(0).getAspect().getValue().asString(java.nio.charset.StandardCharsets.UTF_8);
    assertTrue("Should contain add operation", patchJson.contains("\"op\":\"add\""));
    assertTrue("Should contain remove operation", patchJson.contains("\"op\":\"remove\""));
    assertTrue("Should reference test-tag", patchJson.contains("test-tag"));
  }

  @Test
  public void testRemoveTagThenAddSameTag() throws Exception {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    dataset.removeTag("test-tag");
    dataset.addTag("test-tag");

    List<MetadataChangeProposal> patches = dataset.getPendingPatches();
    assertEquals("Should have exactly one globalTags patch", 1, patches.size());
    assertEquals("globalTags", patches.get(0).getAspectName());

    // Parse the patch to verify operations
    String patchJson =
        patches.get(0).getAspect().getValue().asString(java.nio.charset.StandardCharsets.UTF_8);
    assertTrue("Should contain remove operation", patchJson.contains("\"op\":\"remove\""));
    assertTrue("Should contain add operation", patchJson.contains("\"op\":\"add\""));
    assertTrue("Should reference test-tag", patchJson.contains("test-tag"));
  }

  @Test
  public void testAddMultipleTagsThenRemoveOne() throws Exception {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    dataset.addTag("tag1");
    dataset.addTag("tag2");
    dataset.addTag("tag3");
    dataset.removeTag("tag2");

    List<MetadataChangeProposal> patches = dataset.getPendingPatches();
    assertEquals("Should have exactly one globalTags patch", 1, patches.size());

    String patchJson =
        patches.get(0).getAspect().getValue().asString(java.nio.charset.StandardCharsets.UTF_8);
    assertTrue("Should contain tag1", patchJson.contains("tag1"));
    assertTrue("Should contain tag2", patchJson.contains("tag2"));
    assertTrue("Should contain tag3", patchJson.contains("tag3"));
    // Should have 3 adds and 1 remove
    int addCount = patchJson.split("\"op\":\"add\"").length - 1;
    int removeCount = patchJson.split("\"op\":\"remove\"").length - 1;
    assertTrue("Should have add operations for tags", addCount >= 3);
    assertTrue("Should have remove operation", removeCount >= 1);
  }

  @Test
  public void testAddOwnerThenRemoveSameOwner() throws Exception {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    dataset.addOwner("urn:li:corpuser:john", OwnershipType.TECHNICAL_OWNER);
    dataset.removeOwner("urn:li:corpuser:john");

    List<MetadataChangeProposal> patches = dataset.getPendingPatches();
    assertEquals("Should have exactly one ownership patch", 1, patches.size());
    assertEquals("ownership", patches.get(0).getAspectName());

    String patchJson =
        patches.get(0).getAspect().getValue().asString(java.nio.charset.StandardCharsets.UTF_8);
    assertTrue("Should contain add operation", patchJson.contains("\"op\":\"add\""));
    assertTrue("Should contain remove operation", patchJson.contains("\"op\":\"remove\""));
    assertTrue("Should reference john", patchJson.contains("john"));
  }

  @Test
  public void testRemoveOwnerThenAddSameOwner() throws Exception {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    dataset.removeOwner("urn:li:corpuser:jane");
    dataset.addOwner("urn:li:corpuser:jane", OwnershipType.DATA_STEWARD);

    List<MetadataChangeProposal> patches = dataset.getPendingPatches();
    assertEquals("Should have exactly one ownership patch", 1, patches.size());
    assertEquals("ownership", patches.get(0).getAspectName());

    String patchJson =
        patches.get(0).getAspect().getValue().asString(java.nio.charset.StandardCharsets.UTF_8);
    assertTrue("Should contain remove operation", patchJson.contains("\"op\":\"remove\""));
    assertTrue("Should contain add operation", patchJson.contains("\"op\":\"add\""));
    assertTrue("Should reference jane", patchJson.contains("jane"));
  }

  @Test
  public void testAddTermThenRemoveSameTerm() throws Exception {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    dataset.addTerm("urn:li:glossaryTerm:TestTerm");
    dataset.removeTerm("urn:li:glossaryTerm:TestTerm");

    List<MetadataChangeProposal> patches = dataset.getPendingPatches();
    assertEquals("Should have exactly one glossaryTerms patch", 1, patches.size());
    assertEquals("glossaryTerms", patches.get(0).getAspectName());

    String patchJson =
        patches.get(0).getAspect().getValue().asString(java.nio.charset.StandardCharsets.UTF_8);
    assertTrue("Should contain add operation", patchJson.contains("\"op\":\"add\""));
    assertTrue("Should contain remove operation", patchJson.contains("\"op\":\"remove\""));
    assertTrue("Should reference TestTerm", patchJson.contains("TestTerm"));
  }

  @Test
  public void testRemoveTermThenAddSameTerm() throws Exception {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    dataset.removeTerm("urn:li:glossaryTerm:AnotherTerm");
    dataset.addTerm("urn:li:glossaryTerm:AnotherTerm");

    List<MetadataChangeProposal> patches = dataset.getPendingPatches();
    assertEquals("Should have exactly one glossaryTerms patch", 1, patches.size());
    assertEquals("glossaryTerms", patches.get(0).getAspectName());

    String patchJson =
        patches.get(0).getAspect().getValue().asString(java.nio.charset.StandardCharsets.UTF_8);
    assertTrue("Should contain remove operation", patchJson.contains("\"op\":\"remove\""));
    assertTrue("Should contain add operation", patchJson.contains("\"op\":\"add\""));
    assertTrue("Should reference AnotherTerm", patchJson.contains("AnotherTerm"));
  }

  @Test
  public void testSetDomainThenRemoveSameDomain() throws Exception {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    dataset.setDomain("urn:li:domain:Engineering");
    dataset.removeDomain("urn:li:domain:Engineering");

    // Verify domain aspect was cached with empty domains (domains use aspect cache, not patches)
    List<MetadataChangeProposalWrapper> mcps = dataset.toMCPs();
    boolean hasDomainAspect = mcps.stream().anyMatch(mcp -> "domains".equals(mcp.getAspectName()));
    assertTrue("Should have domain aspect in cache", hasDomainAspect);
  }

  @Test
  public void testSetDomainThenClearDomains() throws Exception {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    dataset.setDomain("urn:li:domain:Marketing");
    dataset.clearDomains();

    // Verify domain aspect was cached with empty domains (domains use aspect cache, not patches)
    List<MetadataChangeProposalWrapper> mcps = dataset.toMCPs();
    boolean hasDomainAspect = mcps.stream().anyMatch(mcp -> "domains".equals(mcp.getAspectName()));
    assertTrue("Should have domain aspect in cache", hasDomainAspect);
  }

  @Test
  public void testMixedTagOperations() throws Exception {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    // Complex sequence: add, set (replacement), then remove
    dataset.addTag("initial-tag");
    dataset.setTags(java.util.Arrays.asList("replaced-tag-1", "replaced-tag-2"));
    dataset.removeTag("replaced-tag-1");

    List<MetadataChangeProposal> patches = dataset.getPendingPatches();
    assertEquals("Should have exactly one globalTags patch", 1, patches.size());
    assertEquals("globalTags", patches.get(0).getAspectName());

    String patchJson =
        patches.get(0).getAspect().getValue().asString(java.nio.charset.StandardCharsets.UTF_8);
    // setTags() resets the patch builder, clearing previous operations
    // Only the setTags operations and subsequent remove should be in the patch
    assertTrue("Should have operations in patch", patchJson.contains("\"op\":"));
  }

  @Test
  public void testAddTagSetTagsAddTag() throws Exception {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    // Add, then replace with setTags, then add again
    dataset.addTag("tag-before-set");
    dataset.setTags(java.util.Arrays.asList("set-tag-1", "set-tag-2"));
    dataset.addTag("tag-after-set");

    List<MetadataChangeProposal> patches = dataset.getPendingPatches();
    assertEquals("Should have exactly one globalTags patch", 1, patches.size());

    String patchJson =
        patches.get(0).getAspect().getValue().asString(java.nio.charset.StandardCharsets.UTF_8);
    // setTags() resets the patch builder, clearing previous operations
    // Only the setTags operations and subsequent add should be in the patch
    assertTrue("Should have operations in patch", patchJson.contains("\"op\":"));
  }

  @Test
  public void testMultipleOwnerAddRemoveSequence() throws Exception {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    dataset.addOwner("urn:li:corpuser:owner1", OwnershipType.TECHNICAL_OWNER);
    dataset.addOwner("urn:li:corpuser:owner2", OwnershipType.BUSINESS_OWNER);
    dataset.removeOwner("urn:li:corpuser:owner1");
    dataset.addOwner("urn:li:corpuser:owner3", OwnershipType.DATA_STEWARD);

    List<MetadataChangeProposal> patches = dataset.getPendingPatches();
    assertEquals("Should have exactly one ownership patch", 1, patches.size());
    assertEquals("ownership", patches.get(0).getAspectName());

    String patchJson =
        patches.get(0).getAspect().getValue().asString(java.nio.charset.StandardCharsets.UTF_8);
    assertTrue("Should contain owner1", patchJson.contains("owner1"));
    assertTrue("Should contain owner2", patchJson.contains("owner2"));
    assertTrue("Should contain owner3", patchJson.contains("owner3"));
  }

  @Test
  public void testAddCustomPropertyThenRemove() throws Exception {
    Dataset dataset = Dataset.builder().platform("postgres").name("my_table").build();

    dataset.addCustomProperty("test-key", "test-value");
    dataset.removeCustomProperty("test-key");

    List<MetadataChangeProposal> patches = dataset.getPendingPatches();
    assertEquals("Should have exactly one datasetProperties patch", 1, patches.size());
    assertEquals("datasetProperties", patches.get(0).getAspectName());

    String patchJson =
        patches.get(0).getAspect().getValue().asString(java.nio.charset.StandardCharsets.UTF_8);
    assertTrue("Should contain add operation", patchJson.contains("\"op\":\"add\""));
    assertTrue("Should contain remove operation", patchJson.contains("\"op\":\"remove\""));
    assertTrue("Should reference test-key", patchJson.contains("test-key"));
  }
}
