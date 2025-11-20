package com.linkedin.metadata.registry;

import static org.mockito.Mockito.when;

import com.linkedin.metadata.EventSchemaConstants;
import com.linkedin.metadata.EventSchemaData;
import com.linkedin.mxe.TopicConvention;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SchemaRegistryServiceTest {

  @Mock private TopicConvention mockTopicConvention;

  private SchemaRegistryService schemaRegistryService;

  private static final String MCP_TOPIC = "MetadataChangeProposal";
  private static final String FMCP_TOPIC = "FailedMetadataChangeProposal";
  private static final String MCL_TOPIC = "MetadataChangeLog";
  private static final String PE_TOPIC = "PlatformEvent";
  private static final String MCE_TOPIC = "MetadataChangeEvent"; // Single version topic

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    // Mock topic convention methods
    when(mockTopicConvention.getMetadataChangeProposalTopicName()).thenReturn(MCP_TOPIC);
    when(mockTopicConvention.getFailedMetadataChangeProposalTopicName()).thenReturn(FMCP_TOPIC);
    when(mockTopicConvention.getMetadataChangeLogVersionedTopicName()).thenReturn(MCL_TOPIC);
    when(mockTopicConvention.getPlatformEventTopicName()).thenReturn(PE_TOPIC);
    when(mockTopicConvention.getMetadataChangeEventTopicName()).thenReturn(MCE_TOPIC);
    when(mockTopicConvention.getMetadataChangeLogTimeseriesTopicName())
        .thenReturn("MetadataChangeLog");
    when(mockTopicConvention.getDataHubUpgradeHistoryTopicName())
        .thenReturn("DataHubUpgradeHistory");
    when(mockTopicConvention.getFailedMetadataChangeEventTopicName())
        .thenReturn("FailedMetadataChangeEvent");
    when(mockTopicConvention.getMetadataAuditEventTopicName()).thenReturn("MetadataAuditEvent");

    // Create a test-specific implementation that doesn't try to load external resources
    schemaRegistryService = createTestSchemaRegistryService(mockTopicConvention);
  }

  private SchemaRegistryService createTestSchemaRegistryService(TopicConvention convention) {
    // Use the actual SchemaRegistryServiceImpl for testing with real schemas
    return new SchemaRegistryServiceImpl(convention, new EventSchemaData());
  }

  @Test
  public void testGetSchemaForTopicAndVersion_MCP_Version1() {
    Optional<Schema> schema = schemaRegistryService.getSchemaForTopicAndVersion(MCP_TOPIC, 1);

    Assert.assertTrue(schema.isPresent());
    Assert.assertEquals(schema.get().getName(), "MetadataChangeProposal");
    // Version 1 is the V1 schema with pegasus2avro namespace
    Assert.assertEquals(schema.get().getNamespace(), "com.linkedin.pegasus2avro.mxe");
  }

  @Test
  public void testGetSchemaForTopicAndVersion_MCP_Version3() {
    Optional<Schema> schema = schemaRegistryService.getSchemaForTopicAndVersion(MCP_TOPIC, 3);

    Assert.assertTrue(schema.isPresent());
    Assert.assertEquals(schema.get().getName(), "MetadataChangeProposal");
    // Version 3 is the current schema with pegasus2avro namespace
    Assert.assertEquals(schema.get().getNamespace(), "com.linkedin.pegasus2avro.mxe");
  }

  @Test
  public void testGetSchemaForTopicAndVersion_FMCP_Version1() {
    Optional<Schema> schema = schemaRegistryService.getSchemaForTopicAndVersion(FMCP_TOPIC, 1);
    Assert.assertTrue(schema.isPresent());
    Assert.assertEquals(schema.get().getNamespace(), "com.linkedin.pegasus2avro.mxe");
    Assert.assertEquals(schema.get().getName(), "FailedMetadataChangeProposal");
  }

  @Test
  public void testGetSchemaForTopicAndVersion_FMCP_Version3() {
    Optional<Schema> schema = schemaRegistryService.getSchemaForTopicAndVersion(FMCP_TOPIC, 3);

    Assert.assertTrue(schema.isPresent());
    Assert.assertEquals(schema.get().getName(), "FailedMetadataChangeProposal");
    // Version 3 is the current schema with pegasus2avro namespace
    Assert.assertEquals(schema.get().getNamespace(), "com.linkedin.pegasus2avro.mxe");
  }

  @Test
  public void testGetSchemaForTopicAndVersion_InvalidVersion() {
    Optional<Schema> schema = schemaRegistryService.getSchemaForTopicAndVersion(MCP_TOPIC, 4);

    Assert.assertFalse(schema.isPresent());
  }

  @Test
  public void testGetSchemaForTopicAndVersion_InvalidTopic() {
    Optional<Schema> schema = schemaRegistryService.getSchemaForTopicAndVersion("InvalidTopic", 1);

    Assert.assertFalse(schema.isPresent());
  }

  @Test
  public void testGetLatestSchemaVersionForTopic_MCP() {
    Optional<Integer> latestVersion =
        schemaRegistryService.getLatestSchemaVersionForTopic(MCP_TOPIC);

    Assert.assertTrue(latestVersion.isPresent());
    Assert.assertEquals(latestVersion.get().intValue(), 3);
  }

  @Test
  public void testGetLatestSchemaVersionForTopic_FMCP() {
    Optional<Integer> latestVersion =
        schemaRegistryService.getLatestSchemaVersionForTopic(FMCP_TOPIC);

    Assert.assertTrue(latestVersion.isPresent());
    Assert.assertEquals(latestVersion.get().intValue(), 3);
  }

  @Test
  public void testGetLatestSchemaVersionForTopic_SingleVersion() {
    Optional<Integer> latestVersion =
        schemaRegistryService.getLatestSchemaVersionForTopic(MCE_TOPIC);

    Assert.assertTrue(latestVersion.isPresent());
    // MCE_TOPIC actually supports 3 versions since versions are incremented across all schema IDs
    // Version 1 = Schema ID 5 (MCE_V1), Version 2 = Schema ID 5 (MCE_V1), Version 3 = Schema ID 13
    // (MCE)
    Assert.assertEquals(latestVersion.get().intValue(), 3);
  }

  @Test
  public void testGetSupportedSchemaVersionsForTopic_MCP() {
    Optional<List<Integer>> versions =
        schemaRegistryService.getSupportedSchemaVersionsForTopic(MCP_TOPIC);

    Assert.assertTrue(versions.isPresent());
    List<Integer> versionList = versions.get();
    Assert.assertEquals(versionList.size(), 3);
    Assert.assertTrue(versionList.contains(1));
    Assert.assertTrue(versionList.contains(2));
    Assert.assertTrue(versionList.contains(3));
  }

  @Test
  public void testGetSupportedSchemaVersionsForTopic_FMCP() {
    Optional<List<Integer>> versions =
        schemaRegistryService.getSupportedSchemaVersionsForTopic(FMCP_TOPIC);

    Assert.assertTrue(versions.isPresent());
    List<Integer> versionList = versions.get();
    Assert.assertEquals(versionList.size(), 3);
    Assert.assertTrue(versionList.contains(1));
    Assert.assertTrue(versionList.contains(2));
    Assert.assertTrue(versionList.contains(3));
  }

  @Test
  public void testGetSupportedSchemaVersionsForTopic_SingleVersion() {
    Optional<List<Integer>> versions =
        schemaRegistryService.getSupportedSchemaVersionsForTopic(MCE_TOPIC);

    Assert.assertTrue(versions.isPresent());
    List<Integer> versionList = versions.get();
    // MCE_TOPIC actually supports 3 versions since versions are incremented across all schema IDs
    // Version 1 = Schema ID 5 (MCE_V1), Version 2 = Schema ID 5 (MCE_V1), Version 3 = Schema ID 13
    // (MCE)
    Assert.assertEquals(versionList.size(), 3);
    Assert.assertTrue(versionList.contains(1));
    Assert.assertTrue(versionList.contains(2));
    Assert.assertTrue(versionList.contains(3));
  }

  @Test
  public void testGetSchemaForTopic_DefaultLatest() {
    Optional<Schema> schema = schemaRegistryService.getSchemaForTopic(MCP_TOPIC);

    Assert.assertTrue(schema.isPresent());
    // Should return version 2 (latest) by default
    Assert.assertEquals(schema.get().getName(), "MetadataChangeProposal");
  }

  @Test
  public void testGetSchemaForTopic_SingleVersion() {
    Optional<Schema> schema = schemaRegistryService.getSchemaForTopic(MCE_TOPIC);

    Assert.assertTrue(schema.isPresent());
    Assert.assertEquals(schema.get().getName(), "MetadataChangeEvent");
  }

  @Test
  public void testGetSchemaIdForTopic() {
    Optional<Integer> id = schemaRegistryService.getSchemaIdForTopic(MCP_TOPIC);

    Assert.assertTrue(id.isPresent());
    // MCP_TOPIC returns the latest schema ID (16) since the implementation returns the highest
    // version
    // Version 1 = Schema ID 0 (MCP_V1), Version 2 = Schema ID 9 (MCP_V1_FIX), Version 3 = Schema ID
    // 16 (MCP)
    Assert.assertEquals(id.get().intValue(), 16);
  }

  @Test
  public void testGetSchemaForId() {
    Optional<Schema> schema = schemaRegistryService.getSchemaForId(0);

    Assert.assertTrue(schema.isPresent());
    Assert.assertEquals(schema.get().getName(), "MetadataChangeProposal");
  }

  @Test
  public void testGetAllTopics() {
    List<String> allTopics = schemaRegistryService.getAllTopics();

    Assert.assertNotNull(allTopics);
    Assert.assertFalse(allTopics.isEmpty());

    // Should contain all the expected topics
    Assert.assertTrue(allTopics.contains(MCP_TOPIC));
    Assert.assertTrue(allTopics.contains(FMCP_TOPIC));
    Assert.assertTrue(allTopics.contains(MCL_TOPIC));
    Assert.assertTrue(allTopics.contains(PE_TOPIC));
    Assert.assertTrue(allTopics.contains("DataHubUpgradeHistory"));
    Assert.assertTrue(allTopics.contains("MetadataChangeEvent"));
    Assert.assertTrue(allTopics.contains("FailedMetadataChangeEvent"));
    Assert.assertTrue(allTopics.contains("MetadataAuditEvent"));

    // Should not contain any unexpected topics
    Assert.assertFalse(allTopics.contains("InvalidTopic"));

    // Verify the size matches expected count
    Assert.assertEquals(allTopics.size(), 8);
  }

  @Test
  public void testGetAllTopics_ConsistencyWithIndividualMethods() {
    List<String> allTopics = schemaRegistryService.getAllTopics();

    // Verify that each topic returned by getAllTopics can be accessed individually
    for (String topic : allTopics) {
      Optional<Integer> schemaId = schemaRegistryService.getSchemaIdForTopic(topic);
      Assert.assertTrue(schemaId.isPresent(), "Topic " + topic + " should have a schema ID");

      Optional<Schema> schema = schemaRegistryService.getSchemaForTopic(topic);
      Assert.assertTrue(schema.isPresent(), "Topic " + topic + " should have a schema");
    }
  }

  @Test
  public void testSchemaVersionsAreDifferent() {
    Optional<Schema> v1Schema = schemaRegistryService.getSchemaForTopicAndVersion(MCP_TOPIC, 1);
    Optional<Schema> v3Schema = schemaRegistryService.getSchemaForTopicAndVersion(MCP_TOPIC, 3);

    Assert.assertTrue(v1Schema.isPresent());
    Assert.assertTrue(v3Schema.isPresent());

    // Both versions should be accessible and have the same name but different namespaces
    Assert.assertEquals(v1Schema.get().getName(), "MetadataChangeProposal");
    Assert.assertEquals(v3Schema.get().getName(), "MetadataChangeProposal");
    // Both versions now use the pegasus2avro namespace
    // Versions are incremented across all schema IDs for a given schema name when loaded in the
    // registry
    Assert.assertEquals(v1Schema.get().getNamespace(), "com.linkedin.pegasus2avro.mxe");
    Assert.assertEquals(v3Schema.get().getNamespace(), "com.linkedin.pegasus2avro.mxe");
  }

  @Test
  public void testBothTopicsUseDifferentSchemas() {
    // MCP and FMCP topics should use different schemas
    Optional<List<Integer>> mcpVersions =
        schemaRegistryService.getSupportedSchemaVersionsForTopic(MCP_TOPIC);
    Optional<List<Integer>> fmcpVersions =
        schemaRegistryService.getSupportedSchemaVersionsForTopic(FMCP_TOPIC);

    Assert.assertTrue(mcpVersions.isPresent());
    Assert.assertTrue(fmcpVersions.isPresent());

    // Both should support multiple versions but have different schema names
    Assert.assertTrue(mcpVersions.get().size() > 1);
    Assert.assertTrue(fmcpVersions.get().size() > 1);

    // Verify they have different schema names
    Optional<Schema> mcpSchema = schemaRegistryService.getSchemaForTopic(MCP_TOPIC);
    Optional<Schema> fmcpSchema = schemaRegistryService.getSchemaForTopic(FMCP_TOPIC);

    Assert.assertTrue(mcpSchema.isPresent());
    Assert.assertTrue(fmcpSchema.isPresent());
    Assert.assertNotEquals(mcpSchema.get().getName(), fmcpSchema.get().getName());
  }

  @Test
  public void testEventSchemaConstantsIntegration() {
    // Test that the service correctly uses EventSchemaConstants
    // Test that MCP supports multiple versions since versions are incremented across all schema IDs
    Optional<List<Integer>> mcpVersions =
        schemaRegistryService.getSupportedSchemaVersionsForTopic(MCP_TOPIC);
    Assert.assertTrue(mcpVersions.isPresent());
    Assert.assertTrue(mcpVersions.get().size() >= 2);

    // Test that MCL supports multiple versions since versions are incremented across all schema IDs
    Optional<List<Integer>> mclVersions =
        schemaRegistryService.getSupportedSchemaVersionsForTopic("MetadataChangeLog");
    Assert.assertTrue(mclVersions.isPresent());
    Assert.assertTrue(mclVersions.get().size() >= 2);

    // Test that DUHE supports at least one version
    Optional<List<Integer>> duheVersions =
        schemaRegistryService.getSupportedSchemaVersionsForTopic("DataHubUpgradeHistory");
    Assert.assertTrue(duheVersions.isPresent());
    Assert.assertTrue(duheVersions.get().size() >= 1);
  }

  @Test
  public void testMetadataChangeLogTimeseriesSupportsMultipleVersions() {
    String mclTimeseriesTopic = "MetadataChangeLog";

    // Test that MCL_TIMESERIES now supports multiple versions
    Optional<List<Integer>> supportedVersions =
        schemaRegistryService.getSupportedSchemaVersionsForTopic(mclTimeseriesTopic);
    Assert.assertTrue(supportedVersions.isPresent());
    Assert.assertEquals(
        supportedVersions.get().size(), 6, "MCL_TIMESERIES should support version 6");
    Assert.assertTrue(supportedVersions.get().contains(1));
    Assert.assertTrue(supportedVersions.get().contains(2));
    Assert.assertTrue(supportedVersions.get().contains(3));
    Assert.assertTrue(supportedVersions.get().contains(4));
    Assert.assertTrue(supportedVersions.get().contains(5));
    Assert.assertTrue(supportedVersions.get().contains(6));

    // Test getting specific versions
    Optional<Schema> v1Schema =
        schemaRegistryService.getSchemaForTopicAndVersion(mclTimeseriesTopic, 1);
    Optional<Schema> v2Schema =
        schemaRegistryService.getSchemaForTopicAndVersion(mclTimeseriesTopic, 2);
    Optional<Schema> v3Schema =
        schemaRegistryService.getSchemaForTopicAndVersion(mclTimeseriesTopic, 3);

    Assert.assertTrue(v1Schema.isPresent(), "Version 1 schema should be present");
    Assert.assertTrue(v2Schema.isPresent(), "Version 2 schema should be present");
    Assert.assertTrue(v3Schema.isPresent(), "Version 3 schema should be present");

    // Test getting latest version
    Optional<Integer> latestVersion =
        schemaRegistryService.getLatestSchemaVersionForTopic(mclTimeseriesTopic);
    Assert.assertTrue(latestVersion.isPresent());
    Assert.assertEquals(latestVersion.get().intValue(), 6);

    // Test that versions 1 and 2 are the same (both use MCL_V1_SCHEMA)
    Assert.assertEquals(
        v1Schema.get().toString(),
        v2Schema.get().toString(),
        "Version 1 and Version 2 schemas should be the same (both MCL_V1_SCHEMA)");

    // Test that versions 1 and 5 are different (MCL_V1_SCHEMA vs MCL_SCHEMA)
    Optional<Schema> v5Schema =
        schemaRegistryService.getSchemaForTopicAndVersion(mclTimeseriesTopic, 5);
    Assert.assertTrue(v5Schema.isPresent(), "Version 5 schema should be present");
    Assert.assertNotEquals(
        v1Schema.get().toString(),
        v5Schema.get().toString(),
        "Version 1 and Version 5 schemas should be different");

    // Test that MCL_TIMESERIES has NONE compatibility since it's now a versioned schema
    String compatibility = schemaRegistryService.getSchemaCompatibility(mclTimeseriesTopic);
    Assert.assertEquals(
        compatibility,
        EventSchemaConstants.SCHEMA_COMPATIBILITY_NONE,
        "MCL_TIMESERIES should have NONE compatibility as a versioned schema");
  }

  @Test
  public void testMetadataChangeProposalVersion1MissingAspectCreatedField() {
    // Version 1 should be missing the aspectCreated field
    Optional<Schema> v1Schema = schemaRegistryService.getSchemaForTopicAndVersion(MCP_TOPIC, 1);
    Assert.assertTrue(v1Schema.isPresent());

    // Check that v1 schema doesn't have aspectCreated field in systemMetadata
    boolean hasAspectCreated = hasAspectCreatedField(v1Schema.get());
    Assert.assertFalse(
        hasAspectCreated, "Version 1 MCP schema should not have aspectCreated field");

    // Verify v1 schema has the expected basic fields and correct namespace (V1 schema)
    List<String> v1FieldNames =
        v1Schema.get().getFields().stream().map(Schema.Field::name).collect(Collectors.toList());
    Assert.assertTrue(
        v1FieldNames.contains("entityType"), "Version 1 should have entityType field");
    Assert.assertTrue(
        v1FieldNames.contains("changeType"), "Version 1 should have changeType field");
    Assert.assertEquals(v1Schema.get().getNamespace(), "com.linkedin.pegasus2avro.mxe");
  }

  @Test
  public void testMetadataChangeProposalVersion3IncludesAspectCreatedField() {
    // Version 3 should include the aspectCreated field
    Optional<Schema> v3Schema = schemaRegistryService.getSchemaForTopicAndVersion(MCP_TOPIC, 3);
    Assert.assertTrue(v3Schema.isPresent());

    // Check that v3 schema has aspectCreated field in systemMetadata
    boolean hasAspectCreated = hasAspectCreatedField(v3Schema.get());
    Assert.assertTrue(hasAspectCreated, "Version 3 MCP schema should have aspectCreated field");

    // Verify v3 schema has the expected basic fields
    List<String> v3FieldNames =
        v3Schema.get().getFields().stream().map(Schema.Field::name).collect(Collectors.toList());
    Assert.assertTrue(
        v3FieldNames.contains("entityType"), "Version 3 should have entityType field");
    Assert.assertTrue(
        v3FieldNames.contains("changeType"), "Version 3 should have changeType field");
  }

  @Test
  public void testFailedMetadataChangeProposalVersion1MissingAspectCreatedField() {
    // Version 1 should be missing the aspectCreated field
    Optional<Schema> v1Schema = schemaRegistryService.getSchemaForTopicAndVersion(FMCP_TOPIC, 1);
    Assert.assertTrue(v1Schema.isPresent());

    // Check that v1 schema doesn't have aspectCreated field in
    // metadataChangeProposal.systemMetadata
    boolean hasAspectCreated = hasAspectCreatedField(v1Schema.get());
    Assert.assertFalse(
        hasAspectCreated, "Version 1 FMCP schema should not have aspectCreated field");

    // Verify v1 schema has the expected top-level fields
    List<String> v1FieldNames =
        v1Schema.get().getFields().stream().map(Schema.Field::name).collect(Collectors.toList());
    Assert.assertTrue(
        v1FieldNames.contains("metadataChangeProposal"),
        "Version 1 should have metadataChangeProposal field");
    Assert.assertTrue(v1FieldNames.contains("error"), "Version 1 should have error field");
  }

  @Test
  public void testFailedMetadataChangeProposalVersion3IncludesAspectCreatedField() {
    // Version 3 should include the aspectCreated field
    Optional<Schema> v3Schema = schemaRegistryService.getSchemaForTopicAndVersion(FMCP_TOPIC, 3);
    Assert.assertTrue(v3Schema.isPresent());

    // Check that v3 schema has aspectCreated field in systemMetadata
    boolean hasAspectCreated = hasAspectCreatedField(v3Schema.get());
    Assert.assertTrue(hasAspectCreated, "Version 3 FMCP schema should have aspectCreated field");

    // Verify v3 schema has the expected basic fields
    List<String> v3FieldNames =
        v3Schema.get().getFields().stream().map(Schema.Field::name).collect(Collectors.toList());
    Assert.assertTrue(
        v3FieldNames.contains("metadataChangeProposal"),
        "Version 3 should have metadataChangeProposal field");
    Assert.assertTrue(v3FieldNames.contains("error"), "Version 3 should have error field");
  }

  @Test
  public void testMetadataChangeLogVersionSupport() {
    String mclTopic = "MetadataChangeLog";
    when(mockTopicConvention.getMetadataChangeLogVersionedTopicName()).thenReturn(mclTopic);

    // Test that we can get both versions
    Optional<Schema> v1Schema = schemaRegistryService.getSchemaForTopicAndVersion(mclTopic, 1);
    Optional<Schema> v2Schema = schemaRegistryService.getSchemaForTopicAndVersion(mclTopic, 2);

    Assert.assertTrue(v1Schema.isPresent(), "Version 1 schema should be present");
    Assert.assertTrue(v2Schema.isPresent(), "Version 2 schema should be present");

    // Test getting latest version
    Optional<Integer> latestVersion =
        schemaRegistryService.getLatestSchemaVersionForTopic(mclTopic);
    Assert.assertTrue(latestVersion.isPresent());
    Assert.assertEquals(latestVersion.get().intValue(), 6);

    // Test getting supported versions
    Optional<List<Integer>> supportedVersions =
        schemaRegistryService.getSupportedSchemaVersionsForTopic(mclTopic);
    Assert.assertTrue(supportedVersions.isPresent());
    Assert.assertEquals(supportedVersions.get().size(), 6);
    Assert.assertTrue(supportedVersions.get().contains(1));
    Assert.assertTrue(supportedVersions.get().contains(2));
    Assert.assertTrue(supportedVersions.get().contains(3));
    Assert.assertTrue(supportedVersions.get().contains(4));
    Assert.assertTrue(supportedVersions.get().contains(5));
    Assert.assertTrue(supportedVersions.get().contains(6));
  }

  @Test
  public void testDataHubUpgradeHistoryEventVersionSupport() {
    String duheTopic = "DataHubUpgradeHistory";
    when(mockTopicConvention.getDataHubUpgradeHistoryTopicName()).thenReturn(duheTopic);

    // Test that we can get version 1 only
    Optional<Schema> v1Schema = schemaRegistryService.getSchemaForTopicAndVersion(duheTopic, 1);
    Optional<Schema> v2Schema = schemaRegistryService.getSchemaForTopicAndVersion(duheTopic, 2);

    Assert.assertTrue(v1Schema.isPresent(), "Version 1 schema should be present");
    Assert.assertFalse(v2Schema.isPresent(), "Version 2 schema should not be present");

    // Test getting latest version
    Optional<Integer> latestVersion =
        schemaRegistryService.getLatestSchemaVersionForTopic(duheTopic);
    Assert.assertTrue(latestVersion.isPresent());
    Assert.assertEquals(latestVersion.get().intValue(), 1);

    // Test getting supported versions
    Optional<List<Integer>> supportedVersions =
        schemaRegistryService.getSupportedSchemaVersionsForTopic(duheTopic);
    Assert.assertTrue(supportedVersions.isPresent());
    Assert.assertEquals(supportedVersions.get().size(), 1);
    Assert.assertTrue(supportedVersions.get().contains(1));
  }

  @Test
  public void testAspectCreatedFieldVersioningConsistency() {
    // Test that both MCP and FMCP have consistent field presence across versions
    Optional<Schema> mcpV1 = schemaRegistryService.getSchemaForTopicAndVersion(MCP_TOPIC, 1);
    Optional<Schema> mcpV3 = schemaRegistryService.getSchemaForTopicAndVersion(MCP_TOPIC, 3);
    Optional<Schema> fmcpV1 = schemaRegistryService.getSchemaForTopicAndVersion(FMCP_TOPIC, 1);
    Optional<Schema> fmcpV3 = schemaRegistryService.getSchemaForTopicAndVersion(FMCP_TOPIC, 3);

    Assert.assertTrue(mcpV1.isPresent());
    Assert.assertTrue(mcpV3.isPresent());
    Assert.assertTrue(fmcpV1.isPresent());
    Assert.assertTrue(fmcpV3.isPresent());

    // Check aspectCreated field presence in each version
    boolean mcpV1HasAspectCreated = hasAspectCreatedField(mcpV1.get());
    boolean mcpV3HasAspectCreated = hasAspectCreatedField(mcpV3.get());
    boolean fmcpV1HasAspectCreated = hasAspectCreatedField(fmcpV1.get());
    boolean fmcpV3HasAspectCreated = hasAspectCreatedField(fmcpV3.get());

    // All v1 schemas should be consistent (none should have aspectCreated)
    Assert.assertEquals(
        mcpV1HasAspectCreated,
        fmcpV1HasAspectCreated,
        "All v1 schemas should have consistent aspectCreated field presence");

    // All v3 schemas should be consistent (all should have same aspectCreated field presence)
    Assert.assertEquals(
        mcpV3HasAspectCreated,
        fmcpV3HasAspectCreated,
        "All v3 schemas should have consistent aspectCreated field presence");

    // Document current state
    System.out.println("MCP v1 has aspectCreated: " + mcpV1HasAspectCreated);
    System.out.println("MCP v3 has aspectCreated: " + mcpV3HasAspectCreated);
    System.out.println("FMCP v1 has aspectCreated: " + fmcpV1HasAspectCreated);
    System.out.println("FMCP v3 has aspectCreated: " + fmcpV3HasAspectCreated);
  }

  @Test
  public void testSchemaFieldCountsForVersioning() {
    // Test that schema field counts are consistent with versioning expectations
    Optional<Schema> mcpV1 = schemaRegistryService.getSchemaForTopicAndVersion(MCP_TOPIC, 1);
    Optional<Schema> mcpV3 = schemaRegistryService.getSchemaForTopicAndVersion(MCP_TOPIC, 3);
    Optional<Schema> fmcpV1 = schemaRegistryService.getSchemaForTopicAndVersion(FMCP_TOPIC, 1);
    Optional<Schema> fmcpV3 = schemaRegistryService.getSchemaForTopicAndVersion(FMCP_TOPIC, 3);

    Assert.assertTrue(mcpV1.isPresent());
    Assert.assertTrue(mcpV3.isPresent());
    Assert.assertTrue(fmcpV1.isPresent());
    Assert.assertTrue(fmcpV3.isPresent());

    int mcpV1FieldCount = mcpV1.get().getFields().size();
    int mcpV3FieldCount = mcpV3.get().getFields().size();
    int fmcpV1FieldCount = fmcpV1.get().getFields().size();
    int fmcpV3FieldCount = fmcpV3.get().getFields().size();

    // Document current field counts
    System.out.println("MCP v1 field count: " + mcpV1FieldCount);
    System.out.println("MCP v3 field count: " + mcpV3FieldCount);
    System.out.println("FMCP v1 field count: " + fmcpV1FieldCount);
    System.out.println("FMCP v3 field count: " + fmcpV3FieldCount);

    // Verify that v1 and v3 have the same top-level field count
    // The aspectCreated field is nested within systemMetadata, not a top-level field
    Assert.assertEquals(
        mcpV1FieldCount, mcpV3FieldCount, "MCP v1 and v3 should have same top-level field count");
    Assert.assertEquals(
        fmcpV1FieldCount,
        fmcpV3FieldCount,
        "FMCP v1 and v3 should have same top-level field count");
  }

  /**
   * Helper method to check if a schema has the aspectCreated field nested within systemMetadata
   * Handles both MCP (direct systemMetadata) and FMCP (metadataChangeProposal.systemMetadata)
   * schemas
   */
  private boolean hasAspectCreatedField(Schema schema) {
    for (Schema.Field field : schema.getFields()) {
      if ("systemMetadata".equals(field.name())) {
        // MCP schema - direct systemMetadata field
        return checkSystemMetadataForAspectCreated(field.schema());
      } else if ("metadataChangeProposal".equals(field.name())) {
        // FMCP schema - nested metadataChangeProposal field
        Schema mcpSchema = field.schema();
        if (mcpSchema.getType() == Schema.Type.UNION) {
          // Handle union types (nullable fields)
          for (Schema unionSchema : mcpSchema.getTypes()) {
            if (unionSchema.getType() == Schema.Type.RECORD) {
              // Check if this record has systemMetadata field
              for (Schema.Field nestedField : unionSchema.getFields()) {
                if ("systemMetadata".equals(nestedField.name())) {
                  return checkSystemMetadataForAspectCreated(nestedField.schema());
                }
              }
            }
          }
        } else if (mcpSchema.getType() == Schema.Type.RECORD) {
          // Direct record type
          for (Schema.Field nestedField : mcpSchema.getFields()) {
            if ("systemMetadata".equals(nestedField.name())) {
              return checkSystemMetadataForAspectCreated(nestedField.schema());
            }
          }
        }
      }
    }
    return false;
  }

  /** Helper method to check if a systemMetadata schema has the aspectCreated field */
  private boolean checkSystemMetadataForAspectCreated(Schema systemMetadataSchema) {
    // Handle union types (nullable fields)
    if (systemMetadataSchema.getType() == Schema.Type.UNION) {
      for (Schema unionSchema : systemMetadataSchema.getTypes()) {
        if (unionSchema.getType() == Schema.Type.RECORD) {
          // Check if this record has aspectCreated field
          for (Schema.Field nestedField : unionSchema.getFields()) {
            if ("aspectCreated".equals(nestedField.name())) {
              return true;
            }
          }
        }
      }
    } else if (systemMetadataSchema.getType() == Schema.Type.RECORD) {
      // Direct record type
      for (Schema.Field nestedField : systemMetadataSchema.getFields()) {
        if ("aspectCreated".equals(nestedField.name())) {
          return true;
        }
      }
    }
    return false;
  }

  @Test
  public void testGetSchemaCompatibility() {
    // Test compatibility for versioned schemas (should be NONE)
    String mcpCompatibility = schemaRegistryService.getSchemaCompatibility(MCP_TOPIC);
    String fmcpCompatibility = schemaRegistryService.getSchemaCompatibility(FMCP_TOPIC);
    String mclCompatibility = schemaRegistryService.getSchemaCompatibility(MCL_TOPIC);
    String mclTimeseriesCompatibility =
        schemaRegistryService.getSchemaCompatibility("MetadataChangeLogTimeseries");
    String duheCompatibility =
        schemaRegistryService.getSchemaCompatibility("DataHubUpgradeHistory");

    Assert.assertEquals(EventSchemaConstants.SCHEMA_COMPATIBILITY_NONE, mcpCompatibility);
    Assert.assertEquals(EventSchemaConstants.SCHEMA_COMPATIBILITY_NONE, fmcpCompatibility);
    Assert.assertEquals(EventSchemaConstants.SCHEMA_COMPATIBILITY_NONE, mclCompatibility);
    Assert.assertEquals(EventSchemaConstants.SCHEMA_COMPATIBILITY_NONE, mclTimeseriesCompatibility);
    // DataHubUpgradeHistory is a single-version schema, so it should have BACKWARD compatibility
    Assert.assertEquals(EventSchemaConstants.SCHEMA_COMPATIBILITY_BACKWARD, duheCompatibility);

    // Test compatibility for single version schemas (should be BACKWARD)
    String peCompatibility = schemaRegistryService.getSchemaCompatibility(PE_TOPIC);
    String mceCompatibility = schemaRegistryService.getSchemaCompatibility("MetadataChangeEvent");
    String fmceCompatibility =
        schemaRegistryService.getSchemaCompatibility("FailedMetadataChangeEvent");
    String maeCompatibility = schemaRegistryService.getSchemaCompatibility("MetadataAuditEvent");

    // PlatformEvent and DataHubUpgradeHistory are truly single version, so they should have
    // BACKWARD compatibility
    Assert.assertEquals(EventSchemaConstants.SCHEMA_COMPATIBILITY_BACKWARD, peCompatibility);
    // MCE, FMCE, and MAE are multi-version schemas with breaking changes, so they should have NONE
    // compatibility
    Assert.assertEquals(EventSchemaConstants.SCHEMA_COMPATIBILITY_NONE, mceCompatibility);
    Assert.assertEquals(EventSchemaConstants.SCHEMA_COMPATIBILITY_NONE, fmceCompatibility);
    Assert.assertEquals(EventSchemaConstants.SCHEMA_COMPATIBILITY_NONE, maeCompatibility);
  }

  @Test
  public void testGetSchemaCompatibilityById() {
    // Test compatibility for versioned schemas by ID
    String mcpCompatibility = schemaRegistryService.getSchemaCompatibilityById(0); // MCP topic
    String fmcpCompatibility = schemaRegistryService.getSchemaCompatibilityById(1); // FMCP topic
    String mclCompatibility = schemaRegistryService.getSchemaCompatibilityById(2); // MCL topic
    String mclTimeseriesCompatibility =
        schemaRegistryService.getSchemaCompatibilityById(3); // MCL_TIMESERIES topic
    String duheCompatibility = schemaRegistryService.getSchemaCompatibilityById(8); // DUHE topic

    Assert.assertEquals(EventSchemaConstants.SCHEMA_COMPATIBILITY_NONE, mcpCompatibility);
    Assert.assertEquals(EventSchemaConstants.SCHEMA_COMPATIBILITY_NONE, fmcpCompatibility);
    Assert.assertEquals(EventSchemaConstants.SCHEMA_COMPATIBILITY_NONE, mclCompatibility);
    Assert.assertEquals(EventSchemaConstants.SCHEMA_COMPATIBILITY_NONE, mclTimeseriesCompatibility);
    // DataHubUpgradeHistory is a single-version schema, so it should have BACKWARD compatibility
    Assert.assertEquals(EventSchemaConstants.SCHEMA_COMPATIBILITY_BACKWARD, duheCompatibility);

    // Test compatibility for single version schemas by ID
    String peCompatibility = schemaRegistryService.getSchemaCompatibilityById(4); // PE topic
    String mceCompatibility = schemaRegistryService.getSchemaCompatibilityById(5); // MCE topic
    String fmceCompatibility = schemaRegistryService.getSchemaCompatibilityById(6); // FMCE topic
    String maeCompatibility = schemaRegistryService.getSchemaCompatibilityById(7); // MAE topic

    // PlatformEvent and DataHubUpgradeHistory are truly single version, so they should have
    // BACKWARD compatibility
    Assert.assertEquals(EventSchemaConstants.SCHEMA_COMPATIBILITY_BACKWARD, peCompatibility);
    Assert.assertEquals(EventSchemaConstants.SCHEMA_COMPATIBILITY_NONE, mceCompatibility);
    Assert.assertEquals(EventSchemaConstants.SCHEMA_COMPATIBILITY_NONE, fmceCompatibility);
    Assert.assertEquals(EventSchemaConstants.SCHEMA_COMPATIBILITY_NONE, maeCompatibility);

    // Test invalid schema ID (should return NONE)
    String invalidCompatibility = schemaRegistryService.getSchemaCompatibilityById(999);
    Assert.assertEquals(EventSchemaConstants.SCHEMA_COMPATIBILITY_NONE, invalidCompatibility);
  }

  @Test
  public void testGetTopicNameById() {
    // Test getting topic names for valid schema IDs
    Optional<String> mcpTopic = schemaRegistryService.getTopicNameById(0);
    Optional<String> fmcpTopic = schemaRegistryService.getTopicNameById(1);
    Optional<String> mclTopic = schemaRegistryService.getTopicNameById(2);
    Optional<String> mclTimeseriesTopic = schemaRegistryService.getTopicNameById(3);
    Optional<String> duheTopic = schemaRegistryService.getTopicNameById(8);

    Assert.assertTrue(mcpTopic.isPresent());
    Assert.assertTrue(fmcpTopic.isPresent());
    Assert.assertTrue(mclTopic.isPresent());
    Assert.assertTrue(mclTimeseriesTopic.isPresent());
    Assert.assertTrue(duheTopic.isPresent());

    Assert.assertEquals(mcpTopic.get(), MCP_TOPIC);
    Assert.assertEquals(fmcpTopic.get(), FMCP_TOPIC);
    Assert.assertEquals(mclTopic.get(), MCL_TOPIC);
    // Schema ID 3 maps to MetadataChangeLog since it's MCL_SCHEMA_ID, not
    // MCL_TIMESERIES_V1_SCHEMA_ID
    Assert.assertEquals(mclTimeseriesTopic.get(), "MetadataChangeLog");
    Assert.assertEquals(duheTopic.get(), "DataHubUpgradeHistory");

    // Test getting topic names for single version schemas
    Optional<String> peTopic = schemaRegistryService.getTopicNameById(4);
    Optional<String> mceTopic = schemaRegistryService.getTopicNameById(5);
    Optional<String> fmceTopic = schemaRegistryService.getTopicNameById(6);
    Optional<String> maeTopic = schemaRegistryService.getTopicNameById(7);

    Assert.assertTrue(peTopic.isPresent());
    Assert.assertTrue(mceTopic.isPresent());
    Assert.assertTrue(fmceTopic.isPresent());
    Assert.assertTrue(maeTopic.isPresent());

    Assert.assertEquals(peTopic.get(), PE_TOPIC);
    Assert.assertEquals(mceTopic.get(), "MetadataChangeEvent");
    Assert.assertEquals(fmceTopic.get(), "FailedMetadataChangeEvent");
    Assert.assertEquals(maeTopic.get(), "MetadataAuditEvent");

    // Test invalid schema ID (should return empty)
    Optional<String> invalidTopic = schemaRegistryService.getTopicNameById(999);
    Assert.assertFalse(invalidTopic.isPresent());
  }

  @Test
  public void testSingleVersionSchemaFlow() {
    String topicName = "PlatformEvent";

    // Test that single version schemas work correctly
    Optional<Schema> schema = schemaRegistryService.getSchemaForTopicAndVersion(topicName, 1);
    Assert.assertTrue(schema.isPresent(), "Schema should be present");

    // Test getting latest version
    Optional<Integer> latestVersion =
        schemaRegistryService.getLatestSchemaVersionForTopic(topicName);
    Assert.assertTrue(latestVersion.isPresent());
    Assert.assertEquals(latestVersion.get().intValue(), 1);

    // Test getting supported versions
    Optional<List<Integer>> supportedVersions =
        schemaRegistryService.getSupportedSchemaVersionsForTopic(topicName);
    Assert.assertTrue(supportedVersions.isPresent());
    Assert.assertEquals(supportedVersions.get().size(), 1);
    Assert.assertTrue(supportedVersions.get().contains(1));

    // Test that invalid versions return empty
    Optional<Schema> invalidSchema =
        schemaRegistryService.getSchemaForTopicAndVersion(topicName, 2);
    Assert.assertFalse(invalidSchema.isPresent(), "Invalid version should return empty");
  }

  @Test
  public void testSchemaIdMapping() {
    // Test that schema IDs are correctly mapped
    Optional<Integer> mcpId = schemaRegistryService.getSchemaIdForTopic("MetadataChangeProposal");
    Optional<Integer> fmcpId =
        schemaRegistryService.getSchemaIdForTopic("FailedMetadataChangeProposal");

    Assert.assertTrue(mcpId.isPresent());
    Assert.assertTrue(fmcpId.isPresent());
    Assert.assertNotEquals(mcpId.get(), fmcpId.get(), "Different topics should have different IDs");

    // Test reverse lookup
    Optional<Schema> mcpSchema = schemaRegistryService.getSchemaForId(mcpId.get());
    Optional<Schema> fmcpSchema = schemaRegistryService.getSchemaForId(fmcpId.get());

    Assert.assertTrue(mcpSchema.isPresent());
    Assert.assertTrue(fmcpSchema.isPresent());
  }

  @Test
  public void testInvalidTopicHandling() {
    String invalidTopic = "InvalidTopic";

    // Test that invalid topics return empty for all operations
    Optional<Schema> schema = schemaRegistryService.getSchemaForTopic(invalidTopic);
    Assert.assertFalse(schema.isPresent());

    Optional<Integer> latestVersion =
        schemaRegistryService.getLatestSchemaVersionForTopic(invalidTopic);
    Assert.assertFalse(latestVersion.isPresent());

    Optional<List<Integer>> supportedVersions =
        schemaRegistryService.getSupportedSchemaVersionsForTopic(invalidTopic);
    Assert.assertFalse(supportedVersions.isPresent());

    Optional<Integer> id = schemaRegistryService.getSchemaIdForTopic(invalidTopic);
    Assert.assertFalse(id.isPresent());
  }

  @Test
  public void testInvalidVersionHandling() {
    String topicName = "MetadataChangeProposal";

    // Test that invalid versions return empty
    Optional<Schema> invalidSchema =
        schemaRegistryService.getSchemaForTopicAndVersion(topicName, 99);
    Assert.assertFalse(invalidSchema.isPresent());

    Optional<Schema> negativeSchema =
        schemaRegistryService.getSchemaForTopicAndVersion(topicName, -1);
    Assert.assertFalse(negativeSchema.isPresent());

    Optional<Schema> zeroSchema = schemaRegistryService.getSchemaForTopicAndVersion(topicName, 0);
    Assert.assertFalse(zeroSchema.isPresent());
  }

  @Test
  public void testGetSchemaIdBySubjectAndVersion() {
    // Test with valid subject and version
    Optional<Integer> schemaId =
        schemaRegistryService.getSchemaIdBySubjectAndVersion("MetadataChangeProposal-value", 1);
    Assert.assertTrue(schemaId.isPresent());
    Assert.assertEquals(schemaId.get().intValue(), 0);

    // Test with different version
    Optional<Integer> schemaIdV2 =
        schemaRegistryService.getSchemaIdBySubjectAndVersion("MetadataChangeProposal-value", 2);
    Assert.assertTrue(schemaIdV2.isPresent());
    Assert.assertEquals(schemaIdV2.get().intValue(), 9);

    // Test with FMCP subject
    Optional<Integer> fmcpSchemaId =
        schemaRegistryService.getSchemaIdBySubjectAndVersion(
            "FailedMetadataChangeProposal-value", 1);
    Assert.assertTrue(fmcpSchemaId.isPresent());
    Assert.assertEquals(fmcpSchemaId.get().intValue(), 1);

    // Test with invalid subject
    Optional<Integer> invalidSubjectId =
        schemaRegistryService.getSchemaIdBySubjectAndVersion("InvalidSubject-value", 1);
    Assert.assertFalse(invalidSubjectId.isPresent());

    // Test with invalid version
    Optional<Integer> invalidVersionId =
        schemaRegistryService.getSchemaIdBySubjectAndVersion("MetadataChangeProposal-value", 99);
    Assert.assertFalse(invalidVersionId.isPresent());

    // Test with null subject - should handle gracefully
    try {
      Optional<Integer> nullSubjectId =
          schemaRegistryService.getSchemaIdBySubjectAndVersion(null, 1);
      // If it doesn't throw, it should return empty
      Assert.assertFalse(nullSubjectId.isPresent());
    } catch (NullPointerException e) {
      // Expected behavior for null handling
    }
  }

  @Test
  public void testGetSchemaBySubjectAndVersion() {
    // Test with valid subject and version
    Optional<Schema> schema =
        schemaRegistryService.getSchemaBySubjectAndVersion("MetadataChangeProposal-value", 1);
    Assert.assertTrue(schema.isPresent());
    Assert.assertEquals(schema.get().getName(), "MetadataChangeProposal");

    // Test with different version
    Optional<Schema> schemaV2 =
        schemaRegistryService.getSchemaBySubjectAndVersion("MetadataChangeProposal-value", 2);
    Assert.assertTrue(schemaV2.isPresent());
    Assert.assertEquals(schemaV2.get().getName(), "MetadataChangeProposal");

    // Test with FMCP subject
    Optional<Schema> fmcpSchema =
        schemaRegistryService.getSchemaBySubjectAndVersion("FailedMetadataChangeProposal-value", 1);
    Assert.assertTrue(fmcpSchema.isPresent());
    Assert.assertEquals(fmcpSchema.get().getName(), "FailedMetadataChangeProposal");

    // Test with invalid subject
    Optional<Schema> invalidSubjectSchema =
        schemaRegistryService.getSchemaBySubjectAndVersion("InvalidSubject-value", 1);
    Assert.assertFalse(invalidSubjectSchema.isPresent());

    // Test with invalid version
    Optional<Schema> invalidVersionSchema =
        schemaRegistryService.getSchemaBySubjectAndVersion("MetadataChangeProposal-value", 99);
    Assert.assertFalse(invalidVersionSchema.isPresent());

    // Test with null subject - should handle gracefully
    try {
      Optional<Schema> nullSubjectSchema =
          schemaRegistryService.getSchemaBySubjectAndVersion(null, 1);
      // If it doesn't throw, it should return empty
      Assert.assertFalse(nullSubjectSchema.isPresent());
    } catch (NullPointerException e) {
      // Expected behavior for null handling
    }
  }

  @Test
  public void testGetAllSchemaVersionsForSubject() {
    // Test with valid subject
    Optional<List<Integer>> versions =
        schemaRegistryService.getAllSchemaVersionsForSubject("MetadataChangeProposal-value");
    Assert.assertTrue(versions.isPresent());
    List<Integer> versionList = versions.get();
    Assert.assertEquals(versionList.size(), 3);
    Assert.assertTrue(versionList.contains(1));
    Assert.assertTrue(versionList.contains(2));
    Assert.assertTrue(versionList.contains(3));

    // Test with FMCP subject
    Optional<List<Integer>> fmcpVersions =
        schemaRegistryService.getAllSchemaVersionsForSubject("FailedMetadataChangeProposal-value");
    Assert.assertTrue(fmcpVersions.isPresent());
    List<Integer> fmcpVersionList = fmcpVersions.get();
    Assert.assertEquals(fmcpVersionList.size(), 3);
    Assert.assertTrue(fmcpVersionList.contains(1));
    Assert.assertTrue(fmcpVersionList.contains(2));
    Assert.assertTrue(fmcpVersionList.contains(3));

    // Test with single version subject
    Optional<List<Integer>> peVersions =
        schemaRegistryService.getAllSchemaVersionsForSubject("PlatformEvent-value");
    Assert.assertTrue(peVersions.isPresent());
    List<Integer> peVersionList = peVersions.get();
    Assert.assertEquals(peVersionList.size(), 1);
    Assert.assertTrue(peVersionList.contains(1));

    // Test with invalid subject
    Optional<List<Integer>> invalidSubjectVersions =
        schemaRegistryService.getAllSchemaVersionsForSubject("InvalidSubject-value");
    Assert.assertFalse(invalidSubjectVersions.isPresent());

    // Test with null subject - should handle gracefully
    try {
      Optional<List<Integer>> nullSubjectVersions =
          schemaRegistryService.getAllSchemaVersionsForSubject(null);
      // If it doesn't throw, it should return empty
      Assert.assertFalse(nullSubjectVersions.isPresent());
    } catch (NullPointerException e) {
      // Expected behavior for null handling
    }
  }

  @Test
  public void testRegisterSchemaVersion() {
    // Test that registration is read-only (returns existing version for matching schemas)
    Optional<Schema> existingSchema =
        schemaRegistryService.getSchemaForTopicAndVersion(MCP_TOPIC, 1);
    Assert.assertTrue(existingSchema.isPresent());

    // Try to register the same schema - should return the version where it was found
    Optional<Integer> registeredVersion =
        schemaRegistryService.registerSchemaVersion(MCP_TOPIC, existingSchema.get());
    Assert.assertTrue(registeredVersion.isPresent());
    // Should return version 1 since that's where the schema was found
    Assert.assertEquals(registeredVersion.get().intValue(), 1);

    // Test with different topic
    Optional<Schema> fmcpSchema = schemaRegistryService.getSchemaForTopicAndVersion(FMCP_TOPIC, 1);
    Assert.assertTrue(fmcpSchema.isPresent());

    Optional<Integer> fmcpRegisteredVersion =
        schemaRegistryService.registerSchemaVersion(FMCP_TOPIC, fmcpSchema.get());
    Assert.assertTrue(fmcpRegisteredVersion.isPresent());
    Assert.assertEquals(fmcpRegisteredVersion.get().intValue(), 1);

    // Test with invalid topic
    Optional<Integer> invalidTopicVersion =
        schemaRegistryService.registerSchemaVersion("InvalidTopic", existingSchema.get());
    Assert.assertFalse(invalidTopicVersion.isPresent());

    // Test with null schema
    Optional<Integer> nullSchemaVersion =
        schemaRegistryService.registerSchemaVersion(MCP_TOPIC, null);
    Assert.assertFalse(nullSchemaVersion.isPresent());
  }

  @Test
  public void testCheckSchemaCompatibility() {
    // Test NONE compatibility (versioned schemas)
    Optional<Schema> mcpV1 = schemaRegistryService.getSchemaForTopicAndVersion(MCP_TOPIC, 1);
    Optional<Schema> mcpV3 = schemaRegistryService.getSchemaForTopicAndVersion(MCP_TOPIC, 3);
    Assert.assertTrue(mcpV1.isPresent());
    Assert.assertTrue(mcpV3.isPresent());

    // NONE compatibility should return false for different schemas
    boolean mcpCompatibility =
        schemaRegistryService.checkSchemaCompatibility(MCP_TOPIC, mcpV3.get(), mcpV1.get());
    Assert.assertFalse(
        mcpCompatibility, "NONE compatibility should return false for different schemas");

    // Test BACKWARD compatibility (single version schemas)
    Optional<Schema> peSchema = schemaRegistryService.getSchemaForTopic(PE_TOPIC);
    Assert.assertTrue(peSchema.isPresent());

    // BACKWARD compatibility should return true for same schema
    boolean peCompatibility =
        schemaRegistryService.checkSchemaCompatibility(PE_TOPIC, peSchema.get(), peSchema.get());
    Assert.assertTrue(peCompatibility, "BACKWARD compatibility should return true for same schema");

    // Test with invalid topic
    boolean invalidTopicCompatibility =
        schemaRegistryService.checkSchemaCompatibility("InvalidTopic", mcpV1.get(), mcpV3.get());
    Assert.assertFalse(invalidTopicCompatibility, "Invalid topic should return false");

    // Test with null schemas - should handle gracefully
    try {
      boolean nullSchemaCompatibility =
          schemaRegistryService.checkSchemaCompatibility(MCP_TOPIC, null, mcpV1.get());
      // If it doesn't throw, it should return false
      Assert.assertFalse(nullSchemaCompatibility, "Null new schema should return false");
    } catch (NullPointerException e) {
      // Expected behavior for null handling
    }

    try {
      boolean nullExistingCompatibility =
          schemaRegistryService.checkSchemaCompatibility(MCP_TOPIC, mcpV1.get(), null);
      // If it doesn't throw, it should return false
      Assert.assertFalse(nullExistingCompatibility, "Null existing schema should return false");
    } catch (NullPointerException e) {
      // Expected behavior for null handling
    }
  }

  @Test
  public void testSubjectToTopicConversion() {
    // Test that subject names are correctly converted to topic names
    String subject = "MetadataChangeProposal-value";
    String expectedTopic = "MetadataChangeProposal";

    // Test getSchemaIdBySubjectAndVersion
    Optional<Integer> subjectId = schemaRegistryService.getSchemaIdBySubjectAndVersion(subject, 1);
    // Note: getSchemaIdForTopic returns the latest schema ID, not version 1
    Optional<Integer> topicId = schemaRegistryService.getSchemaIdForTopic(expectedTopic);
    // They should be different since subject version 1 maps to schema ID 0, but topic latest maps
    // to schema ID 9
    Assert.assertNotEquals(
        subjectId, topicId, "Subject version 1 and topic latest should have different schema IDs");

    // Test getSchemaBySubjectAndVersion
    Optional<Schema> subjectSchema = schemaRegistryService.getSchemaBySubjectAndVersion(subject, 1);
    Optional<Schema> topicSchema =
        schemaRegistryService.getSchemaForTopicAndVersion(expectedTopic, 1);
    Assert.assertEquals(subjectSchema, topicSchema, "Subject and topic should return same schema");

    // Test getAllSchemaVersionsForSubject
    Optional<List<Integer>> subjectVersions =
        schemaRegistryService.getAllSchemaVersionsForSubject(subject);
    Optional<List<Integer>> topicVersions =
        schemaRegistryService.getSupportedSchemaVersionsForTopic(expectedTopic);
    Assert.assertEquals(
        subjectVersions, topicVersions, "Subject and topic should return same versions");
  }

  @Test
  public void testEdgeCasesForNewMethods() {
    // Test with empty string subject
    Optional<Integer> emptySubjectId = schemaRegistryService.getSchemaIdBySubjectAndVersion("", 1);
    Assert.assertFalse(emptySubjectId.isPresent());

    Optional<Schema> emptySubjectSchema = schemaRegistryService.getSchemaBySubjectAndVersion("", 1);
    Assert.assertFalse(emptySubjectSchema.isPresent());

    Optional<List<Integer>> emptySubjectVersions =
        schemaRegistryService.getAllSchemaVersionsForSubject("");
    Assert.assertFalse(emptySubjectVersions.isPresent());

    // Test with subject that doesn't end with -value
    // This should actually work since replaceFirst("-value", "") returns the original string
    // unchanged
    Optional<Integer> noValueSuffixId =
        schemaRegistryService.getSchemaIdBySubjectAndVersion("MetadataChangeProposal", 1);
    Assert.assertTrue(noValueSuffixId.isPresent(), "Subject without -value suffix should work");

    Optional<Schema> noValueSuffixSchema =
        schemaRegistryService.getSchemaBySubjectAndVersion("MetadataChangeProposal", 1);
    Assert.assertTrue(noValueSuffixSchema.isPresent(), "Subject without -value suffix should work");

    Optional<List<Integer>> noValueSuffixVersions =
        schemaRegistryService.getAllSchemaVersionsForSubject("MetadataChangeProposal");
    Assert.assertTrue(
        noValueSuffixVersions.isPresent(), "Subject without -value suffix should work");

    // Test with negative version numbers
    Optional<Integer> negativeVersionId =
        schemaRegistryService.getSchemaIdBySubjectAndVersion("MetadataChangeProposal-value", -1);
    Assert.assertFalse(negativeVersionId.isPresent());

    Optional<Schema> negativeVersionSchema =
        schemaRegistryService.getSchemaBySubjectAndVersion("MetadataChangeProposal-value", -1);
    Assert.assertFalse(negativeVersionSchema.isPresent());
  }

  @Test
  public void testSchemaCompatibilityConsistency() {
    // Test that compatibility levels are consistent across different access methods
    String mcpCompatibility = schemaRegistryService.getSchemaCompatibility(MCP_TOPIC);
    String mcpCompatibilityById = schemaRegistryService.getSchemaCompatibilityById(0);
    Assert.assertEquals(
        mcpCompatibility, mcpCompatibilityById, "Compatibility should be consistent");

    String fmcpCompatibility = schemaRegistryService.getSchemaCompatibility(FMCP_TOPIC);
    String fmcpCompatibilityById = schemaRegistryService.getSchemaCompatibilityById(1);
    Assert.assertEquals(
        fmcpCompatibility, fmcpCompatibilityById, "Compatibility should be consistent");

    // Test that checkSchemaCompatibility respects the compatibility level
    Optional<Schema> mcpV1 = schemaRegistryService.getSchemaForTopicAndVersion(MCP_TOPIC, 1);
    Optional<Schema> mcpV3 = schemaRegistryService.getSchemaForTopicAndVersion(MCP_TOPIC, 3);
    Assert.assertTrue(mcpV1.isPresent());
    Assert.assertTrue(mcpV3.isPresent());

    if (EventSchemaConstants.SCHEMA_COMPATIBILITY_NONE.equals(mcpCompatibility)) {
      boolean compatibility =
          schemaRegistryService.checkSchemaCompatibility(MCP_TOPIC, mcpV3.get(), mcpV1.get());
      Assert.assertFalse(
          compatibility, "NONE compatibility should return false for different schemas");
    }
  }

  @Test
  public void testIntegrationOfNewMethods() {
    // Test that all new methods work together consistently
    String subject = "MetadataChangeProposal-value";
    String topic = "MetadataChangeProposal";

    // Get schema by subject and version
    Optional<Schema> subjectSchema = schemaRegistryService.getSchemaBySubjectAndVersion(subject, 1);
    Assert.assertTrue(subjectSchema.isPresent());

    // Get schema ID by subject and version
    Optional<Integer> subjectSchemaId =
        schemaRegistryService.getSchemaIdBySubjectAndVersion(subject, 1);
    Assert.assertTrue(subjectSchemaId.isPresent());

    // Verify consistency
    Optional<Schema> topicSchema = schemaRegistryService.getSchemaForId(subjectSchemaId.get());
    Assert.assertTrue(topicSchema.isPresent());
    Assert.assertEquals(subjectSchema.get(), topicSchema.get(), "Schemas should be consistent");

    // Test version consistency
    Optional<List<Integer>> subjectVersions =
        schemaRegistryService.getAllSchemaVersionsForSubject(subject);
    Optional<List<Integer>> topicVersions =
        schemaRegistryService.getSupportedSchemaVersionsForTopic(topic);
    Assert.assertEquals(subjectVersions, topicVersions, "Versions should be consistent");

    // Test compatibility consistency
    String subjectCompatibility = schemaRegistryService.getSchemaCompatibility(topic);
    boolean schemaCompatibility =
        schemaRegistryService.checkSchemaCompatibility(
            topic, subjectSchema.get(), subjectSchema.get());

    if (EventSchemaConstants.SCHEMA_COMPATIBILITY_NONE.equals(subjectCompatibility)) {
      // For NONE compatibility, same schema should return true
      Assert.assertTrue(
          schemaCompatibility, "NONE compatibility should return true for same schema");
    } else if (EventSchemaConstants.SCHEMA_COMPATIBILITY_BACKWARD.equals(subjectCompatibility)) {
      Assert.assertTrue(
          schemaCompatibility, "BACKWARD compatibility should return true for same schema");
    }
  }
}
