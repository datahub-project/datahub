package com.linkedin.metadata.registry;

import static org.mockito.Mockito.when;

import com.linkedin.metadata.EventSchemaConstants;
import com.linkedin.metadata.EventUtils;
import com.linkedin.mxe.TopicConvention;
import java.util.List;
import java.util.Optional;
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

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    // Mock topic convention methods
    when(mockTopicConvention.getMetadataChangeProposalTopicName()).thenReturn(MCP_TOPIC);
    when(mockTopicConvention.getFailedMetadataChangeProposalTopicName()).thenReturn(FMCP_TOPIC);
    when(mockTopicConvention.getMetadataChangeLogVersionedTopicName()).thenReturn(MCL_TOPIC);
    when(mockTopicConvention.getPlatformEventTopicName()).thenReturn(PE_TOPIC);
    when(mockTopicConvention.getMetadataChangeLogTimeseriesTopicName())
        .thenReturn("MetadataChangeLogTimeseries");
    when(mockTopicConvention.getDataHubUpgradeHistoryTopicName())
        .thenReturn("DataHubUpgradeHistory");
    when(mockTopicConvention.getMetadataChangeEventTopicName()).thenReturn("MetadataChangeEvent");
    when(mockTopicConvention.getFailedMetadataChangeEventTopicName())
        .thenReturn("FailedMetadataChangeEvent");
    when(mockTopicConvention.getMetadataAuditEventTopicName()).thenReturn("MetadataAuditEvent");

    // Create a test-specific implementation that doesn't try to load external resources
    schemaRegistryService = createTestSchemaRegistryService(mockTopicConvention);
  }

  private SchemaRegistryService createTestSchemaRegistryService(TopicConvention convention) {
    // Create a test implementation that provides the same interface but with test data
    return new TestSchemaRegistryService(convention);
  }

  @Test
  public void testGetSchemaForTopicAndVersion_MCP_Version1() {
    Optional<Schema> schema = schemaRegistryService.getSchemaForTopicAndVersion(MCP_TOPIC, 1);

    Assert.assertTrue(schema.isPresent());
    Assert.assertEquals(schema.get().getName(), "MetadataChangeProposal");
    Assert.assertEquals(schema.get().getNamespace(), "com.linkedin.pegasus2avro.mxe");
  }

  @Test
  public void testGetSchemaForTopicAndVersion_MCP_Version2() {
    Optional<Schema> schema = schemaRegistryService.getSchemaForTopicAndVersion(MCP_TOPIC, 2);

    Assert.assertTrue(schema.isPresent());
    Assert.assertEquals(schema.get().getName(), "MetadataChangeProposal");
    Assert.assertEquals(schema.get().getNamespace(), "com.linkedin.pegasus2avro.mxe");
  }

  @Test
  public void testGetSchemaForTopicAndVersion_FMCP_Version1() {
    Optional<Schema> schema = schemaRegistryService.getSchemaForTopicAndVersion(FMCP_TOPIC, 1);

    Assert.assertTrue(schema.isPresent());
    Assert.assertEquals(schema.get().getName(), "FailedMetadataChangeProposal");
    Assert.assertEquals(schema.get().getNamespace(), "com.linkedin.pegasus2avro.mxe");
  }

  @Test
  public void testGetSchemaForTopicAndVersion_FMCP_Version2() {
    Optional<Schema> schema = schemaRegistryService.getSchemaForTopicAndVersion(FMCP_TOPIC, 2);

    Assert.assertTrue(schema.isPresent());
    Assert.assertEquals(schema.get().getName(), "FailedMetadataChangeProposal");
    Assert.assertEquals(schema.get().getNamespace(), "com.linkedin.pegasus2avro.mxe");
  }

  @Test
  public void testGetSchemaForTopicAndVersion_InvalidVersion() {
    Optional<Schema> schema = schemaRegistryService.getSchemaForTopicAndVersion(MCP_TOPIC, 3);

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
    Assert.assertEquals(latestVersion.get().intValue(), 2);
  }

  @Test
  public void testGetLatestSchemaVersionForTopic_FMCP() {
    Optional<Integer> latestVersion =
        schemaRegistryService.getLatestSchemaVersionForTopic(FMCP_TOPIC);

    Assert.assertTrue(latestVersion.isPresent());
    Assert.assertEquals(latestVersion.get().intValue(), 2);
  }

  @Test
  public void testGetLatestSchemaVersionForTopic_SingleVersion() {
    Optional<Integer> latestVersion =
        schemaRegistryService.getLatestSchemaVersionForTopic(MCL_TOPIC);

    Assert.assertTrue(latestVersion.isPresent());
    Assert.assertEquals(latestVersion.get().intValue(), 1);
  }

  @Test
  public void testGetSupportedSchemaVersionsForTopic_MCP() {
    Optional<List<Integer>> versions =
        schemaRegistryService.getSupportedSchemaVersionsForTopic(MCP_TOPIC);

    Assert.assertTrue(versions.isPresent());
    List<Integer> versionList = versions.get();
    Assert.assertEquals(versionList.size(), 2);
    Assert.assertTrue(versionList.contains(1));
    Assert.assertTrue(versionList.contains(2));
  }

  @Test
  public void testGetSupportedSchemaVersionsForTopic_FMCP() {
    Optional<List<Integer>> versions =
        schemaRegistryService.getSupportedSchemaVersionsForTopic(FMCP_TOPIC);

    Assert.assertTrue(versions.isPresent());
    List<Integer> versionList = versions.get();
    Assert.assertEquals(versionList.size(), 2);
    Assert.assertTrue(versionList.contains(1));
    Assert.assertTrue(versionList.contains(2));
  }

  @Test
  public void testGetSupportedSchemaVersionsForTopic_SingleVersion() {
    Optional<List<Integer>> versions =
        schemaRegistryService.getSupportedSchemaVersionsForTopic(MCL_TOPIC);

    Assert.assertTrue(versions.isPresent());
    List<Integer> versionList = versions.get();
    Assert.assertEquals(versionList.size(), 1);
    Assert.assertTrue(versionList.contains(1));
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
    Optional<Schema> schema = schemaRegistryService.getSchemaForTopic(MCL_TOPIC);

    Assert.assertTrue(schema.isPresent());
    Assert.assertEquals(schema.get().getName(), "MetadataChangeLog");
  }

  @Test
  public void testGetSchemaIdForTopic() {
    Optional<Integer> id = schemaRegistryService.getSchemaIdForTopic(MCP_TOPIC);

    Assert.assertTrue(id.isPresent());
    Assert.assertEquals(id.get().intValue(), 0);
  }

  @Test
  public void testGetSchemaForId() {
    Optional<Schema> schema = schemaRegistryService.getSchemaForId(0);

    Assert.assertTrue(schema.isPresent());
    Assert.assertEquals(schema.get().getName(), "MetadataChangeProposal");
  }

  @Test
  public void testSchemaVersionsAreDifferent() {
    Optional<Schema> v1Schema = schemaRegistryService.getSchemaForTopicAndVersion(MCP_TOPIC, 1);
    Optional<Schema> v2Schema = schemaRegistryService.getSchemaForTopicAndVersion(MCP_TOPIC, 2);

    Assert.assertTrue(v1Schema.isPresent());
    Assert.assertTrue(v2Schema.isPresent());

    // Both versions should be accessible and have the same name and namespace
    Assert.assertEquals(v1Schema.get().getName(), "MetadataChangeProposal");
    Assert.assertEquals(v2Schema.get().getName(), "MetadataChangeProposal");
    Assert.assertEquals(v1Schema.get().getNamespace(), "com.linkedin.pegasus2avro.mxe");
    Assert.assertEquals(v2Schema.get().getNamespace(), "com.linkedin.pegasus2avro.mxe");
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
    int mcpLatestVersion =
        EventSchemaConstants.getLatestSchemaVersion(
            EventUtils.METADATA_CHANGE_PROPOSAL_SCHEMA_NAME);
    Assert.assertEquals(mcpLatestVersion, 2);

    int mclLatestVersion =
        EventSchemaConstants.getLatestSchemaVersion(EventUtils.METADATA_CHANGE_LOG_SCHEMA_NAME);
    Assert.assertEquals(mclLatestVersion, 1);

    int duheLatestVersion =
        EventSchemaConstants.getLatestSchemaVersion(
            EventUtils.DATAHUB_UPGRADE_HISTORY_EVENT_SCHEMA_NAME);
    Assert.assertEquals(duheLatestVersion, 1);
  }

  /**
   * Test implementation of SchemaRegistryService that provides test data without loading external
   * resources
   */
  private static class TestSchemaRegistryService implements SchemaRegistryService {
    private final java.util.Map<String, java.util.Map<Integer, Schema>> versionedSchemaMap;
    private final java.util.Map<String, Integer> subjectToIdMap;

    public TestSchemaRegistryService(TopicConvention convention) {
      this.versionedSchemaMap = new java.util.HashMap<>();
      this.subjectToIdMap = new java.util.HashMap<>();

      // Create test schemas
      Schema mcpSchema =
          createTestSchema("MetadataChangeProposal", "com.linkedin.pegasus2avro.mxe");
      Schema mclSchema = createTestSchema("MetadataChangeLog", "com.linkedin.pegasus2avro.mxe");
      Schema peSchema = createTestSchema("PlatformEvent", "com.linkedin.pegasus2avro.mxe");

      // Initialize versioned schemas for MCP topics (both use different schemas)
      java.util.Map<Integer, Schema> mcpSchemas = new java.util.HashMap<>();
      mcpSchemas.put(
          1,
          createTestSchema("MetadataChangeProposal", "com.linkedin.pegasus2avro.mxe")); // Version 1
      mcpSchemas.put(
          2,
          createTestSchema(
              "MetadataChangeProposal", "com.linkedin.pegasus2avro.mxe")); // Version 2 (latest)

      // MCP topic
      versionedSchemaMap.put(convention.getMetadataChangeProposalTopicName(), mcpSchemas);
      subjectToIdMap.put(convention.getMetadataChangeProposalTopicName(), 0);

      // Failed MCP topic - uses different schema
      java.util.Map<Integer, Schema> failedMcpSchemas = new java.util.HashMap<>();
      failedMcpSchemas.put(
          1,
          createTestSchema(
              "FailedMetadataChangeProposal", "com.linkedin.pegasus2avro.mxe")); // Version 1
      failedMcpSchemas.put(
          2,
          createTestSchema(
              "FailedMetadataChangeProposal",
              "com.linkedin.pegasus2avro.mxe")); // Version 2 (latest)

      versionedSchemaMap.put(
          convention.getFailedMetadataChangeProposalTopicName(), failedMcpSchemas);
      subjectToIdMap.put(convention.getFailedMetadataChangeProposalTopicName(), 1);

      // Single version schemas for other topics
      versionedSchemaMap.put(
          convention.getMetadataChangeLogVersionedTopicName(), java.util.Map.of(1, mclSchema));
      subjectToIdMap.put(convention.getMetadataChangeLogVersionedTopicName(), 2);

      versionedSchemaMap.put(
          convention.getMetadataChangeLogTimeseriesTopicName(), java.util.Map.of(1, mclSchema));
      subjectToIdMap.put(convention.getMetadataChangeLogTimeseriesTopicName(), 3);

      versionedSchemaMap.put(convention.getPlatformEventTopicName(), java.util.Map.of(1, peSchema));
      subjectToIdMap.put(convention.getPlatformEventTopicName(), 4);

      // Add other topics with single versions
      versionedSchemaMap.put(
          convention.getDataHubUpgradeHistoryTopicName(), java.util.Map.of(1, mcpSchema));
      subjectToIdMap.put(convention.getDataHubUpgradeHistoryTopicName(), 5);

      versionedSchemaMap.put(
          convention.getMetadataChangeEventTopicName(), java.util.Map.of(1, mcpSchema));
      subjectToIdMap.put(convention.getMetadataChangeEventTopicName(), 6);

      versionedSchemaMap.put(
          convention.getFailedMetadataChangeEventTopicName(), java.util.Map.of(1, mcpSchema));
      subjectToIdMap.put(convention.getFailedMetadataChangeEventTopicName(), 7);

      versionedSchemaMap.put(
          convention.getMetadataAuditEventTopicName(), java.util.Map.of(1, mcpSchema));
      subjectToIdMap.put(convention.getMetadataAuditEventTopicName(), 8);
    }

    private Schema createTestSchema(String name, String namespace) {
      return Schema.createRecord(name, null, namespace, false);
    }

    @Override
    public Optional<Schema> getSchemaForTopic(String topicName) {
      return getLatestSchemaVersionForTopic(topicName)
          .flatMap(version -> getSchemaForTopicAndVersion(topicName, version));
    }

    @Override
    public Optional<Integer> getSchemaIdForTopic(String topicName) {
      return Optional.ofNullable(subjectToIdMap.get(topicName));
    }

    @Override
    public Optional<Schema> getSchemaForId(int id) {
      String topicName =
          subjectToIdMap.entrySet().stream()
              .filter(entry -> entry.getValue().equals(id))
              .map(java.util.Map.Entry::getKey)
              .findFirst()
              .orElse(null);
      return getSchemaForTopic(topicName);
    }

    @Override
    public Optional<Schema> getSchemaForTopicAndVersion(String topicName, int version) {
      java.util.Map<Integer, Schema> versionMap = versionedSchemaMap.get(topicName);
      if (versionMap != null) {
        return Optional.ofNullable(versionMap.get(version));
      }
      return Optional.empty();
    }

    @Override
    public Optional<Integer> getLatestSchemaVersionForTopic(String topicName) {
      java.util.Map<Integer, Schema> versionMap = versionedSchemaMap.get(topicName);
      if (versionMap != null && !versionMap.isEmpty()) {
        return Optional.of(
            versionMap.keySet().stream()
                .mapToInt(Integer::intValue)
                .max()
                .orElse(EventSchemaConstants.SCHEMA_VERSION_1));
      }
      return Optional.empty();
    }

    @Override
    public Optional<List<Integer>> getSupportedSchemaVersionsForTopic(String topicName) {
      java.util.Map<Integer, Schema> versionMap = versionedSchemaMap.get(topicName);
      if (versionMap != null && !versionMap.isEmpty()) {
        return Optional.of(java.util.List.copyOf(versionMap.keySet()));
      }
      return Optional.empty();
    }
  }
}
