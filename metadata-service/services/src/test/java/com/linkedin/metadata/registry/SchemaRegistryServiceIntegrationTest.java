package com.linkedin.metadata.registry;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.mxe.TopicConvention;
import java.util.List;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SchemaRegistryServiceIntegrationTest {

  private TopicConvention topicConvention;
  private SchemaRegistryService schemaRegistryService;

  @BeforeMethod
  public void setUp() {
    // Create a real TopicConvention instance for integration testing
    topicConvention =
        new TopicConvention() {
          @Override
          public String getMetadataChangeProposalTopicName() {
            return "MetadataChangeProposal";
          }

          @Override
          public String getFailedMetadataChangeProposalTopicName() {
            return "FailedMetadataChangeProposal";
          }

          @Override
          public String getMetadataChangeLogVersionedTopicName() {
            return "MetadataChangeLog";
          }

          @Override
          public String getMetadataChangeLogTimeseriesTopicName() {
            return "MetadataChangeLogTimeseries";
          }

          @Override
          public String getPlatformEventTopicName() {
            return "PlatformEvent";
          }

          @Override
          public String getDataHubUpgradeHistoryTopicName() {
            return "DataHubUpgradeHistory";
          }

          @Override
          public String getMetadataChangeEventTopicName() {
            return "MetadataChangeEvent";
          }

          @Override
          public String getFailedMetadataChangeEventTopicName() {
            return "FailedMetadataChangeEvent";
          }

          @Override
          public String getMetadataAuditEventTopicName() {
            return "MetadataAuditEvent";
          }

          @Override
          public String getDataHubUsageEventTopicName() {
            return "DataHubUsageEvent";
          }

          @Override
          public String getMetadataChangeEventTopicName(Urn urn, RecordTemplate aspect) {
            return "MetadataChangeEvent_"
                + urn.getEntityType()
                + "_"
                + aspect.getClass().getSimpleName()
                + "_v1";
          }

          @Override
          public String getMetadataAuditEventTopicName(Urn urn, RecordTemplate aspect) {
            return "MetadataAuditEvent_"
                + urn.getEntityType()
                + "_"
                + aspect.getClass().getSimpleName()
                + "_v1";
          }

          @Override
          public String getFailedMetadataChangeEventTopicName(Urn urn, RecordTemplate aspect) {
            return "FailedMetadataChangeEvent_"
                + urn.getEntityType()
                + "_"
                + aspect.getClass().getSimpleName()
                + "_v1";
          }

          @Override
          public Class<? extends SpecificRecord> getMetadataChangeEventType(
              Urn urn, RecordTemplate aspect) {
            throw new UnsupportedOperationException("Not implemented in test");
          }

          @Override
          public Class<? extends SpecificRecord> getMetadataAuditEventType(
              Urn urn, RecordTemplate aspect) {
            throw new UnsupportedOperationException("Not implemented in test");
          }

          @Override
          public Class<? extends SpecificRecord> getFailedMetadataChangeEventType(
              Urn urn, RecordTemplate aspect) {
            throw new UnsupportedOperationException("Not implemented in test");
          }
        };

    schemaRegistryService = new SchemaRegistryServiceImpl(topicConvention);
  }

  @Test
  public void testCompleteFlow_MetadataChangeProposal() {
    String topicName = "MetadataChangeProposal";

    // Test that we can get both versions
    Optional<Schema> v1Schema = schemaRegistryService.getSchemaForTopicAndVersion(topicName, 1);
    Optional<Schema> v2Schema = schemaRegistryService.getSchemaForTopicAndVersion(topicName, 2);

    Assert.assertTrue(v1Schema.isPresent(), "Version 1 schema should be present");
    Assert.assertTrue(v2Schema.isPresent(), "Version 2 schema should be present");

    // Verify both schemas have the same name and pegasus2avro namespace
    Assert.assertEquals(v1Schema.get().getName(), "MetadataChangeProposal");
    Assert.assertEquals(v2Schema.get().getName(), "MetadataChangeProposal");
    Assert.assertEquals(v1Schema.get().getNamespace(), "com.linkedin.pegasus2avro.mxe");
    Assert.assertEquals(v2Schema.get().getNamespace(), "com.linkedin.pegasus2avro.mxe");

    // Verify the schemas are different (different versions)
    Assert.assertNotEquals(
        v1Schema.get().toString(),
        v2Schema.get().toString(),
        "Version 1 and Version 2 schemas should be different");

    // Test getting latest version
    Optional<Integer> latestVersion =
        schemaRegistryService.getLatestSchemaVersionForTopic(topicName);
    Assert.assertTrue(latestVersion.isPresent());
    Assert.assertEquals(latestVersion.get().intValue(), 2);

    // Test getting supported versions
    Optional<List<Integer>> supportedVersions =
        schemaRegistryService.getSupportedSchemaVersionsForTopic(topicName);
    Assert.assertTrue(supportedVersions.isPresent());
    Assert.assertEquals(supportedVersions.get().size(), 2);
    Assert.assertTrue(supportedVersions.get().contains(1));
    Assert.assertTrue(supportedVersions.get().contains(2));

    // Test default behavior (should return latest)
    Optional<Schema> defaultSchema = schemaRegistryService.getSchemaForTopic(topicName);
    Assert.assertTrue(defaultSchema.isPresent());
    Assert.assertEquals(defaultSchema.get().getName(), "MetadataChangeProposal");
  }

  @Test
  public void testCompleteFlow_FailedMetadataChangeProposal() {
    String topicName = "FailedMetadataChangeProposal";

    // Test that Failed MCP topic also supports both versions
    Optional<Schema> v1Schema = schemaRegistryService.getSchemaForTopicAndVersion(topicName, 1);
    Optional<Schema> v2Schema = schemaRegistryService.getSchemaForTopicAndVersion(topicName, 2);

    Assert.assertTrue(v1Schema.isPresent(), "Version 1 schema should be present");
    Assert.assertTrue(v2Schema.isPresent(), "Version 2 schema should be present");

    // Verify both schemas have the correct name and namespace
    Assert.assertEquals(v1Schema.get().getName(), "FailedMetadataChangeProposal");
    Assert.assertEquals(v2Schema.get().getName(), "FailedMetadataChangeProposal");

    // Test getting latest version
    Optional<Integer> latestVersion =
        schemaRegistryService.getLatestSchemaVersionForTopic(topicName);
    Assert.assertTrue(latestVersion.isPresent());
    Assert.assertEquals(latestVersion.get().intValue(), 2);

    // Test getting supported versions
    Optional<List<Integer>> supportedVersions =
        schemaRegistryService.getSupportedSchemaVersionsForTopic(topicName);
    Assert.assertTrue(supportedVersions.isPresent());
    Assert.assertEquals(supportedVersions.get().size(), 2);
    Assert.assertTrue(supportedVersions.get().contains(1));
    Assert.assertTrue(supportedVersions.get().contains(2));
  }

  @Test
  public void testCompleteFlow_SingleVersionSchema() {
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
}
