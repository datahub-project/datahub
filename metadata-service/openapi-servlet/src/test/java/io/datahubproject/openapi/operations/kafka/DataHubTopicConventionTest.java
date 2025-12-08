package io.datahubproject.openapi.operations.kafka;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.mxe.TopicConventionImpl;
import java.util.Optional;
import java.util.Set;
import org.testng.annotations.Test;

/** Unit tests for {@link DataHubTopicConvention}. */
public class DataHubTopicConventionTest {

  private static final String DATAHUB_CONSUMER_GROUP = "datahub-mcp-consumer";

  // ============================================================================
  // Constructor Tests
  // ============================================================================

  @Test
  public void testConstructor_SingleArg() {
    // Arrange & Act - use single-arg constructor
    DataHubTopicConvention convention = new DataHubTopicConvention(new TopicConventionImpl());

    // Assert - should have empty consumer groups
    assertNotNull(convention);
    assertNotNull(convention.getKnownConsumerGroupIds());
    assertTrue(convention.getKnownConsumerGroupIds().isEmpty());
  }

  @Test
  public void testConstructor_TwoArgs() {
    // Arrange & Act
    DataHubTopicConvention convention =
        new DataHubTopicConvention(new TopicConventionImpl(), Set.of(DATAHUB_CONSUMER_GROUP));

    // Assert
    assertNotNull(convention);
    assertEquals(convention.getKnownConsumerGroupIds().size(), 1);
    assertTrue(convention.getKnownConsumerGroupIds().contains(DATAHUB_CONSUMER_GROUP));
  }

  // ============================================================================
  // getTopicNameForAlias Tests
  // ============================================================================

  @Test
  public void testGetTopicNameForAlias_NullAlias() {
    // Arrange
    DataHubTopicConvention convention = new DataHubTopicConvention(new TopicConventionImpl());

    // Act
    Optional<String> result = convention.getTopicNameForAlias(null);

    // Assert - should return empty for null alias
    assertFalse(result.isPresent());
  }

  @Test
  public void testGetTopicNameForAlias_ValidAlias() {
    // Arrange
    DataHubTopicConvention convention = new DataHubTopicConvention(new TopicConventionImpl());

    // Act
    Optional<String> result = convention.getTopicNameForAlias("mcp");

    // Assert
    assertTrue(result.isPresent());
    assertEquals(result.get(), "MetadataChangeProposal_v1");
  }

  @Test
  public void testGetTopicNameForAlias_UnknownAlias() {
    // Arrange
    DataHubTopicConvention convention = new DataHubTopicConvention(new TopicConventionImpl());

    // Act
    Optional<String> result = convention.getTopicNameForAlias("unknown-alias");

    // Assert
    assertFalse(result.isPresent());
  }

  @Test
  public void testGetTopicNameForAlias_CaseInsensitive() {
    // Arrange
    DataHubTopicConvention convention = new DataHubTopicConvention(new TopicConventionImpl());

    // Act
    Optional<String> result = convention.getTopicNameForAlias("MCP");

    // Assert - should work with uppercase
    assertTrue(result.isPresent());
    assertEquals(result.get(), "MetadataChangeProposal_v1");
  }

  // ============================================================================
  // getAliasForTopicName Tests
  // ============================================================================

  @Test
  public void testGetAliasForTopicName_NullTopicName() {
    // Arrange
    DataHubTopicConvention convention = new DataHubTopicConvention(new TopicConventionImpl());

    // Act
    Optional<String> result = convention.getAliasForTopicName(null);

    // Assert - should return empty for null topic name
    assertFalse(result.isPresent());
  }

  @Test
  public void testGetAliasForTopicName_ValidTopicName() {
    // Arrange
    DataHubTopicConvention convention = new DataHubTopicConvention(new TopicConventionImpl());

    // Act
    Optional<String> result = convention.getAliasForTopicName("MetadataChangeProposal_v1");

    // Assert
    assertTrue(result.isPresent());
    assertEquals(result.get(), "mcp");
  }

  @Test
  public void testGetAliasForTopicName_UnknownTopicName() {
    // Arrange
    DataHubTopicConvention convention = new DataHubTopicConvention(new TopicConventionImpl());

    // Act
    Optional<String> result = convention.getAliasForTopicName("unknown-topic");

    // Assert
    assertFalse(result.isPresent());
  }

  // ============================================================================
  // isDataHubTopic Tests
  // ============================================================================

  @Test
  public void testIsDataHubTopic_NullTopicName() {
    // Arrange
    DataHubTopicConvention convention = new DataHubTopicConvention(new TopicConventionImpl());

    // Act & Assert
    assertFalse(convention.isDataHubTopic(null));
  }

  @Test
  public void testIsDataHubTopic_ValidTopic() {
    // Arrange
    DataHubTopicConvention convention = new DataHubTopicConvention(new TopicConventionImpl());

    // Act & Assert
    assertTrue(convention.isDataHubTopic("MetadataChangeProposal_v1"));
  }

  @Test
  public void testIsDataHubTopic_UnknownTopic() {
    // Arrange
    DataHubTopicConvention convention = new DataHubTopicConvention(new TopicConventionImpl());

    // Act & Assert
    assertFalse(convention.isDataHubTopic("unknown-topic"));
  }

  // ============================================================================
  // isDataHubConsumerGroup Tests
  // ============================================================================

  @Test
  public void testIsDataHubConsumerGroup_NullGroupId() {
    // Arrange
    DataHubTopicConvention convention =
        new DataHubTopicConvention(new TopicConventionImpl(), Set.of(DATAHUB_CONSUMER_GROUP));

    // Act & Assert
    assertFalse(convention.isDataHubConsumerGroup(null));
  }

  @Test
  public void testIsDataHubConsumerGroup_ValidGroup() {
    // Arrange
    DataHubTopicConvention convention =
        new DataHubTopicConvention(new TopicConventionImpl(), Set.of(DATAHUB_CONSUMER_GROUP));

    // Act & Assert
    assertTrue(convention.isDataHubConsumerGroup(DATAHUB_CONSUMER_GROUP));
  }

  @Test
  public void testIsDataHubConsumerGroup_UnknownGroup() {
    // Arrange
    DataHubTopicConvention convention =
        new DataHubTopicConvention(new TopicConventionImpl(), Set.of(DATAHUB_CONSUMER_GROUP));

    // Act & Assert
    assertFalse(convention.isDataHubConsumerGroup("unknown-group"));
  }

  // ============================================================================
  // getAllAliases Tests
  // ============================================================================

  @Test
  public void testGetAllAliases() {
    // Arrange
    DataHubTopicConvention convention = new DataHubTopicConvention(new TopicConventionImpl());

    // Act
    Set<String> aliases = convention.getAllAliases();

    // Assert
    assertNotNull(aliases);
    assertTrue(aliases.contains("mcp"));
    assertTrue(aliases.contains("mcp-failed"));
    assertTrue(aliases.contains("mcl-versioned"));
    assertTrue(aliases.contains("mcl-timeseries"));
    assertTrue(aliases.contains("platform-event"));
    assertTrue(aliases.contains("upgrade-history"));
  }

  // ============================================================================
  // getAllDataHubTopicNames Tests
  // ============================================================================

  @Test
  public void testGetAllDataHubTopicNames() {
    // Arrange
    DataHubTopicConvention convention = new DataHubTopicConvention(new TopicConventionImpl());

    // Act
    Set<String> topicNames = convention.getAllDataHubTopicNames();

    // Assert
    assertNotNull(topicNames);
    assertTrue(topicNames.contains("MetadataChangeProposal_v1"));
    assertTrue(topicNames.contains("FailedMetadataChangeProposal_v1"));
    assertTrue(topicNames.contains("MetadataChangeLog_Versioned_v1"));
  }
}
