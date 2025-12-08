package io.datahubproject.openapi.operations.kafka;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Set;
import org.testng.annotations.Test;

/** Unit tests for {@link KafkaAdminException} and its subclasses. */
public class KafkaAdminExceptionTest {

  // ============================================================================
  // KafkaAdminException Tests
  // ============================================================================

  @Test
  public void testKafkaAdminException_MessageOnly() {
    // Arrange & Act
    KafkaAdminException exception = new KafkaAdminException("Test error message");

    // Assert
    assertEquals(exception.getMessage(), "Test error message");
  }

  @Test
  public void testKafkaAdminException_MessageAndCause() {
    // Arrange
    Throwable cause = new RuntimeException("Root cause");

    // Act
    KafkaAdminException exception = new KafkaAdminException("Test error message", cause);

    // Assert
    assertEquals(exception.getMessage(), "Test error message");
    assertEquals(exception.getCause(), cause);
  }

  // ============================================================================
  // InvalidAliasException Tests
  // ============================================================================

  @Test
  public void testInvalidAliasException_Getters() {
    // Arrange
    String alias = "invalid-alias";
    Set<String> validAliases = Set.of("mcp", "mcl-versioned", "mcp-failed");

    // Act
    KafkaAdminException.InvalidAliasException exception =
        new KafkaAdminException.InvalidAliasException(alias, validAliases);

    // Assert
    assertEquals(exception.getAlias(), alias);
    assertEquals(exception.getValidAliases(), validAliases);
    assertTrue(exception.getMessage().contains(alias));
    assertTrue(exception.getMessage().contains("mcp"));
  }

  // ============================================================================
  // TopicNotFoundException Tests
  // ============================================================================

  @Test
  public void testTopicNotFoundException_Getters() {
    // Arrange
    String topicName = "non-existent-topic";

    // Act
    KafkaAdminException.TopicNotFoundException exception =
        new KafkaAdminException.TopicNotFoundException(topicName);

    // Assert
    assertEquals(exception.getTopicName(), topicName);
    assertTrue(exception.getMessage().contains(topicName));
  }

  // ============================================================================
  // TopicAlreadyExistsException Tests
  // ============================================================================

  @Test
  public void testTopicAlreadyExistsException_Getters() {
    // Arrange
    String topicName = "existing-topic";

    // Act
    KafkaAdminException.TopicAlreadyExistsException exception =
        new KafkaAdminException.TopicAlreadyExistsException(topicName);

    // Assert
    assertEquals(exception.getTopicName(), topicName);
    assertTrue(exception.getMessage().contains(topicName));
    assertTrue(exception.getMessage().contains("already exists"));
  }

  // ============================================================================
  // ConsumerGroupNotFoundException Tests
  // ============================================================================

  @Test
  public void testConsumerGroupNotFoundException_Getters() {
    // Arrange
    String groupId = "non-existent-group";

    // Act
    KafkaAdminException.ConsumerGroupNotFoundException exception =
        new KafkaAdminException.ConsumerGroupNotFoundException(groupId);

    // Assert
    assertEquals(exception.getGroupId(), groupId);
    assertTrue(exception.getMessage().contains(groupId));
    assertTrue(exception.getMessage().contains("not found"));
  }

  // ============================================================================
  // ConsumerGroupNotEmptyException Tests
  // ============================================================================

  @Test
  public void testConsumerGroupNotEmptyException_Getters() {
    // Arrange
    String groupId = "active-group";

    // Act
    KafkaAdminException.ConsumerGroupNotEmptyException exception =
        new KafkaAdminException.ConsumerGroupNotEmptyException(groupId);

    // Assert
    assertEquals(exception.getGroupId(), groupId);
    assertTrue(exception.getMessage().contains(groupId));
    assertTrue(exception.getMessage().contains("not empty"));
  }

  // ============================================================================
  // InvalidTopicException Tests
  // ============================================================================

  @Test
  public void testInvalidTopicException_Getters() {
    // Arrange
    String topicName = "external-topic";
    Set<String> validTopics = Set.of("MetadataChangeProposal_v1", "MetadataChangeLog_Versioned_v1");

    // Act
    KafkaAdminException.InvalidTopicException exception =
        new KafkaAdminException.InvalidTopicException(topicName, validTopics);

    // Assert
    assertEquals(exception.getTopicName(), topicName);
    assertEquals(exception.getValidTopics(), validTopics);
    assertTrue(exception.getMessage().contains(topicName));
    assertTrue(exception.getMessage().contains("not a DataHub topic"));
  }

  // ============================================================================
  // InvalidConsumerGroupException Tests
  // ============================================================================

  @Test
  public void testInvalidConsumerGroupException_Getters() {
    // Arrange
    String groupId = "external-group";
    Set<String> validGroups = Set.of("datahub-mcp-consumer", "datahub-mcl-consumer");

    // Act
    KafkaAdminException.InvalidConsumerGroupException exception =
        new KafkaAdminException.InvalidConsumerGroupException(groupId, validGroups);

    // Assert
    assertEquals(exception.getGroupId(), groupId);
    assertEquals(exception.getValidGroups(), validGroups);
    assertTrue(exception.getMessage().contains(groupId));
    assertTrue(exception.getMessage().contains("not a DataHub consumer group"));
  }

  // ============================================================================
  // Inheritance Tests
  // ============================================================================

  @Test
  public void testExceptionsAreRuntimeExceptions() {
    // Assert all exceptions extend RuntimeException
    assertTrue(RuntimeException.class.isAssignableFrom(KafkaAdminException.class));
    assertTrue(
        KafkaAdminException.class.isAssignableFrom(
            KafkaAdminException.InvalidAliasException.class));
    assertTrue(
        KafkaAdminException.class.isAssignableFrom(
            KafkaAdminException.TopicNotFoundException.class));
    assertTrue(
        KafkaAdminException.class.isAssignableFrom(
            KafkaAdminException.TopicAlreadyExistsException.class));
    assertTrue(
        KafkaAdminException.class.isAssignableFrom(
            KafkaAdminException.ConsumerGroupNotFoundException.class));
    assertTrue(
        KafkaAdminException.class.isAssignableFrom(
            KafkaAdminException.ConsumerGroupNotEmptyException.class));
    assertTrue(
        KafkaAdminException.class.isAssignableFrom(
            KafkaAdminException.InvalidTopicException.class));
    assertTrue(
        KafkaAdminException.class.isAssignableFrom(
            KafkaAdminException.InvalidConsumerGroupException.class));
  }
}
