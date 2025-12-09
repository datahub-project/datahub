package io.datahubproject.openapi.operations.kafka;

import java.util.Set;

/** Exception thrown by KafkaAdminService operations. */
public class KafkaAdminException extends RuntimeException {

  public KafkaAdminException(String message) {
    super(message);
  }

  public KafkaAdminException(String message, Throwable cause) {
    super(message, cause);
  }

  /** Thrown when a topic alias is not recognized. */
  public static class InvalidAliasException extends KafkaAdminException {
    private final String alias;
    private final Set<String> validAliases;

    public InvalidAliasException(String alias, Set<String> validAliases) {
      super("Invalid topic alias: " + alias + ". Must be one of: " + validAliases);
      this.alias = alias;
      this.validAliases = validAliases;
    }

    public String getAlias() {
      return alias;
    }

    public Set<String> getValidAliases() {
      return validAliases;
    }
  }

  /** Thrown when a topic is not found. */
  public static class TopicNotFoundException extends KafkaAdminException {
    private final String topicName;

    public TopicNotFoundException(String topicName) {
      super("Topic not found: " + topicName);
      this.topicName = topicName;
    }

    public String getTopicName() {
      return topicName;
    }
  }

  /** Thrown when attempting to create a topic that already exists. */
  public static class TopicAlreadyExistsException extends KafkaAdminException {
    private final String topicName;

    public TopicAlreadyExistsException(String topicName) {
      super("Topic already exists: " + topicName);
      this.topicName = topicName;
    }

    public String getTopicName() {
      return topicName;
    }
  }

  /** Thrown when a consumer group is not found. */
  public static class ConsumerGroupNotFoundException extends KafkaAdminException {
    private final String groupId;

    public ConsumerGroupNotFoundException(String groupId) {
      super("Consumer group not found: " + groupId);
      this.groupId = groupId;
    }

    public String getGroupId() {
      return groupId;
    }
  }

  /** Thrown when attempting to delete a non-empty or active consumer group. */
  public static class ConsumerGroupNotEmptyException extends KafkaAdminException {
    private final String groupId;

    public ConsumerGroupNotEmptyException(String groupId) {
      super("Consumer group is not empty or active: " + groupId);
      this.groupId = groupId;
    }

    public String getGroupId() {
      return groupId;
    }
  }

  /** Thrown when a topic is not a known DataHub topic. */
  public static class InvalidTopicException extends KafkaAdminException {
    private final String topicName;
    private final Set<String> validTopics;

    public InvalidTopicException(String topicName, Set<String> validTopics) {
      super("Topic is not a DataHub topic: " + topicName + ". Valid topics: " + validTopics);
      this.topicName = topicName;
      this.validTopics = validTopics;
    }

    public String getTopicName() {
      return topicName;
    }

    public Set<String> getValidTopics() {
      return validTopics;
    }
  }

  /** Thrown when a consumer group is not a known DataHub consumer group. */
  public static class InvalidConsumerGroupException extends KafkaAdminException {
    private final String groupId;
    private final Set<String> validGroups;

    public InvalidConsumerGroupException(String groupId, Set<String> validGroups) {
      super(
          "Consumer group is not a DataHub consumer group: "
              + groupId
              + ". Valid groups: "
              + validGroups);
      this.groupId = groupId;
      this.validGroups = validGroups;
    }

    public String getGroupId() {
      return groupId;
    }

    public Set<String> getValidGroups() {
      return validGroups;
    }
  }
}
