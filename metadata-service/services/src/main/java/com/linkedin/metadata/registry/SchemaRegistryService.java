package com.linkedin.metadata.registry;

import java.util.List;
import java.util.Optional;
import org.apache.avro.Schema;

/**
 * Internal Service logic to be used to emulate Confluent's Schema Registry component within
 * DataHub. Supports both basic schema retrieval and versioned schema operations.
 */
public interface SchemaRegistryService {

  Optional<Integer> getSchemaIdForTopic(final String topicName);

  Optional<Schema> getSchemaForTopic(final String topicName);

  Optional<Schema> getSchemaForId(final int id);

  /**
   * Get schema for a specific topic and version
   *
   * @param topicName the topic name
   * @param version the schema version
   * @return Optional containing the schema if found
   */
  Optional<Schema> getSchemaForTopicAndVersion(final String topicName, final int version);

  /**
   * Get the latest schema version for a topic
   *
   * @param topicName the topic name
   * @return Optional containing the latest schema version number
   */
  Optional<Integer> getLatestSchemaVersionForTopic(final String topicName);

  /**
   * Get all supported schema versions for a topic
   *
   * @param topicName the topic name
   * @return Optional containing list of supported schema versions
   */
  Optional<List<Integer>> getSupportedSchemaVersionsForTopic(final String topicName);
}
