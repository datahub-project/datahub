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

  /**
   * Get all available topics/subjects in the schema registry
   *
   * @return List of all topic names
   */
  List<String> getAllTopics();

  /**
   * Get the compatibility level for a specific topic
   *
   * @param topicName the topic name
   * @return the compatibility level (e.g., "NONE", "BACKWARD", "FORWARD", "FULL")
   */
  String getSchemaCompatibility(String topicName);

  /**
   * Get the compatibility level for a specific schema ID
   *
   * @param schemaId the schema ID
   * @return the compatibility level (e.g., "NONE", "BACKWARD", "FORWARD", "FULL")
   */
  String getSchemaCompatibilityById(int schemaId);

  /**
   * Get the topic name for a specific schema ID
   *
   * @param schemaId the schema ID
   * @return Optional containing the topic name if found
   */
  Optional<String> getTopicNameById(int schemaId);

  /**
   * Get schema ID for a specific subject and version This is critical for message deserialization
   *
   * @param subject the subject name (topic-value)
   * @param version the schema version
   * @return Optional containing the schema ID if found
   */
  Optional<Integer> getSchemaIdBySubjectAndVersion(String subject, int version);

  /**
   * Register a new schema version for a topic This enables schema evolution
   *
   * @param topicName the topic name
   * @param schema the new schema to register
   * @return Optional containing the new schema version number
   */
  Optional<Integer> registerSchemaVersion(String topicName, Schema schema);

  /**
   * Check compatibility between two schemas This is needed for proper schema evolution validation
   *
   * @param topicName the topic name
   * @param newSchema the new schema to check
   * @param existingSchema the existing schema to check against
   * @return true if the schemas are compatible, false otherwise
   */
  boolean checkSchemaCompatibility(String topicName, Schema newSchema, Schema existingSchema);

  /**
   * Get schema by subject and version (Confluent Schema Registry compatibility) This is the
   * standard way consumers request schemas
   *
   * @param subject the subject name (topic-value)
   * @param version the schema version
   * @return Optional containing the schema if found
   */
  Optional<Schema> getSchemaBySubjectAndVersion(String subject, int version);

  /**
   * Get all schema versions for a subject This provides complete schema history
   *
   * @param subject the subject name (topic-value)
   * @return Optional containing list of all schema versions
   */
  Optional<List<Integer>> getAllSchemaVersionsForSubject(String subject);
}
