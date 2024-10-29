package com.linkedin.metadata.registry;

import java.util.Optional;
import org.apache.avro.Schema;

/**
 * Internal Service logic to be used to emulate Confluent's Schema Registry component within
 * DataHub.
 */
public interface SchemaRegistryService {

  Optional<Integer> getSchemaIdForTopic(final String topicName);

  Optional<Schema> getSchemaForTopic(final String topicName);

  Optional<Schema> getSchemaForId(final int id);
}
