package com.linkedin.metadata.config.kafka;

import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.springframework.lang.Nullable;

@Data
public class KafkaConfiguration {
  // Avoiding dependencies on other libraries (Spring/Kafka) for configuration
  public static final String KEY_SERIALIZER_CLASS_CONFIG = "key.serializer";
  public static final String VALUE_SERIALIZER_CLASS_CONFIG = "value.serializer";
  public static final String KEY_DESERIALIZER_CLASS_CONFIG = "key.deserializer";
  public static final String VALUE_DESERIALIZER_CLASS_CONFIG = "value.deserializer";
  public static final String KEY_DESERIALIZER_DELEGATE_CLASS =
      "spring.deserializer.key.delegate.class";
  public static final String VALUE_DESERIALIZER_DELEGATE_CLASS =
      "spring.deserializer.value.delegate.class";
  public static final String MCP_EVENT_CONSUMER_NAME = "mcpEventConsumer";
  public static final String MCL_EVENT_CONSUMER_NAME = "mclEventConsumer";
  public static final String PE_EVENT_CONSUMER_NAME = "platformEventConsumer";
  public static final String DEFAULT_EVENT_CONSUMER_NAME = "kafkaEventConsumer";

  private String bootstrapServers;

  private ListenerConfiguration listener;

  private SchemaRegistryConfiguration schemaRegistry;

  private ProducerConfiguration producer;

  private ConsumerConfiguration consumer;

  private SerDeConfig serde;

  @Data
  public static class SerDeConfig {
    private SerDeKeyValueConfig usageEvent;
    private SerDeKeyValueConfig event;
  }

  @Data
  @Builder(toBuilder = true)
  @AllArgsConstructor
  @NoArgsConstructor
  public static class SerDeKeyValueConfig {
    private SerDeProperties key;
    private SerDeProperties value;
    @Nullable private Map<String, String> properties;

    protected Map<String, String> getProperties() {
      return getProperties(null);
    }

    public Map<String, String> getProperties(@Nullable SerDeKeyValueConfig schemaRegistryConfig) {
      final HashMap<String, String> props =
          new HashMap<>(properties != null ? properties : Map.of());
      if (schemaRegistryConfig != null) {
        props.putAll(schemaRegistryConfig.getProperties());
      }
      return props;
    }

    public Map<String, String> getProducerProperties(
        @Nullable SerDeKeyValueConfig schemaRegistryConfig) {
      final SerDeKeyValueConfig config =
          schemaRegistryConfig == null ? this : withSchemaRegistryOverride(schemaRegistryConfig);
      return Map.of(
          KEY_SERIALIZER_CLASS_CONFIG, config.getKey().getSerializer(),
          VALUE_SERIALIZER_CLASS_CONFIG, config.getValue().getSerializer());
    }

    public Map<String, String> getConsumerProperties(
        @Nullable SerDeKeyValueConfig schemaRegistryConfig) {
      final SerDeKeyValueConfig config =
          schemaRegistryConfig == null ? this : withSchemaRegistryOverride(schemaRegistryConfig);

      HashMap<String, String> consumerProperties =
          new HashMap<>(
              Map.of(
                  KEY_DESERIALIZER_CLASS_CONFIG, config.getKey().getDeserializer(),
                  VALUE_DESERIALIZER_CLASS_CONFIG, config.getValue().getDeserializer()));

      if (config.getKey().getDelegateDeserializer() == null
          && config.getValue().getDelegateDeserializer() == null) {
        return consumerProperties;
      }

      if (config.getKey().getDelegateDeserializer() != null) {
        consumerProperties.put(
            KEY_DESERIALIZER_DELEGATE_CLASS, config.getKey().getDelegateDeserializer());
      }
      if (config.getValue().getDelegateDeserializer() != null) {
        consumerProperties.put(
            VALUE_DESERIALIZER_DELEGATE_CLASS, config.getValue().getDelegateDeserializer());
      }

      return consumerProperties;
    }

    public SerDeKeyValueConfig withSchemaRegistryOverride(
        @NonNull SerDeKeyValueConfig schemaRegistryConfig) {
      // Handle case where delegation is used, but missing from schemaRegistryConfig
      return schemaRegistryConfig.toBuilder()
          .key(key.withSchemaRegistryOverride(schemaRegistryConfig.getKey()))
          .value(value.withSchemaRegistryOverride(schemaRegistryConfig.getValue()))
          .build();
    }
  }

  @Data
  @Builder(toBuilder = true)
  @AllArgsConstructor
  @NoArgsConstructor
  public static class SerDeProperties {
    private String serializer;
    private String deserializer;
    @Nullable private String delegateDeserializer;

    public SerDeProperties withSchemaRegistryOverride(
        @NonNull SerDeProperties schemaRegistryOverride) {
      if (delegateDeserializer != null
          && schemaRegistryOverride.getDelegateDeserializer() == null) {
        return schemaRegistryOverride.toBuilder()
            .delegateDeserializer(schemaRegistryOverride.getDeserializer())
            .deserializer(deserializer)
            .build();
      }
      return schemaRegistryOverride;
    }
  }
}
