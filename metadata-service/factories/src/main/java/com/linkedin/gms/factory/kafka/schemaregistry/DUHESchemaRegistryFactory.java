package com.linkedin.gms.factory.kafka.schemaregistry;

import static com.linkedin.metadata.boot.kafka.DataHubUpgradeKafkaListener.TOPIC_NAME;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.boot.kafka.MockDUHEDeserializer;
import com.linkedin.metadata.boot.kafka.MockDUHESerializer;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class DUHESchemaRegistryFactory {

  public static final String DUHE_SCHEMA_REGISTRY_TOPIC_KEY = "duheTopicName";

  @Value(TOPIC_NAME)
  private String duheTopicName;

  /** Configure Kafka Producer/Consumer processes with a custom schema registry. */
  @Bean("duheSchemaRegistryConfig")
  protected SchemaRegistryConfig duheSchemaRegistryConfig(ConfigurationProvider provider) {
    Map<String, Object> props = new HashMap<>();
    KafkaConfiguration kafkaConfiguration = provider.getKafka();

    props.put(
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
        kafkaConfiguration.getSchemaRegistry().getUrl());
    props.put(DUHE_SCHEMA_REGISTRY_TOPIC_KEY, duheTopicName);

    log.info("DataHub System Update Registry");
    return new SchemaRegistryConfig(MockDUHESerializer.class, MockDUHEDeserializer.class, props);
  }
}
