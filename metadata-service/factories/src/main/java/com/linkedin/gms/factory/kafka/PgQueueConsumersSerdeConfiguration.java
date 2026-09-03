package com.linkedin.gms.factory.kafka;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.messaging.PgQueueMessagingTransportCondition;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

/**
 * Avro deserializer for pgQueue consumer poll loops (same Schema Registry wiring as Kafka
 * consumers). Separate from {@link
 * com.linkedin.gms.factory.event.PgQueueExternalEventsConfiguration} so consumers work without
 * {@code eventsApi.enabled}.
 */
@Configuration
@Conditional(PgQueueMessagingTransportCondition.class)
public class PgQueueConsumersSerdeConfiguration {

  @Bean(name = "pgQueueConsumerAvroDeserializer")
  public Deserializer<GenericRecord> pgQueueConsumerAvroDeserializer(
      ConfigurationProvider configurationProvider,
      @Qualifier("schemaRegistryConfig")
          KafkaConfiguration.SerDeKeyValueConfig schemaRegistryConfig) {
    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
    KafkaConfiguration kafkaConfiguration = configurationProvider.getKafka();
    Map<String, Object> props = new HashMap<>();
    props.putAll(
        kafkaConfiguration.getSerde().getEvent().getConsumerProperties(schemaRegistryConfig));
    props.putAll(kafkaConfiguration.getSerde().getEvent().getProperties(schemaRegistryConfig));
    deserializer.configure(props, false);
    return (topic, data) -> (GenericRecord) deserializer.deserialize(topic, data);
  }
}
