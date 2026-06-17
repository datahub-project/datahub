package com.linkedin.gms.factory.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.messaging.KafkaMessagingDisabled;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.queue.MetadataQueueStore;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.datahubproject.event.ExternalEventsPollHandler;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@KafkaMessagingDisabled
@ConditionalOnProperty(name = "eventsApi.enabled", havingValue = "true")
public class PgQueueExternalEventsConfiguration {

  @Bean(name = "pgQueueExternalEventsAvroDeserializer")
  public Deserializer<GenericRecord> pgQueueExternalEventsAvroDeserializer(
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

  @Bean
  public ExternalEventsPollHandler pgQueueExternalEventsPollHandler(
      MetadataQueueStore metadataQueueStore,
      PostgresSqlSetupProperties postgresSqlSetupProperties,
      @Qualifier("pgQueueExternalEventsAvroDeserializer")
          Deserializer<GenericRecord> pgQueueExternalEventsAvroDeserializer,
      ObjectMapper objectMapper,
      ConfigurationProvider configurationProvider) {
    return new PgQueueExternalEventsPollHandler(
        metadataQueueStore,
        postgresSqlSetupProperties,
        pgQueueExternalEventsAvroDeserializer,
        objectMapper,
        configurationProvider.getKafka());
  }
}
