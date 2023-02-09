package com.linkedin.gms.factory.kafka;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.kafka.schemaregistry.AwsGlueSchemaRegistryFactory;
import com.linkedin.gms.factory.kafka.schemaregistry.InternalSchemaRegistryFactory;
import com.linkedin.gms.factory.kafka.schemaregistry.KafkaSchemaRegistryFactory;
import com.linkedin.gms.factory.kafka.schemaregistry.SchemaRegistryConfig;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

@Slf4j
@Configuration
@Import({KafkaSchemaRegistryFactory.class, AwsGlueSchemaRegistryFactory.class, InternalSchemaRegistryFactory.class})
public class KafkaEventConsumerFactory {

  @Bean(name = "kafkaEventConsumerConcurrency")
  protected int kafkaEventConsumerConcurrency(@Qualifier("configurationProvider") ConfigurationProvider provider) {
    return provider.getKafka().getListener().getConcurrency();
  }

  @Bean(name = "kafkaEventConsumer")
  protected KafkaListenerContainerFactory<?> createInstance(@Qualifier("configurationProvider") ConfigurationProvider
      provider, SchemaRegistryConfig schemaRegistryConfig, KafkaProperties properties,
      @Qualifier("kafkaEventConsumerConcurrency") int concurrency) {
    KafkaConfiguration kafkaConfiguration = provider.getKafka();
    KafkaProperties.Consumer consumerProps = properties.getConsumer();

    // Specify (de)serializers for record keys and for record values.
    consumerProps.setKeyDeserializer(StringDeserializer.class);
    // Records will be flushed every 10 seconds.
    consumerProps.setEnableAutoCommit(true);
    consumerProps.setAutoCommitInterval(Duration.ofSeconds(10));

    // KAFKA_BOOTSTRAP_SERVER has precedence over SPRING_KAFKA_BOOTSTRAP_SERVERS
    if (kafkaConfiguration.getBootstrapServers() != null && kafkaConfiguration.getBootstrapServers().length() > 0) {
      consumerProps.setBootstrapServers(Arrays.asList(kafkaConfiguration.getBootstrapServers().split(",")));
    } // else we rely on KafkaProperties which defaults to localhost:9092

    Map<String, Object> props = properties.buildConsumerProperties();

    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, schemaRegistryConfig.getDeserializer());

    // Override KafkaProperties with SchemaRegistryConfig only for non-empty values
    schemaRegistryConfig.getProperties().entrySet()
        .stream()
        .filter(entry -> entry.getValue() != null && !entry.getValue().toString().isEmpty())
        .forEach(entry -> props.put(entry.getKey(), entry.getValue()));

    ConcurrentKafkaListenerContainerFactory<String, GenericRecord> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(defaultKafkaConsumerFactory);
    factory.setContainerCustomizer(new ThreadPoolContainerCustomizer());
    factory.setConcurrency(concurrency);

    log.info(String.format("Event-based KafkaListenerContainerFactory built successfully. Consumers = %s",
            concurrency));

    return factory;
  }
}
