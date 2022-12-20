package com.linkedin.gms.factory.kafka;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.kafka.schemaregistry.SchemaRegistryConfig;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.config.KafkaConfiguration;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.PropertySource;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;


@Slf4j
@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
@EnableConfigurationProperties({ConfigurationProvider.class, KafkaProperties.class})
public class KafkaEventConsumerFactory {

  @Value("${kafka.listener.concurrency:1}")
  private Integer kafkaListenerConcurrency;

  // CHANGE THIS
  @Autowired
  @Lazy
  @Qualifier("kafkaSchemaRegistry")
  private SchemaRegistryConfig kafkaSchemaRegistryConfig;

  @Lazy
  @Qualifier("awsGlueSchemaRegistry")
  private SchemaRegistryConfig awsGlueSchemaRegistryConfig;

  @Bean(name = "kafkaEventConsumerConcurrency")
  protected int kafkaEventConsumerConcurrency() {
    return kafkaListenerConcurrency;
  }

  @Bean(name = "kafkaEventConsumer")
  protected KafkaListenerContainerFactory<?> createInstance(KafkaConfiguration kafkaConfiguration,
      KafkaProperties properties, @Qualifier("kafkaEventConsumerConcurrency") int concurrency) {

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

    // TODO: Change if using in memory schema registry
    // Override KafkaProperties with SchemaRegistryConfig only for non-empty values
    schemaRegistryConfig.getProperties().entrySet()
        .stream()
        .filter(entry -> entry.getValue() != null && !entry.getValue().toString().isEmpty())
        .forEach(entry -> props.put(entry.getKey(), entry.getValue()));

    // Override KafkaProperties with SchemaRegistryConfig only for non-empty values
    schemaRegistryConfig.getProperties().entrySet()
      .stream()
      .filter(entry -> entry.getValue() != null && !entry.getValue().toString().isEmpty())
      .forEach(entry -> props.put(entry.getKey(), entry.getValue()));

    ConcurrentKafkaListenerContainerFactory<String, GenericRecord> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(props));
    factory.setContainerCustomizer(new ThreadPoolContainerCustomizer());
    factory.setConcurrency(concurrency);

    log.info(String.format("Event-based KafkaListenerContainerFactory built successfully. Consumers = %s",
            concurrency));

    return factory;
  }
}
