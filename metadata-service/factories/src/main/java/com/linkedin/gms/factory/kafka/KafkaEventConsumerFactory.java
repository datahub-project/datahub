package com.linkedin.gms.factory.kafka;

import com.linkedin.gms.factory.kafka.schemaregistry.AwsGlueSchemaRegistryFactory;
import com.linkedin.gms.factory.kafka.schemaregistry.KafkaSchemaRegistryFactory;
import com.linkedin.gms.factory.kafka.schemaregistry.SchemaRegistryConfig;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.PropertySource;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

@Slf4j
@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
@EnableConfigurationProperties(KafkaProperties.class)
@Import({KafkaSchemaRegistryFactory.class, AwsGlueSchemaRegistryFactory.class})
public class KafkaEventConsumerFactory {

  @Value("${kafka.bootstrapServers}")
  private String kafkaBootstrapServers;

  @Value("${kafka.schemaRegistry.type}")
  private String schemaRegistryType;

  @Value("${kafka.listener.concurrency:1}")
  private Integer kafkaListenerConcurrency;

  @Value("${kafka.retry.attempts:5}")
  private int kafkaRetryAttempts;

  @Value("${kafka.retry.maxInterval:10000}")
  private int kafkaRetryMaxInterval;

  @Value("${kafka.retry.firstInterval:1000}")
  private int kafkaRetryFirstInterval;

  @Value("${kafka.retry.multiplier:2}")
  private int kafkaRetryMultiplier;

  @Autowired
  @Lazy
  @Qualifier("kafkaSchemaRegistry")
  private SchemaRegistryConfig kafkaSchemaRegistryConfig;

  @Autowired
  @Lazy
  @Qualifier("awsGlueSchemaRegistry")
  private SchemaRegistryConfig awsGlueSchemaRegistryConfig;

  @Bean(name = "kafkaEventConsumer")
  protected KafkaListenerContainerFactory<?> createInstance(KafkaProperties properties) {
    return createBaseInstance(properties);
  }

  @Bean(name = "kafkaEventConsumerWithRetry")
  protected KafkaListenerContainerFactory<?> createInstanceWithRetry(KafkaProperties properties) {
    ConcurrentKafkaListenerContainerFactory<String, GenericRecord> factory = createBaseInstance(properties);
    factory.setRetryTemplate(retryTemplate());
    factory.setErrorHandler(((exception, data) -> {
      // TODO: Improve DLQ Handler if needed.
      log.error("Error processing Kafka message. Exception = {}; Record = {}", exception, data);
    }));
    return factory;
  }

  private ConcurrentKafkaListenerContainerFactory<String, GenericRecord> createBaseInstance(KafkaProperties properties) {

    KafkaProperties.Consumer consumerProps = properties.getConsumer();

    // Specify (de)serializers for record keys and for record values.
    consumerProps.setKeyDeserializer(StringDeserializer.class);
    // Records will be flushed every 10 seconds.
    consumerProps.setEnableAutoCommit(true);
    consumerProps.setAutoCommitInterval(Duration.ofSeconds(10));

    // KAFKA_BOOTSTRAP_SERVER has precedence over SPRING_KAFKA_BOOTSTRAP_SERVERS
    if (kafkaBootstrapServers != null && kafkaBootstrapServers.length() > 0) {
      consumerProps.setBootstrapServers(Arrays.asList(kafkaBootstrapServers.split(",")));
    } // else we rely on KafkaProperties which defaults to localhost:9092

    SchemaRegistryConfig schemaRegistryConfig;
    if (schemaRegistryType.equals(KafkaSchemaRegistryFactory.TYPE)) {
      schemaRegistryConfig = kafkaSchemaRegistryConfig;
    } else {
      schemaRegistryConfig = awsGlueSchemaRegistryConfig;
    }

    consumerProps.setValueDeserializer(schemaRegistryConfig.getDeserializer());
    Map<String, Object> props = properties.buildConsumerProperties();

    // Override KafkaProperties with SchemaRegistryConfig only for non-empty values
    schemaRegistryConfig.getProperties().entrySet()
      .stream()
      .filter(entry -> entry.getValue() != null && !entry.getValue().toString().isEmpty())
      .forEach(entry -> props.put(entry.getKey(), entry.getValue())); 

    ConcurrentKafkaListenerContainerFactory<String, GenericRecord> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(props));
    factory.setContainerCustomizer(new ThreadPoolContainerCustomizer());
    factory.setConcurrency(this.kafkaListenerConcurrency);

    log.info("Event-based KafkaListenerContainerFactory built successfully");

    return factory;
  }

  @Bean
  public RetryTemplate retryTemplate() {
    final RetryTemplate retryTemplate = new RetryTemplate();
    retryTemplate.setRetryPolicy(new SimpleRetryPolicy(kafkaRetryAttempts,
            Collections.singletonMap(Exception.class, true)));
    final ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
    backOffPolicy.setInitialInterval(kafkaRetryFirstInterval);
    backOffPolicy.setMaxInterval(kafkaRetryMaxInterval);
    backOffPolicy.setMultiplier(kafkaRetryMultiplier);
    retryTemplate.setBackOffPolicy(backOffPolicy);
    return retryTemplate;
  }
}
