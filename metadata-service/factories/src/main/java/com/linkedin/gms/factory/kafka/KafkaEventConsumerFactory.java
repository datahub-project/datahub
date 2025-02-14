package com.linkedin.gms.factory.kafka;

import static com.linkedin.metadata.config.kafka.KafkaConfiguration.DEFAULT_EVENT_CONSUMER_NAME;
import static com.linkedin.metadata.config.kafka.KafkaConfiguration.MCL_EVENT_CONSUMER_NAME;
import static com.linkedin.metadata.config.kafka.KafkaConfiguration.MCP_EVENT_CONSUMER_NAME;
import static com.linkedin.metadata.config.kafka.KafkaConfiguration.PE_EVENT_CONSUMER_NAME;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.kafka.ConsumerConfiguration;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.CommonContainerStoppingErrorHandler;
import org.springframework.kafka.listener.CommonDelegatingErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;

@Slf4j
@Configuration
public class KafkaEventConsumerFactory {
  private int kafkaEventConsumerConcurrency;

  @Bean(name = "kafkaConsumerFactory")
  protected DefaultKafkaConsumerFactory<String, GenericRecord> createConsumerFactory(
      @Qualifier("configurationProvider") ConfigurationProvider provider,
      KafkaProperties baseKafkaProperties,
      @Qualifier("schemaRegistryConfig")
          KafkaConfiguration.SerDeKeyValueConfig schemaRegistryConfig) {
    kafkaEventConsumerConcurrency = provider.getKafka().getListener().getConcurrency();

    KafkaConfiguration kafkaConfiguration = provider.getKafka();
    Map<String, Object> customizedProperties =
        buildCustomizedProperties(baseKafkaProperties, kafkaConfiguration, schemaRegistryConfig);

    return new DefaultKafkaConsumerFactory<>(customizedProperties);
  }

  @Bean(name = "duheKafkaConsumerFactory")
  protected DefaultKafkaConsumerFactory<String, GenericRecord> duheKafkaConsumerFactory(
      @Qualifier("configurationProvider") ConfigurationProvider provider,
      KafkaProperties baseKafkaProperties,
      @Qualifier("duheSchemaRegistryConfig")
          KafkaConfiguration.SerDeKeyValueConfig schemaRegistryConfig) {

    KafkaConfiguration kafkaConfiguration = provider.getKafka();
    Map<String, Object> customizedProperties =
        buildCustomizedProperties(baseKafkaProperties, kafkaConfiguration, schemaRegistryConfig);

    return new DefaultKafkaConsumerFactory<>(customizedProperties);
  }

  private static Map<String, Object> buildCustomizedProperties(
      KafkaProperties baseKafkaProperties,
      KafkaConfiguration kafkaConfiguration,
      KafkaConfiguration.SerDeKeyValueConfig schemaRegistryConfig) {
    KafkaProperties.Consumer consumerProps = baseKafkaProperties.getConsumer();

    // Records will be flushed every 10 seconds.
    consumerProps.setEnableAutoCommit(true);
    consumerProps.setAutoCommitInterval(Duration.ofSeconds(10));

    // KAFKA_BOOTSTRAP_SERVER has precedence over SPRING_KAFKA_BOOTSTRAP_SERVERS
    if (kafkaConfiguration.getBootstrapServers() != null
        && kafkaConfiguration.getBootstrapServers().length() > 0) {
      consumerProps.setBootstrapServers(
          Arrays.asList(kafkaConfiguration.getBootstrapServers().split(",")));
    } // else we rely on KafkaProperties which defaults to localhost:9092

    Map<String, Object> customizedProperties = baseKafkaProperties.buildConsumerProperties(null);
    customizedProperties.putAll(
        kafkaConfiguration.getSerde().getEvent().getConsumerProperties(schemaRegistryConfig));

    // Override KafkaProperties with SchemaRegistryConfig only for non-empty values
    customizedProperties.putAll(
        kafkaConfiguration.getSerde().getEvent().getProperties(schemaRegistryConfig));

    customizedProperties.put(
        ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,
        kafkaConfiguration.getConsumer().getMaxPartitionFetchBytes());

    return customizedProperties;
  }

  @Bean(name = PE_EVENT_CONSUMER_NAME)
  protected KafkaListenerContainerFactory<?> platformEventConsumer(
      @Qualifier("kafkaConsumerFactory")
          DefaultKafkaConsumerFactory<String, GenericRecord> kafkaConsumerFactory,
      @Qualifier("configurationProvider") ConfigurationProvider configurationProvider) {

    return buildDefaultKafkaListenerContainerFactory(
        PE_EVENT_CONSUMER_NAME,
        kafkaConsumerFactory,
        configurationProvider.getKafka().getConsumer().isStopOnDeserializationError(),
        configurationProvider.getKafka().getConsumer().getPe());
  }

  @Bean(name = MCP_EVENT_CONSUMER_NAME)
  protected KafkaListenerContainerFactory<?> mcpEventConsumer(
      @Qualifier("kafkaConsumerFactory")
          DefaultKafkaConsumerFactory<String, GenericRecord> kafkaConsumerFactory,
      @Qualifier("configurationProvider") ConfigurationProvider configurationProvider) {

    return buildDefaultKafkaListenerContainerFactory(
        MCP_EVENT_CONSUMER_NAME,
        kafkaConsumerFactory,
        configurationProvider.getKafka().getConsumer().isStopOnDeserializationError(),
        configurationProvider.getKafka().getConsumer().getMcp());
  }

  @Bean(name = MCL_EVENT_CONSUMER_NAME)
  protected KafkaListenerContainerFactory<?> mclEventConsumer(
      @Qualifier("kafkaConsumerFactory")
          DefaultKafkaConsumerFactory<String, GenericRecord> kafkaConsumerFactory,
      @Qualifier("configurationProvider") ConfigurationProvider configurationProvider) {

    return buildDefaultKafkaListenerContainerFactory(
        MCL_EVENT_CONSUMER_NAME,
        kafkaConsumerFactory,
        configurationProvider.getKafka().getConsumer().isStopOnDeserializationError(),
        configurationProvider.getKafka().getConsumer().getMcl());
  }

  @Bean(name = DEFAULT_EVENT_CONSUMER_NAME)
  protected KafkaListenerContainerFactory<?> kafkaEventConsumer(
      @Qualifier("kafkaConsumerFactory")
          DefaultKafkaConsumerFactory<String, GenericRecord> kafkaConsumerFactory,
      @Qualifier("configurationProvider") ConfigurationProvider configurationProvider) {

    return buildDefaultKafkaListenerContainerFactory(
        DEFAULT_EVENT_CONSUMER_NAME,
        kafkaConsumerFactory,
        configurationProvider.getKafka().getConsumer().isStopOnDeserializationError(),
        null);
  }

  private KafkaListenerContainerFactory<?> buildDefaultKafkaListenerContainerFactory(
      String consumerFactoryName,
      DefaultKafkaConsumerFactory<String, GenericRecord> kafkaConsumerFactory,
      boolean isStopOnDeserializationError,
      @Nullable ConsumerConfiguration.ConsumerOptions consumerOptions) {

    final DefaultKafkaConsumerFactory<String, GenericRecord> factoryWithOverrides;
    if (consumerOptions != null) {
      // Copy the base config
      Map<String, Object> props = new HashMap<>(kafkaConsumerFactory.getConfigurationProperties());
      // Override just the auto.offset.reset
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, consumerOptions.getAutoOffsetReset());
      factoryWithOverrides =
          new DefaultKafkaConsumerFactory<>(
              props,
              kafkaConsumerFactory.getKeyDeserializer(),
              kafkaConsumerFactory.getValueDeserializer());
    } else {
      factoryWithOverrides = kafkaConsumerFactory;
    }

    ConcurrentKafkaListenerContainerFactory<String, GenericRecord> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(factoryWithOverrides);
    factory.setContainerCustomizer(new ThreadPoolContainerCustomizer());
    factory.setConcurrency(kafkaEventConsumerConcurrency);
    factory.setAutoStartup(false);

    /* Sets up a delegating error handler for Deserialization errors, if disabled will
     use DefaultErrorHandler (does back-off retry and then logs) rather than stopping the container. Stopping the container
     prevents lost messages until the error can be examined, disabling this will allow progress, but may lose data
    */
    if (isStopOnDeserializationError) {
      CommonDelegatingErrorHandler delegatingErrorHandler =
          new CommonDelegatingErrorHandler(new DefaultErrorHandler());
      delegatingErrorHandler.addDelegate(
          DeserializationException.class, new CommonContainerStoppingErrorHandler());
      factory.setCommonErrorHandler(delegatingErrorHandler);
    }
    log.info(
        "Event-based {} KafkaListenerContainerFactory built successfully. Consumer concurrency = {}",
        consumerFactoryName,
        kafkaEventConsumerConcurrency);

    return factory;
  }

  @Bean(name = "duheKafkaEventConsumer")
  protected KafkaListenerContainerFactory<?> duheKafkaEventConsumer(
      @Qualifier("duheKafkaConsumerFactory")
          DefaultKafkaConsumerFactory<String, GenericRecord> kafkaConsumerFactory) {

    ConcurrentKafkaListenerContainerFactory<String, GenericRecord> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(kafkaConsumerFactory);
    factory.setContainerCustomizer(new ThreadPoolContainerCustomizer());
    factory.setConcurrency(1);
    factory.setAutoStartup(false);

    log.info(
        "Event-based DUHE KafkaListenerContainerFactory built successfully. Consumer concurrency = 1");
    return factory;
  }
}
