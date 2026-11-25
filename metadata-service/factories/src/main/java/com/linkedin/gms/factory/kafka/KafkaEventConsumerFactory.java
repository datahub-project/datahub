package com.linkedin.gms.factory.kafka;

import static com.linkedin.metadata.config.kafka.KafkaConfiguration.DEFAULT_EVENT_CONSUMER_NAME;
import static com.linkedin.metadata.config.kafka.KafkaConfiguration.MCL_BATCH_EVENT_CONSUMER_NAME;
import static com.linkedin.metadata.config.kafka.KafkaConfiguration.MCL_EVENT_CONSUMER_NAME;
import static com.linkedin.metadata.config.kafka.KafkaConfiguration.MCP_BATCH_EVENT_CONSUMER_NAME;
import static com.linkedin.metadata.config.kafka.KafkaConfiguration.MCP_EVENT_CONSUMER_NAME;
import static com.linkedin.metadata.config.kafka.KafkaConfiguration.PE_EVENT_CONSUMER_NAME;

import com.linkedin.gms.factory.aws.AwsClientFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.kafka.ConsumerConfiguration;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.SearchClusterSettings;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.CommonContainerStoppingErrorHandler;
import org.springframework.kafka.listener.CommonDelegatingErrorHandler;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;

@Slf4j
@Configuration
@Import(AwsClientFactory.class)
@DependsOn("defaultAwsCredentialsProvider")
public class KafkaEventConsumerFactory {
  private static final long MCL_ACK_RETRY_INITIAL_INTERVAL_MS = 1000L;
  private static final double MCL_ACK_RETRY_MULTIPLIER = 2.0;
  private static final long MCL_ACK_RETRY_MAX_INTERVAL_MS = 30_000L;
  private static final long MCL_BOUNDED_RETRY_INITIAL_INTERVAL_MS = 1000L;
  private static final double MCL_BOUNDED_RETRY_MULTIPLIER = 2.0;
  private static final long MCL_BOUNDED_RETRY_MAX_INTERVAL_MS = 10_000L;
  private static final long MCL_BOUNDED_RETRY_MAX_ATTEMPTS = 5L;

  private int kafkaEventConsumerConcurrency;
  private int authExceptionRetryIntervalSeconds;

  @Bean(name = "kafkaConsumerFactory")
  protected DefaultKafkaConsumerFactory<String, GenericRecord> createConsumerFactory(
      @Qualifier("configurationProvider") ConfigurationProvider provider,
      KafkaProperties baseKafkaProperties,
      @Qualifier("schemaRegistryConfig")
          KafkaConfiguration.SerDeKeyValueConfig schemaRegistryConfig) {
    kafkaEventConsumerConcurrency = provider.getKafka().getListener().getConcurrency();
    authExceptionRetryIntervalSeconds =
        provider.getKafka().getConsumer().getAuthExceptionRetryIntervalSeconds();

    KafkaConfiguration kafkaConfiguration = provider.getKafka();
    Map<String, Object> customizedProperties =
        buildCustomizedProperties(
            baseKafkaProperties, kafkaConfiguration, schemaRegistryConfig, true);

    return new DefaultKafkaConsumerFactory<>(customizedProperties);
  }

  @Bean(name = "kafkaConsumerPoolConsumerFactory")
  protected DefaultKafkaConsumerFactory<String, GenericRecord> createConsumerPoolConsumerFactory(
      @Qualifier("configurationProvider") ConfigurationProvider provider,
      KafkaProperties baseKafkaProperties,
      @Qualifier("schemaRegistryConfig")
          KafkaConfiguration.SerDeKeyValueConfig schemaRegistryConfig) {
    kafkaEventConsumerConcurrency = provider.getKafka().getListener().getConcurrency();
    authExceptionRetryIntervalSeconds =
        provider.getKafka().getConsumer().getAuthExceptionRetryIntervalSeconds();

    KafkaConfiguration kafkaConfiguration = provider.getKafka();
    Map<String, Object> customizedProperties =
        buildCustomizedProperties(
            baseKafkaProperties, kafkaConfiguration, schemaRegistryConfig, false);

    return new DefaultKafkaConsumerFactory<>(customizedProperties);
  }

  private static Map<String, Object> buildCustomizedProperties(
      KafkaProperties baseKafkaProperties,
      KafkaConfiguration kafkaConfiguration,
      KafkaConfiguration.SerDeKeyValueConfig schemaRegistryConfig,
      boolean enableAutoCommit) {
    return buildCustomizedProperties(
        baseKafkaProperties, kafkaConfiguration, schemaRegistryConfig, enableAutoCommit, false);
  }

  /**
   * @param shareBootstrap Indicates that Bootstrap Server should be shared between producer and
   *     consumer, this is done on the Consumer Factory since we only really want to use this split
   *     for migrating and the producer side is the one we'd be migrating to. Only should share for
   *     topics like Upgrade History which do not need historical data to drain.
   */
  @com.google.common.annotations.VisibleForTesting
  static Map<String, Object> buildCustomizedProperties(
      KafkaProperties baseKafkaProperties,
      KafkaConfiguration kafkaConfiguration,
      KafkaConfiguration.SerDeKeyValueConfig schemaRegistryConfig,
      boolean enableAutoCommit,
      boolean shareBootstrap) {
    KafkaProperties.Consumer consumerProps = baseKafkaProperties.getConsumer();

    if (enableAutoCommit) {
      // Records will be flushed every 10 seconds.
      consumerProps.setEnableAutoCommit(true);
      consumerProps.setAutoCommitInterval(Duration.ofSeconds(10));
    } else {
      consumerProps.setEnableAutoCommit(false);
    }

    String bootstrapOverride =
        shareBootstrap
            ? kafkaConfiguration.getProducer().getBootstrapServers()
            : kafkaConfiguration.getConsumer().getBootstrapServers();
    String bootstrapServers =
        StringUtils.isNotBlank(bootstrapOverride)
            ? bootstrapOverride
            : kafkaConfiguration.getBootstrapServers();
    // KAFKA_BOOTSTRAP_SERVER has precedence over SPRING_KAFKA_BOOTSTRAP_SERVERS
    if (StringUtils.isNotBlank(bootstrapServers)) {
      consumerProps.setBootstrapServers(Arrays.asList(bootstrapServers.split(",")));
    } // else we rely on KafkaProperties which defaults to localhost:9092

    String securityProtocolOverride =
        shareBootstrap
            ? kafkaConfiguration.getProducer().getSecurityProtocol()
            : kafkaConfiguration.getConsumer().getSecurityProtocol();
    String securityProtocol =
        StringUtils.isNotBlank(securityProtocolOverride) ? securityProtocolOverride : null;
    if (StringUtils.isNotBlank(securityProtocol)) {
      consumerProps.getSecurity().setProtocol(securityProtocol);
    }

    Map<String, Object> customizedProperties = baseKafkaProperties.buildConsumerProperties();
    customizedProperties.putAll(
        kafkaConfiguration.getSerde().getEvent().getConsumerProperties(schemaRegistryConfig));

    // Override KafkaProperties with SchemaRegistryConfig only for non-empty values
    customizedProperties.putAll(
        kafkaConfiguration.getSerde().getEvent().getProperties(schemaRegistryConfig));

    String schemaRegistryUrlOverride =
        shareBootstrap
            ? kafkaConfiguration.getProducer().getSchemaRegistryUrl()
            : kafkaConfiguration.getConsumer().getSchemaRegistryUrl();
    if (StringUtils.isNotBlank(schemaRegistryUrlOverride)) {
      customizedProperties.put(
          AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrlOverride);
    }

    customizedProperties.put(
        ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,
        kafkaConfiguration.getConsumer().getMaxPartitionFetchBytes());

    KafkaMskIamAuth.configure(customizedProperties);
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

  @Bean(name = MCP_BATCH_EVENT_CONSUMER_NAME)
  protected KafkaListenerContainerFactory<?> mcpBatchEventConsumer(
      @Qualifier("kafkaConsumerFactory")
          DefaultKafkaConsumerFactory<String, GenericRecord> kafkaConsumerFactory,
      @Qualifier("configurationProvider") ConfigurationProvider configurationProvider) {

    return buildDefaultKafkaListenerContainerFactory(
        MCP_BATCH_EVENT_CONSUMER_NAME,
        kafkaConsumerFactory,
        configurationProvider.getKafka().getConsumer().isStopOnDeserializationError(),
        configurationProvider.getKafka().getConsumer().getMcp(),
        true);
  }

  @Bean(name = MCL_EVENT_CONSUMER_NAME)
  protected KafkaListenerContainerFactory<?> mclEventConsumer(
      @Qualifier("kafkaConsumerFactory")
          DefaultKafkaConsumerFactory<String, GenericRecord> kafkaConsumerFactory,
      @Qualifier("configurationProvider") ConfigurationProvider configurationProvider) {

    return buildMclKafkaListenerContainerFactory(
        MCL_EVENT_CONSUMER_NAME, kafkaConsumerFactory, configurationProvider, false);
  }

  @Bean(name = MCL_BATCH_EVENT_CONSUMER_NAME)
  protected KafkaListenerContainerFactory<?> mclBatchEventConsumer(
      @Qualifier("kafkaConsumerFactory")
          DefaultKafkaConsumerFactory<String, GenericRecord> kafkaConsumerFactory,
      @Qualifier("configurationProvider") ConfigurationProvider configurationProvider) {

    return buildMclKafkaListenerContainerFactory(
        MCL_BATCH_EVENT_CONSUMER_NAME, kafkaConsumerFactory, configurationProvider, true);
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

  private KafkaListenerContainerFactory<?> buildMclKafkaListenerContainerFactory(
      String consumerFactoryName,
      DefaultKafkaConsumerFactory<String, GenericRecord> kafkaConsumerFactory,
      ConfigurationProvider configurationProvider,
      boolean batchListener) {
    boolean ackAfterTransfer = isAckAfterTransferEnabled(configurationProvider);
    boolean stopOnDeserializationError =
        configurationProvider.getKafka().getConsumer().isStopOnDeserializationError();
    return buildDefaultKafkaListenerContainerFactory(
        consumerFactoryName,
        kafkaConsumerFactory,
        stopOnDeserializationError,
        configurationProvider.getKafka().getConsumer().getMcl(),
        batchListener,
        ackAfterTransfer,
        buildMclErrorHandler(stopOnDeserializationError, ackAfterTransfer));
  }

  static boolean isAckAfterTransferEnabled(@Nullable ConfigurationProvider configurationProvider) {
    if (configurationProvider == null) {
      return false;
    }
    ElasticSearchConfiguration esConfig = configurationProvider.getElasticSearch();
    if (esConfig == null) {
      return false;
    }
    if (esConfig.getBulkProcessor() != null && esConfig.getBulkProcessor().isAckAfterTransfer()) {
      return true;
    }
    Map<String, SearchClusterSettings> clusters = esConfig.getClusters();
    if (clusters == null) {
      return false;
    }
    for (SearchClusterSettings cluster : clusters.values()) {
      if (cluster != null && isClusterAckAfterTransferEnabled(cluster.getBulkProcessor())) {
        return true;
      }
    }
    return false;
  }

  private static boolean isClusterAckAfterTransferEnabled(
      @Nullable Map<String, Object> bulkProcessor) {
    if (bulkProcessor == null) {
      return false;
    }
    for (String key :
        new String[] {"ackAfterTransfer", "ack-after-transfer", "ack_after_transfer"}) {
      Object value = bulkProcessor.get(key);
      if (value instanceof Boolean && (Boolean) value) {
        return true;
      }
      if (value instanceof String && Boolean.parseBoolean((String) value)) {
        return true;
      }
    }
    return false;
  }

  static BackOff createMclBackOff(boolean ackAfterTransfer) {
    // When ES_BULK_ACK_AFTER_TRANSFER is enabled, keep retrying with exponential backoff until ES
    // recovers rather than letting Spring's default 9-attempt FixedBackOff(0, 9) drop the offset.
    long initialInterval =
        ackAfterTransfer
            ? MCL_ACK_RETRY_INITIAL_INTERVAL_MS
            : MCL_BOUNDED_RETRY_INITIAL_INTERVAL_MS;
    double multiplier = ackAfterTransfer ? MCL_ACK_RETRY_MULTIPLIER : MCL_BOUNDED_RETRY_MULTIPLIER;
    long maxInterval =
        ackAfterTransfer ? MCL_ACK_RETRY_MAX_INTERVAL_MS : MCL_BOUNDED_RETRY_MAX_INTERVAL_MS;
    long maxAttempts = ackAfterTransfer ? Long.MAX_VALUE : MCL_BOUNDED_RETRY_MAX_ATTEMPTS;

    ExponentialBackOff backOff = new ExponentialBackOff(initialInterval, multiplier);
    backOff.setMaxInterval(maxInterval);
    backOff.setMaxAttempts(maxAttempts);
    return backOff;
  }

  static CommonErrorHandler buildMclErrorHandler(
      boolean isStopOnDeserializationError, boolean ackAfterTransfer) {
    return buildMclErrorHandler(isStopOnDeserializationError, createMclBackOff(ackAfterTransfer));
  }

  static CommonErrorHandler buildMclErrorHandler(
      boolean isStopOnDeserializationError, BackOff backOff) {
    DefaultErrorHandler retryHandler = new DefaultErrorHandler(backOff);
    if (!isStopOnDeserializationError) {
      return retryHandler;
    }
    CommonDelegatingErrorHandler delegatingErrorHandler =
        new CommonDelegatingErrorHandler(retryHandler);
    delegatingErrorHandler.addDelegate(
        DeserializationException.class, new CommonContainerStoppingErrorHandler());
    return delegatingErrorHandler;
  }

  private KafkaListenerContainerFactory<?> buildDefaultKafkaListenerContainerFactory(
      String consumerFactoryName,
      DefaultKafkaConsumerFactory<String, GenericRecord> kafkaConsumerFactory,
      boolean isStopOnDeserializationError,
      @Nullable ConsumerConfiguration.ConsumerOptions consumerOptions) {
    return buildDefaultKafkaListenerContainerFactory(
        consumerFactoryName,
        kafkaConsumerFactory,
        isStopOnDeserializationError,
        consumerOptions,
        false);
  }

  private KafkaListenerContainerFactory<?> buildDefaultKafkaListenerContainerFactory(
      String consumerFactoryName,
      DefaultKafkaConsumerFactory<String, GenericRecord> kafkaConsumerFactory,
      boolean isStopOnDeserializationError,
      @Nullable ConsumerConfiguration.ConsumerOptions consumerOptions,
      boolean batchListener) {
    return buildDefaultKafkaListenerContainerFactory(
        consumerFactoryName,
        kafkaConsumerFactory,
        isStopOnDeserializationError,
        consumerOptions,
        batchListener,
        false,
        null);
  }

  private KafkaListenerContainerFactory<?> buildDefaultKafkaListenerContainerFactory(
      String consumerFactoryName,
      DefaultKafkaConsumerFactory<String, GenericRecord> kafkaConsumerFactory,
      boolean isStopOnDeserializationError,
      @Nullable ConsumerConfiguration.ConsumerOptions consumerOptions,
      boolean batchListener,
      boolean disableAutoCommit,
      @Nullable CommonErrorHandler errorHandlerOverride) {

    final DefaultKafkaConsumerFactory<String, GenericRecord> factoryWithOverrides;
    if (consumerOptions != null || disableAutoCommit) {
      Map<String, Object> props = new HashMap<>(kafkaConsumerFactory.getConfigurationProperties());
      if (consumerOptions != null) {
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, consumerOptions.getAutoOffsetReset());
      }
      if (disableAutoCommit) {
        // Auto-commit advances offsets inside consumer.poll(Duration.ZERO) during
        // DefaultErrorHandler
        // backoff retries; disabling it leaves offset commits strictly under AckMode.BATCH control.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
      }
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
    if (batchListener) {
      factory.setBatchListener(true);
    }

    // Allow consumer containers to survive transient authentication failures (e.g.,
    // MSK IAM credential rotation with EKS Pod Identity) instead of stopping fatally.
    // The Kafka client's NetworkClient automatically reconnects with fresh credentials
    // on the next poll — the same self-healing that the producer side already has.
    if (authExceptionRetryIntervalSeconds > 0) {
      factory
          .getContainerProperties()
          .setAuthExceptionRetryInterval(Duration.ofSeconds(authExceptionRetryIntervalSeconds));
    }

    if (errorHandlerOverride != null) {
      factory.setCommonErrorHandler(errorHandlerOverride);
    } else if (isStopOnDeserializationError) {
      CommonDelegatingErrorHandler delegatingErrorHandler =
          new CommonDelegatingErrorHandler(new DefaultErrorHandler());
      delegatingErrorHandler.addDelegate(
          DeserializationException.class, new CommonContainerStoppingErrorHandler());
      factory.setCommonErrorHandler(delegatingErrorHandler);
    }
    log.info(
        "Event-based {}{} KafkaListenerContainerFactory built successfully. Consumer concurrency = {}, disableAutoCommit = {}",
        consumerFactoryName,
        batchListener ? " (batch)" : "",
        kafkaEventConsumerConcurrency,
        disableAutoCommit);

    return factory;
  }
}
