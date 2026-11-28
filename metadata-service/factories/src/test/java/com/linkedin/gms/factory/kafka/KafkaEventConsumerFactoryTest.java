package com.linkedin.gms.factory.kafka;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.kafka.ConsumerConfiguration;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.kafka.ListenerConfiguration;
import com.linkedin.metadata.config.search.BulkProcessorConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.SearchClusterSettings;
import com.linkedin.metadata.search.elasticsearch.update.BulkTransferException;
import com.linkedin.metadata.service.RetryableIndexingException;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.util.backoff.BackOffExecution;
import org.springframework.util.backoff.ExponentialBackOff;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class KafkaEventConsumerFactoryTest {

  private KafkaEventConsumerFactory factory;
  private ConfigurationProvider configProvider;
  private DefaultKafkaConsumerFactory<String, GenericRecord> kafkaConsumerFactory;

  @BeforeMethod
  public void setup() {
    factory = new KafkaEventConsumerFactory();
    configProvider = mock(ConfigurationProvider.class);

    KafkaConfiguration kafkaConfig = new KafkaConfiguration();
    kafkaConfig.setBootstrapServers("localhost:9092");

    ConsumerConfiguration consumerConfig = new ConsumerConfiguration();
    consumerConfig.setAuthExceptionRetryIntervalSeconds(10);
    consumerConfig.setMaxAuthExceptionRetries(3);
    ConsumerConfiguration.ConsumerOptions peOptions = new ConsumerConfiguration.ConsumerOptions();
    peOptions.setAutoOffsetReset("earliest");
    consumerConfig.setPe(peOptions);
    ConsumerConfiguration.ConsumerOptions mcpOptions = new ConsumerConfiguration.ConsumerOptions();
    mcpOptions.setAutoOffsetReset("earliest");
    consumerConfig.setMcp(mcpOptions);
    ConsumerConfiguration.ConsumerOptions mclOptions = new ConsumerConfiguration.ConsumerOptions();
    mclOptions.setAutoOffsetReset("earliest");
    consumerConfig.setMcl(mclOptions);
    kafkaConfig.setConsumer(consumerConfig);

    ListenerConfiguration listenerConfig = new ListenerConfiguration();
    listenerConfig.setConcurrency(1);
    kafkaConfig.setListener(listenerConfig);

    when(configProvider.getKafka()).thenReturn(kafkaConfig);

    @SuppressWarnings("unchecked")
    Deserializer<GenericRecord> valueDeserializer = mock(Deserializer.class);
    kafkaConsumerFactory =
        new DefaultKafkaConsumerFactory<>(
            Map.of("bootstrap.servers", "localhost:9092"),
            new org.apache.kafka.common.serialization.StringDeserializer(),
            valueDeserializer);

    // Initialize the config-driven instance fields that are normally set by createConsumerFactory.
    // We skip createConsumerFactory because it requires full serde setup.
    try {
      Field retryField =
          KafkaEventConsumerFactory.class.getDeclaredField("authExceptionRetryIntervalSeconds");
      retryField.setAccessible(true);
      retryField.setInt(factory, consumerConfig.getAuthExceptionRetryIntervalSeconds());

      Field concurrencyField =
          KafkaEventConsumerFactory.class.getDeclaredField("kafkaEventConsumerConcurrency");
      concurrencyField.setAccessible(true);
      concurrencyField.setInt(factory, listenerConfig.getConcurrency());
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void testAuthExceptionRetryIntervalOnEventConsumers() {
    // The DEFAULT bean exercises buildDefaultKafkaListenerContainerFactory, which is shared
    // by all event consumers (PE, MCP, MCL, DEFAULT).
    var listenerFactory =
        (ConcurrentKafkaListenerContainerFactory<?, ?>)
            factory.kafkaEventConsumer(kafkaConsumerFactory, configProvider);

    assertEquals(
        listenerFactory.getContainerProperties().getAuthExceptionRetryInterval(),
        Duration.ofSeconds(10),
        "Event consumers should retry on auth exceptions to survive MSK IAM credential rotation");
  }

  @Test
  void testMclBatchEventConsumerEnablesBatchListener() {
    var listenerFactory =
        (ConcurrentKafkaListenerContainerFactory<?, ?>)
            factory.mclBatchEventConsumer(kafkaConsumerFactory, configProvider);

    assertEquals(
        listenerFactory.isBatchListener(),
        Boolean.TRUE,
        "MCL batch event consumer must have batch listener enabled");
    assertEquals(
        listenerFactory.getContainerProperties().getAuthExceptionRetryInterval(),
        Duration.ofSeconds(10));
  }

  @Test
  void testMclEventConsumerDoesNotEnableBatchListener() {
    var listenerFactory =
        (ConcurrentKafkaListenerContainerFactory<?, ?>)
            factory.mclEventConsumer(kafkaConsumerFactory, configProvider);

    // When batchListener is not set, isBatchListener() returns null (not false)
    assertTrue(
        listenerFactory.isBatchListener() == null || !listenerFactory.isBatchListener(),
        "Standard MCL event consumer must not have batch listener enabled");
    assertNull(
        listenerFactory
            .getConsumerFactory()
            .getConfigurationProperties()
            .get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
  }

  @Test
  void testMclAckAfterTransferDisablesAutoCommitAndConfiguresErrorHandler() {
    BulkProcessorConfiguration bulkProcessor = new BulkProcessorConfiguration();
    bulkProcessor.setAckAfterTransfer(true);
    ElasticSearchConfiguration esConfig = new ElasticSearchConfiguration();
    esConfig.setBulkProcessor(bulkProcessor);
    when(configProvider.getElasticSearch()).thenReturn(esConfig);
    configProvider.getKafka().getConsumer().setStopOnDeserializationError(false);

    var singleFactory =
        (ConcurrentKafkaListenerContainerFactory<?, ?>)
            factory.mclEventConsumer(kafkaConsumerFactory, configProvider);
    var batchFactory =
        (ConcurrentKafkaListenerContainerFactory<?, ?>)
            factory.mclBatchEventConsumer(kafkaConsumerFactory, configProvider);
    var defaultFactory =
        (ConcurrentKafkaListenerContainerFactory<?, ?>)
            factory.kafkaEventConsumer(kafkaConsumerFactory, configProvider);

    assertEquals(
        singleFactory
            .getConsumerFactory()
            .getConfigurationProperties()
            .get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG),
        Boolean.FALSE);
    assertEquals(
        batchFactory
            .getConsumerFactory()
            .getConfigurationProperties()
            .get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG),
        Boolean.FALSE);
    assertNull(
        defaultFactory
            .getConsumerFactory()
            .getConfigurationProperties()
            .get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));

    assertNotNull(singleFactory.createContainer("MetadataChangeLog_v1").getCommonErrorHandler());
    assertNotNull(batchFactory.createContainer("MetadataChangeLog_v1").getCommonErrorHandler());
  }

  @Test
  void testMclClusterAckAfterTransferOverlayEnablesOffsetHold() {
    BulkProcessorConfiguration globalBulk = new BulkProcessorConfiguration();
    globalBulk.setAckAfterTransfer(false);

    SearchClusterSettings secondary = new SearchClusterSettings();
    secondary.setBulkProcessor(Map.of("ackAfterTransfer", true));

    ElasticSearchConfiguration esConfig = new ElasticSearchConfiguration();
    esConfig.setBulkProcessor(globalBulk);
    esConfig.setClusters(Map.of("secondary", secondary));
    when(configProvider.getElasticSearch()).thenReturn(esConfig);

    var batchFactory =
        (ConcurrentKafkaListenerContainerFactory<?, ?>)
            factory.mclBatchEventConsumer(kafkaConsumerFactory, configProvider);

    assertTrue(KafkaEventConsumerFactory.isAckAfterTransferEnabled(configProvider));
    assertEquals(
        batchFactory
            .getConsumerFactory()
            .getConfigurationProperties()
            .get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG),
        Boolean.FALSE);
  }

  @Test
  void testMclBackOffMatchesAckAfterTransferSetting() {
    ExponentialBackOff unbounded =
        (ExponentialBackOff) KafkaEventConsumerFactory.createMclBackOff(true);
    assertEquals(unbounded.getMaxAttempts(), Long.MAX_VALUE);
    BackOffExecution unboundedExec = unbounded.start();
    for (int i = 0; i < 20; i++) {
      assertNotEquals(unboundedExec.nextBackOff(), BackOffExecution.STOP);
    }

    ExponentialBackOff bounded =
        (ExponentialBackOff) KafkaEventConsumerFactory.createMclBackOff(false);
    assertEquals(bounded.getMaxAttempts(), 5L);
    BackOffExecution boundedExec = bounded.start();
    for (int i = 0; i < 5; i++) {
      assertNotEquals(boundedExec.nextBackOff(), BackOffExecution.STOP);
    }
    assertEquals(boundedExec.nextBackOff(), BackOffExecution.STOP);
  }

  @Test
  @SuppressWarnings("unchecked")
  void testMclErrorHandlerHoldsOffsetBeyondDefaultRetryLimit() {
    ExponentialBackOff zeroDelayUnbounded = new ExponentialBackOff(0L, 1.0);
    CommonErrorHandler errorHandler =
        KafkaEventConsumerFactory.buildMclErrorHandler(false, zeroDelayUnbounded);

    TopicPartition tp = new TopicPartition("MetadataChangeLog_v1", 0);
    ConsumerRecord<String, GenericRecord> record =
        new ConsumerRecord<>("MetadataChangeLog_v1", 0, 42L, "k", mock(GenericRecord.class));
    Consumer<String, GenericRecord> consumer = mock(Consumer.class);
    MessageListenerContainer container = mock(MessageListenerContainer.class);
    when(container.isRunning()).thenReturn(true);

    RetryableIndexingException retryable =
        new RetryableIndexingException("bulk failed", new BulkTransferException(1, "403"));
    for (int i = 0; i < 12; i++) {
      expectThrows(
          RuntimeException.class,
          () -> errorHandler.handleRemaining(retryable, List.of(record), consumer, container));
    }
    verify(consumer, times(12)).seek(tp, 42L);

    ConsumerRecords<String, GenericRecord> records =
        new ConsumerRecords<>(Map.of(tp, List.of(record)));
    AtomicInteger batchAttempts = new AtomicInteger();
    errorHandler.handleBatch(
        retryable,
        records,
        consumer,
        container,
        () -> {
          if (batchAttempts.incrementAndGet() < 12) {
            throw retryable;
          }
        });
    assertEquals(batchAttempts.get(), 12);
    verify(consumer, atLeastOnce()).pause(any());
  }

  @Test
  public void testOmittedStoreTypeLeavesSpringKafkaProperty() {
    String keystoreType =
        SchemaRegistryClientConfig.CLIENT_NAMESPACE + SslConfigs.SSL_KEYSTORE_TYPE_CONFIG;
    KafkaProperties springKafka = new KafkaProperties();
    springKafka.getProperties().put(keystoreType, "PEM");

    KafkaConfiguration kafka =
        schemaRegistryKafka(Map.of("schema.registry.url", "http://schema-registry:8081"));
    Map<String, Object> props =
        KafkaEventConsumerFactory.buildCustomizedProperties(
            springKafka, kafka, kafka.getSerde().getEvent(), false, false);

    assertEquals(props.get(keystoreType), "PEM");
    assertFalse(
        props.containsKey(
            SchemaRegistryClientConfig.CLIENT_NAMESPACE + SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG));
  }

  @Test
  public void testConfiguredStoreTypeOverridesSpringKafkaProperty() {
    String keystoreType =
        SchemaRegistryClientConfig.CLIENT_NAMESPACE + SslConfigs.SSL_KEYSTORE_TYPE_CONFIG;
    KafkaProperties springKafka = new KafkaProperties();
    springKafka.getProperties().put(keystoreType, "PEM");

    Map<String, String> schemaRegistryProps = new HashMap<>();
    schemaRegistryProps.put("schema.registry.url", "http://schema-registry:8081");
    schemaRegistryProps.put(keystoreType, "PKCS12");

    KafkaConfiguration kafka = schemaRegistryKafka(schemaRegistryProps);
    Map<String, Object> props =
        KafkaEventConsumerFactory.buildCustomizedProperties(
            springKafka, kafka, kafka.getSerde().getEvent(), false, false);

    assertEquals(props.get(keystoreType), "PKCS12");
  }

  private static KafkaConfiguration schemaRegistryKafka(Map<String, String> schemaRegistryProps) {
    KafkaConfiguration.SerDeProperties serDeProperties = new KafkaConfiguration.SerDeProperties();
    serDeProperties.setSerializer("org.apache.kafka.common.serialization.StringSerializer");
    serDeProperties.setDeserializer("org.apache.kafka.common.serialization.StringDeserializer");

    KafkaConfiguration.SerDeKeyValueConfig event = new KafkaConfiguration.SerDeKeyValueConfig();
    event.setKey(serDeProperties);
    event.setValue(serDeProperties);
    event.setProperties(schemaRegistryProps);

    KafkaConfiguration.SerDeConfig serde = new KafkaConfiguration.SerDeConfig();
    serde.setEvent(event);

    KafkaConfiguration kafka = new KafkaConfiguration();
    kafka.setBootstrapServers("kafka:9092");
    kafka.setSerde(serde);
    kafka.setConsumer(new ConsumerConfiguration());
    kafka.getConsumer().setMaxPartitionFetchBytes(5242880);
    return kafka;
  }
}
