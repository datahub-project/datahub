package com.linkedin.gms.factory.kafka;

import static org.mockito.Mockito.mockConstruction;
import static org.testng.Assert.assertEquals;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.kafka.ConsumerConfiguration;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.kafka.ProducerConfiguration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.testng.annotations.Test;

/**
 * The Kafka client builders share one Spring {@link KafkaProperties} bean. They must not mutate it,
 * or each client's config depends on which other clients happened to be built before it.
 */
public class KafkaPropertiesIsolationTest {

  private static final String BASE_PROTOCOL = "PLAINTEXT";
  private static final String PRODUCER_PROTOCOL = "SSL";
  private static final String CONSUMER_PROTOCOL = "SASL_PLAINTEXT";

  @Test
  public void dataHubUsageProducerBuiltAloneGetsProducerSecurityProtocol() {
    ConfigurationProvider provider = splitProvider();

    Map<String, Object> config =
        captureProducerConfigs(
                factory -> factory.createDUEProducer(provider, baseKafkaProperties()))
            .get(0);

    assertEquals(config.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG), PRODUCER_PROTOCOL);
  }

  @Test
  public void producersGetTheSameConfigWhicheverIsBuiltFirst() {
    ConfigurationProvider provider = splitProvider();

    KafkaProperties sharedA = baseKafkaProperties();
    List<Map<String, Object>> eventProducerFirst =
        captureProducerConfigs(
            factory -> {
              factory.createInstance(provider, sharedA, provider.getKafka().getSerde().getEvent());
              factory.createDUEProducer(provider, sharedA);
            });

    KafkaProperties sharedB = baseKafkaProperties();
    List<Map<String, Object>> usageProducerFirst =
        captureProducerConfigs(
            factory -> {
              factory.createDUEProducer(provider, sharedB);
              factory.createInstance(provider, sharedB, provider.getKafka().getSerde().getEvent());
            });

    assertEquals(usageProducerFirst.get(1), eventProducerFirst.get(0), "kafkaProducer");
    assertEquals(usageProducerFirst.get(0), eventProducerFirst.get(1), "dataHubUsageProducer");
  }

  @Test
  public void producerBuilderLeavesSharedKafkaPropertiesUntouched() {
    KafkaConfiguration kafka = splitProvider().getKafka();
    KafkaProperties shared = baseKafkaProperties();
    Map<String, Map<String, Object>> before = snapshot(shared);

    DataHubKafkaProducerFactory.buildProducerProperties(kafka.getSerde().getEvent(), kafka, shared);

    assertEquals(snapshot(shared), before);
  }

  @Test
  public void consumerBuildersLeaveSharedKafkaPropertiesUntouched() {
    ConfigurationProvider provider = splitProvider();
    KafkaConfiguration kafka = provider.getKafka();
    KafkaProperties shared = baseKafkaProperties();
    Map<String, Map<String, Object>> before = snapshot(shared);

    KafkaEventConsumerFactory.buildCustomizedProperties(
        shared, kafka, kafka.getSerde().getEvent(), true, false);
    KafkaEventConsumerFactory.buildCustomizedProperties(
        shared, kafka, kafka.getSerde().getEvent(), false, true);
    new SimpleKafkaConsumerFactory().createInstance(provider, shared);
    new CDCConsumerFactory().createCdcConsumerFactory(provider, shared);

    assertEquals(snapshot(shared), before);
  }

  @Test
  public void sharedBootstrapConsumerDoesNotLeakProducerProtocolIntoLaterConsumers() {
    KafkaConfiguration kafka = splitProvider().getKafka();
    kafka.getConsumer().setSecurityProtocol(null);
    KafkaProperties shared = baseKafkaProperties();

    // Upgrade-history style consumer that reads from the producer-side cluster.
    KafkaEventConsumerFactory.buildCustomizedProperties(
        shared, kafka, kafka.getSerde().getEvent(), false, true);
    Map<String, Object> regular =
        KafkaEventConsumerFactory.buildCustomizedProperties(
            shared, kafka, kafka.getSerde().getEvent(), true, false);

    assertEquals(regular.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG), BASE_PROTOCOL);
  }

  @Test
  public void springPerClientPropertiesStillOverrideDataHubOverrides() {
    ConfigurationProvider provider = splitProvider();
    KafkaConfiguration kafka = provider.getKafka();
    KafkaProperties shared = baseKafkaProperties();
    shared
        .getProducer()
        .getProperties()
        .put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
    shared
        .getConsumer()
        .getProperties()
        .put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
    shared.getConsumer().getProperties().put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    Map<String, Object> producer =
        DataHubKafkaProducerFactory.buildProducerProperties(
            kafka.getSerde().getEvent(), kafka, shared);
    Map<String, Object> usageProducer =
        captureProducerConfigs(factory -> factory.createDUEProducer(provider, shared)).get(0);
    Map<String, Object> eventConsumer =
        KafkaEventConsumerFactory.buildCustomizedProperties(
            shared, kafka, kafka.getSerde().getEvent(), true, false);
    Map<String, Object> simpleConsumer =
        ((ConcurrentKafkaListenerContainerFactory<?, ?>)
                new SimpleKafkaConsumerFactory().createInstance(provider, shared))
            .getConsumerFactory()
            .getConfigurationProperties();
    Map<String, Object> cdcConsumer =
        ((ConcurrentKafkaListenerContainerFactory<?, ?>)
                new CDCConsumerFactory().createCdcConsumerFactory(provider, shared))
            .getConsumerFactory()
            .getConfigurationProperties();

    for (Map<String, Object> config :
        List.of(producer, usageProducer, eventConsumer, simpleConsumer, cdcConsumer)) {
      assertEquals(config.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG), "SASL_SSL");
    }
    for (Map<String, Object> config : List.of(eventConsumer, simpleConsumer, cdcConsumer)) {
      assertEquals(config.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG), "false");
    }
  }

  /** Runs the action with KafkaProducer construction intercepted; returns the configs, in order. */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static List<Map<String, Object>> captureProducerConfigs(
      Consumer<DataHubKafkaProducerFactory> action) {
    List<Map<String, Object>> configs = new ArrayList<>();
    try (var ignored =
        mockConstruction(
            KafkaProducer.class,
            (mock, context) ->
                configs.add(new LinkedHashMap<>((Map) context.arguments().get(0))))) {
      action.accept(new DataHubKafkaProducerFactory());
    }
    return configs;
  }

  private static Map<String, Map<String, Object>> snapshot(KafkaProperties properties) {
    return Map.of(
        "producer", properties.buildProducerProperties(),
        "consumer", properties.buildConsumerProperties(),
        "admin", properties.buildAdminProperties(),
        "streams", properties.buildStreamsProperties());
  }

  private static KafkaProperties baseKafkaProperties() {
    KafkaProperties properties = new KafkaProperties();
    properties.getSecurity().setProtocol(BASE_PROTOCOL);
    return properties;
  }

  /** A split producer/consumer migration: each side has its own bootstrap and protocol. */
  private static ConfigurationProvider splitProvider() {
    KafkaConfiguration.SerDeProperties string = new KafkaConfiguration.SerDeProperties();
    string.setSerializer("org.apache.kafka.common.serialization.StringSerializer");
    string.setDeserializer("org.apache.kafka.common.serialization.StringDeserializer");
    KafkaConfiguration.SerDeKeyValueConfig keyValue = new KafkaConfiguration.SerDeKeyValueConfig();
    keyValue.setKey(string);
    keyValue.setValue(string);
    KafkaConfiguration.SerDeConfig serde = new KafkaConfiguration.SerDeConfig();
    serde.setEvent(keyValue);
    serde.setUsageEvent(keyValue);

    ProducerConfiguration producer = new ProducerConfiguration();
    producer.setBootstrapServers("producer-broker:9092");
    producer.setSecurityProtocol(PRODUCER_PROTOCOL);
    producer.setCompressionType("snappy");
    producer.setInitializationRetryCount(1);

    ConsumerConfiguration consumer = new ConsumerConfiguration();
    consumer.setBootstrapServers("consumer-broker:9092");
    consumer.setSecurityProtocol(CONSUMER_PROTOCOL);
    consumer.setMaxPartitionFetchBytes(1048576);

    KafkaConfiguration kafka = new KafkaConfiguration();
    kafka.setBootstrapServers("base-broker:9092");
    kafka.setProducer(producer);
    kafka.setConsumer(consumer);
    kafka.setSerde(serde);

    ConfigurationProvider provider = new ConfigurationProvider();
    provider.setKafka(kafka);
    return provider;
  }
}
