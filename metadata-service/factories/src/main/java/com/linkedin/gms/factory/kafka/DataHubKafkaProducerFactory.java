package com.linkedin.gms.factory.kafka;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.kafka.ProducerConfiguration;
import com.linkedin.metadata.config.messaging.KafkaMessagingEnabledCondition;
import com.linkedin.metadata.kafka.KafkaProducerInitializationRetry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Configuration
@DependsOn("configurationProvider")
@Conditional(KafkaMessagingEnabledCondition.class)
public class DataHubKafkaProducerFactory {

  @Bean(name = "kafkaProducer")
  protected Producer<String, IndexedRecord> createInstance(
      @Qualifier("configurationProvider") ConfigurationProvider provider,
      final KafkaProperties properties,
      @Qualifier("schemaRegistryConfig")
          final KafkaConfiguration.SerDeKeyValueConfig schemaRegistryConfig) {
    KafkaConfiguration kafkaConfiguration = provider.getKafka();
    Map<String, Object> props =
        buildProducerProperties(schemaRegistryConfig, kafkaConfiguration, properties);
    return createProducerWithRetry(props, kafkaConfiguration.getProducer());
  }

  /**
   * Mirror of KafkaTrackingProducer in Frontend code, uses less Spring dependent configuration
   * because frontend doesn't use it. Ideally this would be shared code, but the Play framework
   * injection and Spring injection don't really intermix
   */
  @Bean(name = "dataHubUsageProducer")
  protected Producer<String, String> createDUEProducer(
      @Qualifier("configurationProvider") ConfigurationProvider provider,
      KafkaProperties properties) {

    KafkaConfiguration kafkaConfiguration = provider.getKafka();
    final ProducerConfiguration producerConfiguration = kafkaConfiguration.getProducer();

    // Initialize with Spring Kafka production configuration
    Map<String, Object> props = properties.buildProducerProperties();

    // Apply DUE specifics
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "datahub-analytics");
    props.put(
        ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
        kafkaConfiguration.getProducer().getDeliveryTimeout());
    props.put(
        ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,
        kafkaConfiguration.getProducer().getRequestTimeout());
    String bootstrapServers =
        StringUtils.isNotBlank(kafkaConfiguration.getProducer().getBootstrapServers())
            ? kafkaConfiguration.getProducer().getBootstrapServers()
            : kafkaConfiguration.getBootstrapServers();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    // key: Actor urn.
    // value: JSON object.
    props.putAll(kafkaConfiguration.getSerde().getUsageEvent().getProducerProperties(null));
    props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, producerConfiguration.getMaxRequestSize());
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, producerConfiguration.getCompressionType());

    return createProducerWithRetry(props, producerConfiguration);
  }

  /**
   * Creates a KafkaProducer with retry logic for initialization failures. This handles transient
   * issues like DNS resolution failures that can occur during container startup in Kubernetes
   * environments.
   *
   * @param props the producer configuration properties
   * @param producerConfiguration the producer configuration containing retry settings
   * @return the created KafkaProducer
   */
  public static <K, V> Producer<K, V> createProducerWithRetry(
      Map<String, Object> props, ProducerConfiguration producerConfiguration) {
    return createProducerWithRetry(props, producerConfiguration, KafkaProducer::new);
  }

  /**
   * Creates a KafkaProducer with retry logic, using the provided factory function to construct the
   * producer. This overload exists to support testing without requiring a real Kafka broker.
   *
   * @param props the producer configuration properties
   * @param producerConfiguration the producer configuration containing retry settings
   * @param producerFactory function that creates a Producer from config properties
   * @return the created Producer
   */
  static <K, V> Producer<K, V> createProducerWithRetry(
      Map<String, Object> props,
      ProducerConfiguration producerConfiguration,
      Function<Map<String, Object>, Producer<K, V>> producerFactory) {
    return KafkaProducerInitializationRetry.createWithRetry(
        props, toInitializationSettings(producerConfiguration), producerFactory);
  }

  private static KafkaProducerInitializationRetry.Settings toInitializationSettings(
      ProducerConfiguration producerConfiguration) {
    return KafkaProducerInitializationRetry.Settings.builder()
        .maxAttempts(producerConfiguration.getInitializationRetryCount())
        .initialBackoffMs(producerConfiguration.getInitializationRetryBackoffMs())
        .maxBackoffMs(producerConfiguration.getInitializationRetryMaxBackoffMs())
        .maxTotalWaitMs(producerConfiguration.getInitializationRetryMaxTotalWaitMs())
        .build();
  }

  public static Map<String, Object> buildProducerProperties(
      KafkaConfiguration.SerDeKeyValueConfig schemaRegistryConfig,
      KafkaConfiguration kafkaConfiguration,
      KafkaProperties properties) {
    KafkaProperties.Producer producerProps = properties.getProducer();

    producerProps.setKeySerializer(StringSerializer.class);
    // KAFKA_BOOTSTRAP_SERVER has precedence over SPRING_KAFKA_BOOTSTRAP_SERVERS
    String bootstrapServers =
        StringUtils.isNotBlank(kafkaConfiguration.getProducer().getBootstrapServers())
            ? kafkaConfiguration.getProducer().getBootstrapServers()
            : kafkaConfiguration.getBootstrapServers();
    if (StringUtils.isNotBlank(bootstrapServers)) {
      producerProps.setBootstrapServers(Arrays.asList(bootstrapServers.split(",")));
    } // else we rely on KafkaProperties which defaults to localhost:9092

    String securityProtocol =
        StringUtils.isNotBlank(kafkaConfiguration.getProducer().getSecurityProtocol())
            ? kafkaConfiguration.getProducer().getSecurityProtocol()
            : null;
    if (StringUtils.isNotBlank(securityProtocol)) {
      producerProps.getSecurity().setProtocol(securityProtocol);
    }

    Map<String, Object> props = properties.buildProducerProperties();
    props.putAll(
        kafkaConfiguration.getSerde().getEvent().getProducerProperties(schemaRegistryConfig));

    props.put(ProducerConfig.RETRIES_CONFIG, kafkaConfiguration.getProducer().getRetryCount());
    props.put(
        ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
        kafkaConfiguration.getProducer().getDeliveryTimeout());
    props.put(
        ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,
        kafkaConfiguration.getProducer().getRequestTimeout());
    props.put(
        ProducerConfig.RETRY_BACKOFF_MS_CONFIG,
        kafkaConfiguration.getProducer().getBackoffTimeout());
    props.put(
        ProducerConfig.COMPRESSION_TYPE_CONFIG,
        kafkaConfiguration.getProducer().getCompressionType());
    props.put(
        ProducerConfig.MAX_REQUEST_SIZE_CONFIG,
        kafkaConfiguration.getProducer().getMaxRequestSize());

    // Override KafkaProperties with SchemaRegistryConfig only for non-empty values
    props.putAll(kafkaConfiguration.getSerde().getEvent().getProperties(schemaRegistryConfig));

    String schemaRegistryUrl = kafkaConfiguration.getProducer().getSchemaRegistryUrl();
    if (StringUtils.isNotBlank(schemaRegistryUrl)) {
      props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    }

    return props;
  }
}
