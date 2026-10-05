package com.linkedin.gms.factory.kafka.schemaregistry;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.kafka.DataHubKafkaProducerFactory;
import com.linkedin.metadata.config.kafka.ConsumerConfiguration;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.kafka.ProducerConfiguration;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import java.util.Map;
import org.apache.kafka.common.config.SslConfigs;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.Test;

public class KafkaSchemaRegistryFactoryTest {

  private static final String KEYSTORE_TYPE =
      SchemaRegistryClientConfig.CLIENT_NAMESPACE + SslConfigs.SSL_KEYSTORE_TYPE_CONFIG;
  private static final String TRUSTSTORE_TYPE =
      SchemaRegistryClientConfig.CLIENT_NAMESPACE + SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG;
  private static final String KEYSTORE_LOCATION =
      SchemaRegistryClientConfig.CLIENT_NAMESPACE + SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG;
  private static final String TRUSTSTORE_LOCATION =
      SchemaRegistryClientConfig.CLIENT_NAMESPACE + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG;

  @Test
  public void testOmitsStoreTypeWhenUnset() {
    Map<String, String> props = schemaRegistryProperties("", "");

    assertFalse(props.containsKey(KEYSTORE_TYPE));
    assertFalse(props.containsKey(TRUSTSTORE_TYPE));
    assertEquals(props.get("schema.registry.url"), "http://schema-registry:8081");
    assertEquals(props.get(KEYSTORE_LOCATION), "/certs/keystore.pem");
    assertEquals(props.get(TRUSTSTORE_LOCATION), "/certs/truststore.pem");
    assertEquals(props.get("schema.registry.security.protocol"), "SSL");
  }

  @Test
  public void testForwardsStoreTypeWhenSet() {
    Map<String, String> props = schemaRegistryProperties("PEM", "PEM");

    assertEquals(props.get(KEYSTORE_TYPE), "PEM");
    assertEquals(props.get(TRUSTSTORE_TYPE), "PEM");
  }

  @Test
  public void testForwardsOnlyTheStoreTypeThatIsSet() {
    Map<String, String> props = schemaRegistryProperties("PKCS12", "");

    assertEquals(props.get(KEYSTORE_TYPE), "PKCS12");
    assertFalse(props.containsKey(TRUSTSTORE_TYPE));
  }

  @Test
  public void testUnsetStoreTypeDoesNotOverrideSpringKafkaProperty() {
    KafkaConfiguration kafka = kafkaConfiguration();
    KafkaConfiguration.SerDeKeyValueConfig schemaRegistryConfig =
        schemaRegistryConfig(kafka, "", "");

    KafkaProperties springKafka = new KafkaProperties();
    springKafka.getProperties().put(KEYSTORE_TYPE, "PEM");
    springKafka.getProperties().put(TRUSTSTORE_TYPE, "PEM");

    Map<String, Object> producerProps =
        DataHubKafkaProducerFactory.buildProducerProperties(
            schemaRegistryConfig, kafka, springKafka);

    assertEquals(producerProps.get(KEYSTORE_TYPE), "PEM");
    assertEquals(producerProps.get(TRUSTSTORE_TYPE), "PEM");
  }

  @Test
  public void testConfiguredStoreTypeOverridesSpringKafkaProperty() {
    KafkaConfiguration kafka = kafkaConfiguration();
    KafkaConfiguration.SerDeKeyValueConfig schemaRegistryConfig =
        schemaRegistryConfig(kafka, "PKCS12", "PKCS12");

    KafkaProperties springKafka = new KafkaProperties();
    springKafka.getProperties().put(KEYSTORE_TYPE, "PEM");
    springKafka.getProperties().put(TRUSTSTORE_TYPE, "PEM");

    Map<String, Object> producerProps =
        DataHubKafkaProducerFactory.buildProducerProperties(
            schemaRegistryConfig, kafka, springKafka);

    assertEquals(producerProps.get(KEYSTORE_TYPE), "PKCS12");
    assertEquals(producerProps.get(TRUSTSTORE_TYPE), "PKCS12");
  }

  private static Map<String, String> schemaRegistryProperties(
      String keystoreType, String truststoreType) {
    KafkaConfiguration kafka = kafkaConfiguration();
    return schemaRegistryConfig(kafka, keystoreType, truststoreType).getProperties(null);
  }

  private static KafkaConfiguration.SerDeKeyValueConfig schemaRegistryConfig(
      KafkaConfiguration kafka, String keystoreType, String truststoreType) {
    KafkaSchemaRegistryFactory factory = new KafkaSchemaRegistryFactory();
    ReflectionTestUtils.setField(factory, "kafkaSchemaRegistryUrl", "http://schema-registry:8081");
    ReflectionTestUtils.setField(factory, "sslTruststoreLocation", "/certs/truststore.pem");
    ReflectionTestUtils.setField(factory, "sslTruststorePassword", "");
    ReflectionTestUtils.setField(factory, "sslTruststoreType", truststoreType);
    ReflectionTestUtils.setField(factory, "sslKeystoreLocation", "/certs/keystore.pem");
    ReflectionTestUtils.setField(factory, "sslKeystorePassword", "");
    ReflectionTestUtils.setField(factory, "sslKeystoreType", keystoreType);
    ReflectionTestUtils.setField(factory, "securityProtocol", "SSL");

    ConfigurationProvider provider = new ConfigurationProvider();
    provider.setKafka(kafka);
    return factory.getInstance(provider);
  }

  private static KafkaConfiguration kafkaConfiguration() {
    KafkaConfiguration.SerDeProperties serDeProperties = new KafkaConfiguration.SerDeProperties();
    serDeProperties.setSerializer("org.apache.kafka.common.serialization.StringSerializer");
    serDeProperties.setDeserializer("org.apache.kafka.common.serialization.StringDeserializer");

    KafkaConfiguration.SerDeKeyValueConfig event = new KafkaConfiguration.SerDeKeyValueConfig();
    event.setKey(serDeProperties);
    event.setValue(serDeProperties);

    KafkaConfiguration.SerDeConfig serde = new KafkaConfiguration.SerDeConfig();
    serde.setEvent(event);

    ProducerConfiguration producer = new ProducerConfiguration();
    producer.setBootstrapServers("kafka:9092");
    producer.setRetryCount(3);
    producer.setDeliveryTimeout(30000);
    producer.setRequestTimeout(3000);
    producer.setBackoffTimeout(100);
    producer.setMaxRequestSize(5242880);
    producer.setCompressionType("snappy");

    ConsumerConfiguration consumer = new ConsumerConfiguration();
    consumer.setBootstrapServers("kafka:9092");
    consumer.setMaxPartitionFetchBytes(5242880);

    KafkaConfiguration kafka = new KafkaConfiguration();
    kafka.setBootstrapServers("kafka:9092");
    kafka.setSerde(serde);
    kafka.setProducer(producer);
    kafka.setConsumer(consumer);
    return kafka;
  }
}
