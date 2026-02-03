package com.linkedin.gms.factory.kafka;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.kafka.common.TopicConventionFactory;
import com.linkedin.gms.factory.kafka.schemaregistry.KafkaSchemaRegistryFactory;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.kafka.ProducerConfiguration;
import com.linkedin.metadata.dao.producer.KafkaHealthChecker;
import com.linkedin.metadata.event.GenericProducer;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@SpringBootTest(
    properties = {
      "kafka.schemaRegistry.type=KAFKA",
      "spring.kafka.properties.security.protocol=SSL"
    },
    classes = {
      DataHubKafkaProducerFactory.class,
      KafkaSchemaRegistryFactory.class,
      ConfigurationProvider.class,
      TopicConventionFactory.class,
      DataHubKafkaEventProducerFactory.class
    })
public class DataHubKafkaProducerFactoryTest extends AbstractTestNGSpringContextTests {
  @Autowired
  @Qualifier("dataHubUsageProducer")
  Producer<String, String> dataHubUsageProducer;

  @Autowired
  @Qualifier("dataHubUsageEventProducer")
  GenericProducer<String> dataHubUsageEventProducer;

  @MockBean KafkaHealthChecker kafkaHealthChecker;

  @MockBean MetricUtils metricUtils;

  @Test
  void testInitialization() throws NoSuchFieldException, IllegalAccessException {
    assertNotNull(dataHubUsageProducer);
    assertNotNull(dataHubUsageEventProducer);

    // Use reflection to access the internal properties of the KafkaProducer
    Field producerConfigField = KafkaProducer.class.getDeclaredField("producerConfig");
    producerConfigField.setAccessible(true);
    ProducerConfig producerConfig = (ProducerConfig) producerConfigField.get(dataHubUsageProducer);

    // Use the ProducerConfig.get() method to access specific properties
    String securityProtocol = producerConfig.getString("security.protocol");
    assertEquals("SSL", securityProtocol, "SSL security protocol should be set");
  }

  @Test
  public void testBuildProducerPropertiesWithOverride() {
    // Setup
    KafkaProperties kafkaProperties = new KafkaProperties();

    KafkaConfiguration kafkaConfig = new KafkaConfiguration();
    kafkaConfig.setBootstrapServers("base-kafka:9092");

    ProducerConfiguration producerConfig = new ProducerConfiguration();
    producerConfig.setBootstrapServers("producer-kafka:9092");
    producerConfig.setRetryCount(3);
    producerConfig.setDeliveryTimeout(30000);
    producerConfig.setRequestTimeout(3000);
    producerConfig.setBackoffTimeout(100);
    producerConfig.setMaxRequestSize(5242880);
    producerConfig.setCompressionType("snappy");

    KafkaConfiguration.SerDeConfig serDeConfig = new KafkaConfiguration.SerDeConfig();
    KafkaConfiguration.SerDeKeyValueConfig eventConfig =
        new KafkaConfiguration.SerDeKeyValueConfig();
    KafkaConfiguration.SerDeProperties serDeProperties = new KafkaConfiguration.SerDeProperties();
    serDeProperties.setSerializer("org.apache.kafka.common.serialization.StringSerializer");
    serDeProperties.setDeserializer("org.apache.kafka.common.serialization.StringSerializer");
    eventConfig.setKey(serDeProperties);
    eventConfig.setValue(serDeProperties);
    serDeConfig.setEvent(eventConfig);

    kafkaConfig.setProducer(producerConfig);
    kafkaConfig.setSerde(serDeConfig);

    Map<String, Object> props =
        DataHubKafkaProducerFactory.buildProducerProperties(null, kafkaConfig, kafkaProperties);

    // Verify - Bootstrap servers list should contain the producer-specific override
    assertEquals(
        props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG),
        Collections.singletonList("producer-kafka:9092"));
  }
}
