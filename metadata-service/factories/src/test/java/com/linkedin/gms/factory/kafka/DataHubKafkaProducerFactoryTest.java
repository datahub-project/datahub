package com.linkedin.gms.factory.kafka;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.kafka.common.TopicConventionFactory;
import com.linkedin.gms.factory.kafka.schemaregistry.KafkaSchemaRegistryFactory;
import com.linkedin.metadata.dao.producer.KafkaHealthChecker;
import com.linkedin.metadata.event.GenericProducer;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.lang.reflect.Field;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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
}
