package com.linkedin.gms.factory.kafka;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.kafka.schemaregistry.KafkaSchemaRegistryFactory;
import java.lang.reflect.Field;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
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
      ConfigurationProvider.class
    })
public class DataHubKafkaProducerFactoryTest extends AbstractTestNGSpringContextTests {
  @Autowired
  @Qualifier("dataHubUsageProducer")
  Producer<String, String> dataHubUsageProducer;

  @Test
  void testInitialization() throws NoSuchFieldException, IllegalAccessException {
    assertNotNull(dataHubUsageProducer);

    // Use reflection to access the internal properties of the KafkaProducer
    Field producerConfigField = KafkaProducer.class.getDeclaredField("producerConfig");
    producerConfigField.setAccessible(true);
    ProducerConfig producerConfig = (ProducerConfig) producerConfigField.get(dataHubUsageProducer);

    // Use the ProducerConfig.get() method to access specific properties
    String securityProtocol = producerConfig.getString("security.protocol");
    assertEquals("SSL", securityProtocol, "SSL security protocol should be set");
  }
}
