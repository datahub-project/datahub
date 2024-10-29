package com.linkedin.gms.factory.kafka;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@SpringBootTest(
    properties = {"spring.kafka.properties.security.protocol=SSL"},
    classes = {SimpleKafkaConsumerFactory.class, ConfigurationProvider.class})
@EnableConfigurationProperties(ConfigurationProvider.class)
public class SimpleKafkaConsumerFactoryTest extends AbstractTestNGSpringContextTests {
  @Autowired ConcurrentKafkaListenerContainerFactory<?, ?> testFactory;

  @Test
  void testInitialization() {
    assertNotNull(testFactory);
    assertEquals(
        testFactory.getConsumerFactory().getConfigurationProperties().get("security.protocol"),
        "SSL");
  }
}
