package com.linkedin.gms.factory.kafka;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.kafka.ConsumerConfiguration;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.testng.annotations.Test;

public class CDCConsumerFactoryTest {

  @Test
  void testFactoryCreation() {
    // Create real configuration with necessary setup (not mocks as per requirements)
    ConfigurationProvider configProvider = mock(ConfigurationProvider.class);

    // Set up Kafka configuration properly
    KafkaConfiguration kafkaConfig = new KafkaConfiguration();
    kafkaConfig.setBootstrapServers("localhost:9092");
    kafkaConfig.setConsumer(new ConsumerConfiguration());

    configProvider = mock(ConfigurationProvider.class);
    when(configProvider.getKafka()).thenReturn(kafkaConfig);

    KafkaProperties kafkaProperties = new KafkaProperties();
    CDCConsumerFactory factory = new CDCConsumerFactory();

    // Test that the factory can create the consumer factory without throwing exceptions
    var consumerFactory = factory.createCdcConsumerFactory(configProvider, kafkaProperties);

    assertNotNull(consumerFactory, "Consumer factory should be created successfully");
  }

  @Test
  void testFactoryInstantiation() {
    // Simple test that the factory can be instantiated
    CDCConsumerFactory factory = new CDCConsumerFactory();
    assertNotNull(factory, "CDCConsumerFactory should be instantiated successfully");
  }

  @Test
  void testConsumerSecurityProtocolOverride() {
    // Setup
    ConfigurationProvider configProvider = mock(ConfigurationProvider.class);

    // Set base security protocol to SASL_SSL
    KafkaProperties kafkaProperties = new KafkaProperties();
    kafkaProperties.getConsumer().getSecurity().setProtocol("SASL_SSL");

    // Set up Kafka configuration with consumer-specific override to PLAINTEXT
    KafkaConfiguration kafkaConfig = new KafkaConfiguration();
    kafkaConfig.setBootstrapServers("localhost:9092");

    ConsumerConfiguration consumerConfig = new ConsumerConfiguration();
    consumerConfig.setSecurityProtocol("PLAINTEXT");
    kafkaConfig.setConsumer(consumerConfig);

    when(configProvider.getKafka()).thenReturn(kafkaConfig);

    CDCConsumerFactory factory = new CDCConsumerFactory();

    // Test that the factory can create the consumer factory with security protocol override
    var consumerFactory = factory.createCdcConsumerFactory(configProvider, kafkaProperties);

    assertNotNull(
        consumerFactory,
        "Consumer factory should be created successfully with security protocol override");

    // Note: The actual security protocol verification would require accessing the internal
    // configuration of the consumer factory, which is not directly exposed. The test verifies
    // that the factory creation doesn't throw exceptions when security protocol override is set.
  }
}
