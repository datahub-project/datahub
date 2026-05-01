package com.linkedin.gms.factory.kafka;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.kafka.ConsumerConfiguration;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.config.kafka.ListenerConfiguration;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
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
    consumerConfig.setPe(new ConsumerConfiguration.ConsumerOptions());
    consumerConfig.setMcp(new ConsumerConfiguration.ConsumerOptions());
    consumerConfig.setMcl(new ConsumerConfiguration.ConsumerOptions());
    kafkaConfig.setConsumer(consumerConfig);

    ListenerConfiguration listenerConfig = new ListenerConfiguration();
    listenerConfig.setConcurrency(1);
    kafkaConfig.setListener(listenerConfig);

    when(configProvider.getKafka()).thenReturn(kafkaConfig);

    // Create a consumer factory directly — we only need it as input to the listener
    // factory beans, not to test consumer properties themselves.
    kafkaConsumerFactory =
        new DefaultKafkaConsumerFactory<>(Map.of("bootstrap.servers", "localhost:9092"));

    // Initialize the config-driven instance fields that are normally set by createConsumerFactory.
    // We skip createConsumerFactory because it requires full serde setup.
    try {
      Field field =
          KafkaEventConsumerFactory.class.getDeclaredField("authExceptionRetryIntervalSeconds");
      field.setAccessible(true);
      field.setInt(factory, consumerConfig.getAuthExceptionRetryIntervalSeconds());
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
  void testAuthExceptionRetryIntervalOnDuheConsumer() {
    var listenerFactory =
        (ConcurrentKafkaListenerContainerFactory<?, ?>)
            factory.duheKafkaEventConsumer(kafkaConsumerFactory);

    assertEquals(
        listenerFactory.getContainerProperties().getAuthExceptionRetryInterval(),
        Duration.ofSeconds(10),
        "DUHE consumer should retry on auth exceptions to survive MSK IAM credential rotation");
  }
}
