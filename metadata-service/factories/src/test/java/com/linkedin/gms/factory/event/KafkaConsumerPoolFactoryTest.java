package com.linkedin.gms.factory.event;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.event.kafka.CheckedConsumer;
import io.datahubproject.event.kafka.KafkaConsumerPool;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@SpringBootTest(
    classes = {
      KafkaConsumerPoolFactory.class,
      KafkaConsumerPoolFactoryTest.TestConfiguration.class
    })
@TestPropertySource(
    properties = {
      "eventsApi.enabled=true",
      "kafka.consumerPool.initialSize=3",
      "kafka.consumerPool.maxSize=5",
      "kafka.consumerPool.validationTimeoutSeconds=10",
      "kafka.consumerPool.validationCacheIntervalMinutes=2"
    })
public class KafkaConsumerPoolFactoryTest extends AbstractTestNGSpringContextTests {

  @MockitoBean private MetricUtils mockMetricUtils;

  @Autowired
  @Qualifier("kafkaConsumerPool")
  private KafkaConsumerPool kafkaConsumerPool;

  @Test
  public void testKafkaConsumerPoolBeanCreation() {
    assertNotNull(kafkaConsumerPool, "KafkaConsumerPool bean should be created");
  }

  @Test
  public void testKafkaConsumerPoolInitializationWithCorrectInitialSize() {
    assertNotNull(kafkaConsumerPool, "KafkaConsumerPool should be created");
    assertEquals(
        kafkaConsumerPool.getTotalConsumersCreated().get(),
        3,
        "Should create 3 initial consumers (initialSize=3)");
  }

  @Test
  public void testKafkaConsumerPoolRespectsMaxPoolSize() throws Exception {
    CheckedConsumer consumer1 =
        kafkaConsumerPool.borrowConsumer(100, TimeUnit.MILLISECONDS, "PlatformEvent_v1");
    CheckedConsumer consumer2 =
        kafkaConsumerPool.borrowConsumer(100, TimeUnit.MILLISECONDS, "PlatformEvent_v1");
    CheckedConsumer consumer3 =
        kafkaConsumerPool.borrowConsumer(100, TimeUnit.MILLISECONDS, "PlatformEvent_v1");
    CheckedConsumer consumer4 =
        kafkaConsumerPool.borrowConsumer(100, TimeUnit.MILLISECONDS, "PlatformEvent_v1");
    CheckedConsumer consumer5 =
        kafkaConsumerPool.borrowConsumer(100, TimeUnit.MILLISECONDS, "PlatformEvent_v1");
    CheckedConsumer consumer6 =
        kafkaConsumerPool.borrowConsumer(100, TimeUnit.MILLISECONDS, "PlatformEvent_v1");

    assertNotNull(consumer1, "Should be able to borrow consumer 1");
    assertNotNull(consumer2, "Should be able to borrow consumer 2");
    assertNotNull(consumer3, "Should be able to borrow consumer 3");
    assertNotNull(consumer4, "Should be able to borrow consumer 4");
    assertNotNull(consumer5, "Should be able to borrow consumer 5");
    assertNull(consumer6, "Should not be able to borrow more than maxPoolSize (5) consumers");

    assertEquals(
        kafkaConsumerPool.getTotalConsumersCreated().get(),
        5,
        "Should not create more than maxPoolSize (5) consumers");

    if (consumer1 != null) kafkaConsumerPool.returnConsumer(consumer1);
    if (consumer2 != null) kafkaConsumerPool.returnConsumer(consumer2);
    if (consumer3 != null) kafkaConsumerPool.returnConsumer(consumer3);
    if (consumer4 != null) kafkaConsumerPool.returnConsumer(consumer4);
    if (consumer5 != null) kafkaConsumerPool.returnConsumer(consumer5);
  }

  @Test
  public void testValidationTimeoutIsSetCorrectly() throws Exception {
    Field validationTimeoutField = KafkaConsumerPool.class.getDeclaredField("validationTimeout");
    validationTimeoutField.setAccessible(true);
    Duration validationTimeout = (Duration) validationTimeoutField.get(kafkaConsumerPool);

    assertEquals(
        validationTimeout,
        Duration.ofSeconds(10),
        "Validation timeout should be set to 10 seconds");
  }

  @Test
  public void testValidationCacheIntervalIsSetCorrectly() throws Exception {
    Field validationCacheIntervalField =
        KafkaConsumerPool.class.getDeclaredField("validationCacheInterval");
    validationCacheIntervalField.setAccessible(true);
    Duration validationCacheInterval =
        (Duration) validationCacheIntervalField.get(kafkaConsumerPool);

    assertEquals(
        validationCacheInterval,
        Duration.ofMinutes(2),
        "Validation cache interval should be set to 2 minutes");
  }

  @Test
  public void testMetricUtilsIsInjected() throws Exception {
    Field metricUtilsField = KafkaConsumerPool.class.getDeclaredField("metricUtils");
    metricUtilsField.setAccessible(true);
    MetricUtils injectedMetricUtils = (MetricUtils) metricUtilsField.get(kafkaConsumerPool);

    assertNotNull(injectedMetricUtils, "MetricUtils should be injected when provided");
    assertEquals(
        injectedMetricUtils, mockMetricUtils, "Injected MetricUtils should match the mock");
  }

  @Configuration
  static class TestConfiguration {
    @Bean(name = "kafkaConsumerPoolConsumerFactory")
    public DefaultKafkaConsumerFactory<String, GenericRecord> testConsumerFactory() {
      DefaultKafkaConsumerFactory<String, GenericRecord> factory =
          mock(DefaultKafkaConsumerFactory.class);
      KafkaConsumer<String, GenericRecord> consumer = mock(KafkaConsumer.class);
      when(consumer.assignment()).thenReturn(Collections.emptySet());
      when(consumer.partitionsFor(anyString(), any(Duration.class)))
          .thenReturn(Collections.emptyList());
      when(factory.createConsumer()).thenReturn(consumer);
      return factory;
    }
  }
}

@SpringBootTest(
    classes = {
      KafkaConsumerPoolFactory.class,
      KafkaConsumerPoolFactoryWithoutMetricUtilsTest.TestConfiguration.class
    })
@TestPropertySource(
    properties = {
      "eventsApi.enabled=true",
      "kafka.consumerPool.initialSize=2",
      "kafka.consumerPool.maxSize=4",
      "kafka.consumerPool.validationTimeoutSeconds=15",
      "kafka.consumerPool.validationCacheIntervalMinutes=3"
    })
class KafkaConsumerPoolFactoryWithoutMetricUtilsTest extends AbstractTestNGSpringContextTests {

  @Autowired
  @Qualifier("kafkaConsumerPool")
  private KafkaConsumerPool kafkaConsumerPool;

  @Test
  public void testKafkaConsumerPoolBeanCreationWithoutMetricUtils() {
    assertNotNull(
        kafkaConsumerPool, "KafkaConsumerPool bean should be created without MetricUtils");
  }

  @Test
  public void testKafkaConsumerPoolInitializationWithDifferentProperties() {
    assertNotNull(kafkaConsumerPool, "KafkaConsumerPool should be created");
    assertEquals(
        kafkaConsumerPool.getTotalConsumersCreated().get(),
        2,
        "Should create 2 initial consumers (initialSize=2)");
  }

  @Test
  public void testValidationTimeoutWithDifferentValue() throws Exception {
    Field validationTimeoutField = KafkaConsumerPool.class.getDeclaredField("validationTimeout");
    validationTimeoutField.setAccessible(true);
    Duration validationTimeout = (Duration) validationTimeoutField.get(kafkaConsumerPool);

    assertEquals(
        validationTimeout,
        Duration.ofSeconds(15),
        "Validation timeout should be set to 15 seconds");
  }

  @Test
  public void testValidationCacheIntervalWithDifferentValue() throws Exception {
    Field validationCacheIntervalField =
        KafkaConsumerPool.class.getDeclaredField("validationCacheInterval");
    validationCacheIntervalField.setAccessible(true);
    Duration validationCacheInterval =
        (Duration) validationCacheIntervalField.get(kafkaConsumerPool);

    assertEquals(
        validationCacheInterval,
        Duration.ofMinutes(3),
        "Validation cache interval should be set to 3 minutes");
  }

  @Test
  public void testMetricUtilsIsNullWhenNotProvided() throws Exception {
    Field metricUtilsField = KafkaConsumerPool.class.getDeclaredField("metricUtils");
    metricUtilsField.setAccessible(true);
    Object injectedMetricUtils = metricUtilsField.get(kafkaConsumerPool);

    assertNull(injectedMetricUtils, "MetricUtils should be null when not provided");
  }

  @Configuration
  static class TestConfiguration {
    @Bean(name = "kafkaConsumerPoolConsumerFactory")
    public DefaultKafkaConsumerFactory<String, GenericRecord> testConsumerFactory() {
      DefaultKafkaConsumerFactory<String, GenericRecord> factory =
          mock(DefaultKafkaConsumerFactory.class);
      KafkaConsumer<String, GenericRecord> consumer = mock(KafkaConsumer.class);
      when(consumer.assignment()).thenReturn(Collections.emptySet());
      when(consumer.partitionsFor(anyString(), any(Duration.class)))
          .thenReturn(Collections.emptyList());
      when(factory.createConsumer()).thenReturn(consumer);
      return factory;
    }
  }
}
