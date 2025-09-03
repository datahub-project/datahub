package com.linkedin.datahub.upgrade.system.debezium;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.datahub.upgrade.system.cdc.debezium.WaitForDebeziumReadyStep;
import com.linkedin.metadata.config.DebeziumConfiguration;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class WaitForDebeziumReadyStepTest {

  @Mock private OperationContext mockOpContext;

  private DebeziumConfiguration debeziumConfiguration;
  private KafkaConfiguration kafkaConfiguration;
  private KafkaProperties kafkaProperties;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    // Create real DebeziumConfiguration with test data
    debeziumConfiguration = new DebeziumConfiguration();
    debeziumConfiguration.setName("test-connector");
    debeziumConfiguration.setUrl("http://localhost:8083");

    // Base configuration for MySQL connector
    Map<String, String> config = new HashMap<>();
    config.put("connector.class", "io.debezium.connector.mysql.MySqlConnector");
    config.put("database.include.list", "testdb");
    config.put("table.include.list", "*.metadata_aspect_v2");
    config.put("topic.prefix", "test-prefix");
    debeziumConfiguration.setConfig(config);

    // Create real KafkaConfiguration with test data
    kafkaConfiguration = new KafkaConfiguration();
    kafkaConfiguration.setBootstrapServers("localhost:9092");

    // Create real KafkaProperties with test data
    kafkaProperties = new KafkaProperties();
    kafkaProperties.setBootstrapServers(Arrays.asList("localhost:9092"));
  }

  @Test
  public void testId() {
    WaitForDebeziumReadyStep step =
        new WaitForDebeziumReadyStep(
            mockOpContext, debeziumConfiguration, kafkaConfiguration, kafkaProperties);
    assertEquals(step.id(), "WaitForDebeziumReadyStep");
  }

  @Test
  public void testRetryCount() {
    WaitForDebeziumReadyStep step =
        new WaitForDebeziumReadyStep(
            mockOpContext, debeziumConfiguration, kafkaConfiguration, kafkaProperties);
    assertEquals(step.retryCount(), 20);
  }

  @Test
  public void testConstructor() {
    WaitForDebeziumReadyStep step =
        new WaitForDebeziumReadyStep(
            mockOpContext, debeziumConfiguration, kafkaConfiguration, kafkaProperties);
    assertNotNull(step);
  }

  @Test
  public void testExecutableReturnsFunction() {
    WaitForDebeziumReadyStep step =
        new WaitForDebeziumReadyStep(
            mockOpContext, debeziumConfiguration, kafkaConfiguration, kafkaProperties);
    assertNotNull(step.executable());
  }

  @Test
  public void testWithSystemPropertyUrl() {
    // Set system property for Kafka Connect URL
    System.setProperty("kafka.connect.url", "http://test-connect:8083");

    try {
      WaitForDebeziumReadyStep step =
          new WaitForDebeziumReadyStep(
              mockOpContext, debeziumConfiguration, kafkaConfiguration, kafkaProperties);
      assertNotNull(step);
    } finally {
      // Clean up system property
      System.clearProperty("kafka.connect.url");
    }
  }

  @Test
  public void testWithMultipleKafkaBrokers() {
    // Test with multiple Kafka brokers
    kafkaProperties.setBootstrapServers(
        Arrays.asList("broker1:9092", "broker2:9092", "broker3:9092"));

    WaitForDebeziumReadyStep step =
        new WaitForDebeziumReadyStep(
            mockOpContext, debeziumConfiguration, kafkaConfiguration, kafkaProperties);
    assertNotNull(step);
  }
}
