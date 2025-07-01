package com.linkedin.gms.factory.telemetry;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.telemetry.TrackingService;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.telemetry.TelemetryConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.version.GitVersion;
import com.mixpanel.mixpanelapi.MessageBuilder;
import com.mixpanel.mixpanelapi.MixpanelAPI;
import io.datahubproject.metadata.services.SecretService;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@TestPropertySource(
    properties = {
      "telemetry.enabledServer=false",
      "kafka.topics.dataHubUsage=DataHubUsageEvent_v1",
      "kafka.bootstrapServers=localhost:9092",
      "kafka.schemaRegistry.type=INTERNAL",
      "kafka.schemaRegistry.url=http://localhost:8081",
      "kafka.producer.retryCount=3",
      "kafka.producer.deliveryTimeout=30000",
      "kafka.producer.requestTimeout=3000",
      "kafka.producer.backoffTimeout=500",
      "kafka.producer.compressionType=snappy",
      "kafka.producer.maxRequestSize=5242880",
      "kafka.serde.usageEvent.key.serializer=org.apache.kafka.common.serialization.StringSerializer",
      "kafka.serde.usageEvent.key.deserializer=org.apache.kafka.common.serialization.StringDeserializer",
      "kafka.serde.usageEvent.value.serializer=org.apache.kafka.common.serialization.StringSerializer",
      "kafka.serde.usageEvent.value.deserializer=org.apache.kafka.common.serialization.StringDeserializer"
    })
@ContextConfiguration(classes = {TrackingServiceFactoryTest.TestConfig.class})
public class TrackingServiceFactoryTest extends AbstractTestNGSpringContextTests {

  @Test
  public void testTrackingServiceWithTelemetryDisabled() throws Exception {
    // Get the TrackingService bean
    TrackingService trackingService =
        applicationContext.getBean("trackingService", TrackingService.class);

    // Verify that the TrackingService is created
    assertNotNull(
        trackingService, "TrackingService should be created even when telemetry is disabled");

    // Use reflection to access the Mixpanel components
    java.lang.reflect.Field mixpanelAPIField =
        trackingService.getClass().getDeclaredField("mixpanelAPI");
    mixpanelAPIField.setAccessible(true);
    Object mixpanelAPI = mixpanelAPIField.get(trackingService);

    java.lang.reflect.Field mixpanelMessageBuilderField =
        trackingService.getClass().getDeclaredField("messageBuilder");
    mixpanelMessageBuilderField.setAccessible(true);
    Object mixpanelMessageBuilder = mixpanelMessageBuilderField.get(trackingService);

    // Verify that Mixpanel components are null when telemetry is disabled
    assertNull(mixpanelAPI, "MixpanelAPI should be null when telemetry is disabled");
    assertNull(
        mixpanelMessageBuilder, "MixpanelMessageBuilder should be null when telemetry is disabled");

    // Use reflection to access the Kafka producer
    java.lang.reflect.Field dataHubUsageProducerField =
        trackingService.getClass().getDeclaredField("dataHubUsageProducer");
    dataHubUsageProducerField.setAccessible(true);
    Object dataHubUsageProducer = dataHubUsageProducerField.get(trackingService);

    // Verify that the Kafka producer is still available
    assertNotNull(
        dataHubUsageProducer, "Kafka producer should be available even when telemetry is disabled");
  }

  @Test
  public void testTrackingServiceWithTelemetryEnabled() throws Exception {
    // Create a new application context with telemetry enabled
    org.springframework.context.annotation.AnnotationConfigApplicationContext enabledContext =
        new org.springframework.context.annotation.AnnotationConfigApplicationContext();
    enabledContext.register(TelemetryEnabledTestConfig.class);
    enabledContext.refresh();

    // Get the TrackingService bean
    TrackingService trackingService =
        enabledContext.getBean("trackingService", TrackingService.class);

    // Verify that the TrackingService is created
    assertNotNull(trackingService, "TrackingService should be created when telemetry is enabled");

    // Use reflection to access the Mixpanel components
    java.lang.reflect.Field mixpanelAPIField =
        trackingService.getClass().getDeclaredField("mixpanelAPI");
    mixpanelAPIField.setAccessible(true);
    Object mixpanelAPI = mixpanelAPIField.get(trackingService);

    java.lang.reflect.Field mixpanelMessageBuilderField =
        trackingService.getClass().getDeclaredField("messageBuilder");
    mixpanelMessageBuilderField.setAccessible(true);
    Object mixpanelMessageBuilder = mixpanelMessageBuilderField.get(trackingService);

    // Verify that Mixpanel components are initialized when telemetry is enabled
    assertNotNull(mixpanelAPI, "MixpanelAPI should be initialized when telemetry is enabled");
    assertNotNull(
        mixpanelMessageBuilder,
        "MixpanelMessageBuilder should be initialized when telemetry is enabled");

    // Use reflection to access the Kafka producer
    java.lang.reflect.Field dataHubUsageProducerField =
        trackingService.getClass().getDeclaredField("dataHubUsageProducer");
    dataHubUsageProducerField.setAccessible(true);
    Object dataHubUsageProducer = dataHubUsageProducerField.get(trackingService);

    // Verify that the Kafka producer is still available
    assertNotNull(
        dataHubUsageProducer, "Kafka producer should be available when telemetry is enabled");

    // Close the context
    enabledContext.close();
  }

  @Configuration
  @ComponentScan(basePackages = "com.linkedin.gms.factory.telemetry")
  static class TestConfig {

    @Bean
    @Primary
    public ConfigurationProvider configurationProvider() {
      ConfigurationProvider mockProvider = mock(ConfigurationProvider.class);
      when(mockProvider.getTelemetry()).thenReturn(telemetryConfiguration());
      when(mockProvider.getKafka()).thenReturn(kafkaConfiguration());
      return mockProvider;
    }

    @Bean
    @Primary
    public TelemetryConfiguration telemetryConfiguration() {
      TelemetryConfiguration config = new TelemetryConfiguration();
      config.setEnabledServer(false);
      return config;
    }

    @Bean
    @Primary
    public com.linkedin.metadata.config.kafka.KafkaConfiguration kafkaConfiguration() {
      com.linkedin.metadata.config.kafka.KafkaConfiguration config =
          new com.linkedin.metadata.config.kafka.KafkaConfiguration();
      com.linkedin.metadata.config.kafka.TopicsConfiguration topicsConfig =
          new com.linkedin.metadata.config.kafka.TopicsConfiguration();
      topicsConfig.setDataHubUsage("DataHubUsageEvent_v1");
      config.setTopics(topicsConfig);
      return config;
    }

    @Bean
    @Qualifier("mixpanelApi")
    public MixpanelAPI mixpanelAPI() {
      return mock(MixpanelAPI.class);
    }

    @Bean
    @Qualifier("mixpanelMessageBuilder")
    public MessageBuilder mixpanelMessageBuilder() {
      return mock(MessageBuilder.class);
    }

    @Bean
    @Qualifier("dataHubSecretService")
    public SecretService secretService() {
      return mock(SecretService.class);
    }

    @Bean
    @Qualifier("entityService")
    public EntityService<?> entityService() {
      return mock(EntityService.class);
    }

    @Bean
    @Qualifier("gitVersion")
    public GitVersion gitVersion() {
      return mock(GitVersion.class);
    }

    @Bean
    @Qualifier("dataHubUsageProducer")
    public Producer<String, String> dataHubUsageProducer() {
      return mock(Producer.class);
    }
  }

  @Configuration
  @ComponentScan(basePackages = "com.linkedin.gms.factory.telemetry")
  static class TelemetryEnabledTestConfig {

    @Bean
    @Primary
    public TelemetryConfiguration telemetryConfiguration() {
      TelemetryConfiguration config = new TelemetryConfiguration();
      config.setEnabledServer(true);
      return config;
    }

    @Bean
    @Primary
    public ConfigurationProvider configurationProvider() {
      ConfigurationProvider mockProvider = mock(ConfigurationProvider.class);
      when(mockProvider.getTelemetry()).thenReturn(telemetryConfiguration());
      when(mockProvider.getKafka()).thenReturn(kafkaConfiguration());
      return mockProvider;
    }

    @Bean
    @Primary
    public com.linkedin.metadata.config.kafka.KafkaConfiguration kafkaConfiguration() {
      com.linkedin.metadata.config.kafka.KafkaConfiguration config =
          new com.linkedin.metadata.config.kafka.KafkaConfiguration();
      com.linkedin.metadata.config.kafka.TopicsConfiguration topicsConfig =
          new com.linkedin.metadata.config.kafka.TopicsConfiguration();
      topicsConfig.setDataHubUsage("DataHubUsageEvent_v1");
      config.setTopics(topicsConfig);
      return config;
    }

    @Bean
    @Qualifier("mixpanelApi")
    public MixpanelAPI mixpanelAPI() {
      return mock(MixpanelAPI.class);
    }

    @Bean
    @Qualifier("mixpanelMessageBuilder")
    public MessageBuilder mixpanelMessageBuilder() {
      return mock(MessageBuilder.class);
    }

    @Bean
    @Qualifier("dataHubSecretService")
    public SecretService secretService() {
      return mock(SecretService.class);
    }

    @Bean
    @Qualifier("entityService")
    public EntityService<?> entityService() {
      return mock(EntityService.class);
    }

    @Bean
    @Qualifier("gitVersion")
    public GitVersion gitVersion() {
      return mock(GitVersion.class);
    }

    @Bean
    @Qualifier("dataHubUsageProducer")
    public Producer<String, String> dataHubUsageProducer() {
      return mock(Producer.class);
    }
  }
}
