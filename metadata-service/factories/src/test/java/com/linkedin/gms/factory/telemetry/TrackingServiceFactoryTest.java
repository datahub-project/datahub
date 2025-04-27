package com.linkedin.gms.factory.telemetry;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.telemetry.TrackingService;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.telemetry.MixpanelConfiguration;
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

@TestPropertySource(properties = {"telemetry.enabledServer=false"})
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
      return mockProvider;
    }

    @Bean
    @Primary
    public TelemetryConfiguration telemetryConfiguration() {
      TelemetryConfiguration config = new TelemetryConfiguration();
      config.setEnabledServer(false);

      // Create and configure MixpanelConfiguration
      MixpanelConfiguration mixpanelConfig = new MixpanelConfiguration();
      mixpanelConfig.setEnabled(true); // Make sure Mixpanel is enabled too
      config.setMixpanel(mixpanelConfig);

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
    @Qualifier("mixpanelConfiguration")
    public MixpanelConfiguration mixpanelConfiguration() {
      return mock(MixpanelConfiguration.class);
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
      MixpanelConfiguration mixpanelConfig = new MixpanelConfiguration();
      config.setMixpanel(mixpanelConfig);
      return config;
    }

    @Bean
    @Primary
    public ConfigurationProvider configurationProvider() {
      ConfigurationProvider mockProvider = mock(ConfigurationProvider.class);
      when(mockProvider.getTelemetry()).thenReturn(telemetryConfiguration());
      return mockProvider;
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
    @Qualifier("mixpanelConfiguration")
    public MixpanelConfiguration mixpanelConfiguration() {
      return mock(MixpanelConfiguration.class);
    }

    @Bean
    @Qualifier("dataHubUsageProducer")
    public Producer<String, String> dataHubUsageProducer() {
      return mock(Producer.class);
    }
  }
}
