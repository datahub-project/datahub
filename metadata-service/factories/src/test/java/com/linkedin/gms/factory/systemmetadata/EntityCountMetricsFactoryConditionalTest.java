package com.linkedin.gms.factory.systemmetadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.DataHubConfiguration;
import com.linkedin.metadata.config.EntityCountMetricsConfiguration;
import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountService;
import com.linkedin.metadata.systemmetadata.metrics.EntityCountMetricsPublisher;
import com.linkedin.metadata.systemmetadata.metrics.EntityCountMetricsSinkComposer;
import com.linkedin.metadata.systemmetadata.metrics.MicrometerEntityCountMetricsSink;
import com.linkedin.metadata.systemmetadata.metrics.RecordingEntityCountMetricsSink;
import io.datahubproject.metadata.context.OperationContext;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EntityCountMetricsFactoryConditionalTest {

  @Configuration
  static class TestDependenciesConfiguration {
    @Bean(name = "systemOperationContext")
    OperationContext systemOperationContext() {
      return org.mockito.Mockito.mock(OperationContext.class);
    }

    @Bean(name = "keyAspectEntityCountService")
    KeyAspectEntityCountService keyAspectEntityCountService() {
      return org.mockito.Mockito.mock(KeyAspectEntityCountService.class);
    }
  }

  private ApplicationContextRunner contextRunner;
  private AutoCloseable mockitoCloseable;

  @Mock private ConfigurationProvider configurationProvider;
  @Mock private DataHubConfiguration dataHubConfiguration;
  @Mock private DataHubConfiguration.DataHubMetrics dataHubMetrics;
  @Mock private EntityCountMetricsConfiguration entityCountMetricsConfiguration;

  @BeforeMethod
  public void setUp() {
    mockitoCloseable = MockitoAnnotations.openMocks(this);

    when(configurationProvider.getDatahub()).thenReturn(dataHubConfiguration);
    when(dataHubConfiguration.getMetrics()).thenReturn(dataHubMetrics);
    when(dataHubMetrics.getEntityCounts()).thenReturn(entityCountMetricsConfiguration);
    when(entityCountMetricsConfiguration.getUpdateIntervalSeconds()).thenReturn(0L);
    when(entityCountMetricsConfiguration.getInitialDelaySeconds()).thenReturn(0L);
    when(entityCountMetricsConfiguration.isSkipCache()).thenReturn(false);

    contextRunner =
        new ApplicationContextRunner()
            .withUserConfiguration(
                EntityCountMetricsFactory.class, TestDependenciesConfiguration.class)
            .withBean(ConfigurationProvider.class, () -> configurationProvider)
            .withBean(SimpleMeterRegistry.class, SimpleMeterRegistry::new);
  }

  @AfterMethod
  public void tearDown() throws Exception {
    if (mockitoCloseable != null) {
      mockitoCloseable.close();
    }
  }

  @Test
  public void publisherAbsentWhenDisabled() {
    contextRunner
        .withPropertyValues("datahub.metrics.entityCounts.enabled=false")
        .run(
            context -> {
              assertThat(context).doesNotHaveBean(EntityCountMetricsPublisher.class);
              assertThat(context).doesNotHaveBean(MicrometerEntityCountMetricsSink.class);
              assertThat(context).doesNotHaveBean(EntityCountMetricsSinkComposer.class);
            });
  }

  @Test
  public void publisherPresentWhenEnabled() {
    contextRunner
        .withPropertyValues("datahub.metrics.entityCounts.enabled=true")
        .run(
            context -> {
              assertThat(context).hasSingleBean(EntityCountMetricsPublisher.class);
              assertThat(context).hasSingleBean(MicrometerEntityCountMetricsSink.class);
              assertThat(context).hasSingleBean(EntityCountMetricsSinkComposer.class);
            });
  }

  @Test
  public void publisherPresentByDefault() {
    contextRunner.run(
        context -> {
          assertThat(context).hasSingleBean(EntityCountMetricsPublisher.class);
          assertThat(context).hasSingleBean(MicrometerEntityCountMetricsSink.class);
          assertThat(context).hasSingleBean(EntityCountMetricsSinkComposer.class);
        });
  }

  @Test
  public void additionalSinkBeansAreWiredIntoComposer() {
    contextRunner
        .withUserConfiguration(AdditionalSinkConfiguration.class)
        .run(
            context -> {
              assertThat(context).hasSingleBean(RecordingEntityCountMetricsSink.class);
              EntityCountMetricsSinkComposer composer =
                  context.getBean(EntityCountMetricsSinkComposer.class);
              assertThat(composer).isNotNull();
            });
  }

  @Configuration
  static class AdditionalSinkConfiguration {
    @Bean
    RecordingEntityCountMetricsSink recordingEntityCountMetricsSink() {
      return new RecordingEntityCountMetricsSink();
    }
  }
}
