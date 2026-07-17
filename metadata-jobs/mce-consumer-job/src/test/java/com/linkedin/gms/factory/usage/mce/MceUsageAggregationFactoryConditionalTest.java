package com.linkedin.gms.factory.usage.mce;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.usage.UsageYamlConfigFactory;
import com.linkedin.metadata.config.DataHubConfiguration;
import com.linkedin.metadata.config.UsageAggregationConfiguration;
import com.linkedin.metadata.config.UsageConfiguration;
import com.linkedin.metadata.config.usage.loader.UsageMetricRegistryLoader;
import com.linkedin.metadata.config.usage.loader.UsageOperationsLoader;
import com.linkedin.metadata.kafka.usage.UsageQueueIngestRecorder;
import com.linkedin.metadata.usage.flush.AdaptiveFlushCoordinator;
import com.linkedin.metadata.usage.flush.MicrometerUsageFlushSink;
import com.linkedin.metadata.usage.flush.UsageFlushSinkComposer;
import com.linkedin.metadata.usage.identity.UsageActorClassResolver;
import com.linkedin.metadata.usage.registry.metrics.UsageMetricRegistry;
import com.linkedin.metadata.usage.registry.operations.UsageOperationsRegistry;
import com.linkedin.metadata.usage.store.InMemoryUsageAggregationStore;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Verifies MCE usage aggregation beans are conditional on {@code
 * datahub.usage.aggregation.enabled}.
 */
public class MceUsageAggregationFactoryConditionalTest {

  private ApplicationContextRunner contextRunner;
  private AutoCloseable mockitoCloseable;

  @Mock private ConfigurationProvider configurationProvider;
  @Mock private DataHubConfiguration dataHubConfiguration;
  @Mock private UsageConfiguration usageConfiguration;
  @Mock private UsageAggregationConfiguration aggregationConfiguration;
  @Mock private UsageAggregationConfiguration.DimensionsConfiguration dimensionsConfiguration;
  @Mock private UsageAggregationConfiguration.FlushConfiguration flushConfiguration;

  @BeforeMethod
  public void setUp() {
    mockitoCloseable = MockitoAnnotations.openMocks(this);

    UsageAggregationConfiguration.MicrometerExportConfiguration micrometerExport =
        new UsageAggregationConfiguration.MicrometerExportConfiguration();
    micrometerExport.setEnabled(true);

    when(configurationProvider.getDatahub()).thenReturn(dataHubConfiguration);
    when(dataHubConfiguration.getUsage()).thenReturn(usageConfiguration);
    when(usageConfiguration.getAggregation()).thenReturn(aggregationConfiguration);
    when(aggregationConfiguration.getDimensions()).thenReturn(dimensionsConfiguration);
    when(aggregationConfiguration.getFlush()).thenReturn(flushConfiguration);
    when(aggregationConfiguration.getMicrometerExport()).thenReturn(micrometerExport);
    when(flushConfiguration.getMaxCardinality()).thenReturn(10_000);
    when(flushConfiguration.getMaxWindowSeconds()).thenReturn(300L);
    when(flushConfiguration.getScheduledIntervalSeconds()).thenReturn(60L);
    when(flushConfiguration.getRetryAttempts()).thenReturn(3);
    when(flushConfiguration.getRetryInitialBackoffMillis()).thenReturn(100L);

    contextRunner =
        new ApplicationContextRunner()
            .withUserConfiguration(UsageYamlConfigFactory.class, MceUsageAggregationFactory.class)
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
  public void beansAbsentWhenAggregationDisabled() {
    contextRunner
        .withPropertyValues("datahub.usage.aggregation.enabled=false")
        .run(
            context -> {
              assertTrue(context.getBeansOfType(UsageQueueIngestRecorder.class).isEmpty());
              assertTrue(context.getBeansOfType(InMemoryUsageAggregationStore.class).isEmpty());
            });
  }

  @Test
  public void beansPresentWhenAggregationEnabled() {
    contextRunner
        .withPropertyValues("datahub.usage.aggregation.enabled=true")
        .run(
            context -> {
              assertNotNull(context.getBean(UsageQueueIngestRecorder.class));
              assertNotNull(context.getBean(UsageOperationsLoader.class));
              assertNotNull(context.getBean(UsageMetricRegistryLoader.class));
              assertNotNull(context.getBean(UsageOperationsRegistry.class));
              assertNotNull(context.getBean(UsageMetricRegistry.class));
              assertNotNull(context.getBean(UsageActorClassResolver.class));
              assertNotNull(context.getBean(MicrometerUsageFlushSink.class));
              assertNotNull(context.getBean(UsageFlushSinkComposer.class));
              assertNotNull(context.getBean(InMemoryUsageAggregationStore.class));
              assertNotNull(context.getBean(AdaptiveFlushCoordinator.class));
            });
  }

  @Test
  public void micrometerSinkAbsentWhenExportDisabled() {
    contextRunner
        .withPropertyValues(
            "datahub.usage.aggregation.enabled=true",
            "datahub.usage.aggregation.micrometerExport.enabled=false")
        .run(
            context -> {
              assertNotNull(context.getBean(UsageQueueIngestRecorder.class));
              assertTrue(context.getBeansOfType(MicrometerUsageFlushSink.class).isEmpty());
            });
  }

  @Test
  public void shutdownClearsAdaptiveFlushCoordinator() {
    contextRunner
        .withPropertyValues("datahub.usage.aggregation.enabled=true")
        .run(
            context -> {
              MceUsageAggregationFactory factory =
                  context.getBean(MceUsageAggregationFactory.class);
              assertNotNull(context.getBean(AdaptiveFlushCoordinator.class));
              factory.shutdownMceUsageAggregationFlush();
              factory.shutdownMceUsageAggregationFlush();
            });
  }
}
