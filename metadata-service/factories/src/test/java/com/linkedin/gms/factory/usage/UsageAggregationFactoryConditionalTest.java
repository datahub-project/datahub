package com.linkedin.gms.factory.usage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.graphql.GraphqlUsageClassificationFactory;
import com.linkedin.metadata.config.DataHubConfiguration;
import com.linkedin.metadata.config.UsageAggregationConfiguration;
import com.linkedin.metadata.config.UsageConfiguration;
import com.linkedin.metadata.usage.instrumentation.UsageMetricsSessionEnricher;
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

/**
 * Verifies usage aggregation beans are conditional on {@code datahub.usage.aggregation.enabled}.
 */
public class UsageAggregationFactoryConditionalTest {

  @Configuration
  static class TestSystemOperationContextConfiguration {
    @Bean(name = "systemOperationContext")
    OperationContext systemOperationContext() {
      return org.mockito.Mockito.mock(OperationContext.class);
    }
  }

  private ApplicationContextRunner contextRunner;
  private AutoCloseable mockitoCloseable;

  @Mock private ConfigurationProvider configurationProvider;
  @Mock private DataHubConfiguration dataHubConfiguration;
  @Mock private UsageConfiguration usageConfiguration;
  @Mock private UsageAggregationConfiguration aggregationConfiguration;
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
    when(aggregationConfiguration.getFlush()).thenReturn(flushConfiguration);
    when(aggregationConfiguration.getMicrometerExport()).thenReturn(micrometerExport);
    when(flushConfiguration.getMaxCardinality()).thenReturn(10_000);
    when(flushConfiguration.getMaxWindowSeconds()).thenReturn(300L);
    when(flushConfiguration.getScheduledIntervalSeconds()).thenReturn(60L);

    contextRunner =
        new ApplicationContextRunner()
            .withUserConfiguration(
                UsageYamlConfigFactory.class,
                GraphqlUsageClassificationFactory.class,
                UsageAggregationFactory.class,
                TestSystemOperationContextConfiguration.class)
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
  public void enricherAbsentWhenAggregationDisabled() {
    contextRunner
        .withPropertyValues("datahub.usage.aggregation.enabled=false")
        .run(context -> assertThat(context).doesNotHaveBean(UsageMetricsSessionEnricher.class));
  }

  @Test
  public void enricherPresentWhenAggregationEnabled() {
    contextRunner
        .withPropertyValues("datahub.usage.aggregation.enabled=true")
        .run(
            context -> {
              assertThat(context).hasSingleBean(UsageMetricsSessionEnricher.class);
              assertThat(context)
                  .hasSingleBean(
                      com.linkedin.metadata.usage.store.InMemoryUsageAggregationStore.class);
              assertThat(context)
                  .hasSingleBean(
                      com.linkedin.metadata.config.usage.loader.UsageOperationsLoader.class);
            });
  }
}
