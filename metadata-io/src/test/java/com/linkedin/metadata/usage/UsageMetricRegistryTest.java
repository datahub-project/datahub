package com.linkedin.metadata.usage;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.linkedin.metadata.config.usage.loader.UsageMetricRegistryLoader;
import com.linkedin.metadata.usage.registry.metrics.UsageMetricIncrementResolver;
import com.linkedin.metadata.usage.registry.metrics.UsageMetricRegistry;
import org.testng.Assert;
import org.testng.annotations.Test;

public class UsageMetricRegistryTest {

  @Test
  public void testMetricRegistryLoadsApiUsageFamily() {
    YAMLMapper yamlMapper = new YAMLMapper();
    yamlMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    UsageMetricRegistryLoader loader = new UsageMetricRegistryLoader(yamlMapper);
    UsageMetricRegistry registry = UsageMetricRegistry.loadBundled(loader, java.util.List.of());
    Assert.assertFalse(registry.apiUsageMetrics().isEmpty());
    Assert.assertTrue(registry.apiUsageMetrics().containsKey("api_calls"));
  }

  @Test
  public void testAllOssAdditiveMetricsSupportedByIncrementResolver() {
    YAMLMapper yamlMapper = new YAMLMapper();
    yamlMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    UsageMetricRegistry registry =
        UsageMetricRegistry.loadBundled(
            new UsageMetricRegistryLoader(yamlMapper), java.util.List.of());
    for (UsageMetricRegistry.MetricDefinition metric : registry.apiUsageMetrics().values()) {
      Assert.assertTrue(
          UsageMetricIncrementResolver.isSupported(metric),
          "Unsupported metric: " + metric.metricName());
    }
  }
}
