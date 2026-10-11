package com.linkedin.gms.factory.system_telemetry;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.DataHubConfiguration;
import com.linkedin.metadata.config.UsageAggregationConfiguration;
import com.linkedin.metadata.config.UsageConfiguration;
import org.testng.annotations.Test;

public class MetricUtilsFactoryTest {

  @Test
  public void testSuppressWhenAggregationAndMicrometerExportEnabled() {
    assertTrue(MetricUtilsFactory.shouldSuppressLegacyRequestCountMicrometer(provider(true, true)));
  }

  @Test
  public void testSuppressWhenMicrometerExportUnset() {
    ConfigurationProvider provider = mock(ConfigurationProvider.class);
    DataHubConfiguration datahub = mock(DataHubConfiguration.class);
    UsageConfiguration usage = mock(UsageConfiguration.class);
    UsageAggregationConfiguration aggregation = new UsageAggregationConfiguration();
    aggregation.setEnabled(true);
    aggregation.setMicrometerExport(null);
    when(provider.getDatahub()).thenReturn(datahub);
    when(datahub.getUsage()).thenReturn(usage);
    when(usage.getAggregation()).thenReturn(aggregation);

    assertTrue(MetricUtilsFactory.shouldSuppressLegacyRequestCountMicrometer(provider));
  }

  @Test
  public void testKeepLegacyWhenAggregationDisabled() {
    assertFalse(
        MetricUtilsFactory.shouldSuppressLegacyRequestCountMicrometer(provider(false, true)));
  }

  @Test
  public void testKeepLegacyWhenMicrometerExportDisabled() {
    assertFalse(
        MetricUtilsFactory.shouldSuppressLegacyRequestCountMicrometer(provider(true, false)));
  }

  @Test
  public void testKeepLegacyWhenConfigMissing() {
    assertFalse(MetricUtilsFactory.shouldSuppressLegacyRequestCountMicrometer(null));
    assertFalse(
        MetricUtilsFactory.shouldSuppressLegacyRequestCountMicrometer(
            mock(ConfigurationProvider.class)));
  }

  private static ConfigurationProvider provider(
      boolean aggregationEnabled, boolean micrometerExportEnabled) {
    ConfigurationProvider provider = mock(ConfigurationProvider.class);
    DataHubConfiguration datahub = mock(DataHubConfiguration.class);
    UsageConfiguration usage = mock(UsageConfiguration.class);
    UsageAggregationConfiguration aggregation = new UsageAggregationConfiguration();
    aggregation.setEnabled(aggregationEnabled);
    UsageAggregationConfiguration.MicrometerExportConfiguration export =
        new UsageAggregationConfiguration.MicrometerExportConfiguration();
    export.setEnabled(micrometerExportEnabled);
    aggregation.setMicrometerExport(export);
    when(provider.getDatahub()).thenReturn(datahub);
    when(datahub.getUsage()).thenReturn(usage);
    when(usage.getAggregation()).thenReturn(aggregation);
    return provider;
  }
}
