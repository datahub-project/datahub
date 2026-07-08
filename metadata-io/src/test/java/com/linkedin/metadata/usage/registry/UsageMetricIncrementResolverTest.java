package com.linkedin.metadata.usage.registry;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.linkedin.metadata.config.usage.loader.UsageMetricRegistryLoader;
import com.linkedin.metadata.config.usage.loader.UsageOperationsLoader;
import com.linkedin.metadata.usage.registry.metrics.UsageMetricIncrementResolver;
import com.linkedin.metadata.usage.registry.metrics.UsageMetricRegistry;
import com.linkedin.metadata.usage.registry.operations.ActivityClass;
import com.linkedin.metadata.usage.registry.operations.ActivitySnapshot;
import com.linkedin.metadata.usage.registry.operations.UsageOperationsRegistry;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.RequestContext;
import java.util.Optional;
import org.testng.Assert;
import org.testng.annotations.Test;

public class UsageMetricIncrementResolverTest {

  private static UsageMetricRegistry ossRegistry() {
    YAMLMapper yamlMapper = new YAMLMapper();
    yamlMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    return UsageMetricRegistry.loadBundled(
        new UsageMetricRegistryLoader(yamlMapper), java.util.List.of());
  }

  private static RequestContext requestContextWithQuantity(int usageQuantity) {
    return RequestContext.builder()
        .actorUrn("urn:li:corpuser:test")
        .sourceIP("127.0.0.1")
        .requestAPI(RequestContext.RequestAPI.OPENAPI)
        .requestID("test")
        .userAgent("test")
        .usageQuantity(usageQuantity)
        .build();
  }

  @Test
  public void testAllOssYamlMetricsSupported() {
    for (UsageMetricRegistry.MetricDefinition metric : ossRegistry().apiUsageMetrics().values()) {
      Assert.assertTrue(UsageMetricIncrementResolver.isSupported(metric), metric.metricName());
    }
  }

  @Test
  public void testApiCallsIncrementForNonIngest() {
    UsageMetricRegistry.MetricDefinition metric = ossRegistry().apiUsageMetrics().get("api_calls");
    UsageOperationsRegistry.UsageOperationEntry entry =
        new UsageOperationsRegistry.UsageOperationEntry(
            io.datahubproject.metadata.context.usage.UsageOperation.METADATA_READ,
            ActivityClass.READ,
            false,
            1,
            java.util.Set.of());
    RequestContext requestContext =
        RequestContext.builder()
            .actorUrn("urn:li:corpuser:test")
            .sourceIP("127.0.0.1")
            .requestAPI(RequestContext.RequestAPI.OPENAPI)
            .requestID("test")
            .userAgent("test")
            .build();
    Assert.assertEquals(
        UsageMetricIncrementResolver.resolveRequestPhaseIncrement(metric, entry, requestContext),
        1L);
  }

  @Test
  public void testOutputBytesIsResponsePhaseOnly() {
    UsageMetricRegistry.MetricDefinition metric =
        ossRegistry().apiUsageMetrics().get("output_bytes");
    Assert.assertTrue(UsageMetricIncrementResolver.isResponsePhaseMetric(metric));
    Assert.assertEquals(
        UsageMetricIncrementResolver.resolveRequestPhaseIncrement(metric, null, null), 0L);
    Assert.assertEquals(
        UsageMetricIncrementResolver.resolveResponsePhaseIncrement(metric, 42L), 42L);
  }

  @Test
  public void testMicrometerMapsOssAdditiveMetrics() {
    UsageMetricRegistry registry = ossRegistry();
    Assert.assertEquals(
        UsageMetricIncrementResolver.micrometerCounterName(
            registry.apiUsageMetrics().get("api_calls")),
        Optional.of(MetricUtils.DATAHUB_REQUEST_COUNT));
    Assert.assertEquals(
        UsageMetricIncrementResolver.micrometerCounterName(
            registry.apiUsageMetrics().get("input_bytes")),
        Optional.of(UsageMetricIncrementResolver.INPUT_BYTES_METRIC));
  }

  @Test
  public void testOssCostUnitsFromUsageOperationsYaml() {
    UsageOperationsRegistry registry =
        UsageOperationsRegistry.loadOssOnly(new UsageOperationsLoader(yamlMapper()));
    Assert.assertEquals(registry.require("metadata_write").defaultCostUnits(), 1);
    Assert.assertEquals(registry.require("metadata_ingest").defaultCostUnits(), 1);
    Assert.assertEquals(registry.require("other_read").defaultCostUnits(), 0);
    Assert.assertEquals(registry.require("other_write").defaultCostUnits(), 0);
    Assert.assertEquals(registry.require("other_operations").defaultCostUnits(), 0);
  }

  @Test
  public void testIngestionEndpointUsesUsageQuantityForApiCalls() {
    UsageMetricRegistry.MetricDefinition metric = ossRegistry().apiUsageMetrics().get("api_calls");
    UsageOperationsRegistry ossOps =
        UsageOperationsRegistry.loadOssOnly(new UsageOperationsLoader(yamlMapper()));
    Assert.assertEquals(
        UsageMetricIncrementResolver.resolveRequestPhaseIncrement(
            metric, ossOps.require("metadata_ingest"), requestContextWithQuantity(4)),
        4L);
  }

  @Test
  public void testInputBytesAlwaysUsesMaterializedBody() {
    UsageMetricRegistry.MetricDefinition metric =
        ossRegistry().apiUsageMetrics().get("input_bytes");
    UsageOperationsRegistry ossOps =
        UsageOperationsRegistry.loadOssOnly(new UsageOperationsLoader(yamlMapper()));
    RequestContext requestContext =
        RequestContext.builder()
            .actorUrn("urn:li:corpuser:test")
            .sourceIP("127.0.0.1")
            .requestAPI(RequestContext.RequestAPI.OPENAPI)
            .requestID("test")
            .userAgent("test")
            .inputBytes(2048L)
            .build();
    Assert.assertEquals(
        UsageMetricIncrementResolver.resolveRequestPhaseIncrement(
            metric, ossOps.require("metadata_read"), requestContext),
        2048L);
  }

  @Test
  public void testCostProfileIncrementMultipliesDefaultCostUnits() {
    UsageMetricRegistry.MetricDefinition metric =
        new UsageMetricRegistry.MetricDefinition(
            "api_cost_units",
            UsageMetricRegistry.MergeKind.ADDITIVE,
            null,
            com.linkedin.metadata.usage.registry.metrics.ValueUnit.COST_UNITS,
            UsageMetricRegistry.EmitWhen.COST_PROFILE);
    UsageOperationsRegistry ossOps =
        UsageOperationsRegistry.loadOssOnly(new UsageOperationsLoader(yamlMapper()));
    Assert.assertEquals(
        UsageMetricIncrementResolver.resolveRequestPhaseIncrement(
            metric, ossOps.require("metadata_write"), requestContextWithQuantity(3)),
        3L);
  }

  @Test
  public void testBilledBytesMicrometerMapping() {
    UsageMetricRegistry.MetricDefinition metric =
        new UsageMetricRegistry.MetricDefinition(
            "billed_bytes",
            UsageMetricRegistry.MergeKind.ADDITIVE,
            null,
            com.linkedin.metadata.usage.registry.metrics.ValueUnit.OUTPUT_BYTES,
            UsageMetricRegistry.EmitWhen.ALWAYS);
    Assert.assertEquals(
        UsageMetricIncrementResolver.micrometerCounterName(metric),
        Optional.of(UsageMetricIncrementResolver.BILLED_BYTES_METRIC));
  }

  @Test
  public void testDistinctMetricWithUnsupportedEmitWhenIsNotSupported() {
    UsageMetricRegistry.MetricDefinition metric =
        new UsageMetricRegistry.MetricDefinition(
            "custom_distinct",
            UsageMetricRegistry.MergeKind.DISTINCT,
            "usage_identity",
            com.linkedin.metadata.usage.registry.metrics.ValueUnit.COUNT,
            UsageMetricRegistry.EmitWhen.ALWAYS);
    Assert.assertFalse(UsageMetricIncrementResolver.isSupported(metric));
  }

  @Test
  public void testResponsePhaseIncrementIgnoresNonPositiveBytes() {
    UsageMetricRegistry.MetricDefinition metric =
        ossRegistry().apiUsageMetrics().get("output_bytes");
    Assert.assertEquals(UsageMetricIncrementResolver.resolveResponsePhaseIncrement(metric, 0), 0L);
    Assert.assertEquals(UsageMetricIncrementResolver.resolveResponsePhaseIncrement(metric, -1), 0L);
  }

  @Test
  public void testIngestionRequestInputBytesRequiresIngestionEndpoint() {
    UsageMetricRegistry.MetricDefinition metric =
        new UsageMetricRegistry.MetricDefinition(
            "ingest_input",
            UsageMetricRegistry.MergeKind.ADDITIVE,
            null,
            com.linkedin.metadata.usage.registry.metrics.ValueUnit.INPUT_BYTES,
            UsageMetricRegistry.EmitWhen.INGESTION_REQUEST);
    UsageOperationsRegistry ossOps =
        UsageOperationsRegistry.loadOssOnly(new UsageOperationsLoader(yamlMapper()));
    RequestContext ingestContext =
        RequestContext.builder()
            .actorUrn("urn:li:corpuser:test")
            .sourceIP("127.0.0.1")
            .requestAPI(RequestContext.RequestAPI.OPENAPI)
            .requestID("test")
            .userAgent("test")
            .inputBytes(512L)
            .build();
    Assert.assertEquals(
        UsageMetricIncrementResolver.resolveRequestPhaseIncrement(
            metric, ossOps.require("metadata_ingest"), ingestContext),
        512L);
    Assert.assertEquals(
        UsageMetricIncrementResolver.resolveRequestPhaseIncrement(
            metric, ossOps.require("metadata_read"), ingestContext),
        -1L);
  }

  @Test
  public void testOtherWriteExcludedFromActiveWritersDistinctMetric() {
    UsageMetricRegistry.MetricDefinition activeWriters =
        ossRegistry().apiUsageMetrics().get("active_writers");
    UsageOperationsRegistry ossOps =
        UsageOperationsRegistry.loadOssOnly(new UsageOperationsLoader(yamlMapper()));
    ActivitySnapshot writeActivity = ActivitySnapshot.fromActivityClass(ActivityClass.WRITE);
    ActivitySnapshot operationActivity =
        ActivitySnapshot.fromActivityClass(ActivityClass.OPERATION);

    Assert.assertTrue(
        UsageMetricIncrementResolver.shouldEmitDistinct(
            ossRegistry().apiUsageMetrics().get("active_users"),
            writeActivity,
            ossOps.require("other_write")));
    Assert.assertFalse(
        UsageMetricIncrementResolver.shouldEmitDistinct(
            activeWriters, writeActivity, ossOps.require("other_write")));
    Assert.assertTrue(
        UsageMetricIncrementResolver.shouldEmitDistinct(
            activeWriters, writeActivity, ossOps.require("metadata_write")));
    Assert.assertFalse(
        UsageMetricIncrementResolver.shouldEmitDistinct(
            activeWriters, writeActivity, ossOps.require("other_write")));
    Assert.assertTrue(
        UsageMetricIncrementResolver.shouldEmitDistinct(
            ossRegistry().apiUsageMetrics().get("active_users"),
            operationActivity,
            ossOps.require("other_operations")));
    Assert.assertTrue(
        UsageMetricIncrementResolver.shouldEmitDistinct(
            ossRegistry().apiUsageMetrics().get("active_readers"),
            operationActivity,
            ossOps.require("other_operations")));
    Assert.assertFalse(
        UsageMetricIncrementResolver.shouldEmitDistinct(
            activeWriters, operationActivity, ossOps.require("other_operations")));
  }

  private static YAMLMapper yamlMapper() {
    YAMLMapper yamlMapper = new YAMLMapper();
    yamlMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    return yamlMapper;
  }
}
