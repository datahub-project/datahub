package com.linkedin.metadata.usage;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.linkedin.metadata.config.usage.loader.UsageMetricRegistryLoader;
import com.linkedin.metadata.usage.flush.AdditiveUsageRow;
import com.linkedin.metadata.usage.flush.DistinctIdentityEntry;
import com.linkedin.metadata.usage.flush.DistinctUsageSnapshot;
import com.linkedin.metadata.usage.flush.FlushTrigger;
import com.linkedin.metadata.usage.flush.MicrometerUsageFlushSink;
import com.linkedin.metadata.usage.flush.UsageFlushBatch;
import com.linkedin.metadata.usage.registry.metrics.UsageMetricRegistry;
import io.datahubproject.metadata.context.usage.UsageActorClass;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MicrometerUsageFlushSinkTest {

  private static UsageMetricRegistry metricRegistry() {
    YAMLMapper yamlMapper = new YAMLMapper();
    yamlMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    return UsageMetricRegistry.loadBundled(
        new UsageMetricRegistryLoader(yamlMapper), java.util.List.of());
  }

  @Test
  public void testPublishesRequestAndByteCounters() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MicrometerUsageFlushSink sink = new MicrometerUsageFlushSink(metricRegistry(), registry);

    Map<String, String> dimensions =
        Map.of(
            "usage_operation",
            "metadata_read",
            "agent_class",
            "browser",
            "request_api",
            "openapi",
            "auth_channel",
            "session");
    UsageFlushBatch batch =
        new UsageFlushBatch(
            java.time.Instant.now(),
            java.time.Instant.now(),
            FlushTrigger.SCHEDULED,
            List.of(
                new AdditiveUsageRow("api_calls", UsageActorClass.REGULAR, dimensions, 3),
                new AdditiveUsageRow("input_bytes", UsageActorClass.REGULAR, dimensions, 100),
                new AdditiveUsageRow("output_bytes", UsageActorClass.REGULAR, dimensions, 200)),
            List.of());

    sink.publish(batch);

    Assert.assertEquals(registry.get("datahub_request_count").counter().count(), 3.0);
    Assert.assertEquals(
        registry.get("datahub_request_count").tag("auth_channel", "session").counter().count(),
        3.0);
    Assert.assertEquals(registry.get("datahub.usage.input_bytes").counter().count(), 100.0);
    Assert.assertEquals(registry.get("datahub.usage.output_bytes").counter().count(), 200.0);
  }

  @Test
  public void testDistinctSnapshotsExportActorClassGauge() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MicrometerUsageFlushSink sink = new MicrometerUsageFlushSink(metricRegistry(), registry);

    UsageFlushBatch batch =
        new UsageFlushBatch(
            java.time.Instant.now(),
            java.time.Instant.now(),
            FlushTrigger.SCHEDULED,
            List.of(),
            List.of(
                new DistinctUsageSnapshot(
                    "active_readers",
                    "support",
                    List.of(
                        new DistinctIdentityEntry(
                            "urn:li:corpuser:datahub",
                            io.datahubproject.metadata.context.usage.AttributionType.HUMAN)))));

    sink.publish(batch);

    Assert.assertEquals(
        registry
            .get("datahub.usage.active_identities")
            .tag("actor_class", "support")
            .tag("identity_metric", "active_readers")
            .gauge()
            .value(),
        1.0);
  }

  @Test
  public void testDistinctSnapshotsGaugeUsesActorClassOnlyTags() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MicrometerUsageFlushSink sink = new MicrometerUsageFlushSink(metricRegistry(), registry);

    UsageFlushBatch batch =
        new UsageFlushBatch(
            java.time.Instant.now(),
            java.time.Instant.now(),
            FlushTrigger.SCHEDULED,
            List.of(),
            List.of(
                new DistinctUsageSnapshot(
                    "active_users",
                    "regular",
                    List.of(
                        new DistinctIdentityEntry(
                            "urn:li:corpuser:a",
                            io.datahubproject.metadata.context.usage.AttributionType.HUMAN)))));

    sink.publish(batch);

    Assert.assertEquals(
        registry
            .get("datahub.usage.active_identities")
            .tag("actor_class", "regular")
            .tag("identity_metric", "active_users")
            .gauge()
            .value(),
        1.0);
    var gauge =
        registry
            .get("datahub.usage.active_identities")
            .tag("actor_class", "regular")
            .tag("identity_metric", "active_users")
            .gauge();
    Assert.assertNull(gauge.getId().getTag("auth_channel"));
  }

  @Test
  public void testPublishesActiveIdentityGauge() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MicrometerUsageFlushSink sink = new MicrometerUsageFlushSink(metricRegistry(), registry);

    UsageFlushBatch batch =
        new UsageFlushBatch(
            java.time.Instant.now(),
            java.time.Instant.now(),
            FlushTrigger.SCHEDULED,
            List.of(),
            List.of(
                new DistinctUsageSnapshot(
                    "active_users",
                    "regular",
                    List.of(
                        new DistinctIdentityEntry(
                            "urn:li:corpuser:a",
                            io.datahubproject.metadata.context.usage.AttributionType.HUMAN),
                        new DistinctIdentityEntry(
                            "urn:li:corpuser:b",
                            io.datahubproject.metadata.context.usage.AttributionType.HUMAN)))));

    sink.publish(batch);

    Assert.assertEquals(
        registry
            .get("datahub.usage.active_identities")
            .tag("identity_metric", "active_users")
            .tag("actor_class", "regular")
            .gauge()
            .value(),
        2.0);
  }

  @Test
  public void testPublishesZeroActiveIdentityGauge() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MicrometerUsageFlushSink sink = new MicrometerUsageFlushSink(metricRegistry(), registry);

    UsageFlushBatch batch =
        new UsageFlushBatch(
            java.time.Instant.now(),
            java.time.Instant.now(),
            FlushTrigger.SCHEDULED,
            List.of(),
            List.of(new DistinctUsageSnapshot("active_users", "support", List.of())));

    sink.publish(batch);

    Assert.assertEquals(
        registry
            .get("datahub.usage.active_identities")
            .tag("actor_class", "support")
            .tag("identity_metric", "active_users")
            .gauge()
            .value(),
        0.0);
  }
}
