package com.linkedin.metadata.systemmetadata.metrics;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountEntry;
import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountResult;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MicrometerEntityCountMetricsSinkTest {

  private SimpleMeterRegistry registry;
  private MicrometerEntityCountMetricsSink sink;

  @BeforeMethod
  public void setUp() {
    registry = new SimpleMeterRegistry();
    sink = new MicrometerEntityCountMetricsSink(registry);
  }

  @Test
  public void publishRegistersGaugesForActiveAndSoftDeletedCounts() {
    KeyAspectEntityCountResult result =
        KeyAspectEntityCountResult.builder()
            .counts(
                List.of(
                    KeyAspectEntityCountEntry.builder()
                        .entityType("dataset")
                        .keyAspect("datasetKey")
                        .activeCount(100)
                        .softDeletedCount(5)
                        .build(),
                    KeyAspectEntityCountEntry.builder()
                        .entityType("chart")
                        .keyAspect("chartKey")
                        .activeCount(42)
                        .softDeletedCount(0)
                        .build()))
            .requestedTypes(List.of("dataset", "chart"))
            .computedAt(Instant.now())
            .cacheHit(false)
            .build();

    sink.publish(result);

    assertEquals(
        gaugeValue("dataset", MicrometerEntityCountMetricsSink.REMOVAL_STATUS_ACTIVE), 100.0);
    assertEquals(
        gaugeValue("dataset", MicrometerEntityCountMetricsSink.REMOVAL_STATUS_SOFT_DELETED), 5.0);
    assertEquals(gaugeValue("chart", MicrometerEntityCountMetricsSink.REMOVAL_STATUS_ACTIVE), 42.0);
    assertEquals(
        gaugeValue("chart", MicrometerEntityCountMetricsSink.REMOVAL_STATUS_SOFT_DELETED), 0.0);
  }

  @Test
  public void publishUpdatesExistingGaugesOnSubsequentRefresh() {
    KeyAspectEntityCountResult first = sampleResult("dataset", 10, 1);
    KeyAspectEntityCountResult second = sampleResult("dataset", 20, 3);

    sink.publish(first);
    sink.publish(second);

    assertEquals(
        gaugeValue("dataset", MicrometerEntityCountMetricsSink.REMOVAL_STATUS_ACTIVE), 20.0);
    assertEquals(
        gaugeValue("dataset", MicrometerEntityCountMetricsSink.REMOVAL_STATUS_SOFT_DELETED), 3.0);
    assertEquals(
        registry.get(MicrometerEntityCountMetricsSink.ENTITY_COUNT_METRIC).gauges().size(), 2);
  }

  @Test
  public void publishZerosStaleGaugesWhenEntityTypeDisappears() {
    KeyAspectEntityCountResult withChart =
        KeyAspectEntityCountResult.builder()
            .counts(
                List.of(
                    KeyAspectEntityCountEntry.builder()
                        .entityType("chart")
                        .keyAspect("chartKey")
                        .activeCount(42)
                        .softDeletedCount(3)
                        .build()))
            .requestedTypes(List.of("chart"))
            .computedAt(Instant.now())
            .cacheHit(false)
            .build();
    KeyAspectEntityCountResult datasetOnly =
        KeyAspectEntityCountResult.builder()
            .counts(
                List.of(
                    KeyAspectEntityCountEntry.builder()
                        .entityType("dataset")
                        .keyAspect("datasetKey")
                        .activeCount(10)
                        .softDeletedCount(1)
                        .build()))
            .requestedTypes(List.of("dataset"))
            .computedAt(Instant.now())
            .cacheHit(false)
            .build();

    sink.publish(withChart);
    sink.publish(datasetOnly);

    assertEquals(gaugeValue("chart", MicrometerEntityCountMetricsSink.REMOVAL_STATUS_ACTIVE), 0.0);
    assertEquals(
        gaugeValue("chart", MicrometerEntityCountMetricsSink.REMOVAL_STATUS_SOFT_DELETED), 0.0);
    assertEquals(
        gaugeValue("dataset", MicrometerEntityCountMetricsSink.REMOVAL_STATUS_ACTIVE), 10.0);
  }

  @Test
  public void recordRefreshSuccessAndErrorUpdateMetaMetrics() {
    sink.recordRefreshSuccess();
    sink.recordRefreshError();
    sink.recordRefreshError();

    assertTrue(
        registry.get(MicrometerEntityCountMetricsSink.REFRESH_LAST_SUCCESS_METRIC).gauge().value()
            > 0);
    assertEquals(
        registry.get(MicrometerEntityCountMetricsSink.REFRESH_ERRORS_METRIC).counter().count(),
        2.0);
  }

  @Test
  public void recordRefreshSuccessNotUpdatedOnErrorBeforeSuccess() {
    sink.recordRefreshError();

    assertEquals(
        registry.get(MicrometerEntityCountMetricsSink.REFRESH_LAST_SUCCESS_METRIC).gauge().value(),
        0.0);
    assertEquals(
        registry.get(MicrometerEntityCountMetricsSink.REFRESH_ERRORS_METRIC).counter().count(),
        1.0);
  }

  private static KeyAspectEntityCountResult sampleResult(
      String entityType, long activeCount, long softDeletedCount) {
    return KeyAspectEntityCountResult.builder()
        .counts(
            List.of(
                KeyAspectEntityCountEntry.builder()
                    .entityType(entityType)
                    .keyAspect(entityType + "Key")
                    .activeCount(activeCount)
                    .softDeletedCount(softDeletedCount)
                    .build()))
        .requestedTypes(List.of(entityType))
        .computedAt(Instant.now())
        .cacheHit(false)
        .build();
  }

  private double gaugeValue(String entityType, String removalStatus) {
    Gauge gauge =
        registry
            .get(MicrometerEntityCountMetricsSink.ENTITY_COUNT_METRIC)
            .tag(MicrometerEntityCountMetricsSink.TAG_ENTITY_TYPE, entityType)
            .tag(MicrometerEntityCountMetricsSink.TAG_REMOVAL_STATUS, removalStatus)
            .gauge();
    return gauge.value();
  }
}
