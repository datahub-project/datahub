package com.linkedin.metadata.systemmetadata.metrics;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.EntityCountMetricsConfiguration;
import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountEntry;
import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountResult;
import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EntityCountMetricsPublisherTest {

  private KeyAspectEntityCountService keyAspectEntityCountService;
  private OperationContext systemOperationContext;
  private SimpleMeterRegistry registry;
  private MicrometerEntityCountMetricsSink micrometerSink;
  private EntityCountMetricsConfiguration config;
  private EntityCountMetricsPublisher publisher;

  @BeforeMethod
  public void setUp() {
    keyAspectEntityCountService = mock(KeyAspectEntityCountService.class);
    systemOperationContext = TestOperationContexts.systemContextNoSearchAuthorization();
    registry = new SimpleMeterRegistry();
    micrometerSink = new MicrometerEntityCountMetricsSink(registry);
    config = new EntityCountMetricsConfiguration();
    config.setUpdateIntervalSeconds(0);
    publisher =
        new EntityCountMetricsPublisher(
            keyAspectEntityCountService,
            systemOperationContext,
            micrometerSink,
            micrometerSink,
            config);
  }

  @Test
  public void refreshPublishesCountsWithConfiguredSkipCache() {
    config.setSkipCache(true);
    KeyAspectEntityCountResult result = sampleResult();
    when(keyAspectEntityCountService.getCounts(systemOperationContext, null, true))
        .thenReturn(result);

    publisher.refresh();

    verify(keyAspectEntityCountService).getCounts(systemOperationContext, null, true);
    assertEquals(
        registry
            .get(MicrometerEntityCountMetricsSink.ENTITY_COUNT_METRIC)
            .tag(MicrometerEntityCountMetricsSink.TAG_ENTITY_TYPE, "dataset")
            .tag(
                MicrometerEntityCountMetricsSink.TAG_REMOVAL_STATUS,
                MicrometerEntityCountMetricsSink.REMOVAL_STATUS_ACTIVE)
            .gauge()
            .value(),
        10.0);
  }

  @Test
  public void refreshUsesCacheWhenSkipCacheFalse() {
    config.setSkipCache(false);
    KeyAspectEntityCountResult result = sampleResult();
    when(keyAspectEntityCountService.getCounts(systemOperationContext, null, false))
        .thenReturn(result);

    publisher.refresh();

    verify(keyAspectEntityCountService).getCounts(systemOperationContext, null, false);
  }

  @Test
  public void refreshRecordsErrorWithoutPropagating() {
    when(keyAspectEntityCountService.getCounts(eq(systemOperationContext), isNull(), eq(false)))
        .thenThrow(new RuntimeException("ES unavailable"));

    publisher.refresh();

    assertEquals(micrometerSink.refreshDuration().count(), 1);
    assertEquals(
        registry.get(MicrometerEntityCountMetricsSink.REFRESH_ERRORS_METRIC).counter().count(),
        1.0);
    assertEquals(
        registry.get(MicrometerEntityCountMetricsSink.REFRESH_LAST_SUCCESS_METRIC).gauge().value(),
        0.0);
    assertTrue(
        registry.get(MicrometerEntityCountMetricsSink.REFRESH_DURATION_METRIC).timer().count()
            >= 1);
  }

  @Test
  public void refreshPublishesToAllRegisteredSinks() {
    RecordingEntityCountMetricsSink recordingSink = new RecordingEntityCountMetricsSink();
    EntityCountMetricsSinkComposer composer =
        new EntityCountMetricsSinkComposer(List.of(micrometerSink, recordingSink));
    EntityCountMetricsPublisher multiSinkPublisher =
        new EntityCountMetricsPublisher(
            keyAspectEntityCountService, systemOperationContext, composer, micrometerSink, config);
    KeyAspectEntityCountResult result = sampleResult();
    when(keyAspectEntityCountService.getCounts(systemOperationContext, null, false))
        .thenReturn(result);

    multiSinkPublisher.refresh();

    assertEquals(recordingSink.results(), List.of(result));
    assertEquals(
        registry
            .get(MicrometerEntityCountMetricsSink.ENTITY_COUNT_METRIC)
            .tag(MicrometerEntityCountMetricsSink.TAG_ENTITY_TYPE, "dataset")
            .tag(
                MicrometerEntityCountMetricsSink.TAG_REMOVAL_STATUS,
                MicrometerEntityCountMetricsSink.REMOVAL_STATUS_ACTIVE)
            .gauge()
            .value(),
        10.0);
  }

  @Test
  public void intervalZeroSchedulesOneShotRefresh() throws Exception {
    config.setUpdateIntervalSeconds(0);
    config.setInitialDelaySeconds(0);
    KeyAspectEntityCountResult result = sampleResult();
    when(keyAspectEntityCountService.getCounts(systemOperationContext, null, false))
        .thenReturn(result);

    EntityCountMetricsPublisher oneShotPublisher =
        new EntityCountMetricsPublisher(
            keyAspectEntityCountService,
            systemOperationContext,
            micrometerSink,
            micrometerSink,
            config);

    verify(keyAspectEntityCountService, timeout(3000).times(1))
        .getCounts(systemOperationContext, null, false);
    Thread.sleep(500);
    verify(keyAspectEntityCountService, times(1)).getCounts(systemOperationContext, null, false);

    oneShotPublisher.close();
  }

  @Test
  public void negativeIntervalsAreClampedToZero() throws Exception {
    config.setUpdateIntervalSeconds(-10);
    config.setInitialDelaySeconds(-5);
    KeyAspectEntityCountResult result = sampleResult();
    when(keyAspectEntityCountService.getCounts(systemOperationContext, null, false))
        .thenReturn(result);

    EntityCountMetricsPublisher clampedPublisher =
        new EntityCountMetricsPublisher(
            keyAspectEntityCountService,
            systemOperationContext,
            micrometerSink,
            micrometerSink,
            config);

    verify(keyAspectEntityCountService, timeout(3000).atLeastOnce())
        .getCounts(systemOperationContext, null, false);

    clampedPublisher.close();
  }

  private static KeyAspectEntityCountResult sampleResult() {
    return KeyAspectEntityCountResult.builder()
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
  }
}
