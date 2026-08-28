package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_STRUCT_PROPS_DISABLED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.linkedin.metadata.config.search.BuildIndicesConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.IndexConfiguration;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.responses.RawResponse;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.metadata.version.GitVersion;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.http.HttpEntity;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.core.CountRequest;
import org.opensearch.client.core.CountResponse;
import org.opensearch.client.tasks.GetTaskRequest;
import org.opensearch.client.tasks.GetTaskResponse;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Behavioural coverage for ES reindex-poll Micrometer wiring (ADR 2026-08-25 — next K8s
 * classification candidate after rollback).
 */
public class ESIndexBuilderMetricsTest {

  @Mock private SearchClientShim<?> searchClient;
  @Mock private ElasticSearchConfiguration elasticSearchConfiguration;
  @Mock private BuildIndicesConfiguration buildIndicesConfig;
  @Mock private GitVersion gitVersion;

  private SimpleMeterRegistry meterRegistry;
  private OperationContext opContext;

  @BeforeMethod
  public void setUp() throws IOException {
    MockitoAnnotations.openMocks(this);
    meterRegistry = new SimpleMeterRegistry();
    MetricUtils metricUtils = MetricUtils.builder().registry(meterRegistry).build();
    opContext =
        TestOperationContexts.systemContextTraceNoSearchAuthorization(
            null,
            () ->
                SystemTelemetryContext.builder()
                    .metricUtils(metricUtils)
                    .tracer(OpenTelemetry.noop().getTracer("test"))
                    .build());

    RawResponse jvmResponse = mock(RawResponse.class);
    HttpEntity jvmEntity = mock(HttpEntity.class);
    String jvmJson =
        "{\"nodes\":{\"node1\":{\"roles\":[\"data\"],\"jvm\":{\"mem\":{\"heap_max_in_bytes\":17179869184}}}}}";
    when(jvmEntity.getContent()).thenReturn(new ByteArrayInputStream(jvmJson.getBytes()));
    when(jvmResponse.getEntity()).thenReturn(jvmEntity);
    when(searchClient.performLowLevelRequest(
            any(OperationFingerprint.class),
            argThat(req -> req != null && req.getEndpoint().contains("_nodes/stats"))))
        .thenReturn(jvmResponse);

    when(gitVersion.getVersion()).thenReturn("1.0.0");
    when(elasticSearchConfiguration.getBuildIndices()).thenReturn(buildIndicesConfig);
    when(buildIndicesConfig.isReindexOptimizationEnabled()).thenReturn(true);
    when(buildIndicesConfig.getReindexBatchSize()).thenReturn(5000);
    when(buildIndicesConfig.getReindexMaxSlices()).thenReturn(256);
    when(buildIndicesConfig.getReindexNoProgressRetryMinutes()).thenReturn(0);
    when(buildIndicesConfig.getCountRetryMaxAttempts()).thenReturn(1);
    when(buildIndicesConfig.getCountRetryWaitSeconds()).thenReturn(0);
  }

  @Test
  public void testPollCompleteEmitsLaunchesEntitiesPagesAndDuration() throws Throwable {
    ESIndexBuilder builder = setupPollBuilder(1000L);
    stubTaskCompleted(true);

    ESIndexBuilder.PollReindexResult result =
        builder.pollReindexCompletion(
            opContext, "src_index", "dest_index", () -> 1000L, 1, new HashMap<>(), "node1:42");

    assertTrue(result.completed());

    Counter launches =
        meterRegistry
            .find(ESIndexBuilder.METRIC_PREFIX + ".launches")
            .tag("operation_type", ESIndexBuilder.OPERATION_TYPE)
            .tag("phase", ESIndexBuilder.PHASE_POLL)
            .counter();
    assertNotNull(launches);
    assertEquals(launches.count(), 1.0);

    Counter entities =
        meterRegistry
            .find(ESIndexBuilder.METRIC_PREFIX + ".entities_processed")
            .tag("operation_type", ESIndexBuilder.OPERATION_TYPE)
            .tag("phase", ESIndexBuilder.PHASE_POLL)
            .counter();
    assertNotNull(entities);
    assertEquals(entities.count(), 1000.0);

    Counter pages =
        meterRegistry
            .find(ESIndexBuilder.METRIC_PREFIX + ".pages")
            .tag("phase", ESIndexBuilder.PHASE_POLL)
            .counter();
    assertNotNull(pages);
    assertTrue(pages.count() >= 1.0);

    Timer duration =
        meterRegistry
            .find(ESIndexBuilder.METRIC_PREFIX + ".duration")
            .tag("phase", ESIndexBuilder.PHASE_POLL)
            .tag("status", "completed")
            .timer();
    assertNotNull(duration);
    assertEquals(duration.count(), 1L);
  }

  @Test
  public void testPollTimeoutRecordsFailedDuration() throws Throwable {
    ESIndexBuilder builder = setupPollBuilder(900L);
    stubTaskCompleted(true);

    ESIndexBuilder.PollReindexResult result =
        builder.pollReindexCompletion(
            opContext, "src_index", "dest_index", () -> 1000L, 1, new HashMap<>(), "node1:99");

    assertTrue(!result.completed());

    Counter errors =
        meterRegistry
            .find(ESIndexBuilder.METRIC_PREFIX + ".errors")
            .tag("error_type", "timeout")
            .counter();
    assertNotNull(errors);
    assertEquals(errors.count(), 1.0);

    Timer duration =
        meterRegistry
            .find(ESIndexBuilder.METRIC_PREFIX + ".duration")
            .tag("status", "failed")
            .timer();
    assertNotNull(duration);
    assertEquals(duration.count(), 1L);
  }

  private ESIndexBuilder setupPollBuilder(long destDocCount) throws IOException {
    when(elasticSearchConfiguration.getIndex())
        .thenReturn(
            IndexConfiguration.builder()
                .numShards(1)
                .numReplicas(1)
                .numRetries(0)
                .refreshIntervalSeconds(1)
                .maxReindexHours(1)
                .build());

    CountResponse countResponse = mock(CountResponse.class);
    when(countResponse.getCount()).thenReturn(destDocCount);
    when(searchClient.count(
            any(OperationContext.class), any(CountRequest.class), any(RequestOptions.class)))
        .thenReturn(countResponse);
    when(searchClient.refreshIndex(
            any(OperationFingerprint.class),
            any(org.opensearch.action.admin.indices.refresh.RefreshRequest.class),
            any(RequestOptions.class)))
        .thenReturn(mock(org.opensearch.action.admin.indices.refresh.RefreshResponse.class));

    return new ESIndexBuilder(
        searchClient,
        elasticSearchConfiguration,
        TEST_ES_STRUCT_PROPS_DISABLED,
        Map.of(),
        gitVersion);
  }

  private void stubTaskCompleted(boolean completed) throws IOException {
    GetTaskResponse task = mock(GetTaskResponse.class);
    when(task.isCompleted()).thenReturn(completed);
    when(searchClient.getTask(any(GetTaskRequest.class), any(RequestOptions.class)))
        .thenReturn(Optional.of(task));
  }
}
