package com.linkedin.metadata.service.util;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.form.DynamicFormAssignment;
import com.linkedin.form.FormInfo;
import com.linkedin.form.FormPrompt;
import com.linkedin.form.FormPromptArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Behavioural coverage for form-assignment Micrometer wiring. Each test names the regression it
 * catches (see vault build handoff).
 */
public class SearchBasedFormAssignmentManagerMetricsTest {

  private static final Urn FORM_URN = UrnUtils.getUrn("urn:li:form:metricsTestForm");
  private static final String PROMPT_ID = "prompt-1";

  private SimpleMeterRegistry meterRegistry;
  private MetricUtils metricUtils;
  private OperationContext opContext;

  @BeforeMethod
  public void setUp() {
    meterRegistry = new SimpleMeterRegistry();
    metricUtils = MetricUtils.builder().registry(meterRegistry).build();
    opContext =
        TestOperationContexts.systemContextTraceNoSearchAuthorization(
            null,
            () ->
                SystemTelemetryContext.builder()
                    .metricUtils(metricUtils)
                    .tracer(OpenTelemetry.noop().getTracer("test"))
                    .build());
  }

  @Test
  public void testThreePageScrollEmitsEntitiesAndDuration() throws Exception {
    final List<Urn> page1 = datasetUrns(0, 2);
    final List<Urn> page2 = datasetUrns(2, 2);
    final List<Urn> page3 = datasetUrns(4, 2);
    final SystemEntityClient client = mockClientForPages(List.of(page1, page2, page3));

    SearchBasedFormAssignmentManager.apply(scrollRequest(FORM_URN, 2, client));

    Counter entities =
        meterRegistry
            .find(SearchBasedFormAssignmentManager.METRIC_PREFIX + ".entities_processed")
            .tag("operation_type", SearchBasedFormAssignmentManager.OPERATION_TYPE)
            .tag("phase", "assign")
            .counter();
    assertNotNull(entities, "entities_processed must exist in registry");
    assertEquals(entities.count(), 6.0);

    Timer duration =
        meterRegistry
            .find(SearchBasedFormAssignmentManager.METRIC_PREFIX + ".duration")
            .tag("operation_type", SearchBasedFormAssignmentManager.OPERATION_TYPE)
            .tag("phase", "assign")
            .tag("status", "completed")
            .timer();
    assertNotNull(duration, "duration must exist in registry");
    assertEquals(duration.count(), 1L);

    Counter launches =
        meterRegistry
            .find(SearchBasedFormAssignmentManager.METRIC_PREFIX + ".launches")
            .tag("operation_type", SearchBasedFormAssignmentManager.OPERATION_TYPE)
            .tag("phase", "assign")
            .counter();
    assertNotNull(launches);
    assertEquals(launches.count(), 1.0);

    Counter pages =
        meterRegistry
            .find(SearchBasedFormAssignmentManager.METRIC_PREFIX + ".pages")
            .tag("operation_type", SearchBasedFormAssignmentManager.OPERATION_TYPE)
            .tag("phase", "assign")
            .counter();
    assertNotNull(pages, "pages must exist in registry");
    assertEquals(pages.count(), 3.0);
  }

  @Test
  public void testNullMetricUtilsStillAssigns() throws Exception {
    OperationContext noMetrics =
        TestOperationContexts.systemContextTraceNoSearchAuthorization(
            null,
            () ->
                SystemTelemetryContext.builder()
                    .metricUtils(null)
                    .tracer(OpenTelemetry.noop().getTracer("test"))
                    .build());
    final SystemEntityClient client = mockClientForPages(List.of(datasetUrns(0, 1)));

    SearchBasedFormAssignmentManager.apply(scrollRequest(noMetrics, FORM_URN, 1, client));

    // The scroll ran and the assignment was actually written. Asserting only that the registry is
    // empty would pass if apply() had returned immediately — absence of metrics and absence of work
    // look identical from a registry.
    verify(client, atLeastOnce())
        .scrollAcrossEntities(
            any(), anyList(), anyString(), any(), any(), anyString(), anyList(), anyInt());
    verify(client, atLeastOnce()).batchIngestProposals(any(), anyCollection(), eq(false));
    assertTrue(
        meterRegistry.getMeters().isEmpty(), "no meters expected when MetricUtils is absent");
  }

  @Test
  public void testRemoteInvocationRecordsErrorAndRethrows() throws Exception {
    final SystemEntityClient client = mockClientFailingOnSecondScrollWithRemoteInvocation();

    try {
      SearchBasedFormAssignmentManager.apply(scrollRequest(FORM_URN, 1, client));
      fail("expected RuntimeException wrapping RemoteInvocationException");
    } catch (RuntimeException e) {
      assertTrue(e.getCause() instanceof RemoteInvocationException);
    }

    Counter errors =
        meterRegistry
            .find(SearchBasedFormAssignmentManager.METRIC_PREFIX + ".errors")
            .tag("operation_type", SearchBasedFormAssignmentManager.OPERATION_TYPE)
            .tag("phase", "assign")
            .tag("error_type", "remote_invocation")
            .counter();
    assertNotNull(
        errors, "errors counter missing — RIE catch must run before finally emits duration");
    assertEquals(errors.count(), 1.0);
  }

  @Test
  public void testFailedRunRecordsDurationTaggedFailed() throws Exception {
    final SystemEntityClient client = mockClientFailingOnSecondScrollWithRemoteInvocation();

    try {
      SearchBasedFormAssignmentManager.apply(scrollRequest(FORM_URN, 1, client));
      fail("expected RuntimeException wrapping RemoteInvocationException");
    } catch (RuntimeException e) {
      assertTrue(e.getCause() instanceof RemoteInvocationException);
    }

    Timer duration =
        meterRegistry
            .find(SearchBasedFormAssignmentManager.METRIC_PREFIX + ".duration")
            .tag("operation_type", SearchBasedFormAssignmentManager.OPERATION_TYPE)
            .tag("phase", "assign")
            .tag("status", "failed")
            .timer();
    assertNotNull(duration, "duration timer with status=failed must exist in registry");
    assertEquals(duration.count(), 1L);
  }

  @Test
  public void testUnexpectedFailureRecordsErrorAndFailedDuration() throws Exception {
    final SystemEntityClient client = mock(SystemEntityClient.class);
    stubFormDefinition(client);
    when(client.filterExistingUrns(any(), anyCollection()))
        .thenAnswer(inv -> new HashSet<>((java.util.Collection<Urn>) inv.getArgument(1)));
    stubBatchGetV2(client);

    AtomicInteger scrollCalls = new AtomicInteger();
    when(client.scrollAcrossEntities(
            any(), anyList(), anyString(), any(), any(), anyString(), anyList(), anyInt()))
        .thenAnswer(
            inv -> {
              int n = scrollCalls.getAndIncrement();
              if (n == 0) {
                return scrollPage(datasetUrns(0, 1), "scroll-1");
              }
              return scrollPage(datasetUrns(1, 1), null);
            });
    when(client.exists(any(), eq(FORM_URN))).thenReturn(true, false);

    try {
      SearchBasedFormAssignmentManager.apply(scrollRequest(FORM_URN, 1, client));
      fail("expected RuntimeException when form deleted mid-scroll");
    } catch (RuntimeException e) {
      assertTrue(
          e.getMessage().contains("does not exist"),
          "expected verifyEntityExists failure, got: " + e.getMessage());
    }

    Counter errors =
        meterRegistry
            .find(SearchBasedFormAssignmentManager.METRIC_PREFIX + ".errors")
            .tag("operation_type", SearchBasedFormAssignmentManager.OPERATION_TYPE)
            .tag("phase", "assign")
            .tag("error_type", "unexpected")
            .counter();
    assertNotNull(
        errors, "unexpected errors counter missing — catch-all must record before rethrow");
    assertEquals(errors.count(), 1.0);

    Timer duration =
        meterRegistry
            .find(SearchBasedFormAssignmentManager.METRIC_PREFIX + ".duration")
            .tag("operation_type", SearchBasedFormAssignmentManager.OPERATION_TYPE)
            .tag("phase", "assign")
            .tag("status", "failed")
            .timer();
    assertNotNull(duration);
    assertEquals(duration.count(), 1L);

    Counter entities =
        meterRegistry
            .find(SearchBasedFormAssignmentManager.METRIC_PREFIX + ".entities_processed")
            .tag("operation_type", SearchBasedFormAssignmentManager.OPERATION_TYPE)
            .tag("phase", "assign")
            .counter();
    assertNotNull(entities);
    assertEquals(entities.count(), 1.0, "only the first page should have been assigned");

    Counter pages =
        meterRegistry
            .find(SearchBasedFormAssignmentManager.METRIC_PREFIX + ".pages")
            .tag("operation_type", SearchBasedFormAssignmentManager.OPERATION_TYPE)
            .tag("phase", "assign")
            .counter();
    assertNotNull(pages);
    assertEquals(pages.count(), 1.0, "only the first page completed before form deletion");
  }

  private SystemEntityClient mockClientForPages(List<List<Urn>> pages) throws Exception {
    SystemEntityClient client = mock(SystemEntityClient.class);
    stubFormDefinition(client);
    when(client.exists(any(), eq(FORM_URN))).thenReturn(true);
    when(client.filterExistingUrns(any(), anyCollection()))
        .thenAnswer(inv -> new HashSet<>((java.util.Collection<Urn>) inv.getArgument(1)));

    AtomicInteger pageIdx = new AtomicInteger();
    when(client.scrollAcrossEntities(
            any(), anyList(), anyString(), any(), any(), anyString(), anyList(), anyInt()))
        .thenAnswer(
            inv -> {
              int i = pageIdx.getAndIncrement();
              if (i >= pages.size()) {
                return emptyScroll();
              }
              String next = i + 1 < pages.size() ? "scroll-" + (i + 1) : null;
              return scrollPage(pages.get(i), next);
            });

    stubBatchGetV2(client);

    return client;
  }

  private SystemEntityClient mockClientFailingOnSecondScrollWithRemoteInvocation()
      throws Exception {
    SystemEntityClient client = mock(SystemEntityClient.class);
    stubFormDefinition(client);
    when(client.filterExistingUrns(any(), anyCollection()))
        .thenAnswer(inv -> new HashSet<>((java.util.Collection<Urn>) inv.getArgument(1)));

    AtomicInteger scrollCalls = new AtomicInteger();
    when(client.scrollAcrossEntities(
            any(), anyList(), anyString(), any(), any(), anyString(), anyList(), anyInt()))
        .thenAnswer(
            inv -> {
              if (scrollCalls.getAndIncrement() == 0) {
                return scrollPage(datasetUrns(0, 1), "scroll-1");
              }
              throw new RemoteInvocationException("boom");
            });
    stubBatchGetV2(client);
    when(client.exists(any(), eq(FORM_URN))).thenReturn(true);
    return client;
  }

  private static void stubBatchGetV2(SystemEntityClient client) throws Exception {
    when(client.batchGetV2(any(), anyString(), anySet(), anySet()))
        .thenAnswer(
            inv -> {
              Set<Urn> requested = inv.getArgument(2);
              Map<Urn, EntityResponse> out = new HashMap<>();
              requested.forEach(urn -> out.put(urn, entityResponse(urn, Map.of())));
              return out;
            });
  }

  private void stubFormDefinition(SystemEntityClient client) throws Exception {
    FormInfo formInfo =
        new FormInfo()
            .setName("Test Form")
            .setPrompts(new FormPromptArray(ImmutableList.of(new FormPrompt().setId(PROMPT_ID))));
    when(client.getV2(
            any(), eq(FORM_ENTITY_NAME), eq(FORM_URN), eq(ImmutableSet.of(FORM_INFO_ASPECT_NAME))))
        .thenReturn(entityResponse(FORM_URN, Map.of(FORM_INFO_ASPECT_NAME, formInfo.data())));
  }

  private static DynamicFormAssignment formFilters() {
    return new DynamicFormAssignment().setFilter(new Filter());
  }

  private FormAssignmentScrollRequest scrollRequest(
      Urn formUrn, int batchFormEntityCount, SystemEntityClient entityClient) {
    return scrollRequest(opContext, formUrn, batchFormEntityCount, entityClient);
  }

  private static FormAssignmentScrollRequest scrollRequest(
      OperationContext opContext,
      Urn formUrn,
      int batchFormEntityCount,
      SystemEntityClient entityClient) {
    return FormAssignmentScrollRequest.builder()
        .opContext(opContext)
        .formFilters(formFilters())
        .formUrn(formUrn)
        .batchFormEntityCount(batchFormEntityCount)
        .entityClient(entityClient)
        .build();
  }

  private static List<Urn> datasetUrns(int start, int count) {
    return IntStream.range(start, start + count)
        .mapToObj(
            i ->
                UrnUtils.getUrn(
                    String.format(
                        "urn:li:dataset:(urn:li:dataPlatform:kafka,metrics_table_%d,PROD)", i)))
        .collect(Collectors.toList());
  }

  private static ScrollResult scrollPage(List<Urn> urns, String scrollId) {
    SearchEntityArray entities = new SearchEntityArray();
    urns.forEach(urn -> entities.add(new SearchEntity().setEntity(urn)));
    ScrollResult result = new ScrollResult().setEntities(entities).setNumEntities(urns.size());
    if (scrollId != null) {
      result.setScrollId(scrollId);
    }
    return result;
  }

  private static ScrollResult emptyScroll() {
    return new ScrollResult().setEntities(new SearchEntityArray()).setNumEntities(0);
  }

  private static EntityResponse entityResponse(
      Urn urn, Map<String, com.linkedin.data.DataMap> aspects) {
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspects.forEach(
        (name, data) ->
            aspectMap.put(name, new EnvelopedAspect().setName(name).setValue(new Aspect(data))));
    return new EntityResponse()
        .setUrn(urn)
        .setEntityName(urn.getEntityType())
        .setAspects(aspectMap);
  }
}
