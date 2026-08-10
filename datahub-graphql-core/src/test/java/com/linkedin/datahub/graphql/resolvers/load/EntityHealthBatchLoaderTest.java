package com.linkedin.datahub.graphql.resolvers.load;

import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationshipArray;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringArrayArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Health;
import com.linkedin.datahub.graphql.generated.HealthStatus;
import com.linkedin.datahub.graphql.generated.HealthStatusType;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.incident.IncidentState;
import com.linkedin.incident.IncidentStatus;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.IncidentStats;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.test.TestResult;
import com.linkedin.test.TestResultArray;
import com.linkedin.test.TestResultType;
import com.linkedin.test.TestResults;
import com.linkedin.timeseries.GenericTable;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class EntityHealthBatchLoaderTest {

  private static final String DATASET_A = "urn:li:dataset:(urn:li:dataPlatform:test,a,PROD)";
  private static final String DATASET_B = "urn:li:dataset:(urn:li:dataPlatform:test,b,PROD)";
  private static final String CHART_C = "urn:li:chart:(test,c)";
  private static final String ASSERTION_A = "urn:li:assertion:a";
  private static final String ASSERTION_B = "urn:li:assertion:b";

  /**
   * The core N+1 guarantee: two assertion-bearing URNs produce exactly ONE {@code
   * batchGetAggregatedStats} call and ZERO single-URN {@code getAggregatedStats} calls, and each
   * URN's health is assembled from its own slice of the batch result.
   */
  @Test
  public void testSingleBatchCallForMultipleUrns() throws Exception {
    final EntityClient entityClient = Mockito.mock(EntityClient.class);
    final GraphClient graphClient = Mockito.mock(GraphClient.class);
    final TimeseriesAspectService timeseriesAspectService =
        Mockito.mock(TimeseriesAspectService.class);
    final EntitySearchService entitySearchService = Mockito.mock(EntitySearchService.class);

    stubActiveAssertions(graphClient, DATASET_A, ASSERTION_A);
    stubActiveAssertions(graphClient, DATASET_B, ASSERTION_B);

    final Map<Urn, GenericTable> batchResult = new HashMap<>();
    batchResult.put(Urn.createFromString(DATASET_A), assertionRunTable(ASSERTION_A, "FAILURE"));
    batchResult.put(Urn.createFromString(DATASET_B), assertionRunTable(ASSERTION_B, "SUCCESS"));
    Mockito.when(
            timeseriesAspectService.batchGetAggregatedStats(
                any(),
                Mockito.eq(Constants.ASSERTION_ENTITY_NAME),
                Mockito.eq(Constants.ASSERTION_RUN_EVENT_ASPECT_NAME),
                any(),
                any(),
                any(),
                any(),
                any()))
        .thenReturn(batchResult);

    final EntityHealthBatchLoader loader =
        new EntityHealthBatchLoader(
            entityClient, graphClient, timeseriesAspectService, entitySearchService);

    final List<List<Health>> result =
        loader.batchLoad(
            ImmutableList.of(assertionsOnlyKey(DATASET_A), assertionsOnlyKey(DATASET_B)),
            mockContext());

    // One batched aggregation query for both URNs, and never the single-URN fallback.
    Mockito.verify(timeseriesAspectService, Mockito.times(1))
        .batchGetAggregatedStats(any(), any(), any(), any(), any(), any(), any(), any());
    Mockito.verify(timeseriesAspectService, Mockito.times(0))
        .getAggregatedStats(any(), any(), any(), any(), any(), any());

    // Per-URN assembly: A is failing, B is passing.
    assertEquals(result.size(), 2);
    assertEquals(result.get(0).size(), 1);
    assertEquals(result.get(0).get(0).getType(), HealthStatusType.ASSERTIONS);
    assertEquals(result.get(0).get(0).getStatus(), HealthStatus.FAIL);
    assertEquals(result.get(1).get(0).getStatus(), HealthStatus.PASS);
  }

  /**
   * Keys whose config disables assertions must not contribute their URN to the assertion batch, and
   * the shared filter passed to the batch must carry {@code asserteeUrn} as the urn field path (the
   * per-URN criterion is added by the batch method, not the caller).
   */
  @Test
  public void testAssertionBatchExcludesConfigDisabledUrns() throws Exception {
    final EntityClient entityClient = Mockito.mock(EntityClient.class);
    final GraphClient graphClient = Mockito.mock(GraphClient.class);
    final TimeseriesAspectService timeseriesAspectService =
        Mockito.mock(TimeseriesAspectService.class);
    final EntitySearchService entitySearchService = Mockito.mock(EntitySearchService.class);

    stubActiveAssertions(graphClient, DATASET_A, ASSERTION_A);
    // Incident lookups return "no active incidents" so incident health is PASS with no batchGetV2.
    Mockito.when(entitySearchService.getActiveIncidentStats(any(), any())).thenReturn(Map.of());

    final Map<Urn, GenericTable> batchResult = new HashMap<>();
    batchResult.put(Urn.createFromString(DATASET_A), assertionRunTable(ASSERTION_A, "SUCCESS"));
    Mockito.when(
            timeseriesAspectService.batchGetAggregatedStats(
                any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(batchResult);

    final EntityHealthBatchLoader loader =
        new EntityHealthBatchLoader(
            entityClient, graphClient, timeseriesAspectService, entitySearchService);

    // A: dataset (assertions + incidents). C: chart (incidents only, assertions disabled).
    final EntityHealthBatchLoader.HealthQueryKey datasetKey =
        new EntityHealthBatchLoader.HealthQueryKey(
            Urn.createFromString(DATASET_A), true, true, false);
    final EntityHealthBatchLoader.HealthQueryKey chartKey =
        new EntityHealthBatchLoader.HealthQueryKey(
            Urn.createFromString(CHART_C), false, true, false);

    final List<List<Health>> result =
        loader.batchLoad(ImmutableList.of(datasetKey, chartKey), mockContext());

    @SuppressWarnings("unchecked")
    final ArgumentCaptor<List<Urn>> urnsCaptor = ArgumentCaptor.forClass(List.class);
    final ArgumentCaptor<String> urnFieldCaptor = ArgumentCaptor.forClass(String.class);
    Mockito.verify(timeseriesAspectService)
        .batchGetAggregatedStats(
            any(),
            any(),
            any(),
            any(),
            urnsCaptor.capture(),
            any(),
            any(),
            urnFieldCaptor.capture());

    // Only the assertions-enabled dataset URN is in the batch; the chart URN is excluded.
    assertEquals(urnsCaptor.getValue(), ImmutableList.of(Urn.createFromString(DATASET_A)));
    assertEquals(urnFieldCaptor.getValue(), "asserteeUrn");

    // Dataset gets both incident (PASS) and assertion (PASS) health; chart gets incident only.
    assertEquals(result.get(0).size(), 2);
    assertEquals(result.get(1).size(), 1);
    assertEquals(result.get(1).get(0).getType(), HealthStatusType.INCIDENTS);
  }

  /**
   * Test results are fetched with a single {@code batchGetV2} rather than one {@code getV2} per
   * URN.
   */
  @Test
  public void testTestResultsBatchedViaBatchGetV2() throws Exception {
    final EntityClient entityClient = Mockito.mock(EntityClient.class);
    final GraphClient graphClient = Mockito.mock(GraphClient.class);
    final TimeseriesAspectService timeseriesAspectService =
        Mockito.mock(TimeseriesAspectService.class);
    final EntitySearchService entitySearchService = Mockito.mock(EntitySearchService.class);

    final Map<Urn, EntityResponse> testResponses = new HashMap<>();
    testResponses.put(Urn.createFromString(DATASET_A), testResultsResponse(1, 0)); // failing
    testResponses.put(Urn.createFromString(DATASET_B), testResultsResponse(0, 1)); // passing
    Mockito.when(
            entityClient.batchGetV2(
                any(),
                Mockito.eq("dataset"),
                Mockito.eq(
                    ImmutableSet.of(
                        Urn.createFromString(DATASET_A), Urn.createFromString(DATASET_B))),
                Mockito.eq(ImmutableSet.of(Constants.TEST_RESULTS_ASPECT_NAME))))
        .thenReturn(testResponses);

    final EntityHealthBatchLoader loader =
        new EntityHealthBatchLoader(
            entityClient, graphClient, timeseriesAspectService, entitySearchService);

    final EntityHealthBatchLoader.HealthQueryKey keyA =
        new EntityHealthBatchLoader.HealthQueryKey(
            Urn.createFromString(DATASET_A), false, false, true);
    final EntityHealthBatchLoader.HealthQueryKey keyB =
        new EntityHealthBatchLoader.HealthQueryKey(
            Urn.createFromString(DATASET_B), false, false, true);

    final List<List<Health>> result = loader.batchLoad(ImmutableList.of(keyA, keyB), mockContext());

    Mockito.verify(entityClient, Mockito.times(1)).batchGetV2(any(), any(), any(), any());
    Mockito.verify(entityClient, Mockito.times(0)).getV2(any(), any(), any(), any());

    assertEquals(result.get(0).get(0).getType(), HealthStatusType.TESTS);
    assertEquals(result.get(0).get(0).getStatus(), HealthStatus.FAIL);
    assertEquals(result.get(1).get(0).getStatus(), HealthStatus.PASS);
  }

  /**
   * Per-dimension failure isolation: when the assertion batch call throws, the loader must NOT fail
   * the whole page — it drops only the assertions dimension and still returns incidents and tests
   * health for every entity. This is the batch-only gap the expanded validation surfaced.
   */
  @Test
  public void testAssertionDimensionFailureIsIsolated() throws Exception {
    final EntityClient entityClient = Mockito.mock(EntityClient.class);
    final GraphClient graphClient = Mockito.mock(GraphClient.class);
    final TimeseriesAspectService timeseriesAspectService =
        Mockito.mock(TimeseriesAspectService.class);
    final EntitySearchService entitySearchService = Mockito.mock(EntitySearchService.class);

    stubActiveAssertions(graphClient, DATASET_A, ASSERTION_A);
    // Assertion aggregation blows up (e.g. timeseries index unavailable).
    Mockito.when(
            timeseriesAspectService.batchGetAggregatedStats(
                any(), any(), any(), any(), any(), any(), any(), any()))
        .thenThrow(new RuntimeException("boom: assertion index down"));
    // Incidents: none active -> PASS. Tests: one failing.
    Mockito.when(entitySearchService.getActiveIncidentStats(any(), any())).thenReturn(Map.of());
    final Map<Urn, EntityResponse> testResponses = new HashMap<>();
    testResponses.put(Urn.createFromString(DATASET_A), testResultsResponse(1, 0));
    Mockito.when(entityClient.batchGetV2(any(), any(), any(), any())).thenReturn(testResponses);

    final EntityHealthBatchLoader loader =
        new EntityHealthBatchLoader(
            entityClient, graphClient, timeseriesAspectService, entitySearchService);

    final EntityHealthBatchLoader.HealthQueryKey key =
        new EntityHealthBatchLoader.HealthQueryKey(
            Urn.createFromString(DATASET_A), true, true, true);

    // Must not throw despite the assertion failure.
    final List<List<Health>> result = loader.batchLoad(ImmutableList.of(key), mockContext());

    final List<HealthStatusType> types =
        result.get(0).stream().map(Health::getType).collect(Collectors.toList());
    // Assertions dropped; incidents + tests survive.
    assertFalse(types.contains(HealthStatusType.ASSERTIONS));
    assertTrue(types.contains(HealthStatusType.INCIDENTS));
    assertTrue(types.contains(HealthStatusType.TESTS));
  }

  /**
   * Assembly-phase isolation: a malformed assertion-run row (size != 3) throws while assembling ONE
   * entity's assertion health, but must not fail the page — the malformed entity drops its
   * assertions dimension while other entities resolve normally.
   */
  @Test
  public void testMalformedAssertionRowIsolatedToOneEntity() throws Exception {
    final EntityClient entityClient = Mockito.mock(EntityClient.class);
    final GraphClient graphClient = Mockito.mock(GraphClient.class);
    final TimeseriesAspectService timeseriesAspectService =
        Mockito.mock(TimeseriesAspectService.class);
    final EntitySearchService entitySearchService = Mockito.mock(EntitySearchService.class);

    stubActiveAssertions(graphClient, DATASET_A, ASSERTION_A);
    stubActiveAssertions(graphClient, DATASET_B, ASSERTION_B);

    final Map<Urn, GenericTable> batchResult = new HashMap<>();
    batchResult.put(Urn.createFromString(DATASET_A), malformedAssertionRunTable()); // size-2 row
    batchResult.put(Urn.createFromString(DATASET_B), assertionRunTable(ASSERTION_B, "FAILURE"));
    Mockito.when(
            timeseriesAspectService.batchGetAggregatedStats(
                any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(batchResult);

    final EntityHealthBatchLoader loader =
        new EntityHealthBatchLoader(
            entityClient, graphClient, timeseriesAspectService, entitySearchService);

    // Must not throw despite A's malformed row.
    final List<List<Health>> result =
        loader.batchLoad(
            ImmutableList.of(assertionsOnlyKey(DATASET_A), assertionsOnlyKey(DATASET_B)),
            mockContext());

    assertEquals(result.size(), 2);
    // A: malformed row -> assertions dimension dropped for this entity only.
    assertTrue(result.get(0).isEmpty());
    // B: unaffected, resolves to a failing assertions health.
    assertEquals(result.get(1).size(), 1);
    assertEquals(result.get(1).get(0).getType(), HealthStatusType.ASSERTIONS);
    assertEquals(result.get(1).get(0).getStatus(), HealthStatus.FAIL);
  }

  /**
   * Fetch-phase isolation for a non-assertion dimension: when the test-results batch throws, the
   * tests dimension is dropped but the page still returns and other dimensions survive.
   */
  @Test
  public void testTestsFetchFailureIsIsolated() throws Exception {
    final EntityClient entityClient = Mockito.mock(EntityClient.class);
    final GraphClient graphClient = Mockito.mock(GraphClient.class);
    final TimeseriesAspectService timeseriesAspectService =
        Mockito.mock(TimeseriesAspectService.class);
    final EntitySearchService entitySearchService = Mockito.mock(EntitySearchService.class);

    // Incidents: none active -> PASS. Tests: batch fetch blows up.
    Mockito.when(entitySearchService.getActiveIncidentStats(any(), any())).thenReturn(Map.of());
    Mockito.when(entityClient.batchGetV2(any(), any(), any(), any()))
        .thenThrow(new RuntimeException("boom: test-results backend down"));

    final EntityHealthBatchLoader loader =
        new EntityHealthBatchLoader(
            entityClient, graphClient, timeseriesAspectService, entitySearchService);

    // incidents + tests enabled, assertions off.
    final EntityHealthBatchLoader.HealthQueryKey key =
        new EntityHealthBatchLoader.HealthQueryKey(
            Urn.createFromString(DATASET_A), false, true, true);

    final List<List<Health>> result = loader.batchLoad(ImmutableList.of(key), mockContext());

    final List<HealthStatusType> types =
        result.get(0).stream().map(Health::getType).collect(Collectors.toList());
    assertFalse(types.contains(HealthStatusType.TESTS)); // dropped
    assertTrue(types.contains(HealthStatusType.INCIDENTS)); // survives
  }

  /**
   * Per-URN graph-lookup isolation: one entity's failing active-assertion lookup drops only that
   * entity's assertions dimension; a sibling entity is unaffected.
   */
  @Test
  public void testPerUrnGraphFailureIsolated() throws Exception {
    final EntityClient entityClient = Mockito.mock(EntityClient.class);
    final GraphClient graphClient = Mockito.mock(GraphClient.class);
    final TimeseriesAspectService timeseriesAspectService =
        Mockito.mock(TimeseriesAspectService.class);
    final EntitySearchService entitySearchService = Mockito.mock(EntitySearchService.class);

    // A's graph lookup throws; B's succeeds.
    Mockito.when(
            graphClient.getRelatedEntities(
                Mockito.eq(DATASET_A), any(), any(), Mockito.anyInt(), Mockito.anyInt(), any()))
        .thenThrow(new RuntimeException("boom: graph down for A"));
    stubActiveAssertions(graphClient, DATASET_B, ASSERTION_B);

    final Map<Urn, GenericTable> batchResult = new HashMap<>();
    batchResult.put(Urn.createFromString(DATASET_B), assertionRunTable(ASSERTION_B, "FAILURE"));
    Mockito.when(
            timeseriesAspectService.batchGetAggregatedStats(
                any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(batchResult);

    final EntityHealthBatchLoader loader =
        new EntityHealthBatchLoader(
            entityClient, graphClient, timeseriesAspectService, entitySearchService);

    final List<List<Health>> result =
        loader.batchLoad(
            ImmutableList.of(assertionsOnlyKey(DATASET_A), assertionsOnlyKey(DATASET_B)),
            mockContext());

    assertEquals(result.size(), 2);
    assertTrue(result.get(0).isEmpty()); // A: graph failed -> assertions dropped for A only
    assertEquals(result.get(1).size(), 1); // B: unaffected
    assertEquals(result.get(1).get(0).getType(), HealthStatusType.ASSERTIONS);
  }

  /**
   * The incident dimension's N+1 guarantee: one {@code getActiveIncidentStats} aggregation call
   * produces the count and latest-incident urn, and the latest incident's info is fetched with a
   * single {@code batchGetV2} rather than a per-entity {@code filter}+{@code getV2}.
   */
  @Test
  public void testIncidentsBatchedViaGetActiveIncidentStats() throws Exception {
    final EntityClient entityClient = Mockito.mock(EntityClient.class);
    final GraphClient graphClient = Mockito.mock(GraphClient.class);
    final TimeseriesAspectService timeseriesAspectService =
        Mockito.mock(TimeseriesAspectService.class);
    final EntitySearchService entitySearchService = Mockito.mock(EntitySearchService.class);

    final Urn datasetUrn = Urn.createFromString(DATASET_A);
    final Urn incidentUrn = Urn.createFromString("urn:li:incident:i1");

    Mockito.when(entitySearchService.getActiveIncidentStats(any(), Mockito.eq(Set.of(datasetUrn))))
        .thenReturn(Map.of(datasetUrn, new IncidentStats(2, incidentUrn)));

    final IncidentInfo info =
        new IncidentInfo()
            .setStatus(
                new IncidentStatus()
                    .setState(IncidentState.ACTIVE)
                    .setLastUpdated(
                        new AuditStamp()
                            .setTime(123L)
                            .setActor(Urn.createFromString("urn:li:corpuser:t"))));
    final EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(
        Constants.INCIDENT_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(info.data())));
    Mockito.when(
            entityClient.batchGetV2(
                any(),
                Mockito.eq(Constants.INCIDENT_ENTITY_NAME),
                Mockito.eq(Set.of(incidentUrn)),
                any()))
        .thenReturn(
            Map.of(incidentUrn, new EntityResponse().setUrn(incidentUrn).setAspects(aspectMap)));

    final EntityHealthBatchLoader loader =
        new EntityHealthBatchLoader(
            entityClient, graphClient, timeseriesAspectService, entitySearchService);
    final List<List<Health>> results =
        loader.batchLoad(
            List.of(new EntityHealthBatchLoader.HealthQueryKey(datasetUrn, false, true, false)),
            mockContext());

    assertEquals(results.get(0).get(0).getType(), HealthStatusType.INCIDENTS);
    assertEquals(results.get(0).get(0).getStatus(), HealthStatus.FAIL);
    Mockito.verify(entitySearchService, Mockito.times(1)).getActiveIncidentStats(any(), any());
  }

  /**
   * Fetch-phase isolation for the incident dimension: when the active-incident aggregation throws,
   * incidents are dropped but the page still returns and other dimensions survive. Completes the
   * failure-isolation matrix (assertions/tests/graph are covered above).
   */
  @Test
  public void testIncidentDimensionFailureIsIsolated() throws Exception {
    final EntityClient entityClient = Mockito.mock(EntityClient.class);
    final GraphClient graphClient = Mockito.mock(GraphClient.class);
    final TimeseriesAspectService timeseriesAspectService =
        Mockito.mock(TimeseriesAspectService.class);
    final EntitySearchService entitySearchService = Mockito.mock(EntitySearchService.class);

    // Incident aggregation blows up (e.g. incident search index unavailable).
    Mockito.when(entitySearchService.getActiveIncidentStats(any(), any()))
        .thenThrow(new RuntimeException("boom: incident index down"));
    // Tests: one failing, so the tests dimension is present to prove it survives.
    final Map<Urn, EntityResponse> testResponses = new HashMap<>();
    testResponses.put(Urn.createFromString(DATASET_A), testResultsResponse(1, 0));
    Mockito.when(entityClient.batchGetV2(any(), any(), any(), any())).thenReturn(testResponses);

    final EntityHealthBatchLoader loader =
        new EntityHealthBatchLoader(
            entityClient, graphClient, timeseriesAspectService, entitySearchService);

    // incidents + tests enabled, assertions off.
    final EntityHealthBatchLoader.HealthQueryKey key =
        new EntityHealthBatchLoader.HealthQueryKey(
            Urn.createFromString(DATASET_A), false, true, true);

    final List<List<Health>> result = loader.batchLoad(ImmutableList.of(key), mockContext());

    final List<HealthStatusType> types =
        result.get(0).stream().map(Health::getType).collect(Collectors.toList());
    assertFalse(types.contains(HealthStatusType.INCIDENTS)); // dropped
    assertTrue(types.contains(HealthStatusType.TESTS)); // survives
  }

  /**
   * The incident count survives a failure to fetch the latest incident's info: the aggregation
   * yields count + latest-urn, but if the follow-up {@code batchGetV2} for incidentInfo throws, the
   * dimension still reports FAIL with the correct count — only the title/timestamp are unpopulated.
   */
  @Test
  public void testIncidentInfoFetchFailureKeepsCount() throws Exception {
    final EntityClient entityClient = Mockito.mock(EntityClient.class);
    final GraphClient graphClient = Mockito.mock(GraphClient.class);
    final TimeseriesAspectService timeseriesAspectService =
        Mockito.mock(TimeseriesAspectService.class);
    final EntitySearchService entitySearchService = Mockito.mock(EntitySearchService.class);

    final Urn datasetUrn = Urn.createFromString(DATASET_A);
    final Urn incidentUrn = Urn.createFromString("urn:li:incident:i1");
    Mockito.when(entitySearchService.getActiveIncidentStats(any(), Mockito.eq(Set.of(datasetUrn))))
        .thenReturn(Map.of(datasetUrn, new IncidentStats(3, incidentUrn)));
    // The incidentInfo batchGetV2 fails.
    Mockito.when(entityClient.batchGetV2(any(), any(), any(), any()))
        .thenThrow(new RuntimeException("boom: incident info fetch down"));

    final EntityHealthBatchLoader loader =
        new EntityHealthBatchLoader(
            entityClient, graphClient, timeseriesAspectService, entitySearchService);

    final List<List<Health>> result =
        loader.batchLoad(
            List.of(new EntityHealthBatchLoader.HealthQueryKey(datasetUrn, false, true, false)),
            mockContext());

    final Health h = result.get(0).get(0);
    assertEquals(h.getType(), HealthStatusType.INCIDENTS);
    assertEquals(h.getStatus(), HealthStatus.FAIL);
    assertEquals(h.getMessage(), "3 active incidents");
    // Latest-incident urn is known from the aggregation; the title is null because info fetch
    // failed.
    assertEquals(h.getActiveIncidentHealthDetails().getLatestIncidentUrn(), incidentUrn.toString());
    assertNull(h.getActiveIncidentHealthDetails().getLatestIncidentTitle());
  }

  private static GenericTable malformedAssertionRunTable() {
    // Only two columns instead of the expected [assertionUrn, type, timestampMillis].
    return new GenericTable()
        .setColumnNames(new StringArray(ImmutableList.of("assertionUrn", "type")))
        .setColumnTypes(new StringArray("string", "string"))
        .setRows(
            new StringArrayArray(
                ImmutableList.of(new StringArray(ImmutableList.of(ASSERTION_A, "SUCCESS")))));
  }

  private static EntityHealthBatchLoader.HealthQueryKey assertionsOnlyKey(final String urn)
      throws Exception {
    return new EntityHealthBatchLoader.HealthQueryKey(
        Urn.createFromString(urn), true, false, false);
  }

  private static void stubActiveAssertions(
      final GraphClient graphClient, final String assetUrn, final String assertionUrn)
      throws Exception {
    Mockito.when(
            graphClient.getRelatedEntities(
                Mockito.eq(assetUrn),
                Mockito.eq(ImmutableSet.of("Asserts")),
                any(),
                Mockito.anyInt(),
                Mockito.anyInt(),
                any()))
        .thenReturn(
            new EntityRelationships()
                .setStart(0)
                .setCount(1)
                .setTotal(1)
                .setRelationships(
                    new EntityRelationshipArray(
                        ImmutableList.of(
                            new EntityRelationship()
                                .setEntity(Urn.createFromString(assertionUrn))
                                .setType("Asserts")))));
  }

  private static GenericTable assertionRunTable(
      final String assertionUrn, final String resultType) {
    return new GenericTable()
        .setColumnNames(
            new StringArray(ImmutableList.of("assertionUrn", "type", "timestampMillis")))
        .setColumnTypes(new StringArray("string", "string", "long"))
        .setRows(
            new StringArrayArray(
                ImmutableList.of(
                    new StringArray(ImmutableList.of(assertionUrn, resultType, "0")))));
  }

  private static EntityResponse testResultsResponse(final int failing, final int passing)
      throws Exception {
    final TestResults testResults = new TestResults();
    final TestResultArray failingArray = new TestResultArray();
    for (int i = 0; i < failing; i++) {
      failingArray.add(
          new TestResult()
              .setTest(Urn.createFromString("urn:li:test:fail-" + i))
              .setType(TestResultType.FAILURE));
    }
    final TestResultArray passingArray = new TestResultArray();
    for (int i = 0; i < passing; i++) {
      passingArray.add(
          new TestResult()
              .setTest(Urn.createFromString("urn:li:test:pass-" + i))
              .setType(TestResultType.SUCCESS));
    }
    testResults.setFailing(failingArray);
    testResults.setPassing(passingArray);

    final EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(
        Constants.TEST_RESULTS_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(testResults.data())));
    return new EntityResponse().setAspects(aspectMap);
  }

  private static QueryContext mockContext() {
    final QueryContext context = Mockito.mock(QueryContext.class);
    Mockito.when(context.getActorUrn()).thenReturn("urn:li:corpuser:test");
    Mockito.when(context.getOperationContext()).thenReturn(Mockito.mock(OperationContext.class));
    return context;
  }
}
