package com.linkedin.datahub.graphql.analytics.resolver;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.analytics.service.AnalyticsService;
import com.linkedin.datahub.graphql.analytics.service.EntityStats;
import com.linkedin.datahub.graphql.generated.DateRange;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Highlight;
import com.linkedin.metadata.config.search.QueryCanonicalizationConfiguration;
import com.linkedin.metadata.config.search.TimeCanonicalizationConfiguration;
import com.linkedin.metadata.utils.elasticsearch.canonicalization.QueryTimeCanonicalizer;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GetHighlightsResolverTest {

  private AnalyticsService mockAnalyticsService;
  private DataFetchingEnvironment mockEnv;
  private GetHighlightsResolver resolver;

  @BeforeMethod
  public void setup() {
    mockAnalyticsService = mock(AnalyticsService.class);
    when(mockAnalyticsService.getUsageIndexName(any(OperationContext.class)))
        .thenReturn("datahub_usage_event");

    OperationContext mockOpContext = mock(OperationContext.class);
    // Pass-through canonicalizer: these assert the range shapes the resolver requests, not time
    // rounding, so the bounds stay the exact clock reading.
    when(mockOpContext.canonicalNow()).thenReturn(QueryTimeCanonicalizer.DISABLED.now());
    QueryContext mockContext = mock(QueryContext.class);
    when(mockContext.getOperationContext()).thenReturn(mockOpContext);
    mockEnv = mock(DataFetchingEnvironment.class);
    when(mockEnv.getContext()).thenReturn(mockContext);

    resolver = new GetHighlightsResolver(mockAnalyticsService);
  }

  private void mockActiveUsers(int weeklyCurrent, int weeklyPrevious) {
    when(mockAnalyticsService.getUniqueCountsByRange(any(), anyString(), anyMap(), anyString()))
        .thenReturn(
            ImmutableMap.of(
                "weekly_current",
                weeklyCurrent,
                "weekly_previous",
                weeklyPrevious,
                "monthly_current",
                0,
                "monthly_previous",
                0));
  }

  private void mockEntityStats(Map<EntityType, EntityStats> stats) {
    when(mockAnalyticsService.getEntityStats(any(), anyList(), anyList())).thenReturn(stats);
  }

  private Highlight highlightByTitle(List<Highlight> highlights, String title) {
    return highlights.stream().filter(h -> title.equals(h.getTitle())).findFirst().orElse(null);
  }

  /**
   * The whole point of the batched primitives: one query for active users, one for entity stats -
   * not one per entity type or per facet.
   */
  @Test
  public void testIssuesExactlyTwoQueries() throws Exception {
    mockActiveUsers(0, 0);
    mockEntityStats(Collections.emptyMap());

    resolver.get(mockEnv);

    verify(mockAnalyticsService, times(1))
        .getUniqueCountsByRange(any(), anyString(), anyMap(), anyString());
    verify(mockAnalyticsService, times(1)).getEntityStats(any(), anyList(), anyList());
    verify(mockAnalyticsService, times(1)).getUsageIndexName(any(OperationContext.class));
    verifyNoMoreInteractions(mockAnalyticsService);
  }

  @Test
  public void testRequestsAllSixEntityTypesInOneCall() throws Exception {
    mockActiveUsers(0, 0);
    mockEntityStats(Collections.emptyMap());

    resolver.get(mockEnv);

    ArgumentCaptor<List<EntityType>> typesCaptor = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<List<String>> facetsCaptor = ArgumentCaptor.forClass(List.class);
    verify(mockAnalyticsService)
        .getEntityStats(any(), typesCaptor.capture(), facetsCaptor.capture());

    assertEquals(
        typesCaptor.getValue(),
        List.of(
            EntityType.DATASET,
            EntityType.DASHBOARD,
            EntityType.CHART,
            EntityType.DATA_FLOW,
            EntityType.DATA_JOB,
            EntityType.DOMAIN));
    assertEquals(
        facetsCaptor.getValue(),
        List.of("hasOwners", "hasTags", "hasGlossaryTerms", "hasDescription", "hasDomain"));
  }

  /** Both periods must be requested as current/previous pairs in the single usage query. */
  @Test
  public void testRequestsBothPeriodsInOneCall() throws Exception {
    mockActiveUsers(0, 0);
    mockEntityStats(Collections.emptyMap());

    resolver.get(mockEnv);

    ArgumentCaptor<Map<String, DateRange>> rangesCaptor = ArgumentCaptor.forClass(Map.class);
    verify(mockAnalyticsService)
        .getUniqueCountsByRange(any(), anyString(), rangesCaptor.capture(), anyString());

    Map<String, DateRange> ranges = rangesCaptor.getValue();
    assertEquals(
        ranges.keySet(),
        Set.of("weekly_current", "weekly_previous", "monthly_current", "monthly_previous"));

    // The previous window must end exactly where the current window starts.
    assertEquals(ranges.get("weekly_previous").getEnd(), ranges.get("weekly_current").getStart());
    assertEquals(ranges.get("monthly_previous").getEnd(), ranges.get("monthly_current").getStart());
  }

  @Test
  public void testActiveUsersPercentChange() throws Exception {
    mockActiveUsers(150, 100);
    mockEntityStats(Collections.emptyMap());

    Highlight weekly = highlightByTitle(resolver.get(mockEnv), "Weekly Active Users");

    assertEquals(weekly.getValue(), 150);
    assertEquals(weekly.getBody(), "50.00% increase from last week");
  }

  /** No baseline means no percentage to report - the value still renders. */
  @Test
  public void testActiveUsersNoPreviousPeriod() throws Exception {
    mockActiveUsers(42, 0);
    mockEntityStats(Collections.emptyMap());

    Highlight weekly = highlightByTitle(resolver.get(mockEnv), "Weekly Active Users");

    assertEquals(weekly.getValue(), 42);
    assertEquals(weekly.getBody(), "");
  }

  @Test
  public void testEntityHighlightPercentages() throws Exception {
    mockActiveUsers(0, 0);
    mockEntityStats(
        ImmutableMap.of(
            EntityType.DATASET,
            new EntityStats(
                200,
                ImmutableMap.of(
                    "hasOwners", 100,
                    "hasTags", 50,
                    "hasGlossaryTerms", 20,
                    "hasDescription", 150,
                    "hasDomain", 10))));

    Highlight datasets = highlightByTitle(resolver.get(mockEnv), "Datasets");

    assertEquals(datasets.getValue(), 200);
    assertEquals(
        datasets.getBody(),
        "50.00% have owners, 25.00% have tags, 10.00% have glossary terms, "
            + "75.00% have description, 5.00% have domain assigned!");
  }

  /** Domains never report a "has domain" percentage. */
  @Test
  public void testDomainHighlightOmitsDomainPercentage() throws Exception {
    mockActiveUsers(0, 0);
    mockEntityStats(
        ImmutableMap.of(
            EntityType.DOMAIN,
            new EntityStats(
                10,
                ImmutableMap.of(
                    "hasOwners", 5,
                    "hasTags", 0,
                    "hasGlossaryTerms", 0,
                    "hasDescription", 10,
                    "hasDomain", 3))));

    Highlight domains = highlightByTitle(resolver.get(mockEnv), "Domains");

    assertEquals(
        domains.getBody(),
        "50.00% have owners, 0.00% have tags, 0.00% have glossary terms, 100.00% have description!");
  }

  /** Entity types with no documents are dropped rather than rendered as an empty card. */
  @Test
  public void testEmptyEntityTypesAreSkipped() throws Exception {
    mockActiveUsers(0, 0);
    mockEntityStats(
        ImmutableMap.of(
            EntityType.DATASET,
            new EntityStats(5, ImmutableMap.of("hasOwners", 1)),
            EntityType.CHART,
            new EntityStats(0, ImmutableMap.of())));

    List<String> titles =
        resolver.get(mockEnv).stream().map(Highlight::getTitle).collect(Collectors.toList());

    assertTrue(titles.contains("Datasets"));
    assertTrue(titles.contains("Weekly Active Users"));
    // CHART returned zero docs; DASHBOARD was absent from the response entirely.
    assertTrue(!titles.contains("Charts"));
    assertTrue(!titles.contains("Dashboards"));
  }

  /** A failing backend degrades to an empty panel rather than surfacing an error. */
  @Test
  public void testBackendFailureReturnsEmptyList() throws Exception {
    when(mockAnalyticsService.getUniqueCountsByRange(any(), anyString(), anyMap(), anyString()))
        .thenThrow(new RuntimeException("elasticsearch is down"));

    assertEquals(resolver.get(mockEnv), Collections.emptyList());
  }

  /** An enabled 5m/EXPAND canonicalizer pinned to a fixed instant. */
  private static QueryTimeCanonicalizer canonicalizerAt(String isoInstant) {
    return QueryTimeCanonicalizer.fromConfig(
        QueryCanonicalizationConfiguration.builder()
            .enabled(true)
            .time(
                TimeCanonicalizationConfiguration.builder()
                    .enabled(true)
                    .bucketSize("5m")
                    .timezone("UTC")
                    .rounding("EXPAND")
                    .build())
            .build(),
        null,
        Clock.fixed(Instant.parse(isoInstant), ZoneOffset.UTC));
  }

  private Map<String, DateRange> rangesWithCanonicalizerAt(String isoInstant) throws Exception {
    OperationContext opContext = mock(OperationContext.class);
    when(opContext.canonicalNow()).thenReturn(canonicalizerAt(isoInstant).now());
    QueryContext queryContext = mock(QueryContext.class);
    when(queryContext.getOperationContext()).thenReturn(opContext);
    DataFetchingEnvironment env = mock(DataFetchingEnvironment.class);
    when(env.getContext()).thenReturn(queryContext);

    mockActiveUsers(0, 0);
    mockEntityStats(Collections.emptyMap());
    new GetHighlightsResolver(mockAnalyticsService).get(env);

    ArgumentCaptor<Map<String, DateRange>> captor = ArgumentCaptor.forClass(Map.class);
    verify(mockAnalyticsService)
        .getUniqueCountsByRange(any(), anyString(), captor.capture(), anyString());
    return captor.getValue();
  }

  /**
   * Every bound this resolver requests must land on a bucket boundary, otherwise the aggregation is
   * unique per request and cannot reuse a cached result.
   */
  @Test
  public void testCanonicalizedRangesLandOnBucketBoundaries() throws Exception {
    Map<String, DateRange> ranges = rangesWithCanonicalizerAt("2026-08-16T19:03:42Z");

    for (Map.Entry<String, DateRange> e : ranges.entrySet()) {
      for (String bound : List.of(e.getValue().getStart(), e.getValue().getEnd())) {
        assertEquals(
            Long.parseLong(bound) % 300_000L,
            0L,
            e.getKey() + " bound " + bound + " is not on a 5m boundary");
      }
    }
  }

  /**
   * The highlights render a percent change between the current and previous period, so
   * canonicalization must not make the current window wider than the previous one. Ceiling the
   * current window's end while flooring everything else did exactly that, biasing the percentage
   * upward.
   *
   * <p>Weekly windows are a fixed length and must match exactly. Monthly windows differ by calendar
   * month length (Jul-Aug is 31 days, Jun-Jul is 30) both before and after this change, so the
   * assertion there is that the window still ends on the floored reference rather than a ceiled
   * one.
   */
  @Test
  public void testCanonicalizationDoesNotWidenTheCurrentWindow() throws Exception {
    Map<String, DateRange> ranges = rangesWithCanonicalizerAt("2026-08-16T19:03:42Z");
    // 19:03:42 floors to 19:00:00; the buggy version ceiled the end to 19:05:00.
    String flooredReference = String.valueOf(Instant.parse("2026-08-16T19:00:00Z").toEpochMilli());

    DateRange weeklyCurrent = ranges.get("weekly_current");
    DateRange weeklyPrevious = ranges.get("weekly_previous");
    assertEquals(
        Long.parseLong(weeklyCurrent.getEnd()) - Long.parseLong(weeklyCurrent.getStart()),
        Long.parseLong(weeklyPrevious.getEnd()) - Long.parseLong(weeklyPrevious.getStart()),
        "weekly comparison windows differ in width");

    for (String period : List.of("weekly", "monthly")) {
      assertEquals(
          ranges.get(period + "_current").getEnd(),
          flooredReference,
          period + " current window must end on the floored reference, not a ceiled bound");
      assertEquals(
          ranges.get(period + "_previous").getEnd(),
          ranges.get(period + "_current").getStart(),
          period + " windows are not contiguous");
    }
  }

  /** Two requests inside one bucket must ask for byte-identical ranges. */
  @Test
  public void testRequestsInsideOneBucketProduceIdenticalRanges() throws Exception {
    Map<String, DateRange> first = rangesWithCanonicalizerAt("2026-08-16T19:01:03Z");
    reset(mockAnalyticsService);
    when(mockAnalyticsService.getUsageIndexName(any(OperationContext.class)))
        .thenReturn("datahub_usage_event");
    Map<String, DateRange> second = rangesWithCanonicalizerAt("2026-08-16T19:04:59Z");

    assertEquals(second.toString(), first.toString());
  }
}
