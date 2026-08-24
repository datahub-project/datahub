package com.linkedin.datahub.graphql.analytics.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.google.common.collect.ImmutableMap;
import com.linkedin.datahub.graphql.generated.DateRange;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.filter.Filter;
import org.opensearch.search.aggregations.bucket.filter.Filters;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregator.KeyedFilter;
import org.opensearch.search.aggregations.metrics.Cardinality;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AnalyticsServiceTest {

  private static final String USAGE_INDEX = "datahub_usage_event";
  private static final String BROWSER_ID = "browserId";
  private static final List<String> FACETS = List.of("hasOwners", "hasTags");

  private SearchClientShim<?> mockClient;
  private OperationContext opContext;
  private AnalyticsService service;

  @BeforeMethod
  public void setup() {
    mockClient = mock(SearchClientShim.class);
    // A real context, matching ESSearchDAOIncidentStatsTest - the primitives run inside
    // opContext.withSpan, which a bare mock would swallow without invoking.
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();

    IndexConvention mockIndexConvention = mock(IndexConvention.class);
    when(mockIndexConvention.getEntityIndexName(any(OperationFingerprint.class), any()))
        .thenAnswer(invocation -> invocation.getArgument(1).toString().toLowerCase() + "index_v2");
    when(mockIndexConvention.getIndexName(
            any(OperationFingerprint.class), eq(AnalyticsService.DATAHUB_USAGE_EVENT_INDEX)))
        .thenReturn(USAGE_INDEX);

    service = new AnalyticsService(mockClient, mockIndexConvention);
  }

  private Map<String, DateRange> twoRanges() {
    Map<String, DateRange> ranges = new LinkedHashMap<>();
    ranges.put("weekly_current", new DateRange("100", "200"));
    ranges.put("weekly_previous", new DateRange("0", "100"));
    return ranges;
  }

  private static AggregationBuilder subAggByName(AggregationBuilder parent, String name) {
    return parent.getSubAggregations().stream()
        .filter(agg -> name.equals(agg.getName()))
        .findFirst()
        .orElse(null);
  }

  private static AggregationBuilder topLevelAgg(SearchRequest request) {
    return request.source().aggregations().getAggregatorFactories().stream()
        .findFirst()
        .orElse(null);
  }

  /**
   * Keyed filters are sorted by key inside the builder, so compare as a set - extraction looks
   * buckets up by key and never depends on ordering.
   */
  private static Set<String> keysOf(FiltersAggregationBuilder builder) {
    return builder.filters().stream().map(KeyedFilter::key).collect(Collectors.toSet());
  }

  // ---------------------------------------------------------------------------------------------
  // Request shape
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testUniqueCountsRequestBatchesEveryRange() {
    SearchRequest request =
        service.buildUniqueCountsByRangeRequest(USAGE_INDEX, twoRanges(), BROWSER_ID);

    assertEquals(request.indices(), new String[] {USAGE_INDEX});

    FiltersAggregationBuilder byRange =
        (FiltersAggregationBuilder) subAggByName(topLevelAgg(request), "by_range");
    assertNotNull(byRange);
    assertEquals(keysOf(byRange), Set.of("weekly_current", "weekly_previous"));
    // The cardinality metric hangs off the range buckets rather than off separate queries.
    assertNotNull(subAggByName(byRange, "unique"));

    String source = request.source().toString();
    assertTrue(source.contains(BROWSER_ID), "expected cardinality on browserId");
    assertTrue(source.contains("\"size\":0"), "aggregation-only request should fetch no hits");
  }

  @Test
  public void testEntityStatsRequestTargetsEveryIndexOnce() {
    SearchRequest request =
        service.buildEntityStatsRequest(
            opContext, List.of(EntityType.DATASET, EntityType.CHART), FACETS);

    assertEquals(request.indices(), new String[] {"datasetindex_v2", "chartindex_v2"});
    // A single missing index must not fail the batch.
    assertTrue(request.indicesOptions().ignoreUnavailable());
    assertTrue(request.indicesOptions().allowNoIndices());

    FiltersAggregationBuilder byEntity =
        (FiltersAggregationBuilder) subAggByName(topLevelAgg(request), "by_entity");
    assertNotNull(byEntity);
    assertEquals(keysOf(byEntity), Set.of("DATASET", "CHART"));

    FiltersAggregationBuilder byFacet =
        (FiltersAggregationBuilder) subAggByName(byEntity, "by_facet");
    assertNotNull(byFacet);
    assertEquals(keysOf(byFacet), Set.of("hasOwners", "hasTags"));
  }

  /**
   * The _index predicate is the load-bearing part of the batch: entity indices are aliases over
   * timestamp-suffixed backing indices, so targeting the wrong name yields an empty bucket and
   * silently drops the entity's highlight.
   */
  @Test
  public void testEntityStatsRequestFiltersOnIndexAndFacetValues() {
    SearchRequest request =
        service.buildEntityStatsRequest(
            opContext, List.of(EntityType.DATASET), List.of("hasOwners"));
    String source = request.source().toString();

    assertTrue(source.contains("\"_index\""), "entity buckets must be scoped by _index");
    assertTrue(source.contains("datasetindex_v2"), "expected the dataset index alias as the term");
    assertTrue(source.contains("hasOwners"), "expected the facet field as a term filter");
    assertTrue(source.contains("removed"), "soft-deleted entities must be excluded");
  }

  @Test
  public void testEntityStatsRequestWithoutFacetsOmitsFacetAggregation() {
    SearchRequest request =
        service.buildEntityStatsRequest(opContext, List.of(EntityType.DATASET), List.of());

    FiltersAggregationBuilder byEntity =
        (FiltersAggregationBuilder) subAggByName(topLevelAgg(request), "by_entity");
    assertNotNull(byEntity);
    assertEquals(subAggByName(byEntity, "by_facet"), null);
  }

  @Test
  public void testEntityStatsDeduplicatesEntityTypes() throws Exception {
    Filters.Bucket datasetBucket = entityBucket(100L, ImmutableMap.of("hasOwners", 40L));
    Filters byEntity = mock(Filters.class);
    when(byEntity.getBucketByKey("DATASET")).thenReturn(datasetBucket);
    stubResponse(filterWith("by_entity", byEntity));

    Map<EntityType, EntityStats> result =
        service.getEntityStats(
            opContext, List.of(EntityType.DATASET, EntityType.DATASET), List.of("hasOwners"));

    // Repeated types would otherwise collide as duplicate aggregation bucket keys.
    assertEquals(result.size(), 1);
    assertEquals(result.get(EntityType.DATASET).getTotal(), 100);
  }

  @Test
  public void testEmptyInputsSkipTheQuery() throws Exception {
    assertEquals(service.getEntityStats(opContext, List.of(), List.of()), Map.of());
    assertEquals(
        service.getUniqueCountsByRange(opContext, USAGE_INDEX, Map.of(), BROWSER_ID), Map.of());
    verify(mockClient, times(0)).search(any(), any(SearchRequest.class), any());
  }

  // ---------------------------------------------------------------------------------------------
  // Response extraction
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testExtractUniqueCountReadsCardinality() {
    Filters.Bucket bucket = uniqueBucket(30L);
    Filters byRange = mock(Filters.class);
    when(byRange.getBucketByKey("weekly_current")).thenReturn(bucket);

    assertEquals(
        AnalyticsService.extractUniqueCount(filterWith("by_range", byRange), "weekly_current"), 30);
  }

  @Test
  public void testExtractEntityStatsReadsTotalsAndFacets() {
    Filters.Bucket datasetBucket =
        entityBucket(100L, ImmutableMap.of("hasOwners", 40L, "hasTags", 10L));
    Filters byEntity = mock(Filters.class);
    when(byEntity.getBucketByKey("DATASET")).thenReturn(datasetBucket);

    EntityStats stats =
        AnalyticsService.extractEntityStats(filterWith("by_entity", byEntity), "DATASET", FACETS);

    assertEquals(stats.getTotal(), 100);
    assertEquals(stats.countWithFacet("hasOwners"), 40);
    assertEquals(stats.countWithFacet("hasTags"), 10);
    assertEquals(stats.countWithFacet("neverRequested"), 0);
  }

  /** An entity type with no documents, or no index at all, still arrives as a zeroed bucket. */
  @Test
  public void testExtractEntityStatsAcceptsZeroedBucket() {
    Filters.Bucket emptyBucket = entityBucket(0L, ImmutableMap.of("hasOwners", 0L, "hasTags", 0L));
    Filters byEntity = mock(Filters.class);
    when(byEntity.getBucketByKey("CHART")).thenReturn(emptyBucket);

    EntityStats stats =
        AnalyticsService.extractEntityStats(filterWith("by_entity", byEntity), "CHART", FACETS);

    assertEquals(stats.getTotal(), 0);
    assertEquals(stats.countWithFacet("hasOwners"), 0);
  }

  /**
   * A keyed filters aggregation always emits every declared bucket, so anything missing is a
   * malformed response. Reading it as zero would publish fabricated analytics.
   */
  @Test
  public void testExtractThrowsOnMissingRangeBucket() {
    Filters byRange = mock(Filters.class);
    when(byRange.getBucketByKey(any())).thenReturn(null);
    Filter result = filterWith("by_range", byRange);

    assertThrows(
        IllegalStateException.class, () -> AnalyticsService.extractUniqueCount(result, "weekly"));
  }

  @Test
  public void testExtractThrowsOnMissingRangeAggregation() {
    Filter result = filterWith("by_range", null);

    assertThrows(
        IllegalStateException.class, () -> AnalyticsService.extractUniqueCount(result, "weekly"));
  }

  @Test
  public void testExtractThrowsOnMissingEntityBucket() {
    Filters byEntity = mock(Filters.class);
    when(byEntity.getBucketByKey(any())).thenReturn(null);
    Filter result = filterWith("by_entity", byEntity);

    assertThrows(
        IllegalStateException.class,
        () -> AnalyticsService.extractEntityStats(result, "DATASET", FACETS));
  }

  @Test
  public void testExtractThrowsOnMissingEntityAggregation() {
    Filter result = filterWith("by_entity", null);

    assertThrows(
        IllegalStateException.class,
        () -> AnalyticsService.extractEntityStats(result, "DATASET", FACETS));
  }

  @Test
  public void testExtractThrowsOnMissingFacetAggregation() {
    Aggregations noFacets = mock(Aggregations.class);
    when(noFacets.get("by_facet")).thenReturn(null);
    Filters.Bucket datasetBucket = mock(Filters.Bucket.class);
    when(datasetBucket.getDocCount()).thenReturn(100L);
    when(datasetBucket.getAggregations()).thenReturn(noFacets);
    Filters byEntity = mock(Filters.class);
    when(byEntity.getBucketByKey("DATASET")).thenReturn(datasetBucket);
    Filter result = filterWith("by_entity", byEntity);

    assertThrows(
        IllegalStateException.class,
        () -> AnalyticsService.extractEntityStats(result, "DATASET", FACETS));
  }

  @Test
  public void testExtractThrowsOnMissingFacetBucket() {
    Filters.Bucket datasetBucket = entityBucket(100L, ImmutableMap.of("hasOwners", 40L));
    Filters byEntity = mock(Filters.class);
    when(byEntity.getBucketByKey("DATASET")).thenReturn(datasetBucket);
    Filter result = filterWith("by_entity", byEntity);

    // hasTags was requested but never came back.
    assertThrows(
        IllegalStateException.class,
        () -> AnalyticsService.extractEntityStats(result, "DATASET", FACETS));
  }

  /** A failing search must surface, not degrade into zero counts. */
  @Test
  public void testSearchFailurePropagates() throws Exception {
    when(mockClient.search(any(), any(SearchRequest.class), any()))
        .thenThrow(new IOException("elasticsearch is down"));

    assertThrows(
        RuntimeException.class,
        () -> service.getUniqueCountsByRange(opContext, USAGE_INDEX, twoRanges(), BROWSER_ID));
    assertThrows(
        RuntimeException.class,
        () -> service.getEntityStats(opContext, List.of(EntityType.DATASET), FACETS));
  }

  // ---------------------------------------------------------------------------------------------
  // Mock builders. Nested mocks must be fully built before being handed to thenReturn().
  // ---------------------------------------------------------------------------------------------

  private void stubResponse(Filter filteredAgg) throws Exception {
    Aggregations topLevel = mock(Aggregations.class);
    when(topLevel.get("filtered")).thenReturn(filteredAgg);
    SearchResponse response = mock(SearchResponse.class);
    when(response.getAggregations()).thenReturn(topLevel);
    when(mockClient.search(any(), any(SearchRequest.class), any())).thenReturn(response);
  }

  private Filter filterWith(String subAggName, Filters subAgg) {
    Aggregations aggs = mock(Aggregations.class);
    when(aggs.get(subAggName)).thenReturn(subAgg);
    Filter filter = mock(Filter.class);
    when(filter.getAggregations()).thenReturn(aggs);
    return filter;
  }

  private Filters.Bucket entityBucket(long total, Map<String, Long> facetCounts) {
    Filters byFacet = mock(Filters.class);
    facetCounts.forEach(
        (field, count) -> {
          Filters.Bucket facetBucket = mock(Filters.Bucket.class);
          when(facetBucket.getDocCount()).thenReturn(count);
          when(byFacet.getBucketByKey(field)).thenReturn(facetBucket);
        });
    Aggregations aggs = mock(Aggregations.class);
    when(aggs.get("by_facet")).thenReturn(byFacet);

    Filters.Bucket bucket = mock(Filters.Bucket.class);
    when(bucket.getDocCount()).thenReturn(total);
    when(bucket.getAggregations()).thenReturn(aggs);
    return bucket;
  }

  private Filters.Bucket uniqueBucket(long uniqueValue) {
    Cardinality cardinality = mock(Cardinality.class);
    when(cardinality.getValue()).thenReturn(uniqueValue);
    Aggregations aggs = mock(Aggregations.class);
    when(aggs.get("unique")).thenReturn(cardinality);
    Filters.Bucket bucket = mock(Filters.Bucket.class);
    when(bucket.getAggregations()).thenReturn(aggs);
    return bucket;
  }
}
