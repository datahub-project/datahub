package com.linkedin.datahub.graphql.analytics.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.linkedin.datahub.graphql.analytics.service.AnalyticsService.EntityStats;
import com.linkedin.datahub.graphql.generated.DateRange;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.mockito.ArgumentCaptor;
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

  private SearchClientShim<?> mockClient;
  private OperationContext mockOpContext;
  private AnalyticsService service;

  @BeforeMethod
  public void setup() {
    mockClient = mock(SearchClientShim.class);
    mockOpContext = mock(OperationContext.class);

    IndexConvention mockIndexConvention = mock(IndexConvention.class);
    when(mockIndexConvention.getEntityIndexName(any()))
        .thenAnswer(invocation -> invocation.getArgument(0).toString().toLowerCase() + "index_v2");
    when(mockIndexConvention.getIndexName(AnalyticsService.DATAHUB_USAGE_EVENT_INDEX))
        .thenReturn(USAGE_INDEX);

    service = new AnalyticsService(mockClient, mockIndexConvention);
  }

  private SearchRequest captureRequest() throws Exception {
    ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
    verify(mockClient, times(1)).search(any(), captor.capture(), any());
    return captor.getValue();
  }

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

  private Filters.Bucket bucket(long docCount) {
    Filters.Bucket bucket = mock(Filters.Bucket.class);
    when(bucket.getDocCount()).thenReturn(docCount);
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

  private static AggregationBuilder subAggByName(AggregationBuilder parent, String name) {
    return parent.getSubAggregations().stream()
        .filter(agg -> name.equals(agg.getName()))
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

  private Map<String, DateRange> twoRanges() {
    Map<String, DateRange> ranges = new LinkedHashMap<>();
    ranges.put("weekly_current", new DateRange("100", "200"));
    ranges.put("weekly_previous", new DateRange("0", "100"));
    return ranges;
  }

  @Test
  public void testUniqueCountsByRangeIssuesSingleRequest() throws Exception {
    // Nested mocks must be fully built before they are handed to thenReturn().
    Filters.Bucket currentBucket = uniqueBucket(30L);
    Filters.Bucket previousBucket = uniqueBucket(20L);
    Filters byRange = mock(Filters.class);
    when(byRange.getBucketByKey("weekly_current")).thenReturn(currentBucket);
    when(byRange.getBucketByKey("weekly_previous")).thenReturn(previousBucket);
    stubResponse(filterWith("by_range", byRange));

    Map<String, Integer> result =
        service.getUniqueCountsByRange(mockOpContext, USAGE_INDEX, twoRanges(), "browserId");

    assertEquals(result, ImmutableMap.of("weekly_current", 30, "weekly_previous", 20));

    SearchRequest request = captureRequest();
    assertEquals(request.indices(), new String[] {USAGE_INDEX});

    AggregationBuilder filtered =
        request.source().aggregations().getAggregatorFactories().stream().findFirst().orElse(null);
    assertNotNull(filtered);
    FiltersAggregationBuilder byRangeAgg =
        (FiltersAggregationBuilder) subAggByName(filtered, "by_range");
    assertNotNull(byRangeAgg);
    assertEquals(keysOf(byRangeAgg), Set.of("weekly_current", "weekly_previous"));
    // The cardinality metric hangs off the range buckets, not off separate queries.
    assertNotNull(subAggByName(byRangeAgg, "unique"));
  }

  /** A range whose bucket never came back must read as zero, not blow up the whole panel. */
  @Test
  public void testUniqueCountsByRangeMissingBucket() throws Exception {
    Filters.Bucket currentBucket = uniqueBucket(30L);
    Filters byRange = mock(Filters.class);
    when(byRange.getBucketByKey("weekly_current")).thenReturn(currentBucket);
    when(byRange.getBucketByKey("weekly_previous")).thenReturn(null);
    stubResponse(filterWith("by_range", byRange));

    Map<String, Integer> result =
        service.getUniqueCountsByRange(mockOpContext, USAGE_INDEX, twoRanges(), "browserId");

    assertEquals(result, ImmutableMap.of("weekly_current", 30, "weekly_previous", 0));
  }

  @Test
  public void testEntityStatsIssuesSingleRequestAcrossAllIndices() throws Exception {
    Filters.Bucket ownersBucket = bucket(40L);
    Filters.Bucket tagsBucket = bucket(10L);
    Filters byFacet = mock(Filters.class);
    when(byFacet.getBucketByKey("hasOwners")).thenReturn(ownersBucket);
    when(byFacet.getBucketByKey("hasTags")).thenReturn(tagsBucket);

    Aggregations datasetSubAggs = mock(Aggregations.class);
    when(datasetSubAggs.get("by_facet")).thenReturn(byFacet);
    Filters.Bucket datasetBucket = mock(Filters.Bucket.class);
    when(datasetBucket.getDocCount()).thenReturn(100L);
    when(datasetBucket.getAggregations()).thenReturn(datasetSubAggs);

    Filters byEntity = mock(Filters.class);
    when(byEntity.getBucketByKey("DATASET")).thenReturn(datasetBucket);
    when(byEntity.getBucketByKey("CHART")).thenReturn(null);
    stubResponse(filterWith("by_entity", byEntity));

    Map<EntityType, EntityStats> result =
        service.getEntityStats(
            mockOpContext,
            List.of(EntityType.DATASET, EntityType.CHART),
            List.of("hasOwners", "hasTags"));

    assertEquals(result.get(EntityType.DATASET).getTotal(), 100);
    assertEquals(result.get(EntityType.DATASET).getFacetCount("hasOwners"), 40);
    assertEquals(result.get(EntityType.DATASET).getFacetCount("hasTags"), 10);
    // Absent bucket degrades to empty stats, which the resolver renders as "skip this type".
    assertEquals(result.get(EntityType.CHART).getTotal(), 0);

    SearchRequest request = captureRequest();
    assertEquals(request.indices(), new String[] {"datasetindex_v2", "chartindex_v2"});
    // A single missing index must not fail the batch.
    assertTrue(request.indicesOptions().ignoreUnavailable());
    assertTrue(request.indicesOptions().allowNoIndices());

    AggregationBuilder filtered =
        request.source().aggregations().getAggregatorFactories().stream().findFirst().orElse(null);
    assertNotNull(filtered);
    FiltersAggregationBuilder byEntityAgg =
        (FiltersAggregationBuilder) subAggByName(filtered, "by_entity");
    assertNotNull(byEntityAgg);
    assertEquals(keysOf(byEntityAgg), Set.of("DATASET", "CHART"));

    FiltersAggregationBuilder byFacetAgg =
        (FiltersAggregationBuilder) subAggByName(byEntityAgg, "by_facet");
    assertNotNull(byFacetAgg);
    assertEquals(keysOf(byFacetAgg), Set.of("hasOwners", "hasTags"));
  }

  @Test
  public void testEntityStatsEmptyInputSkipsQuery() throws Exception {
    assertEquals(service.getEntityStats(mockOpContext, List.of(), List.of()), Map.of());
    verify(mockClient, times(0)).search(any(), any(SearchRequest.class), any());
  }
}
