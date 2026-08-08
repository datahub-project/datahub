package com.linkedin.datahub.graphql.analytics.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.datahub.graphql.generated.BarSegment;
import com.linkedin.datahub.graphql.generated.Cell;
import com.linkedin.datahub.graphql.generated.DateInterval;
import com.linkedin.datahub.graphql.generated.DateRange;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.NamedBar;
import com.linkedin.datahub.graphql.generated.NamedLine;
import com.linkedin.datahub.graphql.generated.NumericDataPoint;
import com.linkedin.datahub.graphql.generated.Row;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.metadata.datahubusage.DataHubUsageEventConstants;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.RequestOptions;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.opensearch.search.aggregations.bucket.filter.Filter;
import org.opensearch.search.aggregations.bucket.filter.Filters;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregator.KeyedFilter;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.Cardinality;
import org.opensearch.search.builder.SearchSourceBuilder;

@Slf4j
@RequiredArgsConstructor
public class AnalyticsService {

  private final SearchClientShim<?> _elasticClient;
  private final IndexConvention _indexConvention;

  private static final String FILTERED = "filtered";
  private static final String DATE_HISTOGRAM = "date_histogram";
  private static final String UNIQUE = "unique";
  private static final String DIMENSION = "dimension";
  private static final String SECOND_DIMENSION = "second_dimension";
  private static final String BY_RANGE = "by_range";
  private static final String BY_ENTITY = "by_entity";
  private static final String BY_FACET = "by_facet";
  private static final String INDEX_FIELD = "_index";
  private static final String REMOVED = "removed";
  private static final String TRUE = "true";
  public static final String NA = "N/A";

  public static final String DATAHUB_USAGE_EVENT_INDEX = "datahub_usage_event";

  @Nonnull
  public String getEntityIndexName(EntityType entityType) {
    return _indexConvention.getEntityIndexName(EntityTypeMapper.getName(entityType));
  }

  @Nonnull
  public String getAllEntityIndexName() {
    return _indexConvention.getEntityIndexName("*");
  }

  @Nonnull
  public String getUsageIndexName() {
    return _indexConvention.getIndexName(DATAHUB_USAGE_EVENT_INDEX);
  }

  public List<NamedLine> getTimeseriesChart(
      @Nonnull OperationContext opContext,
      String indexName,
      DateRange dateRange,
      DateInterval granularity,
      Optional<String> dimension, // Length 1 for now
      Map<String, List<String>> filters,
      Map<String, List<String>> mustNotFilters,
      Optional<String> uniqueOn,
      String dateRangeField) {

    log.debug(
        String.format(
                "Invoked getTimeseriesChart with indexName: %s, dateRange: %s to %s, granularity: %s, dimension: %s,",
                indexName, dateRange.getStart(), dateRange.getEnd(), granularity, dimension)
            + String.format("filters: %s, uniqueOn: %s", filters, uniqueOn));

    AggregationBuilder filteredAgg =
        getFilteredAggregation(filters, mustNotFilters, Optional.of(dateRange), dateRangeField);

    AggregationBuilder dateHistogram =
        AggregationBuilders.dateHistogram(DATE_HISTOGRAM)
            .field(dateRangeField)
            .calendarInterval(new DateHistogramInterval(granularity.name().toLowerCase()));
    uniqueOn.ifPresent(s -> dateHistogram.subAggregation(getUniqueQuery(s)));

    if (dimension.isPresent()) {
      filteredAgg.subAggregation(
          AggregationBuilders.terms(DIMENSION)
              .field(dimension.get())
              .subAggregation(dateHistogram));
    } else {
      filteredAgg.subAggregation(dateHistogram);
    }

    SearchRequest searchRequest = constructSearchRequest(indexName, filteredAgg);
    Aggregations aggregationResult = executeAndExtract(opContext, searchRequest).getAggregations();
    try {
      if (dimension.isPresent()) {
        return aggregationResult.<Terms>get(DIMENSION).getBuckets().stream()
            .map(
                bucket ->
                    new NamedLine(
                        bucket.getKeyAsString(),
                        extractPointsFromAggregations(
                            bucket.getAggregations(), uniqueOn.isPresent())))
            .collect(Collectors.toList());
      } else {
        return ImmutableList.of(
            new NamedLine(
                "total", extractPointsFromAggregations(aggregationResult, uniqueOn.isPresent())));
      }
    } catch (Exception e) {
      log.error(
          String.format("Caught exception while getting time series chart: %s", e.getMessage()));
      return ImmutableList.of();
    }
  }

  public List<NamedLine> getTimeseriesChart(
      @Nonnull OperationContext opContext,
      String indexName,
      DateRange dateRange,
      DateInterval granularity,
      Optional<String> dimension, // Length 1 for now
      Map<String, List<String>> filters,
      Map<String, List<String>> mustNotFilters,
      Optional<String> uniqueOn) {
    return getTimeseriesChart(
        opContext,
        indexName,
        dateRange,
        granularity,
        dimension,
        filters,
        mustNotFilters,
        uniqueOn,
        "timestamp");
  }

  private int extractCount(MultiBucketsAggregation.Bucket bucket, boolean didUnique) {
    return didUnique
        ? (int) bucket.getAggregations().<Cardinality>get(UNIQUE).getValue()
        : (int) bucket.getDocCount();
  }

  private List<NumericDataPoint> extractPointsFromAggregations(
      Aggregations aggregations, boolean didUnique) {
    return aggregations.<Histogram>get(DATE_HISTOGRAM).getBuckets().stream()
        .map(
            bucket ->
                new NumericDataPoint(bucket.getKeyAsString(), extractCount(bucket, didUnique)))
        .collect(Collectors.toList());
  }

  public List<NamedBar> getBarChart(
      @Nonnull OperationContext opContext,
      String indexName,
      Optional<DateRange> dateRange,
      List<String> dimensions,
      // Length 1 or 2
      Map<String, List<String>> filters,
      Map<String, List<String>> mustNotFilters,
      Optional<String> uniqueOn,
      boolean showMissing) {
    log.debug(
        String.format(
                "Invoked getBarChart with indexName: %s, dateRange: %s, dimensions: %s,",
                indexName, dateRange, dimensions)
            + String.format("filters: %s, uniqueOn: %s", filters, uniqueOn));

    if (!(dimensions.size() == 1 || dimensions.size() == 2)) {
      throw new IllegalArgumentException("Dimensions must have 1 or 2 specified: " + dimensions);
    }
    AggregationBuilder filteredAgg = getFilteredAggregation(filters, mustNotFilters, dateRange);

    TermsAggregationBuilder termAgg = AggregationBuilders.terms(DIMENSION).field(dimensions.get(0));
    if (showMissing) {
      termAgg.missing(NA);
    }

    if (dimensions.size() == 2) {
      TermsAggregationBuilder secondTermAgg =
          AggregationBuilders.terms(SECOND_DIMENSION).field(dimensions.get(1));
      if (showMissing) {
        secondTermAgg.missing(NA);
      }
      uniqueOn.ifPresent(s -> secondTermAgg.subAggregation(getUniqueQuery(s)));
      termAgg.subAggregation(secondTermAgg);
    } else {
      uniqueOn.ifPresent(s -> termAgg.subAggregation(getUniqueQuery(s)));
    }
    filteredAgg.subAggregation(termAgg);

    SearchRequest searchRequest = constructSearchRequest(indexName, filteredAgg);
    Aggregations aggregationResult = executeAndExtract(opContext, searchRequest).getAggregations();

    try {
      if (dimensions.size() == 1) {
        List<BarSegment> barSegments =
            extractBarSegmentsFromAggregations(aggregationResult, DIMENSION, uniqueOn.isPresent());
        return barSegments.stream()
            .map(
                segment ->
                    new NamedBar(
                        segment.getLabel(),
                        ImmutableList.of(
                            BarSegment.builder()
                                .setLabel("Count")
                                .setValue(segment.getValue())
                                .build())))
            .collect(Collectors.toList());
      } else {
        return aggregationResult.<Terms>get(DIMENSION).getBuckets().stream()
            .map(
                bucket ->
                    new NamedBar(
                        bucket.getKeyAsString(),
                        extractBarSegmentsFromAggregations(
                            bucket.getAggregations(), SECOND_DIMENSION, uniqueOn.isPresent())))
            .collect(Collectors.toList());
      }
    } catch (Exception e) {
      log.error(String.format("Caught exception while getting bar chart: %s", e.getMessage()));
      return ImmutableList.of();
    }
  }

  private List<BarSegment> extractBarSegmentsFromAggregations(
      Aggregations aggregations, String aggregationKey, boolean didUnique) {
    return aggregations.<Terms>get(aggregationKey).getBuckets().stream()
        .map(bucket -> new BarSegment(bucket.getKeyAsString(), extractCount(bucket, didUnique)))
        .collect(Collectors.toList());
  }

  public static Row buildRow(
      String groupByValue, Function<String, Cell> groupByValueToCell, int count) {
    List<String> values = ImmutableList.of(groupByValue, String.valueOf(count));
    List<Cell> cells =
        ImmutableList.of(
            groupByValueToCell.apply(groupByValue),
            Cell.builder().setValue(String.valueOf(count)).build());
    return new Row(values, cells);
  }

  public List<Row> getTopNTableChart(
      @Nonnull OperationContext opContext,
      String indexName,
      Optional<DateRange> dateRange,
      String groupBy,
      Map<String, List<String>> filters,
      Map<String, List<String>> mustNotFilters,
      Optional<String> uniqueOn,
      int maxRows,
      Function<String, Cell> groupByValueToCell) {
    log.debug(
        String.format(
                "Invoked getTopNTableChart with indexName: %s, dateRange: %s, groupBy: %s",
                indexName, dateRange, groupBy)
            + String.format("filters: %s, uniqueOn: %s", filters, uniqueOn));

    AggregationBuilder filteredAgg = getFilteredAggregation(filters, mustNotFilters, dateRange);

    TermsAggregationBuilder termAgg =
        AggregationBuilders.terms(DIMENSION).field(groupBy).size(maxRows);
    if (uniqueOn.isPresent()) {
      termAgg.order(BucketOrder.aggregation(UNIQUE, false));
      termAgg.subAggregation(getUniqueQuery(uniqueOn.get()));
    }
    filteredAgg.subAggregation(termAgg);

    SearchRequest searchRequest = constructSearchRequest(indexName, filteredAgg);
    Aggregations aggregationResult = executeAndExtract(opContext, searchRequest).getAggregations();

    try {
      return aggregationResult.<Terms>get(DIMENSION).getBuckets().stream()
          .map(
              bucket ->
                  buildRow(
                      bucket.getKeyAsString(),
                      groupByValueToCell,
                      extractCount(bucket, uniqueOn.isPresent())))
          .collect(Collectors.toList());
    } catch (Exception e) {
      log.error(String.format("Caught exception while getting top n chart: %s", e.getMessage()));
      return ImmutableList.of();
    }
  }

  public int getHighlights(
      @Nonnull OperationContext opContext,
      String indexName,
      Optional<DateRange> dateRange,
      Map<String, List<String>> filters,
      Map<String, List<String>> mustNotFilters,
      Optional<String> uniqueOn) {
    log.debug(
        String.format(
                "Invoked getHighlights with indexName: %s, dateRange: %s", indexName, dateRange)
            + String.format("filters: %s, uniqueOn: %s", filters, uniqueOn));

    AggregationBuilder filteredAgg = getFilteredAggregation(filters, mustNotFilters, dateRange);
    uniqueOn.ifPresent(s -> filteredAgg.subAggregation(getUniqueQuery(s)));

    SearchRequest searchRequest = constructSearchRequest(indexName, filteredAgg);
    Filter aggregationResult = executeAndExtract(opContext, searchRequest);
    try {
      if (uniqueOn.isPresent()) {
        return (int) aggregationResult.getAggregations().<Cardinality>get(UNIQUE).getValue();
      } else {
        return (int) aggregationResult.getDocCount();
      }
    } catch (Exception e) {
      log.error(String.format("Caught exception while getting highlights: %s", e.getMessage()));
      return 0;
    }
  }

  /**
   * Computes a unique count per date range in a single query, rather than one query per range. Keys
   * of the result mirror the keys of {@code keyedRanges}; a range whose bucket is missing from the
   * response maps to 0.
   */
  public Map<String, Integer> getUniqueCountsByRange(
      @Nonnull OperationContext opContext,
      @Nonnull String indexName,
      @Nonnull Map<String, DateRange> keyedRanges,
      @Nonnull String uniqueOn) {
    if (keyedRanges.isEmpty()) {
      return Collections.emptyMap();
    }

    KeyedFilter[] rangeFilters =
        keyedRanges.entrySet().stream()
            .map(entry -> new KeyedFilter(entry.getKey(), dateRangeQuery(entry.getValue())))
            .toArray(KeyedFilter[]::new);

    AggregationBuilder filteredAgg =
        getFilteredAggregation(ImmutableMap.of(), ImmutableMap.of(), Optional.empty());
    filteredAgg.subAggregation(
        AggregationBuilders.filters(BY_RANGE, rangeFilters)
            .subAggregation(getUniqueQuery(uniqueOn)));

    Filter aggregationResult =
        executeAndExtract(opContext, constructSearchRequest(indexName, filteredAgg));

    Map<String, Integer> results = new LinkedHashMap<>();
    for (String key : keyedRanges.keySet()) {
      results.put(key, extractUniqueCount(aggregationResult, key));
    }
    return results;
  }

  private int extractUniqueCount(Filter aggregationResult, String rangeKey) {
    try {
      Filters byRange = aggregationResult.getAggregations().get(BY_RANGE);
      Filters.Bucket bucket = byRange.getBucketByKey(rangeKey);
      if (bucket == null) {
        return 0;
      }
      return (int) bucket.getAggregations().<Cardinality>get(UNIQUE).getValue();
    } catch (Exception e) {
      log.error(
          String.format(
              "Caught exception while extracting unique count for range %s: %s",
              rangeKey, e.getMessage()));
      return 0;
    }
  }

  /**
   * Computes the total document count plus one count per facet field, for every requested entity
   * type, in a single query across all of their indices.
   *
   * <p>Buckets are keyed by our own entity-type names rather than derived from the {@code _index}
   * term agg, because entity indices are aliases over timestamp-suffixed backing indices and the
   * index-name-to-entity-name round trip is lossy.
   */
  public Map<EntityType, EntityStats> getEntityStats(
      @Nonnull OperationContext opContext,
      @Nonnull List<EntityType> entityTypes,
      @Nonnull List<String> facetFields) {
    if (entityTypes.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<String, EntityType> keyToEntityType = new LinkedHashMap<>();
    entityTypes.forEach(entityType -> keyToEntityType.put(entityType.name(), entityType));

    KeyedFilter[] entityFilters =
        entityTypes.stream()
            .map(
                entityType ->
                    new KeyedFilter(
                        entityType.name(),
                        QueryBuilders.termQuery(INDEX_FIELD, getEntityIndexName(entityType))))
            .toArray(KeyedFilter[]::new);
    KeyedFilter[] facetFilters =
        facetFields.stream()
            .map(field -> new KeyedFilter(field, QueryBuilders.termsQuery(field, TRUE)))
            .toArray(KeyedFilter[]::new);

    AggregationBuilder byEntityAgg = AggregationBuilders.filters(BY_ENTITY, entityFilters);
    if (facetFilters.length > 0) {
      byEntityAgg.subAggregation(AggregationBuilders.filters(BY_FACET, facetFilters));
    }

    AggregationBuilder filteredAgg =
        getFilteredAggregation(
            ImmutableMap.of(), ImmutableMap.of(REMOVED, ImmutableList.of(TRUE)), Optional.empty());
    filteredAgg.subAggregation(byEntityAgg);

    String[] indices =
        entityTypes.stream().map(this::getEntityIndexName).distinct().toArray(String[]::new);
    SearchRequest searchRequest = constructSearchRequest(indices, filteredAgg);
    // A single absent index must not fail the whole batch — per-type queries previously degraded
    // to an empty highlight for that type alone.
    searchRequest.indicesOptions(IndicesOptions.lenientExpandOpen());

    Filter aggregationResult = executeAndExtract(opContext, searchRequest);

    Map<EntityType, EntityStats> results = new LinkedHashMap<>();
    keyToEntityType.forEach(
        (key, entityType) ->
            results.put(entityType, extractEntityStats(aggregationResult, key, facetFields)));
    return results;
  }

  private EntityStats extractEntityStats(
      Filter aggregationResult, String entityKey, List<String> facetFields) {
    try {
      Filters byEntity = aggregationResult.getAggregations().get(BY_ENTITY);
      Filters.Bucket entityBucket = byEntity.getBucketByKey(entityKey);
      if (entityBucket == null) {
        return EntityStats.empty();
      }

      Map<String, Integer> facetCounts = new LinkedHashMap<>();
      if (!facetFields.isEmpty()) {
        Filters byFacet = entityBucket.getAggregations().get(BY_FACET);
        for (String field : facetFields) {
          Filters.Bucket facetBucket = byFacet == null ? null : byFacet.getBucketByKey(field);
          facetCounts.put(field, facetBucket == null ? 0 : (int) facetBucket.getDocCount());
        }
      }
      return new EntityStats((int) entityBucket.getDocCount(), facetCounts);
    } catch (Exception e) {
      log.error(
          String.format(
              "Caught exception while extracting entity stats for %s: %s",
              entityKey, e.getMessage()));
      return EntityStats.empty();
    }
  }

  /** Total document count and per-facet counts for a single entity type. */
  @Value
  public static class EntityStats {
    int total;
    Map<String, Integer> facetCounts;

    public static EntityStats empty() {
      return new EntityStats(0, Collections.emptyMap());
    }

    public int getFacetCount(String facetField) {
      return facetCounts.getOrDefault(facetField, 0);
    }
  }

  private SearchRequest constructSearchRequest(
      String indexName, AggregationBuilder aggregationBuilder) {
    return constructSearchRequest(new String[] {indexName}, aggregationBuilder);
  }

  private SearchRequest constructSearchRequest(
      String[] indexNames, AggregationBuilder aggregationBuilder) {
    SearchRequest searchRequest = new SearchRequest(indexNames);
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.size(0);
    searchSourceBuilder.aggregation(aggregationBuilder);
    searchRequest.source(searchSourceBuilder);
    return searchRequest;
  }

  private Filter executeAndExtract(
      @Nonnull OperationContext opContext, SearchRequest searchRequest) {
    try {
      final SearchResponse searchResponse =
          _elasticClient.search(opContext, searchRequest, RequestOptions.DEFAULT);
      // extract results, validated against document model as well
      return searchResponse.getAggregations().<Filter>get(FILTERED);
    } catch (Exception e) {
      log.error(String.format("Search query failed: %s", e.getMessage()));
      throw new RuntimeException("Search query failed:", e);
    }
  }

  // Make dateRangeField as customizable
  private AggregationBuilder getFilteredAggregation(
      Map<String, List<String>> mustFilters,
      Map<String, List<String>> mustNotFilters,
      Optional<DateRange> dateRange,
      String dateRangeField) {
    BoolQueryBuilder filteredQuery = QueryBuilders.boolQuery();
    filteredQuery.filter(getDefaultFilters());
    mustFilters.forEach((key, values) -> filteredQuery.must(QueryBuilders.termsQuery(key, values)));
    mustNotFilters.forEach(
        (key, values) -> filteredQuery.mustNot(QueryBuilders.termsQuery(key, values)));
    dateRange.ifPresent(range -> filteredQuery.must(dateRangeQuery(range, dateRangeField)));
    return AggregationBuilders.filter(FILTERED, filteredQuery);
  }

  private AggregationBuilder getFilteredAggregation(
      Map<String, List<String>> mustFilters,
      Map<String, List<String>> mustNotFilters,
      Optional<DateRange> dateRange) {
    // Use timestamp as dateRangeField
    return getFilteredAggregation(mustFilters, mustNotFilters, dateRange, "timestamp");
  }

  private QueryBuilder getDefaultFilters() {
    return QueryBuilders.boolQuery()
        .mustNot(
            QueryBuilders.termQuery(
                DataHubUsageEventConstants.USAGE_SOURCE,
                DataHubUsageEventConstants.BACKEND_SOURCE));
  }

  private QueryBuilder dateRangeQuery(DateRange dateRange) {
    // Use timestamp as dateRangeField
    return dateRangeQuery(dateRange, "timestamp");
  }

  // Make dateRangeField as customizable
  private QueryBuilder dateRangeQuery(DateRange dateRange, String dateRangeField) {
    return QueryBuilders.rangeQuery(dateRangeField)
        .gte(dateRange.getStart())
        .lt(dateRange.getEnd());
  }

  private AggregationBuilder getUniqueQuery(String uniqueOn) {
    return AggregationBuilders.cardinality(UNIQUE).field(uniqueOn);
  }
}
