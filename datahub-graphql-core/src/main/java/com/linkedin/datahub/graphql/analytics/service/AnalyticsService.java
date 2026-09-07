package com.linkedin.datahub.graphql.analytics.service;

import com.google.common.annotations.VisibleForTesting;
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
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
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
  public String getEntityIndexName(@Nonnull OperationContext opContext, EntityType entityType) {
    return _indexConvention.getEntityIndexName(opContext, EntityTypeMapper.getName(entityType));
  }

  @Nonnull
  public String getAllEntityIndexName(@Nonnull OperationContext opContext) {
    return _indexConvention.getEntityIndexName(opContext, "*");
  }

  @Nonnull
  public String getUsageIndexName(@Nonnull OperationContext opContext) {
    return _indexConvention.getIndexName(opContext, DATAHUB_USAGE_EVENT_INDEX);
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
   * of the result mirror the keys of {@code keyedRanges}.
   */
  @WithSpan
  public Map<String, Integer> getUniqueCountsByRange(
      @Nonnull OperationContext opContext,
      String indexName,
      Map<String, DateRange> keyedRanges,
      String uniqueOn) {
    log.debug(
        String.format(
            "Invoked getUniqueCountsByRange with indexName: %s, ranges: %s, uniqueOn: %s",
            indexName, keyedRanges.keySet(), uniqueOn));

    if (keyedRanges.isEmpty()) {
      return Collections.emptyMap();
    }

    Filter aggregationResult =
        executeAndExtract(
            opContext, buildUniqueCountsByRangeRequest(indexName, keyedRanges, uniqueOn));

    Map<String, Integer> results = new LinkedHashMap<>();
    for (String key : keyedRanges.keySet()) {
      results.put(key, extractUniqueCount(aggregationResult, key));
    }
    return results;
  }

  @VisibleForTesting
  SearchRequest buildUniqueCountsByRangeRequest(
      String indexName, Map<String, DateRange> keyedRanges, String uniqueOn) {
    KeyedFilter[] rangeFilters =
        keyedRanges.entrySet().stream()
            .map(entry -> new KeyedFilter(entry.getKey(), dateRangeQuery(entry.getValue())))
            .toArray(KeyedFilter[]::new);

    AggregationBuilder filteredAgg = defaultFilteredAggregation();
    filteredAgg.subAggregation(
        AggregationBuilders.filters(BY_RANGE, rangeFilters)
            .subAggregation(getUniqueQuery(uniqueOn)));

    return constructSearchRequest(indexName, filteredAgg);
  }

  /**
   * A keyed filters aggregation always emits every declared bucket, empty ones carrying a zero doc
   * count, so a missing bucket is a malformed response rather than "no data". Batching makes that
   * distinction matter: silently reading it as zero would publish fabricated analytics for the
   * whole batch at once.
   */
  @VisibleForTesting
  static int extractUniqueCount(Filter aggregationResult, String rangeKey) {
    Filters byRange = aggregationResult.getAggregations().get(BY_RANGE);
    Filters.Bucket bucket = byRange == null ? null : byRange.getBucketByKey(rangeKey);
    if (bucket == null) {
      throw new IllegalStateException(
          String.format("Missing '%s' bucket for range %s", BY_RANGE, rangeKey));
    }
    return (int) bucket.getAggregations().<Cardinality>get(UNIQUE).getValue();
  }

  /**
   * Computes the total document count plus one count per facet field, for every requested entity
   * type, in a single query across all of their indices. Soft-deleted entities are excluded.
   *
   * <p>Buckets are keyed by our own entity-type names rather than derived from a terms aggregation
   * on {@code _index}, because entity indices are aliases over timestamp-suffixed backing indices
   * and the index-name-to-entity-name round trip is lossy.
   */
  @WithSpan
  public Map<EntityType, EntityStats> getEntityStats(
      @Nonnull OperationContext opContext, List<EntityType> entityTypes, List<String> facetFields) {
    log.debug(
        String.format(
            "Invoked getEntityStats with entityTypes: %s, facetFields: %s",
            entityTypes, facetFields));

    // Duplicates would collide as repeated aggregation bucket keys.
    List<EntityType> distinctTypes = entityTypes.stream().distinct().collect(Collectors.toList());
    if (distinctTypes.isEmpty()) {
      return Collections.emptyMap();
    }

    Filter aggregationResult =
        executeAndExtract(
            opContext, buildEntityStatsRequest(opContext, distinctTypes, facetFields));

    Map<EntityType, EntityStats> results = new LinkedHashMap<>();
    for (EntityType entityType : distinctTypes) {
      results.put(
          entityType, extractEntityStats(aggregationResult, entityType.name(), facetFields));
    }
    return results;
  }

  @VisibleForTesting
  SearchRequest buildEntityStatsRequest(
      @Nonnull OperationContext opContext, List<EntityType> entityTypes, List<String> facetFields) {
    // Resolve each entity's dynamic index name once, so the _index term filters and the request's
    // target indices are built from the same resolution (and to avoid duplicate resolver work).
    final Map<EntityType, String> indexByType = new LinkedHashMap<>();
    for (EntityType entityType : entityTypes) {
      indexByType.computeIfAbsent(entityType, type -> getEntityIndexName(opContext, type));
    }
    KeyedFilter[] entityFilters =
        entityTypes.stream()
            .map(
                entityType ->
                    new KeyedFilter(
                        entityType.name(),
                        QueryBuilders.termQuery(INDEX_FIELD, indexByType.get(entityType))))
            .toArray(KeyedFilter[]::new);
    KeyedFilter[] facetFilters =
        facetFields.stream()
            .map(field -> new KeyedFilter(field, QueryBuilders.termsQuery(field, TRUE)))
            .toArray(KeyedFilter[]::new);

    AggregationBuilder byEntityAgg = AggregationBuilders.filters(BY_ENTITY, entityFilters);
    if (facetFilters.length > 0) {
      byEntityAgg.subAggregation(AggregationBuilders.filters(BY_FACET, facetFilters));
    }

    AggregationBuilder filteredAgg = nonRemovedFilteredAggregation();
    filteredAgg.subAggregation(byEntityAgg);

    String[] indices = entityTypes.stream().map(indexByType::get).distinct().toArray(String[]::new);
    SearchRequest searchRequest = constructSearchRequest(indices, filteredAgg);
    // A single absent index must not fail the whole batch. Previously a missing index threw out of
    // the per-type query, propagated uncaught, and left the resolver's catch-all to blank the
    // entire panel; skipping it now costs only that entity type's card. An absent index still
    // yields its keyed bucket with a zero doc count, so this does not mask a malformed response.
    searchRequest.indicesOptions(IndicesOptions.lenientExpandOpen());
    return searchRequest;
  }

  /**
   * @see #extractUniqueCount for why a missing bucket is an error rather than a zero.
   */
  @VisibleForTesting
  static EntityStats extractEntityStats(
      Filter aggregationResult, String entityKey, List<String> facetFields) {
    Filters byEntity = aggregationResult.getAggregations().get(BY_ENTITY);
    Filters.Bucket entityBucket = byEntity == null ? null : byEntity.getBucketByKey(entityKey);
    if (entityBucket == null) {
      throw new IllegalStateException(
          String.format("Missing '%s' bucket for entity %s", BY_ENTITY, entityKey));
    }

    Map<String, Integer> facetCounts = new LinkedHashMap<>();
    if (!facetFields.isEmpty()) {
      Filters byFacet = entityBucket.getAggregations().get(BY_FACET);
      if (byFacet == null) {
        throw new IllegalStateException(
            String.format("Missing '%s' aggregation for entity %s", BY_FACET, entityKey));
      }
      for (String field : facetFields) {
        Filters.Bucket facetBucket = byFacet.getBucketByKey(field);
        if (facetBucket == null) {
          throw new IllegalStateException(
              String.format("Missing '%s' bucket for entity %s", field, entityKey));
        }
        facetCounts.put(field, (int) facetBucket.getDocCount());
      }
    }
    return new EntityStats((int) entityBucket.getDocCount(), facetCounts);
  }

  /** The standard filtered wrapper with no criteria beyond the default filters. */
  private AggregationBuilder defaultFilteredAggregation() {
    return getFilteredAggregation(ImmutableMap.of(), ImmutableMap.of(), Optional.empty());
  }

  /** As {@link #defaultFilteredAggregation} but excluding soft-deleted entities. */
  private AggregationBuilder nonRemovedFilteredAggregation() {
    return getFilteredAggregation(
        ImmutableMap.of(), ImmutableMap.of(REMOVED, ImmutableList.of(TRUE)), Optional.empty());
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
