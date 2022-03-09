package com.linkedin.datahub.graphql.analytics.service;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.generated.BarSegment;
import com.linkedin.datahub.graphql.generated.Cell;
import com.linkedin.datahub.graphql.generated.DateInterval;
import com.linkedin.datahub.graphql.generated.DateRange;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.NamedBar;
import com.linkedin.datahub.graphql.generated.NamedLine;
import com.linkedin.datahub.graphql.generated.NumericDataPoint;
import com.linkedin.datahub.graphql.generated.Row;
import com.linkedin.datahub.graphql.resolvers.EntityTypeMapper;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Cardinality;
import org.elasticsearch.search.builder.SearchSourceBuilder;


@Slf4j
@RequiredArgsConstructor
public class AnalyticsService {

  private final RestHighLevelClient _elasticClient;
  private final IndexConvention _indexConvention;

  private static final String FILTERED = "filtered";
  private static final String DATE_HISTOGRAM = "date_histogram";
  private static final String UNIQUE = "unique";
  private static final String DIMENSION = "dimension";
  private static final String SECOND_DIMENSION = "second_dimension";
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

  public List<NamedLine> getTimeseriesChart(String indexName, DateRange dateRange, DateInterval granularity,
      Optional<String> dimension, // Length 1 for now
      Map<String, List<String>> filters, Map<String, List<String>> mustNotFilters, Optional<String> uniqueOn) {

    log.debug(
        String.format("Invoked getTimeseriesChart with indexName: %s, dateRange: %s, granularity: %s, dimension: %s,",
            indexName, dateRange, granularity, dimension) + String.format("filters: %s, uniqueOn: %s", filters,
            uniqueOn));

    AggregationBuilder filteredAgg = getFilteredAggregation(filters, mustNotFilters, Optional.of(dateRange));

    AggregationBuilder dateHistogram = AggregationBuilders.dateHistogram(DATE_HISTOGRAM)
        .field("timestamp")
        .calendarInterval(new DateHistogramInterval(granularity.name().toLowerCase()));
    uniqueOn.ifPresent(s -> dateHistogram.subAggregation(getUniqueQuery(s)));

    if (dimension.isPresent()) {
      filteredAgg.subAggregation(
          AggregationBuilders.terms(DIMENSION).field(dimension.get()).subAggregation(dateHistogram));
    } else {
      filteredAgg.subAggregation(dateHistogram);
    }

    SearchRequest searchRequest = constructSearchRequest(indexName, filteredAgg);
    Aggregations aggregationResult = executeAndExtract(searchRequest).getAggregations();
    try {
      if (dimension.isPresent()) {
        return aggregationResult.<Terms>get(DIMENSION).getBuckets()
            .stream()
            .map(bucket -> new NamedLine(bucket.getKeyAsString(),
                extractPointsFromAggregations(bucket.getAggregations(), uniqueOn.isPresent())))
            .collect(Collectors.toList());
      } else {
        return ImmutableList.of(
            new NamedLine("total", extractPointsFromAggregations(aggregationResult, uniqueOn.isPresent())));
      }
    } catch (Exception e) {
      log.error(String.format("Caught exception while getting time series chart: %s", e.getMessage()));
      return ImmutableList.of();
    }
  }

  private int extractCount(MultiBucketsAggregation.Bucket bucket, boolean didUnique) {
    return didUnique ? (int) bucket.getAggregations().<Cardinality>get(UNIQUE).getValue() : (int) bucket.getDocCount();
  }

  private List<NumericDataPoint> extractPointsFromAggregations(Aggregations aggregations, boolean didUnique) {
    return aggregations.<Histogram>get(DATE_HISTOGRAM).getBuckets()
        .stream()
        .map(bucket -> new NumericDataPoint(bucket.getKeyAsString(), extractCount(bucket, didUnique)))
        .collect(Collectors.toList());
  }

  public List<NamedBar> getBarChart(String indexName, Optional<DateRange> dateRange, List<String> dimensions,
      // Length 1 or 2
      Map<String, List<String>> filters, Map<String, List<String>> mustNotFilters, Optional<String> uniqueOn,
      boolean showMissing) {
    log.debug(
        String.format("Invoked getBarChart with indexName: %s, dateRange: %s, dimensions: %s,", indexName, dateRange,
            dimensions) + String.format("filters: %s, uniqueOn: %s", filters, uniqueOn));

    assert (dimensions.size() == 1 || dimensions.size() == 2);
    AggregationBuilder filteredAgg = getFilteredAggregation(filters, mustNotFilters, dateRange);

    TermsAggregationBuilder termAgg = AggregationBuilders.terms(DIMENSION).field(dimensions.get(0));
    if (showMissing) {
      termAgg.missing(NA);
    }

    if (dimensions.size() == 2) {
      TermsAggregationBuilder secondTermAgg = AggregationBuilders.terms(SECOND_DIMENSION).field(dimensions.get(1));
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
    Aggregations aggregationResult = executeAndExtract(searchRequest).getAggregations();

    try {
      if (dimensions.size() == 1) {
        List<BarSegment> barSegments =
            extractBarSegmentsFromAggregations(aggregationResult, DIMENSION, uniqueOn.isPresent());
        return barSegments.stream()
            .map(segment -> new NamedBar(segment.getLabel(),
                ImmutableList.of(BarSegment.builder().setLabel("Count").setValue(segment.getValue()).build())))
            .collect(Collectors.toList());
      } else {
        return aggregationResult.<Terms>get(DIMENSION).getBuckets()
            .stream()
            .map(bucket -> new NamedBar(bucket.getKeyAsString(),
                extractBarSegmentsFromAggregations(bucket.getAggregations(), SECOND_DIMENSION, uniqueOn.isPresent())))
            .collect(Collectors.toList());
      }
    } catch (Exception e) {
      log.error(String.format("Caught exception while getting bar chart: %s", e.getMessage()));
      return ImmutableList.of();
    }
  }

  private List<BarSegment> extractBarSegmentsFromAggregations(Aggregations aggregations, String aggregationKey,
      boolean didUnique) {
    return aggregations.<Terms>get(aggregationKey).getBuckets()
        .stream()
        .map(bucket -> new BarSegment(bucket.getKeyAsString(), extractCount(bucket, didUnique)))
        .collect(Collectors.toList());
  }

  public Row buildRow(String groupByValue, Function<String, Cell> groupByValueToCell, int count) {
    List<String> values = ImmutableList.of(groupByValue, String.valueOf(count));
    List<Cell> cells = ImmutableList.of(groupByValueToCell.apply(groupByValue),
        Cell.builder().setValue(String.valueOf(count)).build());
    return new Row(values, cells);
  }

  public List<Row> getTopNTableChart(String indexName, Optional<DateRange> dateRange, String groupBy,
      Map<String, List<String>> filters, Map<String, List<String>> mustNotFilters, Optional<String> uniqueOn,
      int maxRows, Function<String, Cell> groupByValueToCell) {
    log.debug(
        String.format("Invoked getTopNTableChart with indexName: %s, dateRange: %s, groupBy: %s", indexName, dateRange,
            groupBy) + String.format("filters: %s, uniqueOn: %s", filters, uniqueOn));

    AggregationBuilder filteredAgg = getFilteredAggregation(filters, mustNotFilters, dateRange);

    TermsAggregationBuilder termAgg = AggregationBuilders.terms(DIMENSION).field(groupBy).size(maxRows);
    if (uniqueOn.isPresent()) {
      termAgg.order(BucketOrder.aggregation(UNIQUE, false));
      termAgg.subAggregation(getUniqueQuery(uniqueOn.get()));
    }
    filteredAgg.subAggregation(termAgg);

    SearchRequest searchRequest = constructSearchRequest(indexName, filteredAgg);
    Aggregations aggregationResult = executeAndExtract(searchRequest).getAggregations();

    try {
      return aggregationResult.<Terms>get(DIMENSION).getBuckets()
          .stream()
          .map(bucket -> buildRow(bucket.getKeyAsString(), groupByValueToCell,
              extractCount(bucket, uniqueOn.isPresent())))
          .collect(Collectors.toList());
    } catch (Exception e) {
      log.error(String.format("Caught exception while getting top n chart: %s", e.getMessage()));
      return ImmutableList.of();
    }
  }

  public int getHighlights(String indexName, Optional<DateRange> dateRange, Map<String, List<String>> filters,
      Map<String, List<String>> mustNotFilters, Optional<String> uniqueOn) {
    log.debug(
        String.format("Invoked getHighlights with indexName: %s, dateRange: %s", indexName, dateRange) + String.format(
            "filters: %s, uniqueOn: %s", filters, uniqueOn));

    AggregationBuilder filteredAgg = getFilteredAggregation(filters, mustNotFilters, dateRange);
    uniqueOn.ifPresent(s -> filteredAgg.subAggregation(getUniqueQuery(s)));

    SearchRequest searchRequest = constructSearchRequest(indexName, filteredAgg);
    Filter aggregationResult = executeAndExtract(searchRequest);
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

  private SearchRequest constructSearchRequest(String indexName, AggregationBuilder aggregationBuilder) {
    SearchRequest searchRequest = new SearchRequest(indexName);
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.size(0);
    searchSourceBuilder.aggregation(aggregationBuilder);
    searchRequest.source(searchSourceBuilder);
    return searchRequest;
  }

  private Filter executeAndExtract(SearchRequest searchRequest) {
    try {
      final SearchResponse searchResponse = _elasticClient.search(searchRequest, RequestOptions.DEFAULT);
      // extract results, validated against document model as well
      return searchResponse.getAggregations().<Filter>get(FILTERED);
    } catch (Exception e) {
      log.error(String.format("Search query failed: %s", e.getMessage()));
      throw new RuntimeException("Search query failed:", e);
    }
  }

  private AggregationBuilder getFilteredAggregation(Map<String, List<String>> mustFilters,
      Map<String, List<String>> mustNotFilters, Optional<DateRange> dateRange) {
    BoolQueryBuilder filteredQuery = QueryBuilders.boolQuery();
    mustFilters.forEach((key, values) -> filteredQuery.must(QueryBuilders.termsQuery(key, values)));
    mustNotFilters.forEach((key, values) -> filteredQuery.mustNot(QueryBuilders.termsQuery(key, values)));
    dateRange.ifPresent(range -> filteredQuery.must(dateRangeQuery(range)));
    return AggregationBuilders.filter(FILTERED, filteredQuery);
  }

  private QueryBuilder dateRangeQuery(DateRange dateRange) {
    return QueryBuilders.rangeQuery("timestamp").gte(dateRange.getStart()).lt(dateRange.getEnd());
  }

  private AggregationBuilder getUniqueQuery(String uniqueOn) {
    return AggregationBuilders.cardinality(UNIQUE).field(uniqueOn);
  }
}
