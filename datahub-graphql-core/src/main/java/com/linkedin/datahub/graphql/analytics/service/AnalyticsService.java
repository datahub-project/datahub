package com.linkedin.datahub.graphql.analytics.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.datahub.graphql.generated.BarSegment;
import com.linkedin.datahub.graphql.generated.DateInterval;
import com.linkedin.datahub.graphql.generated.DateRange;
import com.linkedin.datahub.graphql.generated.NamedBar;
import com.linkedin.datahub.graphql.generated.NamedLine;
import com.linkedin.datahub.graphql.generated.NumericDataPoint;
import com.linkedin.datahub.graphql.generated.Row;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AnalyticsService {

  private final Logger _logger = LoggerFactory.getLogger(AnalyticsService.class.getName());

  private final RestHighLevelClient _elasticClient;
  private final Optional<String> _indexPrefix;

  private static final String FILTERED = "filtered";
  private static final String DATE_HISTOGRAM = "date_histogram";
  private static final String UNIQUE = "unique";
  private static final String DIMENSION = "dimension";
  private static final String SECOND_DIMENSION = "second_dimension";
  private static final String NA = "N/A";

  public static final String DATAHUB_USAGE_EVENT_INDEX = "datahub_usage_event";
  public static final String CHART_INDEX = "chartindex_v2";
  public static final String DASHBOARD_INDEX = "dashboardindex_v2";
  public static final String DATA_FLOW_INDEX = "dataflowindex_v2";
  public static final String DATA_JOB_INDEX = "datajobindex_v2";
  public static final String DATASET_INDEX = "datasetindex_v2";

  public AnalyticsService(final RestHighLevelClient elasticClient, final Optional<String> indexPrefix) {
    _elasticClient = elasticClient;
    _indexPrefix = indexPrefix;
  }

  private String getIndexName(String baseIndexName) {
    return _indexPrefix.map(p -> p + "_").orElse("") + baseIndexName;
  }

  public List<NamedLine> getTimeseriesChart(String indexName, DateRange dateRange, DateInterval granularity,
      Optional<String> dimension, // Length 1 for now
      Map<String, List<String>> filters, Optional<String> uniqueOn) {

    String finalIndexName = getIndexName(indexName);
    _logger.debug(
        String.format("Invoked getTimeseriesChart with indexName: %s, dateRange: %s, granularity: %s, dimension: %s,",
            finalIndexName, dateRange, granularity, dimension) + String.format("filters: %s, uniqueOn: %s", filters,
            uniqueOn));

    AggregationBuilder filteredAgg = getFilteredAggregation(filters, ImmutableMap.of(), Optional.of(dateRange));

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

    SearchRequest searchRequest = constructSearchRequest(finalIndexName, filteredAgg);
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
      _logger.error(String.format("Caught exception while getting time series chart: %s", e.getMessage()));
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
      Map<String, List<String>> filters, Optional<String> uniqueOn) {
    String finalIndexName = getIndexName(indexName);
    _logger.debug(
        String.format("Invoked getBarChart with indexName: %s, dateRange: %s, dimensions: %s,", finalIndexName,
            dateRange, dimensions) + String.format("filters: %s, uniqueOn: %s", filters, uniqueOn));

    assert (dimensions.size() == 1 || dimensions.size() == 2);
    AggregationBuilder filteredAgg = getFilteredAggregation(filters, ImmutableMap.of(), dateRange);

    AggregationBuilder termAgg = AggregationBuilders.terms(DIMENSION).field(dimensions.get(0)).missing(NA);
    if (dimensions.size() == 2) {
      AggregationBuilder secondTermAgg =
          AggregationBuilders.terms(SECOND_DIMENSION).field(dimensions.get(1)).missing(NA);
      uniqueOn.ifPresent(s -> secondTermAgg.subAggregation(getUniqueQuery(s)));
      termAgg.subAggregation(secondTermAgg);
    } else {
      uniqueOn.ifPresent(s -> termAgg.subAggregation(getUniqueQuery(s)));
    }
    filteredAgg.subAggregation(termAgg);

    SearchRequest searchRequest = constructSearchRequest(finalIndexName, filteredAgg);
    Aggregations aggregationResult = executeAndExtract(searchRequest).getAggregations();

    try {
      if (dimensions.size() == 1) {
        List<BarSegment> barSegments =
            extractBarSegmentsFromAggregations(aggregationResult, DIMENSION, uniqueOn.isPresent());
        return barSegments.stream()
            .map(segment -> new NamedBar(segment.getLabel(), ImmutableList.of(segment)))
            .collect(Collectors.toList());
      } else {
        return aggregationResult.<Terms>get(DIMENSION).getBuckets()
            .stream()
            .map(bucket -> new NamedBar(bucket.getKeyAsString(),
                extractBarSegmentsFromAggregations(bucket.getAggregations(), SECOND_DIMENSION, uniqueOn.isPresent())))
            .collect(Collectors.toList());
      }
    } catch (Exception e) {
      _logger.error(String.format("Caught exception while getting bar chart: %s", e.getMessage()));
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

  public List<Row> getTopNTableChart(String indexName, Optional<DateRange> dateRange, String groupBy,
      Map<String, List<String>> filters, Optional<String> uniqueOn, int maxRows) {
    String finalIndexName = getIndexName(indexName);
    _logger.debug(
        String.format("Invoked getTopNTableChart with indexName: %s, dateRange: %s, groupBy: %s", finalIndexName,
            dateRange, groupBy) + String.format("filters: %s, uniqueOn: %s", filters, uniqueOn));

    AggregationBuilder filteredAgg = getFilteredAggregation(filters, ImmutableMap.of(), dateRange);

    TermsAggregationBuilder termAgg = AggregationBuilders.terms(DIMENSION).field(groupBy).size(maxRows);
    if (uniqueOn.isPresent()) {
      termAgg.order(BucketOrder.aggregation(UNIQUE, false));
      termAgg.subAggregation(getUniqueQuery(uniqueOn.get()));
    }
    filteredAgg.subAggregation(termAgg);

    SearchRequest searchRequest = constructSearchRequest(finalIndexName, filteredAgg);
    Aggregations aggregationResult = executeAndExtract(searchRequest).getAggregations();

    try {
      return aggregationResult.<Terms>get(DIMENSION).getBuckets()
          .stream()
          .map(bucket -> new Row(
              ImmutableList.of(bucket.getKeyAsString(), String.valueOf(extractCount(bucket, uniqueOn.isPresent())))))
          .collect(Collectors.toList());
    } catch (Exception e) {
      _logger.error(String.format("Caught exception while getting top n chart: %s", e.getMessage()));
      return ImmutableList.of();
    }
  }

  public int getHighlights(String indexName, Optional<DateRange> dateRange, Map<String, List<String>> filters,
      Map<String, List<String>> mustNotFilters, Optional<String> uniqueOn) {
    String finalIndexName = getIndexName(indexName);
    _logger.debug(String.format("Invoked getHighlights with indexName: %s, dateRange: %s", finalIndexName, dateRange)
        + String.format("filters: %s, uniqueOn: %s", filters, uniqueOn));

    AggregationBuilder filteredAgg = getFilteredAggregation(filters, mustNotFilters, dateRange);
    uniqueOn.ifPresent(s -> filteredAgg.subAggregation(getUniqueQuery(s)));

    SearchRequest searchRequest = constructSearchRequest(finalIndexName, filteredAgg);
    Filter aggregationResult = executeAndExtract(searchRequest);
    try {
      if (uniqueOn.isPresent()) {
        return (int) aggregationResult.getAggregations().<Cardinality>get(UNIQUE).getValue();
      } else {
        return (int) aggregationResult.getDocCount();
      }
    } catch (Exception e) {
      _logger.error(String.format("Caught exception while getting highlights: %s", e.getMessage()));
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
      _logger.error(String.format("Search query failed: %s", e.getMessage()));
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
