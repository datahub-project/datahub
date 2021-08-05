package com.linkedin.metadata.timeseries.elastic.query;

import com.google.common.collect.ImmutableList;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringArrayArray;
import com.linkedin.metadata.dao.exception.ESQueryException;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.utils.elasticsearch.ESUtils;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.timeseries.AggregationSpec;
import com.linkedin.timeseries.GenericTable;
import com.linkedin.timeseries.GroupingBucket;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.PipelineAggregatorBuilders;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.filter.ParsedFilter;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.MaxBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.ParsedBucketMetricValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;


@Slf4j
public class ESAggregatedStatsDAO {
  private static final String ES_FILTERED_STATS = "filtered_stats";
  private static final String ES_AGGREGATION_PREFIX = "agg_";
  private static final String ES_TERMS_AGGREGATION_PREFIX = "terms_";
  private static final String ES_MAX_AGGREGATION_PREFIX = "max_";
  private static final String ES_FIELD_TIMESTAMP = "timestampMillis";
  private static final String ES_AGG_TIMESTAMP = ES_AGGREGATION_PREFIX + ES_FIELD_TIMESTAMP;
  private static final int MAX_TERM_BUCKETS = 24 * 60; // minutes in a day.

  private final IndexConvention _indexConvention;
  private final RestHighLevelClient _searchClient;

  public ESAggregatedStatsDAO(@Nonnull IndexConvention indexConvention, @Nonnull RestHighLevelClient searchClient) {
    _indexConvention = indexConvention;
    _searchClient = searchClient;
  }

  private static String getAggregationSpecAggName(final AggregationSpec aggregationSpec) {
    return aggregationSpec.getAggregationType().toString().toLowerCase() + "_" + aggregationSpec.getMemberName()
        .replace(".", "_");
  }

  private static String getGroupingBucketAggName(final GroupingBucket groupingBucket) {
    if (groupingBucket.isDateGroupingBucket()) {
      return ES_AGG_TIMESTAMP;
    }
    return ES_AGGREGATION_PREFIX + ES_TERMS_AGGREGATION_PREFIX + groupingBucket.getStringGroupingBucket()
        .getKey()
        .replace(".", "_") + ".keyword";
  }

  private static void rowGenHelper(final Aggregations lowestAggs, final int curLevel, final int lastLevel,
      final List<StringArray> rows, final Stack<String> row, final ImmutableList<String> groupAggNames,
      final ImmutableList<String> memberAggNames) {
    if (curLevel == lastLevel) {
      // (Base-case): We are at the lowest level of nested bucket aggregations.
      // Append member aggregation values to the row and add the row to the output.
      for (String memberAggName : memberAggNames) {
        ParsedBucketMetricValue bucketMetricValue = lowestAggs.get(memberAggName);
        row.push(bucketMetricValue.keys()[0]);
      }
      rows.add(new StringArray(row));
      for (int i = 0; i < memberAggNames.size(); ++i) {
        row.pop();
      }
    } else if (curLevel < lastLevel) {
      //(Recursive-case): We are still processing the nested group-by multi-bucket aggregations.
      // For each bucket, add the key to the row and recurse-down for full row construction.
      MultiBucketsAggregation nestedMBAgg = lowestAggs.get(groupAggNames.get(curLevel));
      for (MultiBucketsAggregation.Bucket b : nestedMBAgg.getBuckets()) {
        if (curLevel == 0) {
          // Special-handling for date histogram
          long curDateValue = ((ZonedDateTime) b.getKey()).toInstant().toEpochMilli();
          row.push(String.valueOf(curDateValue));
        } else {
          row.push(b.getKeyAsString());
        }
        rowGenHelper(b.getAggregations(), curLevel + 1, lastLevel, rows, row, groupAggNames, memberAggNames);
        row.pop();
      }
    } else {
      throw new IllegalArgumentException("curLevel:" + curLevel + "> lastLevel:" + lastLevel);
    }
  }

  /**
   * Get the aggregated metrics for the given dataset or column from a time series aspect.
   */
  @Nonnull
  public GenericTable getAggregatedStats(@Nonnull String entityName, @Nonnull String aspectName,
      @Nonnull AggregationSpec[] aggregationSpecs, @Nullable Filter filter,
      @Nullable GroupingBucket[] groupingBuckets) {

    // Setup the filter query builder using the input filter provided.
    final BoolQueryBuilder filterQueryBuilder = ESUtils.buildFilterQuery(filter);
    // Create the high-level aggregation builder with the filter.
    final AggregationBuilder filteredAggBuilder = AggregationBuilders.filter(ES_FILTERED_STATS, filterQueryBuilder);

    // Build and attach the grouping aggregations
    final AggregationBuilder baseAggregationForMembers =
        makeGroupingAggregationBuilder(filteredAggBuilder, groupingBuckets);

    // Add the aggregations for members.
    for (AggregationSpec aggregationSpec : aggregationSpecs) {
      addAggregationBuildersFromAggregationSpec(baseAggregationForMembers, aggregationSpec);
    }

    final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.aggregation(filteredAggBuilder);

    final SearchRequest searchRequest = new SearchRequest();
    searchRequest.source(searchSourceBuilder);

    final String indexName = _indexConvention.getTimeseriesAspectIndexName(entityName, aspectName);
    searchRequest.indices(indexName);

    log.debug("Search request is: " + searchRequest);

    try {
      final SearchResponse searchResponse = _searchClient.search(searchRequest, RequestOptions.DEFAULT);
      return generateResponseFromElastic(searchResponse, groupingBuckets, aggregationSpecs);
    } catch (Exception e) {
      log.error("Search query failed: " + e.getMessage());
      throw new ESQueryException("Search query failed:", e);
    }
  }

  private void addAggregationBuildersFromAggregationSpec(AggregationBuilder baseAggregation,
      AggregationSpec aggregationSpec) {
    String fieldName = aggregationSpec.getMemberName();
    switch (aggregationSpec.getAggregationType()) {
      case LATEST_AGG:
        // Construct the terms aggregation with a max timestamp sub-aggregation.
        String termsAggName = ES_AGGREGATION_PREFIX + ES_TERMS_AGGREGATION_PREFIX + fieldName.replace(".", "_");
        String maxAggName = ES_AGGREGATION_PREFIX + ES_MAX_AGGREGATION_PREFIX + ES_FIELD_TIMESTAMP;
        AggregationBuilder termsAgg = AggregationBuilders.terms(termsAggName)
            .field(fieldName)
            .size(MAX_TERM_BUCKETS)
            .subAggregation(AggregationBuilders.max(maxAggName).field(ES_FIELD_TIMESTAMP));
        baseAggregation.subAggregation(termsAgg);
        // Construct the max_bucket pipeline aggregation
        MaxBucketPipelineAggregationBuilder maxBucketPipelineAgg =
            PipelineAggregatorBuilders.maxBucket(getAggregationSpecAggName(aggregationSpec),
                termsAggName + ">" + maxAggName);
        baseAggregation.subAggregation(maxBucketPipelineAgg);
        break;
      case SUM_AGG:
        AggregationBuilder sumAgg =
            AggregationBuilders.sum(getAggregationSpecAggName(aggregationSpec)).field(fieldName);
        baseAggregation.subAggregation(sumAgg);
        break;
      case CARDINALITY_AGG:
        throw new UnsupportedOperationException("No support for Cardinality aggregation yet");

      case TOP_HITS_AGG:
        throw new UnsupportedOperationException("No support for top_hits aggregation yet");

      default:
        throw new IllegalStateException("Unexpected value: " + aggregationSpec.getAggregationType());
    }
  }

  private AggregationBuilder makeGroupingAggregationBuilder(AggregationBuilder baseAggregationBuilder,
      GroupingBucket[] groupingBuckets) {
    if (groupingBuckets.length < 1) {
      throw new IllegalArgumentException("Need at least one grouping bucket");
    } else if (!groupingBuckets[0].isDateGroupingBucket()) {
      throw new IllegalArgumentException("First grouping bucket is not a DateGroupingBucket");
    } else if (!groupingBuckets[0].getDateGroupingBucket().getKey().equals(ES_FIELD_TIMESTAMP)) {
      throw new IllegalArgumentException("Date Grouping bucket is not:" + ES_FIELD_TIMESTAMP);
    }

    // date histogram aggregation is always the first.
    DateHistogramAggregationBuilder dateHistogramAggregationBuilder =
        AggregationBuilders.dateHistogram(ES_AGG_TIMESTAMP).field(ES_FIELD_TIMESTAMP)
            // TODO: Decipher this from the granlarity.
            .calendarInterval(DateHistogramInterval.DAY);

    AggregationBuilder lastAggregationBuilder = dateHistogramAggregationBuilder;
    for (int i = 1; i < groupingBuckets.length; ++i) {
      GroupingBucket curGroupingBucket = groupingBuckets[i];
      if (!curGroupingBucket.isStringGroupingBucket()) {
        throw new IllegalArgumentException(
            "Grouping buckets after the first grouping buckets can only be String grouping buckets:"
                + curGroupingBucket);
      }
      // We have a inner terms aggregation. Add it as a sub-aggregation of date histogram.
      // TODO: Determine adding the ".keyword" suffix from the actual schema.
      String fieldName = curGroupingBucket.getStringGroupingBucket().getKey() + ".keyword";
      TermsAggregationBuilder termsAggregationBuilder =
          AggregationBuilders.terms(getGroupingBucketAggName(curGroupingBucket))
              .field(fieldName)
              .size(MAX_TERM_BUCKETS)
              .order(BucketOrder.aggregation("_key", true));
      lastAggregationBuilder.subAggregation(termsAggregationBuilder);
      lastAggregationBuilder = termsAggregationBuilder;
    }

    baseAggregationBuilder.subAggregation(dateHistogramAggregationBuilder);

    return lastAggregationBuilder;
  }

  private GenericTable generateResponseFromElastic(SearchResponse searchResponse, GroupingBucket[] groupingBuckets,
      AggregationSpec[] aggregationSpecs) {
    GenericTable resultTable = new GenericTable();

    // 1. Generate the column names
    // Column names = grouping buckets names + aggregations names
    List<String> groupingBucketNames = Arrays.stream(groupingBuckets)
        .map(
            t -> t.isStringGroupingBucket() ? t.getStringGroupingBucket().getKey() : t.getDateGroupingBucket().getKey())
        .collect(Collectors.toList());

    List<String> memberNames =
        Arrays.stream(aggregationSpecs).map(a -> getAggregationSpecAggName(a)).collect(Collectors.toList());

    List<String> columnNames =
        Stream.concat(groupingBucketNames.stream(), memberNames.stream()).collect(Collectors.toList());

    resultTable.setColumnNames(new StringArray(columnNames));

    // 2. Generate column types
    // Column types = grouping buckets types + aggregations types
    List<String> columnTypes = new ArrayList<>();
    for (GroupingBucket g : groupingBuckets) {
      columnTypes.add(g.isDateGroupingBucket() ? "long" : "string");
    }

    for (AggregationSpec aggregationSpec : aggregationSpecs) {
      // TODO: Good for now, but determine from aggregatinSpec
      log.debug("Using type log for AggreationSpec: " + aggregationSpec);
      columnTypes.add("long");
    }
    resultTable.setColumnTypes(new StringArray(columnTypes));

    // 3. Extract and populate the table rows.
    List<StringArray> rows = new ArrayList<>();

    Aggregations aggregations = searchResponse.getAggregations();
    ParsedFilter filterAgg = aggregations.get(ES_FILTERED_STATS);
    Stack<String> rowAcc = new Stack<>();
    // 3.1 Do a DFS of the aggregation tree and generate the rows.
    rowGenHelper(filterAgg.getAggregations(), 0, groupingBuckets.length, rows, rowAcc,
        Arrays.stream(groupingBuckets).map(t -> getGroupingBucketAggName(t)).collect(ImmutableList.toImmutableList()),
        Arrays.stream(aggregationSpecs)
            .map(a -> getAggregationSpecAggName(a))
            .collect(ImmutableList.toImmutableList()));
    assert (rowAcc.isEmpty());

    resultTable.setRows(new StringArrayArray(rows));
    return resultTable;
  }
}
