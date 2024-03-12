package com.linkedin.metadata.timeseries.elastic.query;

import com.datahub.util.exception.ESQueryException;
import com.google.common.collect.ImmutableList;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringArrayArray;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.TimeseriesFieldCollectionSpec;
import com.linkedin.metadata.models.TimeseriesFieldSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.timeseries.elastic.indexbuilder.MappingsBuilder;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.timeseries.AggregationSpec;
import com.linkedin.timeseries.GenericTable;
import com.linkedin.timeseries.GroupingBucket;
import com.linkedin.timeseries.GroupingBucketType;
import com.linkedin.timeseries.TimeWindowSize;
import com.linkedin.util.Pair;
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
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.PipelineAggregatorBuilders;
import org.opensearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.metrics.ParsedCardinality;
import org.opensearch.search.aggregations.metrics.ParsedSum;
import org.opensearch.search.aggregations.pipeline.MaxBucketPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.ParsedBucketMetricValue;
import org.opensearch.search.builder.SearchSourceBuilder;

@Slf4j
public class ESAggregatedStatsDAO {
  private static final String ES_AGGREGATION_PREFIX = "agg_";
  private static final String ES_TERMS_AGGREGATION_PREFIX = "terms_";
  private static final String ES_MAX_AGGREGATION_PREFIX = "max_";
  private static final String ES_FIELD_TIMESTAMP = "timestampMillis";
  private static final String ES_FIELD_URN = "urn";
  private static final String ES_AGG_TIMESTAMP = ES_AGGREGATION_PREFIX + ES_FIELD_TIMESTAMP;
  private static final String ES_AGG_MAX_TIMESTAMP =
      ES_AGGREGATION_PREFIX + ES_MAX_AGGREGATION_PREFIX + ES_FIELD_TIMESTAMP;
  private static final int MAX_TERM_BUCKETS = 24 * 60; // minutes in a day.

  private final IndexConvention _indexConvention;
  private final RestHighLevelClient _searchClient;
  private final EntityRegistry _entityRegistry;

  public ESAggregatedStatsDAO(
      @Nonnull IndexConvention indexConvention,
      @Nonnull RestHighLevelClient searchClient,
      @Nonnull EntityRegistry entityRegistry) {
    _indexConvention = indexConvention;
    _searchClient = searchClient;
    _entityRegistry = entityRegistry;
  }

  private static String toEsAggName(final String aggName) {
    return aggName.replace(".", "_");
  }

  private static String getAggregationSpecAggESName(final AggregationSpec aggregationSpec) {
    return toEsAggName(getAggregationSpecAggDisplayName(aggregationSpec));
  }

  private static String getAggregationSpecAggDisplayName(final AggregationSpec aggregationSpec) {
    String prefix;
    switch (aggregationSpec.getAggregationType()) {
      case LATEST:
        prefix = "latest_";
        break;
      case SUM:
        prefix = "sum_";
        break;
      case CARDINALITY:
        prefix = "cardinality_";
        break;
      default:
        throw new IllegalArgumentException(
            "Unknown AggregationSpec type" + aggregationSpec.getAggregationType());
    }
    return prefix + aggregationSpec.getFieldPath();
  }

  private static String getGroupingBucketAggName(final GroupingBucket groupingBucket) {
    if (groupingBucket.getType() == GroupingBucketType.DATE_GROUPING_BUCKET) {
      return toEsAggName(ES_AGGREGATION_PREFIX + groupingBucket.getKey());
    }
    return toEsAggName(
        ES_AGGREGATION_PREFIX + ES_TERMS_AGGREGATION_PREFIX + groupingBucket.getKey());
  }

  private static void rowGenHelper(
      final Aggregations lowestAggs,
      final int curLevel,
      final int lastLevel,
      final List<StringArray> rows,
      final Stack<String> row,
      final ImmutableList<GroupingBucket> groupingBuckets,
      final ImmutableList<AggregationSpec> aggregationSpecs,
      AspectSpec aspectSpec) {
    if (curLevel == lastLevel) {
      // (Base-case): We are at the lowest level of nested bucket aggregations.
      // Append member aggregation values to the row and add the row to the output.
      for (AggregationSpec aggregationSpec : aggregationSpecs) {
        String value = extractAggregationValue(lowestAggs, aspectSpec, aggregationSpec);
        row.push(value);
      }
      // We have a full row now. Append it to the output.
      rows.add(new StringArray(row));
      // prepare for next recursion.
      for (int i = 0; i < aggregationSpecs.size(); ++i) {
        row.pop();
      }
    } else if (curLevel < lastLevel) {
      // (Recursive-case): We are still processing the nested group-by multi-bucket aggregations.
      // For each bucket, add the key to the row and recur down for full row construction.
      GroupingBucket curGroupingBucket = groupingBuckets.get(curLevel);
      String curGroupingBucketAggName = getGroupingBucketAggName(curGroupingBucket);
      MultiBucketsAggregation nestedMBAgg = lowestAggs.get(curGroupingBucketAggName);
      for (MultiBucketsAggregation.Bucket b : nestedMBAgg.getBuckets()) {
        if (curGroupingBucket.getType() == GroupingBucketType.DATE_GROUPING_BUCKET) {
          long curDateValue = ((ZonedDateTime) b.getKey()).toInstant().toEpochMilli();
          row.push(String.valueOf(curDateValue));
        } else {
          row.push(b.getKeyAsString());
        }
        // Recur down
        rowGenHelper(
            b.getAggregations(),
            curLevel + 1,
            lastLevel,
            rows,
            row,
            groupingBuckets,
            aggregationSpecs,
            aspectSpec);
        // Remove the row value we have added for this level.
        row.pop();
      }
    } else {
      throw new IllegalArgumentException("curLevel:" + curLevel + "> lastLevel:" + lastLevel);
    }
  }

  private static DateHistogramInterval getHistogramInterval(TimeWindowSize timeWindowSize) {
    int multiple = timeWindowSize.getMultiple();
    switch (timeWindowSize.getUnit()) {
      case MINUTE:
        return DateHistogramInterval.minutes(multiple);
      case HOUR:
        return DateHistogramInterval.hours(multiple);
      case WEEK:
        return DateHistogramInterval.weeks(multiple);
      case DAY:
        return DateHistogramInterval.days(multiple);
      case MONTH:
        return new DateHistogramInterval(multiple + "M");
      case QUARTER:
        return new DateHistogramInterval(multiple + "q");
      case YEAR:
        return new DateHistogramInterval(multiple + "y");
      default:
        throw new IllegalArgumentException(
            "Unknown date grouping bucking time window size unit: " + timeWindowSize.getUnit());
    }
  }

  private static DataSchema.Type getTimeseriesFieldType(AspectSpec aspectSpec, String fieldPath) {
    if (fieldPath.equals(ES_FIELD_TIMESTAMP)) {
      return DataSchema.Type.LONG;
    }
    if (fieldPath.equals(ES_FIELD_URN)) {
      return DataSchema.Type.STRING;
    }
    if (fieldPath.equals(MappingsBuilder.EVENT_GRANULARITY)) {
      return DataSchema.Type.RECORD;
    }

    String[] memberParts = fieldPath.split("\\.");
    if (memberParts.length == 1) {
      // Search in the timeseriesFieldSpecs.
      TimeseriesFieldSpec timeseriesFieldSpec =
          aspectSpec.getTimeseriesFieldSpecMap().get(memberParts[0]);
      if (timeseriesFieldSpec != null) {
        return timeseriesFieldSpec.getPegasusSchema().getType();
      }
      // Could be the collection itself.
      TimeseriesFieldCollectionSpec timeseriesFieldCollectionSpec =
          aspectSpec.getTimeseriesFieldCollectionSpecMap().get(memberParts[0]);
      if (timeseriesFieldCollectionSpec != null) {
        return timeseriesFieldCollectionSpec.getPegasusSchema().getType();
      }
    } else if (memberParts.length == 2) {
      // Check if partitionSpec
      if (memberParts[0].equals(MappingsBuilder.PARTITION_SPEC)) {
        if (memberParts[1].equals(MappingsBuilder.PARTITION_SPEC_PARTITION)
            || memberParts[1].equals(MappingsBuilder.PARTITION_SPEC_TIME_PARTITION)) {
          return DataSchema.Type.STRING;
        } else {
          throw new IllegalArgumentException("Unknown partitionSpec member" + memberParts[1]);
        }
      }

      // This is either a collection key/stat.
      TimeseriesFieldCollectionSpec timeseriesFieldCollectionSpec =
          aspectSpec.getTimeseriesFieldCollectionSpecMap().get(memberParts[0]);
      if (timeseriesFieldCollectionSpec != null) {
        if (timeseriesFieldCollectionSpec
            .getTimeseriesFieldCollectionAnnotation()
            .getKey()
            .equals(memberParts[1])) {
          // Matched against the collection stat key.
          return DataSchema.Type.STRING;
        }
        TimeseriesFieldSpec tsFieldSpec =
            timeseriesFieldCollectionSpec.getTimeseriesFieldSpecMap().get(memberParts[1]);
        if (tsFieldSpec != null) {
          // Matched against a collection stat field.
          return tsFieldSpec.getPegasusSchema().getType();
        }
      }
    }
    throw new IllegalArgumentException(
        "Unknown TimeseriesField or TimeseriesFieldCollection: " + fieldPath);
  }

  private static DataSchema.Type getGroupingBucketKeyType(
      @Nonnull AspectSpec aspectSpec, @Nonnull GroupingBucket groupingBucket) {
    return getTimeseriesFieldType(aspectSpec, groupingBucket.getKey());
  }

  private static DataSchema.Type getAggregationSpecMemberType(
      @Nonnull AspectSpec aspectSpec, @Nonnull AggregationSpec aggregationSpec) {
    return getTimeseriesFieldType(aspectSpec, aggregationSpec.getFieldPath());
  }

  private static List<String> genColumnNames(
      GroupingBucket[] groupingBuckets, AggregationSpec[] aggregationSpecs) {
    List<String> groupingBucketNames =
        Arrays.stream(groupingBuckets).map(t -> t.getKey()).collect(Collectors.toList());

    List<String> aggregationNames =
        Arrays.stream(aggregationSpecs)
            .map(ESAggregatedStatsDAO::getAggregationSpecAggDisplayName)
            .collect(Collectors.toList());

    List<String> columnNames =
        Stream.concat(groupingBucketNames.stream(), aggregationNames.stream())
            .collect(Collectors.toList());
    return columnNames;
  }

  private static List<String> genColumnTypes(
      AspectSpec aspectSpec, GroupingBucket[] groupingBuckets, AggregationSpec[] aggregationSpecs) {
    List<String> columnTypes = new ArrayList<>();
    for (GroupingBucket g : groupingBuckets) {
      DataSchema.Type type = getGroupingBucketKeyType(aspectSpec, g);
      String typeStr;
      switch (type) {
        case INT:
        case LONG:
        case DOUBLE:
        case FLOAT:
          typeStr = type.toString().toLowerCase();
          break;
        default:
          // All non-numeric are string types for grouping fields.
          typeStr = "string";
          break;
      }
      columnTypes.add(typeStr);
    }

    for (AggregationSpec aggregationSpec : aggregationSpecs) {
      DataSchema.Type memberType = getAggregationSpecMemberType(aspectSpec, aggregationSpec);
      switch (aggregationSpec.getAggregationType()) {
        case LATEST:
          // same as underlying type
          columnTypes.add(memberType.toString().toLowerCase());
          break;
        case SUM:
          // always a double
          columnTypes.add("double");
          break;
        case CARDINALITY:
          // always a long
          columnTypes.add("long");
          break;
        default:
          throw new IllegalArgumentException(
              "Type generation not yet supported for aggregation type: "
                  + aggregationSpec.getAggregationType());
      }
    }
    return columnTypes;
  }

  private static String extractAggregationValue(
      @Nonnull final Aggregations aggregations,
      @Nonnull final AspectSpec aspectSpec,
      @Nonnull final AggregationSpec aggregationSpec) {
    String memberAggName = getAggregationSpecAggESName(aggregationSpec);
    Object memberAgg = aggregations.get(memberAggName);
    DataSchema.Type memberType = getAggregationSpecMemberType(aspectSpec, aggregationSpec);
    String defaultValue = "NULL";
    if (memberAgg instanceof ParsedBucketMetricValue) {
      String[] values = ((ParsedBucketMetricValue) memberAgg).keys();
      if (values.length > 0) {
        return values[0];
      }
    } else if (memberAgg instanceof ParsedSum) {
      // Underling integral type.
      switch (memberType) {
        case INT:
        case LONG:
          return String.valueOf((long) ((ParsedSum) memberAgg).getValue());
        case DOUBLE:
        case FLOAT:
          return String.valueOf(((ParsedSum) memberAgg).getValue());
        default:
          throw new IllegalArgumentException(
              "Unexpected type encountered for sum aggregation: " + memberType);
      }
    } else if (memberAgg instanceof ParsedCardinality) {
      // This will always be a long value as string.
      return String.valueOf(((ParsedCardinality) memberAgg).getValue());
    } else {
      throw new UnsupportedOperationException(
          "Member aggregations other than latest and sum not supported yet.");
    }
    return defaultValue;
  }

  private AspectSpec getTimeseriesAspectSpec(
      @Nonnull String entityName, @Nonnull String aspectName) {
    EntitySpec entitySpec = _entityRegistry.getEntitySpec(entityName);
    AspectSpec aspectSpec = entitySpec.getAspectSpec(aspectName);
    if (aspectSpec == null) {
      new IllegalArgumentException(
          String.format("Unrecognized aspect name {} for entity {}", aspectName, entityName));
    } else if (!aspectSpec.isTimeseries()) {
      new IllegalArgumentException(
          String.format(
              "aspect name {} for entity {} is not a timeseries aspect", aspectName, entityName));
    }

    return aspectSpec;
  }

  /** Get the aggregated metrics for the given dataset or column from a time series aspect. */
  @Nonnull
  public GenericTable getAggregatedStats(
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull AggregationSpec[] aggregationSpecs,
      @Nullable Filter filter,
      @Nullable GroupingBucket[] groupingBuckets) {

    // Setup the filter query builder using the input filter provided.
    final BoolQueryBuilder filterQueryBuilder = ESUtils.buildFilterQuery(filter, true);

    AspectSpec aspectSpec = getTimeseriesAspectSpec(entityName, aspectName);
    // Build and attach the grouping aggregations
    final Pair<AggregationBuilder, AggregationBuilder> topAndBottomAggregations =
        makeGroupingAggregationBuilder(aspectSpec, null, groupingBuckets);
    AggregationBuilder rootAggregationBuilder = topAndBottomAggregations.getFirst();
    AggregationBuilder mostNested = topAndBottomAggregations.getSecond();

    // Add the aggregations for members.
    for (AggregationSpec aggregationSpec : aggregationSpecs) {
      addAggregationBuildersFromAggregationSpec(aspectSpec, mostNested, aggregationSpec);
    }

    final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.aggregation(rootAggregationBuilder);
    searchSourceBuilder.query(QueryBuilders.boolQuery().must(filterQueryBuilder));

    searchSourceBuilder.size(0);

    final SearchRequest searchRequest = new SearchRequest();
    searchRequest.source(searchSourceBuilder);

    final String indexName = _indexConvention.getTimeseriesAspectIndexName(entityName, aspectName);
    searchRequest.indices(indexName);

    log.debug("Search request is: " + searchRequest);

    try {
      final SearchResponse searchResponse =
          _searchClient.search(searchRequest, RequestOptions.DEFAULT);
      return generateResponseFromElastic(
          searchResponse, groupingBuckets, aggregationSpecs, aspectSpec);
    } catch (Exception e) {
      log.error("Search query failed: " + e.getMessage());
      throw new ESQueryException("Search query failed:", e);
    }
  }

  private void addAggregationBuildersFromAggregationSpec(
      AspectSpec aspectSpec, AggregationBuilder baseAggregation, AggregationSpec aggregationSpec) {
    String fieldPath = aggregationSpec.getFieldPath();
    String esFieldName = fieldPath;

    switch (aggregationSpec.getAggregationType()) {
      case LATEST:
        // Construct the terms aggregation with a max timestamp sub-aggregation.
        String termsAggName =
            toEsAggName(ES_AGGREGATION_PREFIX + ES_TERMS_AGGREGATION_PREFIX + fieldPath);
        AggregationBuilder termsAgg =
            AggregationBuilders.terms(termsAggName)
                .field(esFieldName)
                .size(MAX_TERM_BUCKETS)
                .subAggregation(
                    AggregationBuilders.max(ES_AGG_MAX_TIMESTAMP).field(ES_FIELD_TIMESTAMP));
        baseAggregation.subAggregation(termsAgg);
        // Construct the max_bucket pipeline aggregation
        MaxBucketPipelineAggregationBuilder maxBucketPipelineAgg =
            PipelineAggregatorBuilders.maxBucket(
                getAggregationSpecAggESName(aggregationSpec),
                termsAggName + ">" + ES_AGG_MAX_TIMESTAMP);
        baseAggregation.subAggregation(maxBucketPipelineAgg);
        break;
      case SUM:
        AggregationBuilder sumAgg =
            AggregationBuilders.sum(getAggregationSpecAggESName(aggregationSpec))
                .field(esFieldName);
        baseAggregation.subAggregation(sumAgg);
        break;
      case CARDINALITY:
        AggregationBuilder cardinalityAgg =
            AggregationBuilders.cardinality(getAggregationSpecAggESName(aggregationSpec))
                .field(esFieldName);
        baseAggregation.subAggregation(cardinalityAgg);
        break;
      default:
        throw new IllegalStateException(
            "Unexpected value: " + aggregationSpec.getAggregationType());
    }
  }

  private Pair<AggregationBuilder, AggregationBuilder> makeGroupingAggregationBuilder(
      AspectSpec aspectSpec,
      @Nullable AggregationBuilder baseAggregationBuilder,
      @Nullable GroupingBucket[] groupingBuckets) {

    AggregationBuilder firstAggregationBuilder = baseAggregationBuilder;
    AggregationBuilder lastAggregationBuilder = baseAggregationBuilder;
    if (groupingBuckets != null) {
      for (GroupingBucket curGroupingBucket : groupingBuckets) {
        AggregationBuilder curAggregationBuilder = null;
        if (curGroupingBucket.getType() == GroupingBucketType.DATE_GROUPING_BUCKET) {
          // Process the date grouping bucket using 'date-histogram' aggregation.
          if (!curGroupingBucket.getKey().equals(ES_FIELD_TIMESTAMP)) {
            throw new IllegalArgumentException("Date Grouping bucket is not:" + ES_FIELD_TIMESTAMP);
          }
          curAggregationBuilder =
              AggregationBuilders.dateHistogram(ES_AGG_TIMESTAMP)
                  .field(ES_FIELD_TIMESTAMP)
                  .calendarInterval(getHistogramInterval(curGroupingBucket.getTimeWindowSize()));
        } else if (curGroupingBucket.getType() == GroupingBucketType.STRING_GROUPING_BUCKET) {
          // Process the string grouping bucket using the 'terms' aggregation.
          // The field can be Keyword, Numeric, ip, boolean, or binary.
          String fieldName = ESUtils.toKeywordField(curGroupingBucket.getKey(), true);
          DataSchema.Type fieldType = getGroupingBucketKeyType(aspectSpec, curGroupingBucket);
          curAggregationBuilder =
              AggregationBuilders.terms(getGroupingBucketAggName(curGroupingBucket))
                  .field(fieldName)
                  .size(MAX_TERM_BUCKETS)
                  .order(BucketOrder.aggregation("_key", true));
        }
        if (firstAggregationBuilder == null) {
          firstAggregationBuilder = curAggregationBuilder;
        }
        if (lastAggregationBuilder != null) {
          lastAggregationBuilder.subAggregation(curAggregationBuilder);
        }
        lastAggregationBuilder = curAggregationBuilder;
      }
    }

    return Pair.of(firstAggregationBuilder, lastAggregationBuilder);
  }

  private GenericTable generateResponseFromElastic(
      SearchResponse searchResponse,
      GroupingBucket[] groupingBuckets,
      AggregationSpec[] aggregationSpecs,
      AspectSpec aspectSpec) {
    GenericTable resultTable = new GenericTable();

    // 1. Generate the column names.
    List<String> columnNames = genColumnNames(groupingBuckets, aggregationSpecs);
    resultTable.setColumnNames(new StringArray(columnNames));

    // 2. Generate the column types.
    List<String> columnTypes = genColumnTypes(aspectSpec, groupingBuckets, aggregationSpecs);
    resultTable.setColumnTypes(new StringArray(columnTypes));

    // 3. Extract and populate the table rows.
    List<StringArray> rows = new ArrayList<>();

    Aggregations aggregations = searchResponse.getAggregations();
    Stack<String> rowAcc = new Stack<>();
    rowGenHelper(
        aggregations,
        0,
        groupingBuckets.length,
        rows,
        rowAcc,
        ImmutableList.copyOf(groupingBuckets),
        ImmutableList.copyOf(aggregationSpecs),
        aspectSpec);

    if (!rowAcc.isEmpty()) {
      throw new IllegalStateException("Expected stack to be empty.");
    }

    resultTable.setRows(new StringArrayArray(rows));
    return resultTable;
  }
}
