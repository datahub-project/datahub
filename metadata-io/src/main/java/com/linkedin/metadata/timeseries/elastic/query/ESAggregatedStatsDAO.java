package com.linkedin.metadata.timeseries.elastic.query;

import com.google.common.collect.ImmutableList;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringArrayArray;
import com.linkedin.metadata.dao.exception.ESQueryException;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.TemporalStatCollectionFieldSpec;
import com.linkedin.metadata.models.TemporalStatFieldSpec;
import com.linkedin.metadata.models.annotation.TemporalStatCollectionAnnotation;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.utils.elasticsearch.ESUtils;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.timeseries.AggregationSpec;
import com.linkedin.timeseries.CalendarInterval;
import com.linkedin.timeseries.DateGroupingBucket;
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
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.metrics.ParsedCardinality;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;
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
  private static final String ES_KEYWORD_SUFFIX = ".keyword";
  private static final String ES_AGG_TIMESTAMP = ES_AGGREGATION_PREFIX + ES_FIELD_TIMESTAMP;
  private static final int MAX_TERM_BUCKETS = 24 * 60; // minutes in a day.

  private final IndexConvention _indexConvention;
  private final RestHighLevelClient _searchClient;
  private final EntityRegistry _entityRegistry;

  public ESAggregatedStatsDAO(@Nonnull IndexConvention indexConvention, @Nonnull RestHighLevelClient searchClient,
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
      case TOP_HITS:
        prefix = "top_" + aggregationSpec.getK() + "_by_" + aggregationSpec.getByMember() + "_";
        break;
      default:
        throw new IllegalArgumentException("Unknown AggregationSpec type" + aggregationSpec.getAggregationType());
    }
    return prefix + aggregationSpec.getMemberName();
  }

  private static String getGroupingBucketAggName(final GroupingBucket groupingBucket) {
    if (groupingBucket.isDateGroupingBucket()) {
      return ES_AGG_TIMESTAMP;
    }
    return toEsAggName(
        ES_AGGREGATION_PREFIX + ES_TERMS_AGGREGATION_PREFIX + groupingBucket.getStringGroupingBucket().getKey());
  }

  private static void rowGenHelper(final Aggregations lowestAggs, final int curLevel, final int lastLevel,
      final List<StringArray> rows, final Stack<String> row, final ImmutableList<GroupingBucket> groupingBuckets,
      final ImmutableList<AggregationSpec> aggregationSpecs, AspectSpec aspectSpec) {
    if (curLevel == lastLevel) {
      // (Base-case): We are at the lowest level of nested bucket aggregations.
      // Append member aggregation values to the row and add the row to the output.
      for (AggregationSpec aggregationSpec : aggregationSpecs) {
        String memberAggName = getAggregationSpecAggESName(aggregationSpec);
        Object memberAgg = lowestAggs.get(memberAggName);
        DataSchema.Type memberType = getAggregationSpecMemberType(aspectSpec, aggregationSpec);
        String value = "NULL";
        if (memberAgg instanceof ParsedBucketMetricValue) {
          String[] values = ((ParsedBucketMetricValue) memberAgg).keys();
          if (values.length > 0) {
            value = values[0];
            /*
            if (memberType == DataSchema.Type.ARRAY) {
              // This gets extra encapsulated with '["' and '"]' by our transformer. Strip it away.
              assert (value.length() >= 4);
              value = value.substring(2, value.length() - 2);
            }
            */
          }
        } else if (memberAgg instanceof ParsedSum) {
          // Underling integral type.
          switch (memberType) {
            case INT:
            case LONG:
              value = String.valueOf((long) ((ParsedSum) memberAgg).getValue());
              break;
            case DOUBLE:
            case FLOAT:
              value = String.valueOf(((ParsedSum) memberAgg).getValue());
              break;
            default:
              throw new IllegalArgumentException("Unexpected type encountered for sum aggregation: " + memberType);
          }
        } else if (memberAgg instanceof ParsedCardinality) {
          // This will always be a long value as string.
          value = String.valueOf(((ParsedCardinality) memberAgg).getValue());
        } else {
          throw new UnsupportedOperationException("Member aggregations other than latest and sum not supported yet.");
        }
        row.push(value);
      }
      // We have a full row now. Append it to the output.
      rows.add(new StringArray(row));
      // prepare for next recursion.
      for (int i = 0; i < aggregationSpecs.size(); ++i) {
        row.pop();
      }
    } else if (curLevel < lastLevel) {
      //(Recursive-case): We are still processing the nested group-by multi-bucket aggregations.
      // For each bucket, add the key to the row and recurse-down for full row construction.
      GroupingBucket curGroupingBucket = groupingBuckets.get(curLevel);
      String curGroupingBucketAggName = getGroupingBucketAggName(curGroupingBucket);
      MultiBucketsAggregation nestedMBAgg = lowestAggs.get(curGroupingBucketAggName);
      for (MultiBucketsAggregation.Bucket b : nestedMBAgg.getBuckets()) {
        if (curGroupingBucket.isDateGroupingBucket()) {
          long curDateValue = ((ZonedDateTime) b.getKey()).toInstant().toEpochMilli();
          row.push(String.valueOf(curDateValue));
        } else {
          row.push(b.getKeyAsString());
        }
        rowGenHelper(b.getAggregations(), curLevel + 1, lastLevel, rows, row, groupingBuckets, aggregationSpecs,
            aspectSpec);
        row.pop();
      }
    } else {
      throw new IllegalArgumentException("curLevel:" + curLevel + "> lastLevel:" + lastLevel);
    }
  }

  private static DateHistogramInterval getHistogramInterval(DateGroupingBucket dateGroupingBucket) {
    CalendarInterval granularity = dateGroupingBucket.getGranularity();
    switch (granularity) {
      case MINUTE:
        return DateHistogramInterval.MINUTE;
      case HOUR:
        return DateHistogramInterval.HOUR;
      case DAY:
        return DateHistogramInterval.DAY;
      case MONTH:
        return DateHistogramInterval.MONTH;
      case QUARTER:
        return DateHistogramInterval.QUARTER;
      case YEAR:
        return DateHistogramInterval.YEAR;
      default:
        throw new IllegalArgumentException("Unknown date grouping bucking granularity" + granularity);
    }
  }

  private static DataSchema.Type getTemporalFieldType(AspectSpec aspectSpec, String memberName) {
    if (memberName == ES_FIELD_TIMESTAMP) {
      return DataSchema.Type.LONG;
    }
    String[] memberParts = memberName.split("\\.");
    if (memberParts.length == 1) {
      // Search in the temporalStatFieldSpecs.
      for (TemporalStatFieldSpec tsFieldSpec : aspectSpec.getTemporalStatFieldSpecs()) {
        if (tsFieldSpec.getTemporalStatAnnotation().getStatName().equals(memberParts[0])) {
          return tsFieldSpec.getPegasusSchema().getType();
        }
      }
      // Could be the collection itself.
      for (TemporalStatCollectionFieldSpec statCollectionFieldSpec : aspectSpec.getTemporalStatCollectionFieldSpecs()) {
        TemporalStatCollectionAnnotation tsCollectionAnnotation =
            statCollectionFieldSpec.getTemporalStatCollectionAnnotation();
        if (tsCollectionAnnotation.getCollectionName().equals(memberParts[0])) {
          return statCollectionFieldSpec.getPegasusSchema().getType();
        }
      }
    } else if (memberParts.length == 2) {
      // This is either a collection key/stat.
      for (TemporalStatCollectionFieldSpec statCollectionFieldSpec : aspectSpec.getTemporalStatCollectionFieldSpecs()) {
        TemporalStatCollectionAnnotation tsCollectionAnnotation =
            statCollectionFieldSpec.getTemporalStatCollectionAnnotation();
        if (tsCollectionAnnotation.getCollectionName().equals(memberParts[0])) {
          if (tsCollectionAnnotation.getKey().equals(memberParts[1])) {
            // Matched against key
            return DataSchema.Type.STRING;
          }
          for (TemporalStatFieldSpec tsFieldSpec : statCollectionFieldSpec.getTemporalStats()) {
            if (tsFieldSpec.getTemporalStatAnnotation().getStatName().equals(memberParts[1])) {
              // Matched against one of the member stats.
              return tsFieldSpec.getPegasusSchema().getType();
            }
          }
        }
      }
    }
    throw new IllegalArgumentException("Unknown TemporalStatField or TemportalStatCollectionField: " + memberName);
  }

  private static DataSchema.Type getGroupingBucketKeyType(@Nonnull AspectSpec aspectSpec,
      @Nonnull GroupingBucket groupingBucket) {
    String key = groupingBucket.isDateGroupingBucket() ? groupingBucket.getDateGroupingBucket().getKey()
        : groupingBucket.getStringGroupingBucket().getKey();
    return getTemporalFieldType(aspectSpec, key);
  }

  private static DataSchema.Type getAggregationSpecMemberType(@Nonnull AspectSpec aspectSpec,
      @Nonnull AggregationSpec aggregationSpec) {
    return getTemporalFieldType(aspectSpec, aggregationSpec.getMemberName());
  }

  private static List<String> genColumnNames(GroupingBucket[] groupingBuckets, AggregationSpec[] aggregationSpecs) {
    List<String> groupingBucketNames = Arrays.stream(groupingBuckets)
        .map(
            t -> t.isStringGroupingBucket() ? t.getStringGroupingBucket().getKey() : t.getDateGroupingBucket().getKey())
        .collect(Collectors.toList());

    List<String> memberNames = Arrays.stream(aggregationSpecs)
        .map(ESAggregatedStatsDAO::getAggregationSpecAggDisplayName)
        .collect(Collectors.toList());

    List<String> columnNames =
        Stream.concat(groupingBucketNames.stream(), memberNames.stream()).collect(Collectors.toList());
    return columnNames;
  }

  private static List<String> genColumnTypes(AspectSpec aspectSpec, GroupingBucket[] groupingBuckets,
      AggregationSpec[] aggregationSpecs) {
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
          typeStr = DataSchema.Type.STRING.toString().toLowerCase();
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
              "Type generation not yet supported for aggregation type: " + aggregationSpec.getAggregationType());
      }
    }
    return columnTypes;
  }

  private AspectSpec getTimeseriesAspectSpec(@Nonnull String entityName, @Nonnull String aspectName) {
    EntitySpec entitySpec = _entityRegistry.getEntitySpec(entityName);
    AspectSpec aspectSpec = entitySpec.getAspectSpec(aspectName);
    if (aspectSpec == null) {
      new IllegalArgumentException(String.format("Unrecognized aspect name {} for entity {}", aspectName, entityName));
    } else if (!aspectSpec.isTimeseries()) {
      new IllegalArgumentException(
          String.format("aspect name {} for entity {} is not a timeseries aspect", aspectName, entityName));
    }

    return aspectSpec;
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

    AspectSpec aspectSpec = getTimeseriesAspectSpec(entityName, aspectName);
    // Build and attach the grouping aggregations
    final AggregationBuilder baseAggregationForMembers =
        makeGroupingAggregationBuilder(aspectSpec, filteredAggBuilder, groupingBuckets);

    // Add the aggregations for members.
    for (AggregationSpec aggregationSpec : aggregationSpecs) {
      addAggregationBuildersFromAggregationSpec(aspectSpec, baseAggregationForMembers, aggregationSpec);
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
      return generateResponseFromElastic(searchResponse, groupingBuckets, aggregationSpecs, aspectSpec);
    } catch (Exception e) {
      log.error("Search query failed: " + e.getMessage());
      throw new ESQueryException("Search query failed:", e);
    }
  }

  private void addAggregationBuildersFromAggregationSpec(AspectSpec aspectSpec, AggregationBuilder baseAggregation,
      AggregationSpec aggregationSpec) {
    String memberName = aggregationSpec.getMemberName();
    String esFieldName = memberName;
    if (!isIntegarlType(getAggregationSpecMemberType(aspectSpec, aggregationSpec))) {
      esFieldName += ES_KEYWORD_SUFFIX;
    }

    switch (aggregationSpec.getAggregationType()) {
      case LATEST:
        // Construct the terms aggregation with a max timestamp sub-aggregation.
        String termsAggName = toEsAggName(ES_AGGREGATION_PREFIX + ES_TERMS_AGGREGATION_PREFIX + memberName);
        String maxAggName = ES_AGGREGATION_PREFIX + ES_MAX_AGGREGATION_PREFIX + ES_FIELD_TIMESTAMP;
        AggregationBuilder termsAgg = AggregationBuilders.terms(termsAggName)
            .field(esFieldName)
            .size(MAX_TERM_BUCKETS)
            .subAggregation(AggregationBuilders.max(maxAggName).field(ES_FIELD_TIMESTAMP));
        baseAggregation.subAggregation(termsAgg);
        // Construct the max_bucket pipeline aggregation
        MaxBucketPipelineAggregationBuilder maxBucketPipelineAgg =
            PipelineAggregatorBuilders.maxBucket(getAggregationSpecAggESName(aggregationSpec),
                termsAggName + ">" + maxAggName);
        baseAggregation.subAggregation(maxBucketPipelineAgg);
        break;
      case SUM:
        AggregationBuilder sumAgg =
            AggregationBuilders.sum(getAggregationSpecAggESName(aggregationSpec)).field(esFieldName);
        baseAggregation.subAggregation(sumAgg);
        break;
      case CARDINALITY:
        AggregationBuilder cardinalityAgg =
            AggregationBuilders.cardinality(getAggregationSpecAggESName(aggregationSpec)).field(esFieldName);
        baseAggregation.subAggregation(cardinalityAgg);
        break;
      case TOP_HITS:
        throw new UnsupportedOperationException("No support for top_hits aggregation yet");
      default:
        throw new IllegalStateException("Unexpected value: " + aggregationSpec.getAggregationType());
    }
  }

  private AggregationBuilder makeGroupingAggregationBuilder(AspectSpec aspectSpec,
      AggregationBuilder baseAggregationBuilder, GroupingBucket[] groupingBuckets) {

    AggregationBuilder lastAggregationBuilder = baseAggregationBuilder;
    for (int i = 0; i < groupingBuckets.length; ++i) {
      AggregationBuilder curAggregationBuilder = null;
      GroupingBucket curGroupingBucket = groupingBuckets[i];
      if (curGroupingBucket.isDateGroupingBucket()) {
        // Process the date grouping bucket using 'date-histogram' aggregation.
        DateGroupingBucket dateGroupingBucket = curGroupingBucket.getDateGroupingBucket();
        if (!dateGroupingBucket.getKey().equals(ES_FIELD_TIMESTAMP)) {
          throw new IllegalArgumentException("Date Grouping bucket is not:" + ES_FIELD_TIMESTAMP);
        }
        curAggregationBuilder = AggregationBuilders.dateHistogram(ES_AGG_TIMESTAMP)
            .field(ES_FIELD_TIMESTAMP)
            .calendarInterval(getHistogramInterval(groupingBuckets[0].getDateGroupingBucket()));
      } else if (curGroupingBucket.isStringGroupingBucket()) {
        // Process the string grouping bucket using the 'terms' aggregation.
        String fieldName = curGroupingBucket.getStringGroupingBucket().getKey();
        DataSchema.Type fieldType = getGroupingBucketKeyType(aspectSpec, curGroupingBucket);
        if (!isIntegarlType(fieldType)) {
          fieldName += ES_KEYWORD_SUFFIX;
        }
        curAggregationBuilder = AggregationBuilders.terms(getGroupingBucketAggName(curGroupingBucket))
            .field(fieldName)
            .size(MAX_TERM_BUCKETS)
            .order(BucketOrder.aggregation("_key", true));
      }
      lastAggregationBuilder.subAggregation(curAggregationBuilder);
      lastAggregationBuilder = curAggregationBuilder;
    }

    return lastAggregationBuilder;
  }

  private boolean isIntegarlType(DataSchema.Type fieldType) {
    switch (fieldType) {
      case INT:
      case FLOAT:
      case DOUBLE:
      case LONG:
        return true;
      default:
        return false;
    }
  }

  private GenericTable generateResponseFromElastic(SearchResponse searchResponse, GroupingBucket[] groupingBuckets,
      AggregationSpec[] aggregationSpecs, AspectSpec aspectSpec) {
    GenericTable resultTable = new GenericTable();

    List<String> columnNames = genColumnNames(groupingBuckets, aggregationSpecs);
    resultTable.setColumnNames(new StringArray(columnNames));

    List<String> columnTypes = genColumnTypes(aspectSpec, groupingBuckets, aggregationSpecs);
    resultTable.setColumnTypes(new StringArray(columnTypes));

    // 3. Extract and populate the table rows.
    List<StringArray> rows = new ArrayList<>();

    Aggregations aggregations = searchResponse.getAggregations();
    ParsedFilter filterAgg = aggregations.get(ES_FILTERED_STATS);
    Stack<String> rowAcc = new Stack<>();
    // 3.1 Do a DFS of the aggregation tree and generate the rows.
    rowGenHelper(filterAgg.getAggregations(), 0, groupingBuckets.length, rows, rowAcc,
        ImmutableList.copyOf(groupingBuckets), ImmutableList.copyOf(aggregationSpecs), aspectSpec);
    assert (rowAcc.isEmpty());

    resultTable.setRows(new StringArrayArray(rows));
    return resultTable;
  }
}
