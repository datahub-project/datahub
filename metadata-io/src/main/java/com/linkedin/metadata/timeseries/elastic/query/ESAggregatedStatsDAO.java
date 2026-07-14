package com.linkedin.metadata.timeseries.elastic.query;

import com.datahub.util.exception.ESQueryException;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringArrayArray;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.TimeseriesFieldCollectionSpec;
import com.linkedin.metadata.models.TimeseriesFieldSpec;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.timeseries.elastic.indexbuilder.MappingsBuilder;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.timeseries.AggregationSpec;
import com.linkedin.timeseries.GenericTable;
import com.linkedin.timeseries.GroupingBucket;
import com.linkedin.timeseries.GroupingBucketType;
import com.linkedin.timeseries.TimeWindowSize;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.PipelineAggregatorBuilders;
import org.opensearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.terms.ParsedTerms;
import org.opensearch.search.aggregations.bucket.terms.Terms;
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
  static final String BATCH_URN_AGG_NAME = "batch_urn_outer";
  private final SearchClientShim<?> searchClient;
  @Nonnull private final QueryFilterRewriteChain queryFilterRewriteChain;

  public ESAggregatedStatsDAO(
      @Nonnull SearchClientShim<?> searchClient,
      @Nonnull QueryFilterRewriteChain queryFilterRewriteChain) {
    this.searchClient = searchClient;
    this.queryFilterRewriteChain = queryFilterRewriteChain;
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
      @Nonnull OperationContext opContext, @Nonnull String entityName, @Nonnull String aspectName) {
    EntitySpec entitySpec = opContext.getEntityRegistry().getEntitySpec(entityName);
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
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull AggregationSpec[] aggregationSpecs,
      @Nullable Filter filter,
      @Nullable GroupingBucket[] groupingBuckets) {

    // Setup the filter query builder using the input filter provided.
    final BoolQueryBuilder filterQueryBuilder =
        ESUtils.buildFilterQuery(
            filter,
            true,
            opContext.getEntityRegistry().getEntitySpec(entityName).getSearchableFieldTypes(),
            opContext,
            queryFilterRewriteChain);

    AspectSpec aspectSpec = getTimeseriesAspectSpec(opContext, entityName, aspectName);
    // Build and attach the grouping aggregations
    final Pair<AggregationBuilder, AggregationBuilder> topAndBottomAggregations =
        makeGroupingAggregationBuilder(
            aspectSpec, null, groupingBuckets, opContext, opContext.getAspectRetriever());
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

    final String indexName =
        opContext
            .getSearchContext()
            .getIndexConvention()
            .getTimeseriesAspectIndexName(entityName, aspectName);
    searchRequest.indices(indexName);

    log.debug("Search request is: " + searchRequest);

    try {
      final SearchResponse searchResponse =
          searchClient.search(opContext, searchRequest, RequestOptions.DEFAULT);
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

  /**
   * Returns the first aggregation spec to use for bucket ordering when any bucket has
   * orderByMetric=true, or null.
   */
  private static @Nullable AggregationSpec findOrderBySpec(
      @Nullable GroupingBucket[] buckets, @Nonnull AggregationSpec[] aggregationSpecs) {
    if (buckets == null || aggregationSpecs.length == 0) {
      return null;
    }
    for (GroupingBucket b : buckets) {
      if (b.hasOrderByMetric() && b.isOrderByMetric()) {
        return aggregationSpecs[0];
      }
    }
    return null;
  }

  /** Backward-compatible overload — delegates with no metric ordering. */
  private Pair<AggregationBuilder, AggregationBuilder> makeGroupingAggregationBuilder(
      AspectSpec aspectSpec,
      @Nullable AggregationBuilder baseAggregationBuilder,
      @Nullable GroupingBucket[] groupingBuckets,
      @Nonnull OperationContext opContext,
      @Nonnull AspectRetriever aspectRetriever) {
    return makeGroupingAggregationBuilder(
        aspectSpec, baseAggregationBuilder, groupingBuckets, null, opContext, aspectRetriever);
  }

  private Pair<AggregationBuilder, AggregationBuilder> makeGroupingAggregationBuilder(
      AspectSpec aspectSpec,
      @Nullable AggregationBuilder baseAggregationBuilder,
      @Nullable GroupingBucket[] groupingBuckets,
      @Nullable AggregationSpec orderBySpec,
      @Nonnull OperationContext opContext,
      @Nonnull AspectRetriever aspectRetriever) {

    AggregationBuilder firstAggregationBuilder = baseAggregationBuilder;
    AggregationBuilder lastAggregationBuilder = baseAggregationBuilder;
    if (groupingBuckets != null) {
      for (GroupingBucket curGroupingBucket : groupingBuckets) {
        AggregationBuilder curAggregationBuilder = null;
        if (curGroupingBucket.getType() == GroupingBucketType.DATE_GROUPING_BUCKET) {
          // Process the date grouping bucket using 'date-histogram' aggregation.
          curAggregationBuilder =
              AggregationBuilders.dateHistogram(ES_AGGREGATION_PREFIX + curGroupingBucket.getKey())
                  .field(curGroupingBucket.getKey())
                  .timeZone(getZoneId(curGroupingBucket))
                  .calendarInterval(getHistogramInterval(curGroupingBucket.getTimeWindowSize()));
        } else if (curGroupingBucket.getType() == GroupingBucketType.STRING_GROUPING_BUCKET) {
          // Process the string grouping bucket using the 'terms' aggregation.
          // The field can be Keyword, Numeric, ip, boolean, or binary.
          String fieldName =
              ESUtils.toKeywordField(opContext, curGroupingBucket.getKey(), true, aspectRetriever);
          DataSchema.Type fieldType = getGroupingBucketKeyType(aspectSpec, curGroupingBucket);
          boolean asc = !curGroupingBucket.hasAscending() || curGroupingBucket.isAscending();
          BucketOrder bucketOrder =
              (orderBySpec != null
                      && curGroupingBucket.hasOrderByMetric()
                      && curGroupingBucket.isOrderByMetric())
                  ? BucketOrder.aggregation(getAggregationSpecAggESName(orderBySpec), asc)
                  : BucketOrder.aggregation("_key", asc);
          curAggregationBuilder =
              AggregationBuilders.terms(getGroupingBucketAggName(curGroupingBucket))
                  .field(fieldName)
                  .size(
                      curGroupingBucket.hasSize() ? curGroupingBucket.getSize() : MAX_TERM_BUCKETS)
                  .order(bucketOrder);
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

  @Nonnull
  private ZoneId getZoneId(GroupingBucket groupingBucket) {
    // default to GMT time
    ZoneId zoneId = ZoneId.of("GMT");
    if (groupingBucket.getTimeZone() != null) {
      try {
        zoneId = ZoneId.of(groupingBucket.getTimeZone());
      } catch (DateTimeException exception) {
        log.error("Error converting time zone into ZoneId", exception);
      }
    }
    return zoneId;
  }

  private GenericTable generateResponseFromElastic(
      SearchResponse searchResponse,
      GroupingBucket[] groupingBuckets,
      AggregationSpec[] aggregationSpecs,
      AspectSpec aspectSpec) {
    return buildTableFromAggregations(
        searchResponse.getAggregations(), groupingBuckets, aggregationSpecs, aspectSpec);
  }

  private GenericTable buildTableFromAggregations(
      @Nonnull Aggregations aggregations,
      @Nullable GroupingBucket[] groupingBuckets,
      @Nonnull AggregationSpec[] aggregationSpecs,
      @Nonnull AspectSpec aspectSpec) {
    GroupingBucket[] effectiveBuckets =
        groupingBuckets != null ? groupingBuckets : new GroupingBucket[0];

    GenericTable resultTable = new GenericTable();
    resultTable.setColumnNames(new StringArray(genColumnNames(effectiveBuckets, aggregationSpecs)));
    resultTable.setColumnTypes(
        new StringArray(genColumnTypes(aspectSpec, effectiveBuckets, aggregationSpecs)));

    List<StringArray> rows = new ArrayList<>();
    Stack<String> rowAcc = new Stack<>();
    rowGenHelper(
        aggregations,
        0,
        effectiveBuckets.length,
        rows,
        rowAcc,
        ImmutableList.copyOf(effectiveBuckets),
        ImmutableList.copyOf(aggregationSpecs),
        aspectSpec);

    if (!rowAcc.isEmpty()) {
      throw new IllegalStateException("Expected stack to be empty.");
    }

    resultTable.setRows(new StringArrayArray(rows));
    return resultTable;
  }

  /**
   * Fetch aggregated stats for a batch of URNs in a single ES query. The outer {@code
   * terms(batch_urn_outer)} bucket groups results by {@code urnFieldPath}; the inner aggregation
   * tree is the same as the single-URN path. URNs absent from the ES response (no matching
   * documents) are filled with an empty {@link GenericTable}.
   *
   * <p>Callers are responsible for deciding whether to use this method or the single-URN {@link
   * #getAggregatedStats} based on expected cardinality. With {@code STRING_GROUPING_BUCKET}, each
   * outer URN bucket forces ES to materialise up to {@code MAX_TERM_BUCKETS} inner buckets, so
   * total bucket count grows as {@code urns.size() × string_cardinality}. At high cardinality this
   * can exceed OpenSearch's {@code search.max_buckets} limit and fail, or consume significantly
   * more heap than equivalent individual queries.
   */
  @Nonnull
  public Map<Urn, GenericTable> getBatchAggregatedStats(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull AggregationSpec[] aggregationSpecs,
      @Nonnull List<Urn> urns,
      @Nullable Filter sharedFilter,
      @Nullable GroupingBucket[] groupingBuckets,
      @Nonnull String urnFieldPath) {

    AspectSpec aspectSpec = getTimeseriesAspectSpec(opContext, entityName, aspectName);

    // Pre-filter: restrict to the requested URNs, then apply any shared filter
    BoolQueryBuilder filterQueryBuilder = QueryBuilders.boolQuery();
    List<String> urnStrings = urns.stream().map(Urn::toString).collect(Collectors.toList());
    filterQueryBuilder.filter(QueryBuilders.termsQuery(urnFieldPath, urnStrings));
    if (sharedFilter != null) {
      filterQueryBuilder.must(
          ESUtils.buildFilterQuery(
              sharedFilter,
              true,
              opContext.getEntityRegistry().getEntitySpec(entityName).getSearchableFieldTypes(),
              opContext,
              queryFilterRewriteChain));
    }

    // Build the inner agg tree (grouping buckets + metric leaf aggs)
    Pair<AggregationBuilder, AggregationBuilder> innerTree =
        makeGroupingAggregationBuilder(
            aspectSpec,
            null,
            groupingBuckets,
            findOrderBySpec(groupingBuckets, aggregationSpecs),
            opContext,
            opContext.getAspectRetriever());
    AggregationBuilder innerRoot = innerTree.getFirst();
    AggregationBuilder mostNested = innerTree.getSecond();

    // Outer URN bucket wrapping the inner tree
    AggregationBuilder urnOuterAgg =
        AggregationBuilders.terms(BATCH_URN_AGG_NAME).field(urnFieldPath).size(urns.size());

    AggregationBuilder metricsTarget;
    if (innerRoot != null) {
      urnOuterAgg.subAggregation(innerRoot);
      metricsTarget = mostNested;
    } else {
      metricsTarget = urnOuterAgg;
    }
    for (AggregationSpec aggSpec : aggregationSpecs) {
      addAggregationBuildersFromAggregationSpec(aspectSpec, metricsTarget, aggSpec);
    }

    SearchSourceBuilder searchSourceBuilder =
        new SearchSourceBuilder()
            .aggregation(urnOuterAgg)
            .query(QueryBuilders.boolQuery().must(filterQueryBuilder))
            .size(0);

    SearchRequest searchRequest = new SearchRequest();
    searchRequest.source(searchSourceBuilder);
    searchRequest.indices(
        opContext
            .getSearchContext()
            .getIndexConvention()
            .getTimeseriesAspectIndexName(entityName, aspectName));

    log.debug("Batch aggregated stats search request: {}", searchRequest);

    final SearchResponse searchResponse;
    try {
      searchResponse = searchClient.search(opContext, searchRequest, RequestOptions.DEFAULT);
    } catch (Exception e) {
      log.error("Batch aggregated stats query failed: {}", e.getMessage());
      throw new ESQueryException("Batch aggregated stats query failed:", e);
    }

    GroupingBucket[] effectiveBuckets =
        groupingBuckets != null ? groupingBuckets : new GroupingBucket[0];
    StringArray columnNames = new StringArray(genColumnNames(effectiveBuckets, aggregationSpecs));
    StringArray columnTypes =
        new StringArray(genColumnTypes(aspectSpec, effectiveBuckets, aggregationSpecs));

    Map<Urn, GenericTable> result = new HashMap<>();
    ParsedTerms urnBuckets = searchResponse.getAggregations().get(BATCH_URN_AGG_NAME);
    for (Terms.Bucket bucket : urnBuckets.getBuckets()) {
      Urn urn = UrnUtils.getUrn(bucket.getKeyAsString());
      result.put(
          urn,
          buildTableFromAggregations(
              bucket.getAggregations(), effectiveBuckets, aggregationSpecs, aspectSpec));
    }

    // URNs absent from the ES response (no matching docs) get empty tables
    GenericTable emptyTable =
        new GenericTable()
            .setColumnNames(columnNames)
            .setColumnTypes(columnTypes)
            .setRows(new StringArrayArray());
    for (Urn urn : urns) {
      result.putIfAbsent(urn, emptyTable);
    }

    return result;
  }
}
