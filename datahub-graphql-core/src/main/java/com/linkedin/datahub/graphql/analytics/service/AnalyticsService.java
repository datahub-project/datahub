package com.linkedin.datahub.graphql.analytics.service;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.generated.Cell;
import com.linkedin.datahub.graphql.generated.DateInterval;
import com.linkedin.datahub.graphql.generated.DateRange;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.NamedBar;
import com.linkedin.datahub.graphql.generated.NamedLine;
import com.linkedin.datahub.graphql.generated.Row;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;

/**
 * Platform analytics over usage and entity indices. Use {@link PostgresAnalyticsService} when usage
 * events are stored in Postgres (pgAnalytics), or {@link DefaultAnalyticsService} when charts are
 * served from the search index.
 */
public interface AnalyticsService {

  String NA = "N/A";

  String DATAHUB_USAGE_EVENT_INDEX = "datahub_usage_event";

  static Row buildRow(String groupByValue, Function<String, Cell> groupByValueToCell, int count) {
    List<String> values = ImmutableList.of(groupByValue, String.valueOf(count));
    List<Cell> cells =
        ImmutableList.of(
            groupByValueToCell.apply(groupByValue),
            Cell.builder().setValue(String.valueOf(count)).build());
    return new Row(values, cells);
  }

  @Nonnull
  String getEntityIndexName(@Nonnull OperationContext opContext, EntityType entityType);

  @Nonnull
  String getAllEntityIndexName(@Nonnull OperationContext opContext);

  @Nonnull
  String getUsageIndexName(@Nonnull OperationContext opContext);

  List<NamedLine> getTimeseriesChart(
      @Nonnull OperationContext opContext,
      String indexName,
      DateRange dateRange,
      DateInterval granularity,
      Optional<String> dimension,
      Map<String, List<String>> filters,
      Map<String, List<String>> mustNotFilters,
      Optional<String> uniqueOn,
      String dateRangeField);

  default List<NamedLine> getTimeseriesChart(
      @Nonnull OperationContext opContext,
      String indexName,
      DateRange dateRange,
      DateInterval granularity,
      Optional<String> dimension,
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

  List<NamedBar> getBarChart(
      @Nonnull OperationContext opContext,
      String indexName,
      Optional<DateRange> dateRange,
      List<String> dimensions,
      Map<String, List<String>> filters,
      Map<String, List<String>> mustNotFilters,
      Optional<String> uniqueOn,
      boolean showMissing);

  List<Row> getTopNTableChart(
      @Nonnull OperationContext opContext,
      String indexName,
      Optional<DateRange> dateRange,
      String groupBy,
      Map<String, List<String>> filters,
      Map<String, List<String>> mustNotFilters,
      Optional<String> uniqueOn,
      int maxRows,
      Function<String, Cell> groupByValueToCell);

  int getHighlights(
      @Nonnull OperationContext opContext,
      String indexName,
      Optional<DateRange> dateRange,
      Map<String, List<String>> filters,
      Map<String, List<String>> mustNotFilters,
      Optional<String> uniqueOn);

  /** Unique count per date range in one query. Keys of the result match {@code keyedRanges}. */
  Map<String, Integer> getUniqueCountsByRange(
      @Nonnull OperationContext opContext,
      String indexName,
      Map<String, DateRange> keyedRanges,
      String uniqueOn);

  /**
   * Total document count plus one count per facet field, for every requested entity type.
   * Soft-deleted entities are excluded.
   */
  Map<EntityType, EntityStats> getEntityStats(
      @Nonnull OperationContext opContext, List<EntityType> entityTypes, List<String> facetFields);
}
