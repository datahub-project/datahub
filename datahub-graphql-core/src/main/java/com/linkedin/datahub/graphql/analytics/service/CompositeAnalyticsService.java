package com.linkedin.datahub.graphql.analytics.service;

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
import lombok.RequiredArgsConstructor;

/**
 * Routes usage-index queries to a Postgres-backed service and entity-index queries to the search
 * analytics service.
 */
@RequiredArgsConstructor
public final class CompositeAnalyticsService implements AnalyticsService {

  @Nonnull private final AnalyticsService usageAnalytics;
  @Nonnull private final AnalyticsService entityAnalytics;

  private boolean isUsageIndex(@Nonnull OperationContext opContext, @Nonnull String indexName) {
    return getUsageIndexName(opContext).equals(indexName);
  }

  private AnalyticsService delegateFor(
      @Nonnull OperationContext opContext, @Nonnull String indexName) {
    return isUsageIndex(opContext, indexName) ? usageAnalytics : entityAnalytics;
  }

  @Override
  @Nonnull
  public String getEntityIndexName(@Nonnull OperationContext opContext, EntityType entityType) {
    return entityAnalytics.getEntityIndexName(opContext, entityType);
  }

  @Override
  @Nonnull
  public String getAllEntityIndexName(@Nonnull OperationContext opContext) {
    return entityAnalytics.getAllEntityIndexName(opContext);
  }

  @Override
  @Nonnull
  public String getUsageIndexName(@Nonnull OperationContext opContext) {
    return usageAnalytics.getUsageIndexName(opContext);
  }

  @Override
  public List<NamedLine> getTimeseriesChart(
      @Nonnull OperationContext opContext,
      String indexName,
      DateRange dateRange,
      DateInterval granularity,
      Optional<String> dimension,
      Map<String, List<String>> filters,
      Map<String, List<String>> mustNotFilters,
      Optional<String> uniqueOn,
      String dateRangeField) {
    return delegateFor(opContext, indexName)
        .getTimeseriesChart(
            opContext,
            indexName,
            dateRange,
            granularity,
            dimension,
            filters,
            mustNotFilters,
            uniqueOn,
            dateRangeField);
  }

  @Override
  public List<NamedBar> getBarChart(
      @Nonnull OperationContext opContext,
      String indexName,
      Optional<DateRange> dateRange,
      List<String> dimensions,
      Map<String, List<String>> filters,
      Map<String, List<String>> mustNotFilters,
      Optional<String> uniqueOn,
      boolean showMissing) {
    return delegateFor(opContext, indexName)
        .getBarChart(
            opContext,
            indexName,
            dateRange,
            dimensions,
            filters,
            mustNotFilters,
            uniqueOn,
            showMissing);
  }

  @Override
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
    return delegateFor(opContext, indexName)
        .getTopNTableChart(
            opContext,
            indexName,
            dateRange,
            groupBy,
            filters,
            mustNotFilters,
            uniqueOn,
            maxRows,
            groupByValueToCell);
  }

  @Override
  public int getHighlights(
      @Nonnull OperationContext opContext,
      String indexName,
      Optional<DateRange> dateRange,
      Map<String, List<String>> filters,
      Map<String, List<String>> mustNotFilters,
      Optional<String> uniqueOn) {
    return delegateFor(opContext, indexName)
        .getHighlights(opContext, indexName, dateRange, filters, mustNotFilters, uniqueOn);
  }

  @Override
  public Map<String, Integer> getUniqueCountsByRange(
      @Nonnull OperationContext opContext,
      String indexName,
      Map<String, DateRange> keyedRanges,
      String uniqueOn) {
    return delegateFor(opContext, indexName)
        .getUniqueCountsByRange(opContext, indexName, keyedRanges, uniqueOn);
  }

  @Override
  public Map<EntityType, EntityStats> getEntityStats(
      @Nonnull OperationContext opContext, List<EntityType> entityTypes, List<String> facetFields) {
    return entityAnalytics.getEntityStats(opContext, entityTypes, facetFields);
  }
}
