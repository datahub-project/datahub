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

  private boolean isUsageIndex(@Nonnull String indexName) {
    return getUsageIndexName().equals(indexName);
  }

  private AnalyticsService delegateFor(@Nonnull String indexName) {
    return isUsageIndex(indexName) ? usageAnalytics : entityAnalytics;
  }

  @Override
  @Nonnull
  public String getEntityIndexName(EntityType entityType) {
    return entityAnalytics.getEntityIndexName(entityType);
  }

  @Override
  @Nonnull
  public String getAllEntityIndexName() {
    return entityAnalytics.getAllEntityIndexName();
  }

  @Override
  @Nonnull
  public String getUsageIndexName() {
    return usageAnalytics.getUsageIndexName();
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
    return delegateFor(indexName)
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
    return delegateFor(indexName)
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
    return delegateFor(indexName)
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
    return delegateFor(indexName)
        .getHighlights(opContext, indexName, dateRange, filters, mustNotFilters, uniqueOn);
  }
}
