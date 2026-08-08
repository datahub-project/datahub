package com.linkedin.datahub.graphql.analytics.service;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.analytics.service.postgres.PostgresAnalyticsQueries;
import com.linkedin.datahub.graphql.generated.Cell;
import com.linkedin.datahub.graphql.generated.DateInterval;
import com.linkedin.datahub.graphql.generated.DateRange;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.NamedBar;
import com.linkedin.datahub.graphql.generated.NamedLine;
import com.linkedin.datahub.graphql.generated.Row;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Platform analytics using JDBC against pgAnalytics. Non-usage indices return empty results —
 * entity-level aggregations require {@link DefaultAnalyticsService}.
 */
@Slf4j
@RequiredArgsConstructor
public final class PostgresAnalyticsService implements AnalyticsService {

  @Nonnull private final IndexConvention indexConvention;
  @Nonnull private final PostgresAnalyticsQueries postgresAnalytics;

  private boolean isUsageIndex(@Nonnull String indexName) {
    return getUsageIndexName().equals(indexName);
  }

  @Override
  @Nonnull
  public String getEntityIndexName(EntityType entityType) {
    return indexConvention.getEntityIndexName(EntityTypeMapper.getName(entityType));
  }

  @Override
  @Nonnull
  public String getAllEntityIndexName() {
    return indexConvention.getEntityIndexName("*");
  }

  @Override
  @Nonnull
  public String getUsageIndexName() {
    return indexConvention.getIndexName(AnalyticsService.DATAHUB_USAGE_EVENT_INDEX);
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
    if (!isUsageIndex(indexName)) {
      return ImmutableList.of();
    }
    return postgresAnalytics.getTimeseriesChart(
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
    if (!isUsageIndex(indexName)) {
      return ImmutableList.of();
    }
    return postgresAnalytics.getBarChart(
        indexName, dateRange, dimensions, filters, mustNotFilters, uniqueOn, showMissing);
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
    if (!isUsageIndex(indexName)) {
      return ImmutableList.of();
    }
    return postgresAnalytics.getTopNTableChart(
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
    if (!isUsageIndex(indexName)) {
      return 0;
    }
    return postgresAnalytics.getHighlights(indexName, dateRange, filters, mustNotFilters, uniqueOn);
  }
}
