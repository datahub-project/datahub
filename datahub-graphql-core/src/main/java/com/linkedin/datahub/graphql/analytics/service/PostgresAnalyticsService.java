package com.linkedin.datahub.graphql.analytics.service;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.analytics.service.postgres.PostgresUsageEventsAnalyticsQueries;
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
 * Platform analytics using only JDBC ({@link PostgresUsageEventsAnalyticsQueries}) against the
 * partitioned usage-events store. Does not use Elasticsearch/OpenSearch. Requests for non-usage
 * indices return empty results — entity-level aggregations require the search-backed implementation
 * ({@link DefaultAnalyticsService}).
 */
@Slf4j
@RequiredArgsConstructor
public final class PostgresAnalyticsService implements AnalyticsService {

  @Nonnull private final IndexConvention indexConvention;
  @Nonnull private final PostgresUsageEventsAnalyticsQueries postgresUsageAnalytics;

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
      log.debug("Postgres analytics: skip timeseries for non-usage index {}", indexName);
      return ImmutableList.of();
    }
    return postgresUsageAnalytics.getTimeseriesChart(
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
      log.debug("Postgres analytics: skip bar chart for non-usage index {}", indexName);
      return ImmutableList.of();
    }
    return postgresUsageAnalytics.getBarChart(
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
      log.debug("Postgres analytics: skip table chart for non-usage index {}", indexName);
      return ImmutableList.of();
    }
    return postgresUsageAnalytics.getTopNTableChart(
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
      log.debug("Postgres analytics: skip highlights for non-usage index {}", indexName);
      return 0;
    }
    return postgresUsageAnalytics.getHighlights(
        indexName, dateRange, filters, mustNotFilters, uniqueOn);
  }
}
