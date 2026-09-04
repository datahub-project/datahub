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
import java.util.Collections;
import java.util.LinkedHashMap;
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

  private boolean isUsageIndex(@Nonnull OperationContext opContext, @Nonnull String indexName) {
    return getUsageIndexName(opContext).equals(indexName);
  }

  @Override
  @Nonnull
  public String getEntityIndexName(@Nonnull OperationContext opContext, EntityType entityType) {
    return indexConvention.getEntityIndexName(opContext, EntityTypeMapper.getName(entityType));
  }

  @Override
  @Nonnull
  public String getAllEntityIndexName(@Nonnull OperationContext opContext) {
    return indexConvention.getEntityIndexName(opContext, "*");
  }

  @Override
  @Nonnull
  public String getUsageIndexName(@Nonnull OperationContext opContext) {
    return indexConvention.getIndexName(opContext, AnalyticsService.DATAHUB_USAGE_EVENT_INDEX);
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
    if (!isUsageIndex(opContext, indexName)) {
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
    if (!isUsageIndex(opContext, indexName)) {
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
    if (!isUsageIndex(opContext, indexName)) {
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
    if (!isUsageIndex(opContext, indexName)) {
      return 0;
    }
    return postgresAnalytics.getHighlights(indexName, dateRange, filters, mustNotFilters, uniqueOn);
  }

  @Override
  public Map<String, Integer> getUniqueCountsByRange(
      @Nonnull OperationContext opContext,
      String indexName,
      Map<String, DateRange> keyedRanges,
      String uniqueOn) {
    if (!isUsageIndex(opContext, indexName) || keyedRanges.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, Integer> results = new LinkedHashMap<>();
    for (Map.Entry<String, DateRange> entry : keyedRanges.entrySet()) {
      results.put(
          entry.getKey(),
          postgresAnalytics.getHighlights(
              indexName,
              Optional.of(entry.getValue()),
              Collections.emptyMap(),
              Collections.emptyMap(),
              Optional.of(uniqueOn)));
    }
    return results;
  }

  @Override
  public Map<EntityType, EntityStats> getEntityStats(
      @Nonnull OperationContext opContext, List<EntityType> entityTypes, List<String> facetFields) {
    return Collections.emptyMap();
  }
}
