package com.linkedin.metadata.search.elasticsearch.query.template;

import com.linkedin.metadata.config.search.custom.QueryConfiguration;
import com.linkedin.metadata.query.SearchFlags;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Templates stay authoritative when Search V3 template reads are on. A request {@code
 * fieldConfiguration} and a matching {@code queryConfigurations} entry are not applied. Each one
 * that would have applied is counted and logged.
 */
@Slf4j
public final class CustomSearchMigration {
  public static final String FIELD_CONFIGURATION = "ignoredFieldConfiguration";
  public static final String QUERY_CONFIGURATION = "ignoredQueryConfiguration";

  private CustomSearchMigration() {}

  public record Notice(@Nullable String fieldConfiguration, @Nullable String queryRegex) {
    public boolean any() {
      return fieldConfiguration != null || queryRegex != null;
    }
  }

  @Nonnull
  public static Notice note(
      @Nonnull OperationContext opContext,
      @Nullable SearchFlags flags,
      @Nullable QueryConfiguration matchedQuery) {
    String fieldConfiguration = null;
    if (flags != null
        && flags.getFieldConfiguration() != null
        && !flags.getFieldConfiguration().isBlank()) {
      fieldConfiguration = flags.getFieldConfiguration();
      log.warn(
          "Search template ignored deprecated fieldConfiguration '{}'. Template field settings remain authoritative.",
          fieldConfiguration);
      count(opContext, FIELD_CONFIGURATION);
    }
    String queryRegex = null;
    if (matchedQuery != null && matchedQuery.getQueryRegex() != null) {
      queryRegex = matchedQuery.getQueryRegex();
      log.warn(
          "Search template ignored deprecated queryConfiguration '{}'. The template remains authoritative.",
          queryRegex);
      count(opContext, QUERY_CONFIGURATION);
    }
    return new Notice(fieldConfiguration, queryRegex);
  }

  private static void count(@Nonnull OperationContext opContext, @Nonnull String metric) {
    opContext
        .getMetricUtils()
        .ifPresent(metrics -> metrics.increment(CustomSearchMigration.class, metric, 1));
  }
}
