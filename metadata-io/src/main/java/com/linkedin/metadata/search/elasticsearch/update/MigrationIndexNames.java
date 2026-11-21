package com.linkedin.metadata.search.elasticsearch.update;

import com.datahub.context.OperationFingerprint;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.util.Locale;
import java.util.function.UnaryOperator;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Maps a family's index names on the serving cluster to their names on the migration target.
 *
 * <p>The serving prefix comes from the convention (the serving cluster's overlay or the
 * per-operation resolver); the target prefix is the target cluster's overlay when it sets one,
 * otherwise the same resolver prefix, so names are identical unless the clusters disagree on their
 * overlay. Families whose live index is a rollover alias or a data stream (usage) do not mirror
 * into the target's alias: those writes go to a concrete {@linkplain #USAGE_LIVE_STAGING_SUFFIX
 * live staging index} that the copy also fills, so {@code op_type=create} deduplicates exactly and
 * the index is adopted into the alias or data stream once the migration completes.
 */
public final class MigrationIndexNames {
  /**
   * Base of every usage staging index. Deliberately outside both usage template patterns ({@code
   * <prefix>datahub_usage_event-*} on OpenSearch, which would attach the ISM policy, and {@code
   * <prefix>datahub_usage_event*} on Elasticsearch, whose data-stream template rejects a plain
   * index outright).
   */
  public static final String USAGE_STAGING_BASE = "datahub_usage_migration";

  public static final String USAGE_LIVE_STAGING = USAGE_STAGING_BASE + "-live";
  private static final String LOG_INDEX_BASE = "cluster_migration_log_";

  private final IndexConvention convention;
  private final SearchComponent family;
  @Nullable private final String targetPrefixOverlay;
  private final UnaryOperator<String> baseNameMapper;

  private MigrationIndexNames(
      @Nonnull final IndexConvention convention,
      @Nonnull final SearchComponent family,
      @Nullable final String targetPrefixOverlay,
      @Nonnull final UnaryOperator<String> baseNameMapper) {
    this.convention = convention;
    this.family = family;
    this.targetPrefixOverlay =
        targetPrefixOverlay == null || targetPrefixOverlay.trim().isEmpty()
            ? null
            : targetPrefixOverlay.trim().toLowerCase(Locale.ROOT);
    this.baseNameMapper = baseNameMapper;
  }

  /**
   * @param targetPrefixOverlay the target cluster's {@code index.prefix}, blank or null to inherit
   */
  @Nonnull
  public static MigrationIndexNames forFamily(
      @Nonnull final IndexConvention convention,
      @Nonnull final SearchComponent family,
      @Nullable final String targetPrefixOverlay) {
    final UnaryOperator<String> mapper =
        family == SearchComponent.USAGE ? MigrationIndexNames::usageLiveStaging : base -> base;
    return new MigrationIndexNames(convention, family, targetPrefixOverlay, mapper);
  }

  @Nonnull
  public SearchComponent getFamily() {
    return family;
  }

  /** The target-cluster name a write to {@code servingIndexName} is mirrored to. */
  @Nonnull
  public String target(
      @Nonnull final OperationFingerprint operation, @Nonnull final String servingIndexName) {
    return targetToken(operation) + baseNameMapper.apply(baseName(operation, servingIndexName));
  }

  /** The family-scoped delete log on the target cluster. */
  @Nonnull
  public String logIndex(@Nonnull final OperationFingerprint operation) {
    return targetToken(operation) + LOG_INDEX_BASE + family.getConfigKey().toLowerCase(Locale.ROOT);
  }

  /**
   * The delete log an earlier migration of this family onto the serving cluster left there: after a
   * flip the cluster that was the target serves, and its log is removed from this side.
   */
  @Nonnull
  public String logIndexOnServing(@Nonnull final OperationFingerprint operation) {
    return servingToken(operation)
        + LOG_INDEX_BASE
        + family.getConfigKey().toLowerCase(Locale.ROOT);
  }

  /** The prefix token ({@code "prod_"} or {@code ""}) the target cluster uses for this family. */
  @Nonnull
  public String targetToken(@Nonnull final OperationFingerprint operation) {
    final String prefix =
        targetPrefixOverlay != null
            ? targetPrefixOverlay
            : convention.getPrefix(operation).map(p -> p.toLowerCase(Locale.ROOT)).orElse("");
    return prefix.isEmpty() ? "" : prefix + "_";
  }

  /** {@code servingIndexName} without the serving cluster's prefix token for this family. */
  @Nonnull
  public String baseName(
      @Nonnull final OperationFingerprint operation, @Nonnull final String servingIndexName) {
    final String servingToken = servingToken(operation);
    return !servingToken.isEmpty() && servingIndexName.startsWith(servingToken)
        ? servingIndexName.substring(servingToken.length())
        : servingIndexName;
  }

  /** The prefix token the serving cluster uses for this family. */
  @Nonnull
  private String servingToken(@Nonnull final OperationFingerprint operation) {
    return convention
        .getPrefix(operation, family)
        .map(p -> p.toLowerCase(Locale.ROOT) + "_")
        .orElse("");
  }

  /**
   * The staging index a usage backing index is copied into: {@code datahub_usage_migration-<gen>}
   * where {@code <gen>} is the part after the alias or data-stream name in the source's backing
   * index name ({@code -000003}, {@code -2026.10.01-000003}), or {@code legacy} for a pre-rollover
   * concrete index.
   */
  @Nonnull
  public String usageStagingFor(
      @Nonnull final OperationFingerprint operation, @Nonnull final String sourceBackingIndex) {
    String base = baseName(operation, sourceBackingIndex);
    if (base.startsWith(".ds-")) {
      base = baseName(operation, base.substring(".ds-".length()));
    }
    final String generation =
        base.startsWith(Constants.DATAHUB_USAGE_EVENT_INDEX + "-")
            ? base.substring(Constants.DATAHUB_USAGE_EVENT_INDEX.length() + 1)
            : "legacy";
    return targetToken(operation) + USAGE_STAGING_BASE + "-" + generation;
  }

  /**
   * Whether {@code indexName}, on any cluster and under any prefix, is a staging index of a usage
   * migration: a member of the rollover alias or data stream that a copy or a sweep adopted,
   * holding another cluster's history rather than one of the alias's own generations.
   */
  public static boolean isUsageStagingIndex(@Nonnull final String indexName) {
    final int at = indexName.indexOf(USAGE_STAGING_BASE + "-");
    return at == 0 || (at > 0 && indexName.charAt(at - 1) == '_');
  }

  @Nonnull
  private static String usageLiveStaging(@Nonnull final String baseName) {
    return Constants.DATAHUB_USAGE_EVENT_INDEX.equals(baseName) ? USAGE_LIVE_STAGING : baseName;
  }
}
