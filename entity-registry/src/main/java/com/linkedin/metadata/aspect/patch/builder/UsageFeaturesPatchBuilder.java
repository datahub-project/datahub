package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.*;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.metadata.aspect.patch.PatchOperationType;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.tuple.ImmutableTriple;

public class UsageFeaturesPatchBuilder
    extends AbstractMultiFieldPatchBuilder<UsageFeaturesPatchBuilder> {

  public static final String BASE_PATH = "/";
  private static final String URN_KEY = "urn";
  private static final String CONTEXT_KEY = "context";
  public static final String QUERY_COUNT_LAST_30_DAYS_KEY = "queryCountLast30Days";
  public static final String VIEW_COUNT_LAST_30_DAYS_KEY = "viewCountLast30Days";
  public static final String VIEW_COUNT_TOTAL_KEY = "viewCountTotal";
  public static final String VIEW_COUNT_PERCENTILE_LAST_30_DAYS_KEY =
      "viewCountPercentileLast30Days";
  public static final String USAGE_COUNT_LAST_30_DAYS_KEY = "usageCountLast30Days";
  public static final String UNIQUE_USER_COUNT_LAST_30_DAYS_KEY = "uniqueUserCountLast30Days";
  public static final String WRITE_COUNT_LAST_30_DAYS_KEY = "writeCountLast30Days";
  public static final String QUERY_COUNT_PERCENTILE_LAST_30_DAYS_KEY =
      "queryCountPercentileLast30Days";
  public static final String QUERY_COUNT_RANK_LAST_30_DAYS_KEY = "queryCountRankLast30Days";
  public static final String UNIQUE_USER_PERCENTILE_LAST_30_DAYS_KEY =
      "uniqueUserPercentileLast30Days";
  public static final String UNIQUE_USER_RANK_LAST_30_DAYS_KEY = "uniqueUserRankLast30Days";
  public static final String WRITE_COUNT_PERCENTILE_LAST_30_DAYS_KEY =
      "writeCountPercentileLast30Days";
  public static final String WRITE_COUNT_RANK_LAST_30_DAYS_KEY = "writeCountRankLast30Days";
  public static final String TOP_USERS_LAST_30_DAYS_KEY = "topUsersLast30Days";
  public static final String SIZE_IN_BYTES_PERCENTILE_KEY = "sizeInBytesPercentile";
  public static final String SIZE_IN_BYTES_RANK_KEY = "sizeInBytesRank";
  public static final String ROW_COUNT_PERCENTILE_KEY = "rowCountPercentile";
  public static final String USAGE_SEARCH_SCORE_MULTIPLIER_KEY = "usageSearchScoreMultiplier";
  public static final String USAGE_FRESHNESS_SCORE_MULTIPLIER_KEY = "usageFreshnessScoreMultiplier";
  public static final String CUSTOM_DATAHUB_SCORE_MULTIPLIER_KEY = "customDatahubScoreMultiplier";
  public static final String COMBINED_SEARCH_RANKING_MULTIPLIER_KEY =
      "combinedSearchRankingMultiplier";

  public UsageFeaturesPatchBuilder setQueryCountLast30Days(@Nonnull Long value) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH + QUERY_COUNT_LAST_30_DAYS_KEY,
            instance.numberNode(value)));
    return this;
  }

  public UsageFeaturesPatchBuilder setViewCountLast30Days(@Nonnull Long value) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH + VIEW_COUNT_LAST_30_DAYS_KEY,
            instance.numberNode(value)));
    return this;
  }

  public UsageFeaturesPatchBuilder setViewCountTotal(@Nonnull Long value) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH + VIEW_COUNT_TOTAL_KEY,
            instance.numberNode(value)));
    return this;
  }

  public UsageFeaturesPatchBuilder setViewCountPercentileLast30Days(@Nonnull int value) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH + VIEW_COUNT_PERCENTILE_LAST_30_DAYS_KEY,
            instance.numberNode(value)));
    return this;
  }

  public UsageFeaturesPatchBuilder setUsageCountLast30Days(@Nonnull Long value) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH + USAGE_COUNT_LAST_30_DAYS_KEY,
            instance.numberNode(value)));
    return this;
  }

  public UsageFeaturesPatchBuilder setUniqueUserCountLast30Days(@Nonnull Long value) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH + UNIQUE_USER_COUNT_LAST_30_DAYS_KEY,
            instance.numberNode(value)));
    return this;
  }

  public UsageFeaturesPatchBuilder setWriteCountLast30Days(@Nonnull Long value) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH + WRITE_COUNT_LAST_30_DAYS_KEY,
            instance.numberNode(value)));
    return this;
  }

  public UsageFeaturesPatchBuilder setQueryCountPercentileLast30Days(@Nonnull int value) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH + QUERY_COUNT_PERCENTILE_LAST_30_DAYS_KEY,
            instance.numberNode(value)));
    return this;
  }

  public UsageFeaturesPatchBuilder setQueryCountRankLast30Days(@Nonnull int value) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH + QUERY_COUNT_RANK_LAST_30_DAYS_KEY,
            instance.numberNode(value)));
    return this;
  }

  public UsageFeaturesPatchBuilder setUniqueUserPercentileLast30Days(@Nonnull int value) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH + UNIQUE_USER_PERCENTILE_LAST_30_DAYS_KEY,
            instance.numberNode(value)));
    return this;
  }

  public UsageFeaturesPatchBuilder setUniqueUserRankLast30Days(@Nonnull int value) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH + UNIQUE_USER_RANK_LAST_30_DAYS_KEY,
            instance.numberNode(value)));
    return this;
  }

  public UsageFeaturesPatchBuilder setWriteCountPercentileLast30Days(@Nonnull int value) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH + WRITE_COUNT_PERCENTILE_LAST_30_DAYS_KEY,
            instance.numberNode(value)));
    return this;
  }

  public UsageFeaturesPatchBuilder setWriteCountRankLast30Days(@Nonnull Long value) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH + WRITE_COUNT_RANK_LAST_30_DAYS_KEY,
            instance.numberNode(value)));
    return this;
  }

  public UsageFeaturesPatchBuilder setTopUsersLast30Days(@Nonnull Long value) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH + TOP_USERS_LAST_30_DAYS_KEY,
            instance.numberNode(value)));
    return this;
  }

  public UsageFeaturesPatchBuilder setSizeInBytesPercentile(@Nonnull int value) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH + SIZE_IN_BYTES_PERCENTILE_KEY,
            instance.numberNode(value)));
    return this;
  }

  public UsageFeaturesPatchBuilder setSizeInBytesRank(@Nonnull int value) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH + SIZE_IN_BYTES_RANK_KEY,
            instance.numberNode(value)));
    return this;
  }

  public UsageFeaturesPatchBuilder setRowCountPercentile(@Nonnull int value) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH + ROW_COUNT_PERCENTILE_KEY,
            instance.numberNode(value)));
    return this;
  }

  public UsageFeaturesPatchBuilder setUsageSearchScoreMultiplier(@Nonnull Float value) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH + USAGE_SEARCH_SCORE_MULTIPLIER_KEY,
            instance.numberNode(value)));
    return this;
  }

  public UsageFeaturesPatchBuilder setUsageFreshnessScoreMultiplier(@Nonnull Float value) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH + USAGE_FRESHNESS_SCORE_MULTIPLIER_KEY,
            instance.numberNode(value)));
    return this;
  }

  public UsageFeaturesPatchBuilder setCustomDatahubScoreMultiplier(@Nonnull Float value) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH + CUSTOM_DATAHUB_SCORE_MULTIPLIER_KEY,
            instance.numberNode(value)));
    return this;
  }

  public UsageFeaturesPatchBuilder setCombinedSearchRankingMultiplier(@Nonnull Float value) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH + COMBINED_SEARCH_RANKING_MULTIPLIER_KEY,
            instance.numberNode(value)));
    return this;
  }

  @Override
  protected String getAspectName() {
    return USAGE_FEATURES_ASPECT_NAME;
  }

  @Override
  protected String getEntityType() {
    if (this.targetEntityUrn == null) {
      throw new IllegalStateException(
          "Target Entity Urn must be set to determine entity type before building Patch.");
    }
    return this.targetEntityUrn.getEntityType();
  }
}
