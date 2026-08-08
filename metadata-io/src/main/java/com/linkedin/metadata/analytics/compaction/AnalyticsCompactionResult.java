package com.linkedin.metadata.analytics.compaction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

/** Outcome of one analytics compaction invocation. */
@Value
@Builder
public class AnalyticsCompactionResult {
  boolean lockNotAcquired;
  boolean moreWorkRemaining;
  int hoursSealed;
  int daysCompacted;
  int monthsCompacted;
  long durationMillis;
  @Nullable String implementation;
  @Nullable String message;

  @Nonnull
  public static AnalyticsCompactionResult lockNotAcquired(@Nullable String implementation) {
    return AnalyticsCompactionResult.builder()
        .lockNotAcquired(true)
        .moreWorkRemaining(true)
        .implementation(implementation)
        .message("Compaction lock not acquired; another run is in progress")
        .build();
  }
}
