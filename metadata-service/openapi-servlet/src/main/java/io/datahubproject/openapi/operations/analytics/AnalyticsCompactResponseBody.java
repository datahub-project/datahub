package io.datahubproject.openapi.operations.analytics;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.linkedin.metadata.analytics.compaction.AnalyticsCompactionResult;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AnalyticsCompactResponseBody {
  boolean lockNotAcquired;
  boolean moreWorkRemaining;
  int hoursSealed;
  int daysCompacted;
  int monthsCompacted;
  long durationMillis;
  @Nullable String implementation;
  @Nullable String message;

  @Nonnull
  public static AnalyticsCompactResponseBody from(@Nonnull AnalyticsCompactionResult result) {
    return AnalyticsCompactResponseBody.builder()
        .lockNotAcquired(result.isLockNotAcquired())
        .moreWorkRemaining(result.isMoreWorkRemaining())
        .hoursSealed(result.getHoursSealed())
        .daysCompacted(result.getDaysCompacted())
        .monthsCompacted(result.getMonthsCompacted())
        .durationMillis(result.getDurationMillis())
        .implementation(result.getImplementation())
        .message(result.getMessage())
        .build();
  }
}
