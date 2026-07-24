package com.linkedin.metadata.utils.elasticsearch;

import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Capped failure details plus the full {@code response.failures[]} array length from task JSON.
 * {@link #totalCount()} may exceed {@link #details()} size when parsing was capped.
 *
 * <p>When structured parse fails, {@link #rawFallback()} holds a truncated raw task JSON snippet so
 * on-call still has something to inspect.
 */
public record TaskFailureParseResult(
    @Nonnull List<TaskFailureDetail> details, int totalCount, @Nullable String rawFallback) {

  public static final TaskFailureParseResult EMPTY = new TaskFailureParseResult(List.of(), 0, null);

  public TaskFailureParseResult {
    details = details == null ? List.of() : details;
    if (totalCount < details.size()) {
      totalCount = details.size();
    }
  }

  /** Convenience when there is no raw fallback (structured parse succeeded or no failures). */
  public TaskFailureParseResult(@Nonnull List<TaskFailureDetail> details, int totalCount) {
    this(details, totalCount, null);
  }

  /** True when there are no structured details and no raw fallback to log. */
  public boolean isEmpty() {
    return details.isEmpty() && (rawFallback == null || rawFallback.isBlank());
  }
}
